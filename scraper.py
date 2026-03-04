# ==========================================================
# #유상증자_코드V6.2_Ultimate (RSS 누락 함수 완벽 복구 & 무결성 종결판)
# - [복구] 실수로 누락되었던 `parse_rss_targets`, `pick_best_frame_html` 함수 완전 복구
# - [유지] '신규주식수'와 '증자전주식수' 섹션(구역) 가두기 스캔 적용
# - [유지] 가격/주식수 스캔 시 2026 등 연도 및 날짜를 완전히 증발시키는 정규식
# - [유지] 확정발행금액은 무조건 '자금조달의 목적' 표 합산액을 1순위로 적용
# ==========================================================
import os
import re
import json
import time
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple, Dict

import feedparser
import pandas as pd
from bs4 import BeautifulSoup
import gspread
from gspread.utils import rowcol_to_a1
from playwright.sync_api import sync_playwright

# ==========================================================
# 설정 (ENV)
# ==========================================================
BASE = "https://kind.krx.co.kr"
DEFAULT_RSS = (
    "http://kind.krx.co.kr:80/disclosure/rsstodaydistribute.do"
    "?method=searchRssTodayDistribute&mktTpCd=0&currentPageSize=100"
)

RSS_URL = os.getenv("RSS_URL", DEFAULT_RSS)
KEYWORDS = [x.strip() for x in os.getenv("KEYWORDS", "유상증자결정").split(",") if x.strip()]

HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
LIMIT = int(os.getenv("LIMIT", "0"))
RUN_ONE_ACPTNO = os.getenv("RUN_ONE_ACPTNO", "").strip()

GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "").strip()
GOOGLE_CREDENTIALS_JSON = (
    os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip() or
    os.environ.get("GOOGLE_CREDS", "").strip()
)

RIGHTS_OUT_SHEET = os.getenv("RIGHTS_OUT_SHEET", "유상증자")
SEEN_SHEET_NAME = os.getenv("SEEN_SHEET_NAME", "seen")

RIGHTS_COLUMNS = [
    "회사명", "보고서명", "상장시장", "최초 이사회결의일", "증자방식", "발행상품",
    "신규발행주식수", "확정발행가(원)", "기준주가", "확정발행금액(억원)",
    "할인(할증률)", "증자전 주식수", "증자비율", "납입일",
    "신주의 배당기산일", "신주의 상장 예정일", "이사회결의일",
    "자금용도", "투자자", "링크", "접수번호"
]

SEEN_HEADERS = ["acptNo", "ts"]

@dataclass
class Target:
    acpt_no: str
    title: str
    link: str
    market: str = ""

# ==========================================================
# 초정밀 데이터 추출 헬퍼 함수
# ==========================================================
def _norm(s: str) -> str:
    s = (s or "").strip()
    return re.sub(r"\s+", "", s).replace(":", "")

def _clean_label(s: str) -> str:
    s = _norm(s)
    return re.sub(r"^([①-⑩]|\(\d+\)|\d+\.)+", "", s)

def norm_company_name(name: str) -> str:
    if not name: return ""
    n = name.replace("주식회사", "").replace("(주)", "").strip()
    return _norm(n)

def _norm_date(s: str) -> str:
    return re.sub(r"[^\d]", "", (s or "").strip())

def extract_date_strictly(text: str) -> str:
    """날짜 포맷(YYYY-MM-DD 등)만 완벽하게 뽑아냅니다."""
    if not text: return ""
    text = str(text).strip()
    m = re.search(r'(20[2-3]\d)[\s년\.\-\/]+(\d{1,2})[\s월\.\-\/]+(\d{1,2})', text)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    return ""

def extract_number_strictly(text: str, is_price_or_shares=True) -> Optional[int]:
    """연도(2026)나 순번(1.)을 숫자로 오인하지 않고 오직 순수 금액/주식수만 추출합니다."""
    if not text: return None
    t = str(text)
    
    # 1. YYYY년 MM월 DD일 패턴 완전 삭제
    t = re.sub(r'20[1-3]\d\s*[\.\-\/년]\s*\d{1,2}\s*[\.\-\/월]\s*\d{1,2}\s*[일]?', '', t)
    # 2. 단독 연도 (2025년, 2026.) 삭제
    t = re.sub(r'20[1-3]\d\s*[년\.]', '', t)
    # 3. 항목 번호 (1. 2. ① 등) 삭제
    t = re.sub(r'(^|\s)[\(①-⑩]?\s*\d{1,2}\s*[\.\)]\s+', ' ', t)

    nums = re.findall(r'\d{1,3}(?:,\d{3})+|\d+', t)
    valid_nums = []
    
    for n in nums:
        val = int(n.replace(',', ''))
        if val <= 50: continue # 50 이하는 번호 찌꺼기로 간주
        
        # 가격/주식수일 경우, 콤마 없는 4자리 숫자(2024~2030)는 남은 연도 찌꺼기일 확률 99%이므로 파기
        if is_price_or_shares and 2020 <= val <= 2030 and ',' not in n:
            continue
            
        valid_nums.append(val)
        
    return max(valid_nums) if valid_nums else None

def _to_float(s: str) -> Optional[float]:
    if s is None: return None
    t = re.sub(r"[^\d\.\-]", "", str(s).replace(",", ""))
    if t in ("", "-", "."): return None
    try: return float(t)
    except Exception: return None

# ==========================================================
# 기본 유틸 & URL 처리
# ==========================================================
def extract_acpt_no(text: str) -> Optional[str]:
    m = re.search(r"acptNo=(\d{14})", text or "")
    return m.group(1) if m else None

def company_from_title(title: str) -> str:
    if not title: return ""
    t = re.sub(r"\[(유|코|넥|코넥|KOSPI|KOSDAQ|KONEX)\]", "", title).strip()
    t = re.sub(r"\[.*?정정.*?\]", "", t).strip()
    parts = t.split()
    if not parts: return ""
    if parts[0] in ("주식회사", "(주)", "㈜"):
        return f"{parts[0]} {parts[1]}" if len(parts) > 1 else parts[0]
    return parts[0]

def market_from_html(html: str) -> str:
    if not html: return ""
    h_low = html.lower()
    if "mark_kosdaq" in h_low or "alt=\"코스닥\"" in h_low or "코스닥시장" in h_low: return "코스닥"
    if "mark_kospi" in h_low or "alt=\"유가증권\"" in h_low or "유가증권시장" in h_low: return "유가증권"
    if "mark_konex" in h_low or "alt=\"코넥스\"" in h_low or "코넥스시장" in h_low: return "코넥스"
    if "코스닥" in html: return "코스닥"
    if "유가증권" in html: return "유가증권"
    if "코넥스" in html: return "코넥스"
    return ""

def viewer_url(acpt_no: str, docno: str = "") -> str:
    return f"{BASE}/common/disclsviewer.do?method=searchInitInfo&acptNo={acpt_no}&docno={docno}"

def match_keyword(title: str) -> bool:
    return bool(title) and any(k in title for k in KEYWORDS)

def is_correction_title(title: str) -> bool:
    return "정정" in (title or "")

def make_event_key(company: str, first_board_date: str, method: str) -> str:
    return f"{_norm(company)}|{_norm_date(first_board_date)}|{_norm(method)}"

# ==========================================================
# 커스텀 HTML 표 파서 (칸 밀림 방지)
# ==========================================================
def parse_html_table_to_df(tbl_soup) -> Optional[pd.DataFrame]:
    rows = tbl_soup.find_all('tr')
    grid = []
    for r in rows: grid.append([])
        
    for i, row in enumerate(rows):
        cells = row.find_all(['td', 'th'])
        j = 0
        for cell in cells:
            while j < len(grid[i]) and grid[i][j] is not None: j += 1
            text = cell.get_text(" ", strip=True)
            try: rowspan = int(cell.get('rowspan', 1))
            except: rowspan = 1
            try: colspan = int(cell.get('colspan', 1))
            except: colspan = 1
            
            for r_span in range(rowspan):
                for c_span in range(colspan):
                    row_idx = i + r_span
                    col_idx = j + c_span
                    while len(grid) <= row_idx: grid.append([])
                    while len(grid[row_idx]) <= col_idx: grid[row_idx].append(None)
                    grid[row_idx][col_idx] = text
    clean_grid = []
    for row in grid:
        clean_row = [c if c is not None else "" for c in row]
        if any(clean_row): clean_grid.append(clean_row)
    if clean_grid: return pd.DataFrame(clean_grid)
    return None

def extract_tables_from_html_robust(html: str) -> List[pd.DataFrame]:
    html = (html or "").replace("\x00", "")
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript"]): tag.decompose()
    results = []
    for tbl in soup.find_all("table"):
        df = parse_html_table_to_df(tbl)
        if df is not None and not df.empty: results.append(df)
    if not results: raise ValueError("표 파싱 실패")
    return results

# ==========================================================
# RSS / Playwright 추출 (누락되었던 핵심 복구 영역)
# ==========================================================
def parse_rss_targets() -> List[Target]:
    feed = feedparser.parse(RSS_URL)
    targets = []
    for it in (feed.entries or []):
        title = getattr(it, "title", "") or ""
        link = getattr(it, "link", "") or ""
        if not match_keyword(title): continue
        acpt_no = extract_acpt_no(link) or extract_acpt_no(getattr(it, "guid", ""))
        if acpt_no: targets.append(Target(acpt_no=acpt_no, title=title, link=link))
    return list({t.acpt_no: t for t in targets}.values())

def pick_best_frame_html(page) -> str:
    best_html, best_score = "", -1
    for fr in page.frames:
        try:
            html = fr.content()
            if not html: continue
            lower = html.lower()
            tcnt = lower.count("<table")
            if tcnt == 0: continue
            bonus = sum(1 for w in ["기준주가", "납입", "이사회", "할인", "할증", "발행", "청약", "증자방식", "자금조달", "정정사항"] if w in lower)
            sc = tcnt * 100 + bonus * 30 + min(len(lower) // 2000, 50)
            if sc > best_score:
                best_score = sc
                best_html = html
        except Exception: continue
    return best_html

def scrape_one(context, acpt_no: str) -> Tuple[List[pd.DataFrame], str, str]:
    url = viewer_url(acpt_no)
    page = context.new_page()
    header_html = ""
    try:
        try:
            header_url = f"{BASE}/common/disclsviewer.do?method=searchHeaderInfo&acptNo={acpt_no}"
            req = urllib.request.Request(header_url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=5) as response:
                header_html = response.read().decode('utf-8', errors='ignore')
        except Exception: pass
        page.goto(url, wait_until="networkidle", timeout=60000)
        page.wait_for_timeout(1500) 
        all_frames_html = header_html + " " + page.content() + " " + " ".join([fr.content() for fr in page.frames])
        best_html = pick_best_frame_html(page) or ""
        if best_html.lower().count("<table") == 0: raise RuntimeError("table 못 찾음")
        return extract_tables_from_html_robust(best_html), url, all_frames_html
    finally:
        try: page.close()
        except Exception: pass

# ==========================================================
# Google Sheets 연동
# ==========================================================
def gs_open():
    gc = gspread.service_account_from_dict(json.loads(GOOGLE_CREDENTIALS_JSON))
    sh = gc.open_by_key(GOOGLE_SHEET_ID)
    try: seen_ws = sh.worksheet(SEEN_SHEET_NAME)
    except:
        seen_ws = sh.add_worksheet(title=SEEN_SHEET_NAME, rows=2000, cols=2)
        seen_ws.update("A1:B1", [SEEN_HEADERS])
    try: rights_ws = sh.worksheet(RIGHTS_OUT_SHEET)
    except: rights_ws = sh.add_worksheet(title=RIGHTS_OUT_SHEET, rows=2000, cols=len(RIGHTS_COLUMNS) + 5)
    return sh, rights_ws, seen_ws

def ensure_headers(ws, headers):
    if ws.row_values(1) != headers: ws.update(f"A1:{rowcol_to_a1(1, len(headers))}", [headers])

def load_sheet_values(ws, headers):
    ensure_headers(ws, headers)
    vals = ws.get_all_values()
    if not vals:
        ws.update(f"A1:{rowcol_to_a1(1, len(headers))}", [headers])
        vals = ws.get_all_values()
    return vals

def build_indices(values: List[List[str]], headers: List[str]):
    col_acpt = headers.index("접수번호")
    col_comp = headers.index("회사명")
    col_first = headers.index("최초 이사회결의일")
    col_method = headers.index("증자방식")
    r_idx, e_idx = {}, {}
    for r, row in enumerate(values[1:], start=2):
        acpt = row[col_acpt].strip() if col_acpt < len(row) else ""
        if acpt.isdigit(): r_idx[acpt] = r
        comp = row[col_comp].strip() if col_comp < len(row) else ""
        first = row[col_first].strip() if col_first < len(row) else ""
        method = row[col_method].strip() if col_method < len(row) else ""
        k = make_event_key(comp, first, method)
        if k.strip("|") and k not in e_idx: e_idx[k] = (r, acpt)
    return r_idx, e_idx

def upsert(ws, headers, index, record, key_field, last_row_ref):
    key = str(record.get(key_field, "")).strip()
    row_vals = [record.get(h, "") for h in headers]
    if key in index:
        r = index[key]
        ws.update(f"A{r}:{rowcol_to_a1(r, len(headers))}", [row_vals])
        return "update", r
    ws.append_row(row_vals, value_input_option="RAW")
    last_row_ref[0] += 1
    index[key] = last_row_ref[0]
    return "append", last_row_ref[0]

def touch_seen(seen_ws, seen_idx, acpt_no, last_ref):
    key = str(acpt_no).strip()
    if not key.isdigit(): return
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if key in seen_idx: seen_ws.update(f"B{seen_idx[key]}", [[ts]])
    else:
        seen_ws.append_row([key, ts], value_input_option="RAW")
        last_ref[0] += 1
        seen_idx[key] = last_ref[0]

# ==========================================================
# [격리 스캔 엔진] 구역을 이탈하지 않는 무결성 파서
# ==========================================================
def get_section_number(dfs: List[pd.DataFrame], start_kws: List[str], end_pattern=r'^\s*[\(①-⑩]?\s*\d+\s*[\.\)]') -> Optional[int]:
    """해당 라벨이 나타난 곳부터 새로운 번호(구역)가 시작되기 전까지만 숫자를 스캔합니다."""
    start_kws = [_norm(x) for x in start_kws]
    
    for df in dfs:
        arr = df.astype(str).values
        R, C = arr.shape
        for r in range(R):
            row_str = _norm("".join(arr[r]))
            if any(kw in row_str for kw in start_kws):
                nums = []
                for rr in range(r, R):
                    curr_row_str = "".join(arr[rr])
                    
                    if rr > r:
                        first_cell = str(arr[rr][0]).strip()
                        if re.match(end_pattern, first_cell): break
                        if "증자전" in _norm(curr_row_str) or "자금조달" in _norm(curr_row_str) or "발행가액" in _norm(curr_row_str):
                            break
                    
                    for c in range(C):
                        cell = str(arr[rr][c])
                        amt = extract_number_strictly(cell, is_price_or_shares=True)
                        if amt: nums.append(amt)
                
                if nums:
                    return max(nums)
    return None

def get_corr_override(corr_after: Dict[str, str], kws: List[str], avoid_kws=["일", "기일", "예정"]) -> Optional[int]:
    """정정사항 표에서 날짜를 가격으로 오인하지 않도록 방어하며 숫자를 추출합니다."""
    if not corr_after: return None
    kws = [_norm(x) for x in kws]
    for k, v in corr_after.items():
        if any(x in _norm(k) for x in kws):
            if any(bad in _norm(k) for bad in avoid_kws): continue
            amt = extract_number_strictly(v, is_price_or_shares=True)
            if amt: return amt
    return None

# ==========================================================
# 파싱 보조 함수들
# ==========================================================
def extract_correction_after_map(dfs: List[pd.DataFrame]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for df in dfs:
        try: arr = df.astype(str).values
        except Exception: continue
        R, C = arr.shape
        header_r = after_col = item_col = None

        for r in range(R):
            row_norm = [_norm(x) for x in arr[r].tolist()]
            has_before = any(w in x for w in ["정정전", "변경전"] for x in row_norm)
            has_after = any(w in x for w in ["정정후", "변경후"] for x in row_norm)
            
            if has_before and has_after:
                header_r = r
                after_col = next((i for i, x in enumerate(row_norm) if "정정후" in x or "변경후" in x), None)
                item_col = next((i for i, x in enumerate(row_norm) if ("정정사항" in x or "항목" in x or "구분" in x)), 0)
                break

        if header_r is None or after_col is None: continue

        last_item = ""
        for rr in range(header_r + 1, R):
            item = str(arr[rr][item_col]).strip() if item_col is not None and item_col < C else ""
            item = item if item and item.lower() != "nan" else last_item
            if not item: continue
            last_item = item

            after_val = ""
            if 0 <= after_col < C:
                v = str(arr[rr][after_col]).strip()
                if v and v.lower() != "nan" and _norm(v) not in ("정정후", "정정전", "항목", "변경사유", "정정사유", "-"):
                    after_val = v
            if after_val: 
                out[_norm(item)] = after_val
                out[_clean_label(item)] = after_val
    return out

def get_valid_date(dfs: List[pd.DataFrame], labels: List[str], corr_after: Dict[str, str]) -> str:
    cand_clean = {_clean_label(x) for x in labels}
    if corr_after:
        for k, v in corr_after.items():
            if any(c in _clean_label(k) for c in cand_clean):
                d = extract_date_strictly(v)
                if d: return d

    for df in dfs:
        arr = df.astype(str).values
        R, C = arr.shape
        for r in range(R):
            row_vals = [str(x).strip() for x in arr[r].tolist()]
            if any(_clean_label(x) in cand_clean for x in row_vals):
                for c in range(C):
                    cell = str(arr[r][c]).strip()
                    if _clean_label(cell) in cand_clean: continue
                    # 쓰레기 텍스트 가차없이 버림
                    if any(b in cell for b in ["정정", "변경", "사유", "추가상장", "상장주식", "총수", "교부예정일", "사항", "항목"]): continue
                    d = extract_date_strictly(cell)
                    if d: return d
    return ""

def scan_label_value_preferring_correction(dfs, label_candidates, corr_after) -> str:
    cand_clean = {_clean_label(x) for x in label_candidates}
    if corr_after:
        for c in cand_clean:
            if c in corr_after and str(corr_after[c]).strip(): return str(corr_after[c]).strip()
        for k, v in corr_after.items():
            if str(v).strip() and any(c in _clean_label(k) for c in cand_clean): return str(v).strip()

    for df in dfs:
        arr = df.astype(str).values
        R, C = arr.shape
        for r in range(R):
            for c in range(C):
                if _clean_label(arr[r][c]) in cand_clean:
                    checks = []
                    if c + 1 < C: checks.append(arr[r][c+1])
                    if r + 1 < R: checks.append(arr[r+1][c])
                    if c + 2 < C: checks.append(arr[r][c+2])
                    
                    for v in checks:
                        if not v or str(v).lower() == 'nan': continue
                        if _clean_label(v) in cand_clean: continue
                        if re.fullmatch(r"([①-⑩]|\(\d+\)|\d+\.)", _norm(v)): continue
                        return str(v).strip()
    return ""

def extract_fund_use_and_amount(dfs, corr_after) -> Tuple[str, float]:
    """자금조달의 목적 표를 섹션 단위로 스캔하여 완벽한 합산 금액을 뽑아냅니다."""
    keys_map = {
        "시설자금": "시설자금", "영업양수자금": "영업양수자금", "운영자금": "운영자금",
        "채무상환자금": "채무상환자금", "타법인증권취득자금": "타법인 증권 취득자금",
        "타법인증권": "타법인 증권 취득자금", "기타자금": "기타자금"
    }
    found_amts = {}
    
    if corr_after:
        for itemk, v in corr_after.items():
            for k, std_name in keys_map.items():
                if _norm(k) in _norm(itemk):
                    amt = extract_number_strictly(v, is_price_or_shares=False)
                    if amt and amt >= 100000: # 자금은 무조건 10만 단위 이상
                        found_amts[std_name] = amt

    for df in dfs:
        arr = df.astype(str).values
        R, C = arr.shape
        for r in range(R):
            row_str = _norm("".join(arr[r]))
            if "자금조달의목적" in row_str or "자금용도" in row_str or "자금의사용" in row_str:
                # 자금조달 구역 진입: 아래로 10줄 이내만 한정 스캔
                for rr in range(r, min(R, r+12)):
                    curr_str = _norm("".join(arr[rr]))
                    for k, std_name in keys_map.items():
                        if _norm(k) in curr_str:
                            for c in range(C):
                                amt = extract_number_strictly(arr[rr][c], is_price_or_shares=False)
                                if amt and amt >= 100000:
                                    if std_name not in found_amts:
                                        found_amts[std_name] = amt
                break

    std_order = ["시설자금", "영업양수자금", "운영자금", "채무상환자금", "타법인 증권 취득자금", "기타자금"]
    uses = [name for name in std_order if found_amts.get(name, 0) > 0]
    total_sum = sum(found_amts.get(name, 0) for name in uses)
    return ", ".join(uses), total_sum

def extract_investors(dfs: List[pd.DataFrame], corr_after: Dict[str, str]) -> str:
    investors = []
    blacklist = [
        "관계", "지분", "%", "주식", "배정", "선정", "경위", "비고", "해당사항", 
        "정정전", "정정후", "정정", "변경", "합계", "소계", "총계", "발행", "납입", 
        "예정", "목적", "주1", "주2", "주)", "기타", "참고", 
        "출자자수", "본점", "소재지", "(명)", "명"
    ]

    def is_valid_name(s: str) -> bool:
        sn = s.strip()
        if not sn or sn in ("-", ".", ",", "(", ")", "0", "1"): return False
        if len(sn) > 40: return False
        if re.fullmatch(r'[\d,\.\s]+', sn): return False 
        
        sn_norm = _norm(sn)
        for bw in blacklist:
            if bw in sn_norm: return False
        return True

    # 수직 스캔
    for df in dfs:
        arr = df.astype(str).values
        R, C = arr.shape
        target_col = -1
        start_row = -1
        
        for r in range(R):
            row_str = "".join([_norm(str(x)) for x in arr[r]])
            if any(kw in row_str for kw in ["제3자배정대상자", "배정대상자", "성명(법인명)", "출자자"]):
                for c in range(C):
                    cell_norm = _norm(str(arr[r][c]))
                    if any(kw in cell_norm for kw in ["성명", "법인명", "대상자", "출자자", "투자자"]) and "관계" not in cell_norm and "주식" not in cell_norm:
                        target_col = c
                        start_row = r
                        break
            if target_col != -1: break
        
        if target_col != -1:
            for rr in range(start_row + 1, R):
                val = str(arr[rr][target_col]).strip()
                val_norm = _norm(val)
                
                if "합계" in val_norm or "소계" in val_norm or "기타투자" in val_norm or val_norm.startswith("주1)"):
                    break
                    
                chunks = [x.strip() for x in val.split('\n')]
                for chunk in chunks:
                    if is_valid_name(chunk) and chunk not in investors:
                        investors.append(chunk)
            
            if investors:
                return ", ".join(investors)

    # 수평 스캔 
    val = scan_label_value_preferring_correction(dfs, ["제3자배정대상자", "배정대상자", "투자자", "성명(법인명)"], corr_after)
    if val:
        chunks = re.split(r'[\n,]', val)
        valid_chunks = []
        for chunk in chunks:
            chunk = chunk.strip()
            if is_valid_name(chunk) and chunk not in valid_chunks:
                valid_chunks.append(chunk)
        if valid_chunks:
            return ", ".join(valid_chunks)

    return ""

def extract_stock_type(dfs: List[pd.DataFrame], corr_after: Dict[str, str]) -> str:
    stock_type = "보통주식"
    
    if corr_after:
        for k, v in corr_after.items():
            if any(c in _norm(k) for c in ["신주의종류와수", "발행예정주식수"]):
                if "상환전환우선주" in v: return "상환전환우선주"
                elif "전환우선주" in v: return "전환우선주"
                elif "우선주" in v: return "우선주식"
                elif "종류주" in v: return "종류주식"

    for df in dfs:
        arr = df.astype(str).values
        R, C = arr.shape
        for r in range(R):
            row_joined = _norm("".join(arr[r]))
            if any(kw in row_joined for kw in ["신주의종류", "발행예정주식", "신주배정"]):
                if "상환전환우선주" in row_joined: return "상환전환우선주"
                elif "전환우선주" in row_joined: return "전환우선주"
                elif "우선주" in row_joined: return "우선주식"
                elif "종류주" in row_joined: return "종류주식"
                elif "보통주" in row_joined: return "보통주식"
    return stock_type

# ==========================================================
# 레코드 파싱 로직 
# ==========================================================
def parse_rights_issue_record(dfs, t: Target, corr_after, html_raw, company_market_map) -> dict:
    rec = {k: "" for k in RIGHTS_COLUMNS}
    rec["접수번호"] = t.acpt_no
    rec["링크"] = t.link if t.link else viewer_url(t.acpt_no)

    title_clean = t.title.replace("[자동복구대상]", "").strip()
    rec["보고서명"] = title_clean
    
    comp_cands = ["회사명", "회사 명", "발행회사", "발행회사명", "법인명", "종속회사명"]
    table_comp = scan_label_value_preferring_correction(dfs, comp_cands, corr_after)
    # 상장 여부 같은 쓰레기값 완벽 차단
    if table_comp and (re.search(r'[A-Za-z]', table_comp) or len(table_comp) > 15 or "상장" in table_comp or "여부" in table_comp):
        table_comp = ""
    
    rec["회사명"] = table_comp or company_from_title(title_clean) or title_clean
    if not rec["회사명"] or rec["회사명"] in ["유", "코", "넥"]:
        rec["회사명"] = title_clean
    
    mkt = scan_label_value_preferring_correction(dfs, ["상장시장", "시장구분"], corr_after)
    if mkt and ("해당사항" in mkt or len(mkt) < 2 or mkt in ("-", ".")): mkt = ""
    
    rec["상장시장"] = (
        mkt 
        or market_from_title(title_clean) 
        or t.market 
        or company_market_map.get(norm_company_name(rec["회사명"]))
        or company_market_map.get(norm_company_name(title_clean))
        or market_from_html(html_raw)
    )

    if rec["상장시장"] and rec["회사명"]:
        company_market_map[norm_company_name(rec["회사명"])] = rec["상장시장"]
        company_market_map[norm_company_name(title_clean)] = rec["상장시장"]

    rec["이사회결의일"] = get_valid_date(dfs, ["이사회결의일(결정일)", "이사회결의일", "결정일"], corr_after)
    rec["최초 이사회결의일"] = get_valid_date(dfs, ["최초 이사회결의일", "최초이사회결의일"], corr_after) or rec["이사회결의일"]
    rec["납입일"] = get_valid_date(dfs, ["납입일", "납입기일", "청약기일 및 납입일", "신주의 납입기일"], corr_after)
    rec["신주의 배당기산일"] = get_valid_date(dfs, ["신주의 배당기산일", "배당기산일"], corr_after)
    rec["신주의 상장 예정일"] = get_valid_date(dfs, ["신주의 상장 예정일", "상장예정일", "신주 상장예정일"], corr_after)

    rec["증자방식"] = scan_label_value_preferring_correction(dfs, ["증자방식", "발행방법", "배정방식"], corr_after)
    rec["발행상품"] = extract_stock_type(dfs, corr_after)
    
    # [핵심] 주식수 & 가격 섹션 가두기 스캔 완벽 적용
    issue_shares = get_corr_override(corr_after, ["신주의종류와수", "발행예정주식수"])
    if not issue_shares: issue_shares = get_section_number(dfs, ["신주의종류와수", "발행예정주식수"])
    if issue_shares: rec["신규발행주식수"] = f"{issue_shares:,}"

    prev_shares = get_corr_override(corr_after, ["증자전발행주식총수", "기발행주식총수", "발행주식총수", "증자전주식수"])
    if not prev_shares: prev_shares = get_section_number(dfs, ["증자전발행주식총수", "기발행주식총수", "발행주식총수", "증자전 주식수"])
    if prev_shares: rec["증자전 주식수"] = f"{prev_shares:,}"

    price = get_corr_override(corr_after, ["신주발행가액", "예정발행가", "확정발행가", "발행가액"], avoid_kws=["일", "기일", "예정일"])
    if not price: price = get_section_number(dfs, ["신주발행가액", "예정발행가", "확정발행가", "1주당확정발행가액"])
    if price: rec["확정발행가(원)"] = f"{price:,}"

    # 기준주가는 기존 스캔 사용하되 날짜 차단
    base_price_val = scan_label_value_preferring_correction(dfs, ["기준주가", "기준발행가액"], corr_after)
    base_price = extract_number_strictly(base_price_val, is_price_or_shares=True)
    if base_price: rec["기준주가"] = f"{base_price:,}"

    disc_str = scan_label_value_preferring_correction(dfs, ["할인율", "할증률", "할인율 또는 할증률", "할인(할증)율"], corr_after)
    disc = _to_float(disc_str)
    if disc is not None: rec["할인(할증률)"] = f"{disc}"

    uses_text, total_fund_amt = extract_fund_use_and_amount(dfs, corr_after)
    rec["자금용도"] = uses_text
    rec["투자자"] = extract_investors(dfs, corr_after)

    # [핵심] 확정발행금액 및 가격 상호 교차검증 (수천조 단위 뻥튀기 원천 차단)
    sh = _to_int(rec.get("신규발행주식수")) or 0
    pr = _to_int(rec.get("확정발행가(원)")) or 0
    calc_amt = sh * pr
    
    final_fund_amt = 0
    if total_fund_amt > 0:
        # 합산액이 계산값보다 터무니없이 작으면 단위 오류(백만원 등)로 간주하고 정상 공식 사용
        if calc_amt > 0 and total_fund_amt < (calc_amt / 100):
            final_fund_amt = calc_amt
        else:
            final_fund_amt = total_fund_amt
            # 가격이 비어있거나 잘못되었으면 총액/주식수로 역산하여 가격 자동 복구
            if pr == 0 and sh > 0:
                derived_pr = int(total_fund_amt / sh)
                if derived_pr > 0:
                    rec["확정발행가(원)"] = f"{derived_pr:,}"
                    pr = derived_pr
    elif calc_amt > 0:
        final_fund_amt = calc_amt

    if final_fund_amt > 0: 
        rec["확정발행금액(억원)"] = f"{final_fund_amt / 100_000_000:,.2f}"

    if sh > 0 and prev_shares and prev_shares > 0: 
        rec["증자비율"] = f"{sh / prev_shares * 100:.2f}%"

    return rec

# ==========================================================
# 실행 메인
# ==========================================================
def run():
    sh, rights_ws, seen_ws = gs_open()
    values = load_sheet_values(rights_ws, RIGHTS_COLUMNS)
    last_row_ref = [len(values)]
    rights_index, event_index = build_indices(values, RIGHTS_COLUMNS)

    seen_values = load_sheet_values(seen_ws, SEEN_HEADERS)
    last_seen_row_ref = [len(seen_values)]
    seen_index = {}
    for r, row in enumerate(seen_values[1:], start=2):
        if row and row[0].strip().isdigit(): seen_index[row[0].strip()] = r

    company_market_map = {}
    for row in values[1:]:
        c_name = row[RIGHTS_COLUMNS.index("회사명")].strip() if len(row) > RIGHTS_COLUMNS.index("회사명") else ""
        c_mkt = row[RIGHTS_COLUMNS.index("상장시장")].strip() if len(row) > RIGHTS_COLUMNS.index("상장시장") else ""
        if c_name and c_mkt in ["코스닥", "유가증권", "코넥스"]:
            company_market_map[norm_company_name(c_name)] = c_mkt

    targets_dict = {t.acpt_no: t for t in parse_rss_targets()}

    def get_val(row_data, col_name):
        idx = RIGHTS_COLUMNS.index(col_name)
        return row_data[idx].strip() if len(row_data) > idx else ""

    for row in values[1:]:
        acpt = get_val(row, "접수번호")
        if not acpt.isdigit(): continue
            
        fund = get_val(row, "자금용도")
        price = get_val(row, "확정발행가(원)")
        fund_amt = get_val(row, "확정발행금액(억원)")
        market = get_val(row, "상장시장")
        pay_date = get_val(row, "납입일")
        first_date = get_val(row, "최초 이사회결의일")
        link_val = get_val(row, "링크")
        investor_val = get_val(row, "투자자")
        comp_name = get_val(row, "회사명")
        prev_shares = get_val(row, "증자전 주식수")
        product_val = get_val(row, "발행상품")
        
        div_date = get_val(row, "신주의 배당기산일")
        list_date = get_val(row, "신주의 상장 예정일")
        board_date = get_val(row, "이사회결의일")
        
        bad_date_kws = ["상장", "총수", "교부", "추가", "사항", "항목"]
        date_needs_fix = (
            any(k in div_date for k in bad_date_kws) or
            any(k in list_date for k in bad_date_kws) or
            any(k in board_date for k in bad_date_kws) or
            not re.match(r'^\d{4}-\d{2}-\d{2}$', pay_date) if pay_date else True
        )
        
        bad_inv_kws = ["관계", "최대주주", "지분", "%", "정정", "주1", "합계", "소계", "출자자", "소재지", "명"]
        investor_needs_fix = any(k in investor_val for k in bad_inv_kws) or bool(re.search(r'\d{4,}', investor_val))
        
        fund_amt_clean = fund_amt.replace(",", "").replace(".", "")
        
        # [트리거 강화] 금액이 지나치게 크거나(1조 이상) 가격에 2026 등이 있으면 무조건 재파싱
        needs_fix = (
            not link_val or 
            not fund or "(원)" in fund or
            not price or price in ["2,024", "2,025", "2,026", "2,027"] or 
            not fund_amt or len(fund_amt_clean) >= 8 or float(fund_amt.replace(",", "") or 0) > 50000 or 
            not market or
            not first_date or
            investor_needs_fix or
            date_needs_fix or  
            not comp_name or comp_name in ["유", "코", "넥", "상장 여부", "상장여부"] or
            prev_shares == "3" or
            not product_val 
        )
        
        if needs_fix and acpt not in targets_dict:
            title = get_val(row, "보고서명") or get_val(row, "회사명") or "[자동복구대상]"
            restored_link = link_val if link_val else viewer_url(acpt)
            targets_dict[acpt] = Target(acpt_no=acpt, title=title, link=restored_link, market=market)
            print(f"[INFO] 빈칸/오류 감지됨: {title} ({acpt}) -> 강제 재파싱 대기열 추가")

    if RUN_ONE_ACPTNO:
        targets = [Target(acpt_no=RUN_ONE_ACPTNO, title=f"[MANUAL]{RUN_ONE_ACPTNO}", link="")]
    else:
        targets = list(targets_dict.values())
        targets = targets[:LIMIT] if LIMIT > 0 else targets

    if not targets:
        print("[INFO] 처리할 대상이 없습니다.")
        return

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS, args=["--disable-blink-features=AutomationControlled", "--no-sandbox"])
        context = browser.new_context(viewport={"width": 1400, "height": 900})
        
        ok = 0
        for t in targets:
            try:
                dfs, src, html_raw = scrape_one(context, t.acpt_no)

                corr_after = extract_correction_after_map(dfs) if is_correction_title(t.title) else None
                rec = parse_rights_issue_record(dfs, t, corr_after, html_raw, company_market_map)

                evk = make_event_key(
                    rec.get("회사명", ""),
                    rec.get("최초 이사회결의일", "") or rec.get("이사회결의일", ""),
                    rec.get("증자방식", "")
                )
                
                mode = "APPEND"
                row = -1
                
                if evk in event_index:
                    row, old_acpt = event_index[evk]
                    mode = "UPDATE"
                elif rec["접수번호"] in rights_index:
                    row = rights_index[rec["접수번호"]]
                    mode = "UPDATE"

                if mode == "UPDATE":
                    ws_row_vals = [rec.get(h, "") for h in RIGHTS_COLUMNS]
                    rights_ws.update(f"A{row}:{rowcol_to_a1(row, len(RIGHTS_COLUMNS))}", [ws_row_vals])
                    rights_index[rec["접수번호"]] = row
                    event_index[evk] = (row, rec["접수번호"])
                else:
                    mode, row = upsert(rights_ws, RIGHTS_COLUMNS, rights_index, rec, "접수번호", last_row_ref)
                    event_index[evk] = (row, rec["접수번호"])

                print(f"[OK] {t.acpt_no} mode={mode} row={row}")
                touch_seen(seen_ws, seen_index, t.acpt_no, last_seen_row_ref)
                ok += 1
            except Exception as e:
                print(f"[FAIL] {t.acpt_no} {t.title} :: {e}")
            
            time.sleep(0.4)

        context.close()
        browser.close()
        print(f"[DONE] ok={ok}")

if __name__ == "__main__":
    run()
