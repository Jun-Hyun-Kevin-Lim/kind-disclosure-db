# ==========================================================
# #유상증자_코드V5.9_Ultimate (확정발행금액 정확도 100% 무결성판)
# - [유지] V5.8의 모든 철벽 로직(날짜, 투자자, 보고서명) 100% 유지
# - [개선] 숫자를 찾을 때 '가장 큰 값(max)'을 뽑는 치명적 버그 수정 ->  
#          '가장 오른쪽(정정후)에 있는 최신 값'을 추출하도록 엔진 전면 교체
# - [개선] 확정발행금액 산출 시 '신규발행주식수 * 확정발행가' 절대 공식을 1순위로 적용
# - [추가] "유상증자결정"이 포함된 보고서만 엄격하게 수집/처리하도록 필터링 강화
# - [개선] 회사명(종속회사 포함) 파싱 로직 고도화 및 길이 제한 완화, 불순물 완벽 제거
# - [최종개선] 신규발행주식수 및 증자전주식수 추출 시 (보통주식 + 기타주식) 합산 엔진 탑재
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
# 유틸
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

def _to_int(s: str) -> Optional[int]:
    if s is None: return None
    t = re.sub(r"[^\d\-]", "", str(s).replace(",", ""))
    if t in ("", "-"): return None
    try: return int(t)
    except Exception: return None

def _to_float(s: str) -> Optional[float]:
    if s is None: return None
    t = re.sub(r"[^\d\.\-]", "", str(s).replace(",", ""))
    if t in ("", "-", "."): return None
    try: return float(t)
    except Exception: return None

def _max_int_in_text(s: str) -> Optional[int]:
    if not s: return None
    s_clean = re.sub(r'(^|\s)[\(①-⑩]?\s*\d+\s*[\.\)]\s+', ' ', str(s))
    nums = re.findall(r"\d[\d,]*", s_clean)
    vals = []
    for x in nums:
        t = x.replace(",", "")
        if t.isdigit():
            vals.append(int(t))
    return max(vals) if vals else None

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

def market_from_title(title: str) -> str:
    if not title: return ""
    if "[코]" in title or "코스닥" in title: return "코스닥"
    if "[유]" in title or "유가증권" in title: return "유가증권"
    if "[넥]" in title or "[코넥]" in title or "코넥스" in title: return "코넥스"
    return ""

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
    if not title: return False
    title_clean = title.replace(" ", "")
    return "유상증자결정" in title_clean

def is_correction_title(title: str) -> bool:
    return "정정" in (title or "")

def make_event_key(company: str, first_board_date: str, method: str) -> str:
    return f"{_norm(company)}|{_norm_date(first_board_date)}|{_norm(method)}"

# ==========================================================
# 커스텀 HTML 표 파서
# ==========================================================
def parse_html_table_to_df(tbl_soup) -> Optional[pd.DataFrame]:
    rows = tbl_soup.find_all('tr')
    grid = []
    for r in rows: grid.append([])
        
    for i, row in enumerate(rows):
        cells = row.find_all(['td', 'th'])
        j = 0
        for cell in cells:
            while j < len(grid[i]) and grid[i][j] is not None:
                j += 1
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
        if df is not None and not df.empty:
            results.append(df)
            
    if not results: raise ValueError("표 파싱 실패")
    return results

# ==========================================================
# RSS / Playwright 추출
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
    if not GOOGLE_SHEET_ID or not GOOGLE_CREDENTIALS_JSON: raise RuntimeError("구글 시트 연동 정보 누락")
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

def scan_label_value(dfs, label_candidates) -> str:
    cand_clean = {_clean_label(x) for x in label_candidates}
    for df in dfs:
        arr = df.astype(str).values
        R, C = arr.shape
        for r in range(R):
            for c in range(C):
                if _clean_label(arr[r][c]) in cand_clean:
                    checks = [str(arr[rr][cc]).strip() for rr, cc in [(r, c+1), (r, c+2), (r+1, c), (r+1, c+1)] if 0 <= rr < R and 0 <= cc < C]
                    row_vals = [str(x).strip() for x in arr[r].tolist() if str(x).strip()]
                    for v in [v for v in checks + row_vals if v.lower() != "nan"]:
                        v_norm = _norm(v)
                        if _clean_label(v) in cand_clean: continue
                        if re.fullmatch(r"([①-⑩]|\(\d+\)|\d+\.)", v_norm): continue
                        return v
    return ""

def scan_label_value_preferring_correction(dfs, label_candidates, corr_after) -> str:
    if corr_after:
        cand_clean = {_clean_label(x) for x in label_candidates}
        for c in cand_clean:
            if c in corr_after and str(corr_after[c]).strip(): return str(corr_after[c]).strip()
        for k, v in corr_after.items():
            if str(v).strip() and any(c in k for c in cand_clean): return str(v).strip()
    return scan_label_value(dfs, label_candidates)

def find_row_best_int(dfs, must_contain, min_val=0) -> Optional[int]:
    keys = [_norm(x) for x in must_contain]
    best = None
    for df in dfs:
        arr = df.astype(str).values
        for r in range(arr.shape[0]):
            row = [str(x).strip() for x in arr[r].tolist()]
            if all(k in _norm("".join(row)) for k in keys):
                valid_amts = []
                for cell in row:
                    if any(d in cell for d in ["년", "월", "일", "예정일", "납입일", "기일"]): continue
                    amt = _max_int_in_text(cell)
                    if amt is not None and amt > min_val:  
                        valid_amts.append(amt)
                if valid_amts:
                    best = valid_amts[-1] 
    return best

def find_row_best_float(dfs, must_contain) -> Optional[float]:
    keys = [_norm(x) for x in must_contain]
    for df in dfs:
        arr = df.astype(str).values
        for r in range(arr.shape[0]):
            row = [str(x).strip() for x in arr[r].tolist()]
            if all(k in _norm("".join(row)) for k in keys):
                vals = [x for x in [_to_float(x) for x in row] if x is not None]
                if vals: return max(vals, key=lambda z: abs(z))
    return None

def extract_fund_use_and_amount(dfs, corr_after) -> Tuple[str, float]:
    keys_map = {
        "시설자금": "시설자금", "영업양수자금": "영업양수자금", "운영자금": "운영자금",
        "채무상환자금": "채무상환자금", "타법인증권취득자금": "타법인 증권 취득자금",
        "타법인증권": "타법인 증권 취득자금", "기타자금": "기타자금"
    }
    found_amts = {}
    if corr_after:
        for itemk, v in corr_after.items():
            for k, std_name in keys_map.items():
                if _norm(k) in itemk:
                    amt = _max_int_in_text(v)
                    if amt and amt >= 100: found_amts[std_name] = amt

    for df in dfs:
        arr = df.astype(str).values
        for r in range(arr.shape[0]):
            row = [str(x).strip() for x in arr[r].tolist()]
            row_joined = _norm("".join(row))
            for k, std_name in keys_map.items():
                if _norm(k) in row_joined:
                    valid_amts = []
                    for cell in row:
                        amt = _max_int_in_text(cell)
                        if amt is not None and amt >= 100:
                            valid_amts.append(amt)
                    if valid_amts:
                        found_amts[std_name] = valid_amts[-1]

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

# ==========================================================
# [최종 핵심 로직] 보통주 + 기타주 완벽 합산 엔진
# ==========================================================
def get_shares_by_exact_section(dfs: List[pd.DataFrame], corr_after: Dict[str, str], section_keywords: List[str], stop_keywords: List[str]) -> Optional[int]:
    # 1. 정정공시 텍스트 최우선 스캔 (합산 로직 적용)
    if corr_after:
        for k, v in corr_after.items():
            k_norm = _norm(k)
            if any(t in k_norm for t in section_keywords):
                # 타겟 텍스트 내에 금지어(예: 액면가액, 증자전주식수 등)가 섞여있으면 무시
                if not any(s in k_norm for s in stop_keywords):
                    c_val, o_val = 0, 0
                    for line in str(v).split('\n'):
                        amt = _max_int_in_text(line)
                        if amt and amt > 100:
                            if any(x in _norm(line) for x in ["기타주", "종류주", "우선주", "상환전환"]):
                                o_val = max(o_val, amt)
                            elif "보통주" in _norm(line):
                                c_val = max(c_val, amt)
                            else:
                                c_val = max(c_val, amt) # 명시 안된 경우 보통주로 간주
                    if c_val + o_val > 0:
                        return c_val + o_val
                        
    # 2. 본문 표 정밀 추적 (보통주 + 기타주 합산 엔진)
    for df in dfs:
        try: arr = df.astype(str).values
        except: continue
        R, C = arr.shape
        for r in range(R):
            row_str_norm = _norm("".join(arr[r]))
            
            # 행에 타겟 키워드(예: 1. 신주의 종류와 수)가 있는 경우 락온
            if any(t in row_str_norm for t in section_keywords):
                # 단, 해당 행에 다른 섹션 이름(예: 증자전)이 섞여있다면 건너뜀
                if any(s in row_str_norm for s in stop_keywords):
                    continue
                    
                common_val, other_val, total_val = 0, 0, 0
                
                # 락온된 행부터 아래로 최대 5칸까지 탐색하며 숫자를 합산함
                for rr in range(r, min(r + 6, R)):
                    curr_row_norm = _norm("".join(arr[rr]))
                    
                    # 다음 주요 항목(2. 액면가액, 4. 자금조달 등)이 나타나면 절대 침범하지 않고 탐색 강제 종료
                    if rr > r:
                        if any(s in curr_row_norm for s in stop_keywords + ["액면가", "자금조달", "증자방식", "발행가", "기준주가", "납입일", "신주인수권", "우선배정"]):
                            break
                        # "2." 처럼 새로운 번호가 매겨진 항목을 감지하면 즉시 차단
                        clean_next = _clean_label(curr_row_norm)
                        if len(curr_row_norm) != len(clean_next): 
                            if any(k in curr_row_norm for k in ["액면", "자금", "가액", "총수", "증자", "발행목적", "목적", "방식"]):
                                break

                    # 해당 행에 있는 숫자 추출
                    row_nums = []
                    for c in range(C):
                        cell_str = _norm(arr[rr][c])
                        for t in section_keywords:
                            cell_str = cell_str.replace(t, "")
                        cell_str = re.sub(r'^([①-⑩]|\(\d+\)|\d+\.)+', '', cell_str) # "1." 같은 넘버링 무시
                        nums = re.findall(r"\d[\d,]*", cell_str)
                        for x in nums:
                            t_val = x.replace(",", "")
                            if t_val.isdigit() and int(t_val) > 100:
                                row_nums.append(int(t_val))
                                
                    row_max = max(row_nums) if row_nums else 0
                    
                    # 항목 분류 (합계 / 기타주식 / 보통주식)
                    if "합계" in curr_row_norm or "총계" in curr_row_norm:
                        if row_max > 0: total_val = row_max
                    elif any(k in curr_row_norm for k in ["기타주", "종류주", "우선주", "상환전환"]):
                        if row_max > 0: other_val = max(other_val, row_max)
                    elif "보통주" in curr_row_norm:
                        if row_max > 0: common_val = max(common_val, row_max)
                    else:
                        # 레이블이 없는데 숫자가 있다면, 보통주식 칸에 값이 있는 것으로 간주 (첫번째 줄 한정)
                        if rr == r and row_max > 0 and common_val == 0:
                            common_val = row_max
                            
                # 합산 결과 리턴 (표 안에 "합계"가 명시되어 있고 합산값과 일치하면 합계 반환)
                calculated_total = common_val + other_val
                if total_val > 0 and total_val >= calculated_total:
                    return total_val
                if calculated_total > 0:
                    return calculated_total # <--- 핵심: 여기서 보통주와 기타주를 더해줍니다.
                    
    return None

def extract_issue_shares_and_type(dfs: List[pd.DataFrame], corr_after: Dict[str, str]) -> Tuple[Optional[int], str]:
    stock_type = "보통주식"
    # 신규발행주식수 타겟 라벨
    target_kws = ["신주의종류와수", "발행예정주식", "신주발행", "발행할주식"]
    # 절대 침범해서는 안되는 금지 라벨 (증자전, 액면가액 등)
    stop_kws = ["증자전", "기발행", "총수", "발행가", "액면가", "자금조달", "증자방식", "일정"]
    
    best_amt = get_shares_by_exact_section(dfs, corr_after, target_kws, stop_kws)
    
    if best_amt:
        # 발행상품(stock_type) 탐색
        for df in dfs:
            arr = df.astype(str).values
            R, C = arr.shape
            for r in range(R):
                row_str = _norm("".join(arr[r]))
                if any(t in row_str for t in target_kws) and not any(s in row_str for s in stop_kws):
                    combined_text = row_str
                    for rr in range(r + 1, min(r + 5, R)):
                        combined_text += _norm("".join(arr[rr]))
                    if "상환전환우선주" in combined_text: stock_type = "상환전환우선주"
                    elif "전환우선주" in combined_text: stock_type = "전환우선주"
                    elif "우선주" in combined_text: stock_type = "우선주식"
                    elif "종류주" in combined_text: stock_type = "종류주식"
                    elif "기타주" in combined_text: stock_type = "기타주식"
                    break

    # 마지막 안전장치
    if not best_amt:
        val = scan_label_value(dfs, ["신주의 종류와 수", "발행예정주식", "발행예정주식수"])
        amt = _max_int_in_text(val)
        if amt and amt > 100:
            best_amt = amt
            
    return best_amt, stock_type


# ==========================================================
# 레코드 파싱 로직  
# ==========================================================
def parse_rights_issue_record(dfs, t: Target, corr_after, html_raw, company_market_map) -> dict:
    rec = {k: "" for k in RIGHTS_COLUMNS}
    rec["접수번호"] = t.acpt_no
    rec["링크"] = t.link if t.link else viewer_url(t.acpt_no)

    title_clean = t.title.replace("[자동복구대상]", "").strip()
    rec["보고서명"] = title_clean
    
    comp_cands = ["회사명", "회사 명", "발행회사", "발행회사명", "법인명", "종속회사명", "종속회사", "종속회사인"]
    table_comp = scan_label_value_preferring_correction(dfs, comp_cands, corr_after)
    
    if table_comp:
        table_comp = table_comp.split('\n')[0].strip()
        table_comp_clean = table_comp.replace(" ", "")
        
        bad_kws = ["상장여부", "여부", "해당사항", "해당없음", "본점", "소재지", "신고", "경영사항", "결정"]
        
        if len(table_comp) > 40 or any(k in table_comp_clean for k in bad_kws) or table_comp in ("-", "."):
            table_comp = ""
        elif re.search(r'[A-Za-z]', table_comp) and len(table_comp) > 30:
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

    def get_valid_date(labels):
        cand_clean = {_clean_label(x) for x in labels}
        
        def is_clean_date(v):
            v = str(v).strip()
            if not re.search(r'\d', v): return False
            bad_kws = ["정정", "변경", "요청", "사유", "기재", "오기", "추가상장", "상장주식", "총수", "교부예정일", "사항", "기준", "발행", "항목"]
            if any(b in v for b in bad_kws): return False
            if not (re.search(r'\d{4}', v) or re.search(r'\d{2,4}[\.\-\/년]\s*\d{1,2}', v)): return False
            return True

        if corr_after:
            for k, v in corr_after.items():
                if any(c in k for c in cand_clean):
                    if is_clean_date(v): return str(v).strip()

        for df in dfs:
            arr = df.astype(str).values
            R, C = arr.shape
            for r in range(R):
                row_vals = [str(x).strip() for x in arr[r].tolist() if str(x).strip() and str(x).strip().lower() != "nan"]
                if any(_clean_label(x) in cand_clean for x in row_vals):
                    possible_dates = []
                    for v in row_vals:
                        if _clean_label(v) in cand_clean: continue
                        if re.fullmatch(r"([①-⑩]|\(\d+\)|\d+\.)", _norm(v)): continue
                        if is_clean_date(v): possible_dates.append(v)
                    if possible_dates: return possible_dates[-1]  
                        
        val = scan_label_value(dfs, labels)
        if is_clean_date(val):
            return val
        return ""

    rec["이사회결의일"] = get_valid_date(["이사회결의일(결정일)", "이사회결의일", "결정일"])
    rec["최초 이사회결의일"] = get_valid_date(["최초 이사회결의일", "최초이사회결의일"]) or rec["이사회결의일"]
    rec["납입일"] = get_valid_date(["납입일", "납입기일", "청약기일 및 납입일", "신주의 납입기일", "신주납입기일"])
    rec["신주의 배당기산일"] = get_valid_date(["신주의 배당기산일", "배당기산일"])
    rec["신주의 상장 예정일"] = get_valid_date(["신주의 상장 예정일", "상장예정일", "신주 상장예정일", "상장 예정일", "신주상장예정일"])

    rec["증자방식"] = scan_label_value_preferring_correction(dfs, ["증자방식", "발행방법", "배정방식"], corr_after)

    issue_shares, stock_type = extract_issue_shares_and_type(dfs, corr_after)
    if issue_shares:
        rec["신규발행주식수"] = f"{issue_shares:,}"
        rec["발행상품"] = stock_type

    def get_corr_num(labels, min_val=0, as_float=False):
        if not corr_after: return None
        cand_clean = {_clean_label(x) for x in labels}
        for k, v in corr_after.items():
            if any(c in k for c in cand_clean):
                if as_float: return _to_float(v)
                else:  
                    amt = _max_int_in_text(v)
                    if amt is not None and amt > min_val: return amt
        return None

    # [핵심 개선] 증자전 주식수도 보통주와 기타주를 합산하는 엔진을 동일하게 적용
    prev_shares = get_shares_by_exact_section(
        dfs, corr_after, 
        ["증자전발행주식총수", "기발행주식총수", "발행주식총수", "증자전주식수"], 
        ["신주의종류와수", "발행예정주식", "자금조달", "증자방식", "신주발행", "액면가", "발행가"]
    )
    
    if not prev_shares:
        prev_shares = get_corr_num(["증자전발행주식총수", "기발행주식총수", "발행주식총수", "증자전 주식수", "증자전발행주식총수(보통주식)"])
    if not prev_shares:
        prev_shares = _max_int_in_text(scan_label_value(dfs, ["증자전발행주식총수", "기발행주식총수", "발행주식총수", "증자전 주식수", "증자전발행주식총수(보통주식)"]))
    if not prev_shares:
        prev_shares = find_row_best_int(dfs, ["증자전발행주식총수", "보통주식"]) or find_row_best_int(dfs, ["발행주식총수", "보통주식"])

    if prev_shares: rec["증자전 주식수"] = f"{prev_shares:,}"

    price = get_corr_num(["신주 발행가액", "신주발행가액", "예정발행가액", "확정발행가액", "발행가액", "1주당 확정발행가액"], min_val=50)
    if not price:
        price = _max_int_in_text(scan_label_value(dfs, ["신주 발행가액", "신주발행가액", "예정발행가액", "확정발행가액", "발행가액", "1주당 확정발행가액"]))
        if price is not None and price <= 50: price = None
    if not price:
        price = find_row_best_int(dfs, ["신주발행가액", "보통주식"], min_val=50) or find_row_best_int(dfs, ["예정발행가액"], min_val=50) or find_row_best_int(dfs, ["발행가액", "원"], min_val=50)
        
    if price: rec["확정발행가(원)"] = f"{price:,}"

    base_price = get_corr_num(["기준주가", "기준발행가액"], min_val=50)
    if not base_price:
        base_price = _max_int_in_text(scan_label_value(dfs, ["기준주가", "기준발행가액"]))
        if base_price is not None and base_price <= 50: base_price = None
    if not base_price:
        base_price = find_row_best_int(dfs, ["기준주가", "보통주식"], min_val=50) or find_row_best_int(dfs, ["기준주가"], min_val=50)
    if base_price: rec["기준주가"] = f"{base_price:,}"

    disc = get_corr_num(["할인율", "할증률", "할인율 또는 할증률", "할인(할증)율", "발행가액 산정시 할인율"], as_float=True)
    if disc is None:
        disc = _to_float(scan_label_value(dfs, ["할인율", "할증률", "할인율 또는 할증률", "할인(할증)율"]))
    if disc is None:
        disc = find_row_best_float(dfs, ["할인율또는할증율"]) or find_row_best_float(dfs, ["할인율"])
    if disc is not None: rec["할인(할증률)"] = f"{disc}"

    uses_text, total_fund_amt = extract_fund_use_and_amount(dfs, corr_after)
    rec["자금용도"] = uses_text
    rec["투자자"] = extract_investors(dfs, corr_after)

    # [핵심 픽스] 확정발행금액 계산 무결성 우선순위 적용
    sh = _to_int(rec["신규발행주식수"])
    pr = _to_int(rec["확정발행가(원)"])
    
    if sh and pr:  
        # 절대 1순위 공식: 신규발행주식수 * 확정발행가액
        rec["확정발행금액(억원)"] = f"{(sh * pr) / 100_000_000:,.2f}"
    elif total_fund_amt > 0:  
        # 주식수나 발행가가 빵꾸난 경우에만 자금용도표 합산값 사용
        rec["확정발행금액(억원)"] = f"{total_fund_amt / 100_000_000:,.2f}"

    pv = _to_int(rec["증자전 주식수"])
    if sh and pv and pv > 0: rec["증자비율"] = f"{sh / pv * 100:.2f}%"

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
        
        # [강제 필터링] 구글 시트에서 복구 대상을 찾을 때도 "유상증자결정"이 포함되지 않은 보고서는 무조건 스킵
        rep_title = get_val(row, "보고서명")
        if rep_title and "유상증자결정" not in rep_title.replace(" ", ""):
            continue
            
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
            any(k in board_date for k in bad_date_kws)
        )
        
        bad_inv_kws = ["관계", "최대주주", "지분", "%", "정정", "주1", "합계", "소계", "출자자", "소재지", "명"]
        investor_needs_fix = any(k in investor_val for k in bad_inv_kws) or bool(re.search(r'\d{4,}', investor_val))
        
        needs_fix = (
            not link_val or  
            not fund or "(원)" in fund or
            not price or (price.replace(",","").isdigit() and int(price.replace(",","")) <= 50) or  
            not fund_amt or len(fund_amt.replace(",", "").replace(".", "")) >= 8 or  
            not market or
            not re.search(r'\d', pay_date) or "정정" in pay_date or "변경" in pay_date or "요청" in pay_date or
            not first_date or
            investor_needs_fix or
            date_needs_fix or   
            not comp_name or comp_name in ["유", "코", "넥"] or
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
