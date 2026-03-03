# ==========================================================
# #주식연계채권_코드V10.1_Ultimate_Targeting (자금용도 라벨 전용판)
# 1. [수정] 자금용도: 금액(원)을 제외하고 해당되는 라벨만 추출 (예: "운영자금, 채무상환자금")
# 2. Put/Call Option: 정규식을 이용해 해당 제목부터 다음 목차 전까지 완벽하게 블록 추출
# 3. 발행상품: '사채의 종류' 셀의 우측/하단 데이터를 찾고 불필요한 '회차' 텍스트 제거
# 4. 기존 V9.2의 0.0% 보존, 정정공시 덮어쓰기, 텍스트 한 줄 정렬 로직 완벽 유지
# ==========================================================
import os
import re
import json
import time
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Tuple, Dict

import feedparser
import pandas as pd
from bs4 import BeautifulSoup
import gspread
from gspread.utils import rowcol_to_a1
from playwright.sync_api import sync_playwright

# ==========================================================
# 1. 설정 (ENV)
# ==========================================================
BASE = "https://kind.krx.co.kr"
DEFAULT_RSS = "http://kind.krx.co.kr:80/disclosure/rsstodaydistribute.do?method=searchRssTodayDistribute&mktTpCd=0&currentPageSize=100"
RSS_URL = os.getenv("RSS_URL", DEFAULT_RSS)

TARGET_KWS = "전환사채권발행결정,교환사채권발행결정,신주인수권부사채권발행결정"
KEYWORDS = [x.strip() for x in os.getenv("KEYWORDS", TARGET_KWS).split(",") if x.strip()]

HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
LIMIT = int(os.getenv("LIMIT", "0"))
RUN_ONE_ACPTNO = os.getenv("RUN_ONE_ACPTNO", "").strip()

GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "").strip()
GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip() or os.environ.get("GOOGLE_CREDS", "").strip()

BOND_OUT_SHEET = os.getenv("BOND_OUT_SHEET", "주식연계채권")
SEEN_SHEET_NAME = os.getenv("SEEN_SHEET_NAME", "seen_bonds")

BOND_COLUMNS = [
    "구분", "회사명", "보고서명", "상장시장", "최초 이사회결의일", "권면총액(원)",
    "Coupon", "YTM", "만기", "전환청구 시작", "전환청구 종료", "Put Option",
    "Call Option", "Call 비율", "YTC", "모집방식", "발행상품", "행사(전환)가액(원)",
    "전환주식수", "주식총수대비 비율", "Refixing Floor", "납입일", "자금용도",
    "투자자", "링크", "접수번호"
]
SEEN_HEADERS = ["acptNo", "ts"]

@dataclass
class Target:
    acpt_no: str
    title: str
    link: str
    market: str = ""

# ==========================================================
# 2. 강력한 유틸리티
# ==========================================================
def _norm(s: str) -> str:
    return re.sub(r"\s+", "", str(s or "")).replace(":", "")

def _clean_label(s: str) -> str:
    return re.sub(r"^([①-⑩]|\(\d+\)|\d+\.)+", "", _norm(s))

def _single_line(s: str) -> str:
    if not s: return ""
    return re.sub(r'\s+', ' ', str(s)).strip()

def _format_date(s: str) -> str:
    m = re.search(r'(\d{4})[-년\.\s]+(\d{1,2})[-월\.\s]+(\d{1,2})', str(s))
    if m: return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    return _single_line(s)

def _to_int(s: str) -> Optional[int]:
    t = re.sub(r"[^\d\-]", "", str(s or "").replace(",", ""))
    if t in ("", "-"): return None
    try: return int(t)
    except: return None

def _to_float(s: str) -> Optional[float]:
    t = re.sub(r"[^\d\.\-]", "", str(s or "").replace(",", ""))
    if t in ("", "-", "."): return None
    try: return float(t)
    except: return None

def _max_int_in_text(s: str) -> Optional[int]:
    if not s: return None
    s_clean = re.sub(r'(^|\s)[\(①-⑩]?\s*\d+\s*[\.\)]\s+', ' ', str(s))
    nums = re.findall(r"\d[\d,]*", s_clean)
    vals = [int(x.replace(",", "")) for x in nums if x.replace(",", "").isdigit()]
    return max(vals) if vals else None

def norm_company_name(name: str) -> str:
    if not name: return ""
    return _norm(name.replace("주식회사", "").replace("(주)", ""))

def extract_acpt_no(text: str) -> Optional[str]:
    m = re.search(r"acptNo=(\d{14})", str(text or ""))
    return m.group(1) if m else None

def company_from_title(title: str) -> str:
    t2 = re.sub(r"\[(유|코|넥|코넥|KOSPI|KOSDAQ|KONEX)\]", "", title or "").strip()
    t2 = re.sub(r"\[.*?정정.*?\]", "", t2).strip()
    parts = t2.split()
    if not parts: return ""
    if parts[0] in ("주식회사", "(주)", "㈜") and len(parts) > 1: return parts[1]
    return parts[0]

def market_from_title(title: str) -> str:
    if not title: return ""
    if "[코]" in title or "코스닥" in title: return "코스닥"
    if "[유]" in title or "유가증권" in title: return "유가증권"
    if "[넥]" in title or "[코넥]" in title or "코넥스" in title: return "코넥스"
    return ""

def viewer_url(acpt_no: str, docno: str = "") -> str:
    return f"{BASE}/common/disclsviewer.do?method=searchInitInfo&acptNo={acpt_no}&docno={docno}"

def match_strict_keyword(title: str) -> bool:
    if not title: return False
    t_no_space = title.replace(" ", "")
    return any(kw in t_no_space for kw in ["전환사채권발행결정", "교환사채권발행결정", "신주인수권부사채권발행결정"])

def is_correction_title(title: str) -> bool:
    return "정정" in (title or "")

def _norm_date(s: str) -> str:
    return re.sub(r"[^\d]", "", str(s or ""))

def make_event_key(company: str, first_board_date: str, bond_type: str) -> str:
    return f"{_norm(company)}|{_norm_date(first_board_date)}|{_norm(bond_type)}"

# ==========================================================
# 3. 무결성 보장 HTML 파서
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
                    row_idx, col_idx = i + r_span, j + c_span
                    while len(grid) <= row_idx: grid.append([])
                    while len(grid[row_idx]) <= col_idx: grid[row_idx].append(None)
                    grid[row_idx][col_idx] = text
                    
    clean_grid = [[c if c is not None else "" for c in row] for row in grid if any(row)]
    return pd.DataFrame(clean_grid) if clean_grid else None

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

def parse_rss_targets() -> List[Target]:
    feed = feedparser.parse(RSS_URL)
    targets = []
    for it in getattr(feed, "entries", []):
        title = getattr(it, "title", "") or ""
        link = getattr(it, "link", "") or ""
        if not match_strict_keyword(title): continue
        acpt_no = extract_acpt_no(link) or extract_acpt_no(getattr(it, "guid", ""))
        if acpt_no: targets.append(Target(acpt_no=acpt_no, title=title, link=link))
    return list({t.acpt_no: t for t in targets}.values())

def pick_best_frame_html(page) -> str:
    best_html, best_score = "", -1
    for fr in page.frames:
        try:
            html = fr.content()
            lower = html.lower()
            tcnt = lower.count("<table")
            if tcnt == 0: continue
            bonus = sum(1 for w in ["권면총액", "표면이자율", "만기", "행사가액", "조기상환", "매도청구", "정정사항"] if w in lower)
            sc = tcnt * 100 + bonus * 30 + min(len(lower) // 2000, 50)
            if sc > best_score:
                best_score = sc
                best_html = html
        except: continue
    return best_html

def scrape_one(context, acpt_no: str) -> Tuple[List[pd.DataFrame], str, str]:
    url = viewer_url(acpt_no)
    page = context.new_page()
    try:
        page.goto(url, wait_until="networkidle", timeout=60000)
        page.wait_for_timeout(1500) 
        
        all_frames_html = page.content() + " " + " ".join([fr.content() for fr in page.frames])
        
        best_html = pick_best_frame_html(page) or ""
        if best_html.count("<table") == 0: raise RuntimeError("table 못 찾음")
        return extract_tables_from_html_robust(best_html), url, all_frames_html
    finally:
        try: page.close()
        except: pass

# ==========================================================
# 4. 정정사항 엔진 및 기본 스캔
# ==========================================================
def extract_correction_after_map(dfs: List[pd.DataFrame]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for df in dfs:
        arr = df.astype(str).values
        R, C = arr.shape
        header_r = after_col = item_col = None

        for r in range(R):
            row_norm = [_norm(x) for x in arr[r].tolist()]
            if any(w in x for w in ["정정전", "변경전"] for x in row_norm) and any(w in x for w in ["정정후", "변경후"] for x in row_norm):
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
                if v and v.lower() != "nan" and _norm(v) not in ("정정후", "정정전", "항목", "-"):
                    after_val = _single_line(v)
            if after_val: 
                out[_norm(item)] = after_val
                out[_clean_label(item)] = after_val
    return out

def scan_label_value_preferring_correction(dfs, label_candidates, corr_after) -> str:
    if corr_after:
        cand_clean = {_clean_label(x) for x in label_candidates}
        for c in cand_clean:
            if c in corr_after and str(corr_after[c]).strip(): return _single_line(str(corr_after[c]))
        for k, v in corr_after.items():
            if str(v).strip() and any(c in k for c in cand_clean): return _single_line(str(v))
            
    cand_clean = {_clean_label(x) for x in label_candidates}
    for df in dfs:
        arr = df.astype(str).values
        R, C = arr.shape
        for r in range(R):
            for c in range(C):
                if _clean_label(arr[r][c]) in cand_clean:
                    for cc in range(c + 1, min(C, c + 8)):
                        v = str(arr[r][cc]).strip()
                        if v and v.lower() != "nan" and _clean_label(v) not in cand_clean:
                            if not re.fullmatch(r"([①-⑩]|\(\d+\)|\d+\.)", _norm(v)): return _single_line(v)
                    for rr in range(r + 1, min(R, r + 4)):
                        v = str(arr[rr][c]).strip()
                        if v and v.lower() != "nan" and _clean_label(v) not in cand_clean:
                            if not re.fullmatch(r"([①-⑩]|\(\d+\)|\d+\.)", _norm(v)): return _single_line(v)
    return ""

def find_row_best_int(dfs, must_contain, min_val=-1) -> Optional[int]:
    keys = [_norm(x) for x in must_contain]
    best = None
    for df in dfs:
        arr = df.astype(str).values
        for r in range(arr.shape[0]):
            row = [str(x).strip() for x in arr[r].tolist()]
            if all(k in _norm("".join(row)) for k in keys):
                for cell in row:
                    if any(d in cell for d in ["년", "월", "일", "예정일"]): continue
                    amt = _max_int_in_text(cell)
                    if amt is not None and amt > min_val: 
                        best = max(best or 0, amt)
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

# ==========================================================
# 5. 핵심 컬럼 전용 추출기
# ==========================================================

def extract_product_type(dfs: List[pd.DataFrame], corr_after: Dict) -> str:
    """[발행상품 완벽 추출] '사채의 종류'를 찾아 우측/하단 내용 스캔 및 쓰레기값(회차 등) 정제"""
    val = scan_label_value_preferring_correction(dfs, ["1. 사채의 종류", "사채의 종류", "사채종류", "종류", "사태의 종류"], corr_after)
    
    def clean_product(text):
        text = re.sub(r'(회차|제)\s*\d+\s*(회|차)?', ' ', text)
        text = re.sub(r'(^종류|종류$)', ' ', text).strip()
        m = re.search(r'([가-힣a-zA-Z\s]*(?:무기명|기명|보증|무보증|사모|공모|분리|이권)[가-힣a-zA-Z\s]*(?:전환사채|교환사채|신주인수권부사채|사채))', text)
        if m: return _single_line(m.group(1))
        return _single_line(text)

    if val and "사채" in val: return clean_product(val)
    
    for df in dfs:
        arr = df.astype(str).values
        for r in range(min(5, arr.shape[0])): 
            row_str = " ".join(arr[r])
            if "사채" in row_str and any(kw in row_str for kw in ["무보증", "무기명", "사모", "공모", "이권부"]):
                return clean_product(row_str)
    return ""

def extract_fund_usage(dfs: List[pd.DataFrame], corr_after: Dict) -> str:
    """[자금용도 라벨 전용 추출] 금액(원) 없이 '운영자금, 채무상환자금' 형태의 라벨만 추출"""
    target_keys = ["시설자금", "영업양수자금", "운영자금", "채무상환자금", "타법인 증권 취득자금", "타법인증권취득자금", "기타자금"]
    found_funds = {}
    
    # 1. 표 내부 탐색
    for df in dfs:
        arr = df.astype(str).values
        R, C = arr.shape
        for r in range(R):
            for c in range(C):
                cell_norm = _norm(str(arr[r][c]))
                for tk in target_keys:
                    if _norm(tk) in cell_norm:
                        amt = 0
                        for cc in range(c + 1, min(C, c + 3)):
                            a = _max_int_in_text(arr[r][cc])
                            if a and a > 100: amt = max(amt, a)
                        if amt == 0 and r + 1 < R:
                            a = _max_int_in_text(arr[r+1][c])
                            if a and a > 100: amt = max(amt, a)
                        
                        if amt > 0:
                            std_key = "타법인 증권 취득자금" if "타법인" in tk else tk
                            found_funds[std_key] = max(found_funds.get(std_key, 0), amt)

    # 2. 정정공시 반영
    if corr_after:
        for k, v in corr_after.items():
            for tk in target_keys:
                if _norm(tk) in _norm(k):
                    amt = _max_int_in_text(v)
                    if amt and amt > 100:
                        std_key = "타법인 증권 취득자금" if "타법인" in tk else tk
                        found_funds[std_key] = amt

    # 3. 라벨만 조립 (요청사항 반영)
    if found_funds:
        result = [k for k, v in sorted(found_funds.items(), key=lambda x: x[1], reverse=True)]
        return _single_line(", ".join(result))
    
    val = scan_label_value_preferring_correction(dfs, ["조달자금의 구체적 사용 목적", "자금용도"], corr_after)
    return _single_line(val)

def extract_option_details(html_raw: str, option_type: str) -> str:
    """[Put/Call Option 완벽 절단기] 본문에서 '제목'부터 '다음 목차 전'까지 블록 통째로 추출"""
    soup = BeautifulSoup(html_raw, 'lxml')
    for br in soup.find_all("br"): br.replace_with("\n")
    text = soup.get_text(separator='\n', strip=True) 
    
    if option_type == 'put':
        pattern = r'(?:\[?\s*조기상환청구권\s*\([^)]*Put[^)]*\)[^\]\n]*\]?)(.*?)(?=\n\s*(?:\[?\s*매도청구권|1[0-9]\.\s*|2[0-9]\.\s*|【\s*특정인에|기타\s*투자판단))'
    else:
        pattern = r'(?:\[?\s*매도청구권\s*\([^)]*Call[^)]*\)[^\]\n]*\]?)(.*?)(?=\n\s*(?:1[0-9]\.\s*|2[0-9]\.\s*|【\s*특정인에|기타\s*투자판단))'
        
    match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
    if match:
        result = match.group(1).strip()
        if len(result) > 10: return _single_line(result)
        
    kws = ["조기상환청구권(Put", "조기상환청구권"] if option_type == 'put' else ["매도청구권(Call", "매도청구권"]
    idx = -1
    for kw in kws:
        idx = text.find(kw)
        if idx != -1: break
    if idx != -1:
        snippet = text[idx:idx+4000]
        m = re.search(r'\n\s*(?:【특정인|2[0-9]\.\s*기타|1[0-9]\.\s*신주)', snippet[50:])
        if m: snippet = snippet[:50+m.start()]
        return _single_line(snippet)
        
    return ""

def extract_call_ratio_and_ytc(call_text: str) -> Tuple[str, str]:
    if not call_text: return "", ""
    ratio, ytc = "", ""
    r_match = re.findall(r'(\d{1,3}(?:\.\d+)?)\s*(?:%|/\s*100|퍼센트)', call_text)
    if r_match:
        vals = [float(v) for v in r_match if 0 < float(v) <= 100]
        if vals: ratio = f"{max(vals):g}%"
        
    y_match = re.findall(r'(?:수익률|이율|연|복리|적용)[^\d]{0,15}?(\d{1,2}(?:\.\d+)?)\s*(?:%|퍼센트)', call_text)
    if y_match:
        vals = [float(v) for v in y_match if 0 <= float(v) <= 20]
        if vals: ytc = f"{max(vals):g}%"
    else:
        y_match2 = re.findall(r'(\d+(?:\.\d+)?)\s*(?:%|퍼센트)', call_text)
        vals = [float(v) for v in y_match2 if 0 <= float(v) <= 20 and f"{float(v):g}%" != ratio]
        if vals: ytc = f"{vals[0]:g}%"
    return ratio, ytc

def extract_period_dates(dfs, corr_after, period_kws) -> Tuple[str, str]:
    s_date, e_date = "", ""
    for df in dfs:
        arr = df.astype(str).values
        for r in range(arr.shape[0]):
            row_str = _norm("".join(arr[r]))
            if any(p in row_str for p in period_kws) or "시작일" in row_str or "종료일" in row_str:
                dates = re.findall(r'\d{4}[-년\.\s]+\d{1,2}[-월\.\s]+\d{1,2}', " ".join(arr[r]))
                if "시작" in row_str and not s_date and dates: s_date = _format_date(dates[0])
                elif "종료" in row_str and not e_date and dates: e_date = _format_date(dates[-1])
                elif len(dates) >= 2:
                    return _format_date(dates[0]), _format_date(dates[-1])
    return s_date, e_date

def extract_investors(dfs: List[pd.DataFrame], corr_after: Dict[str, str]) -> str:
    investors = []
    blacklist = ["관계", "지분", "%", "배정", "비고", "합계", "소계", "명", "출자자", "해당사항"]
    def is_valid(sn):
        sn = sn.strip()
        if not sn or len(sn) > 40 or re.fullmatch(r'[\d,\.\s]+', sn): return False
        for bw in blacklist:
            if bw in _norm(sn): return False
        return True

    val = scan_label_value_preferring_correction(dfs, ["발행대상자", "배정대상자", "투자자", "성명(법인명)", "인수인"], corr_after)
    if val:
        for chunk in re.split(r'[\n,]', val):
            if is_valid(chunk) and chunk.strip() not in investors: investors.append(chunk.strip())
            
    if not investors:
        for df in dfs:
            if any(kw in _norm(df.to_string()) for kw in ["발행대상자명", "대상자명"]):
                arr = df.astype(str).values
                for r in range(1, arr.shape[0]):
                    name = arr[r][0].split('\n')[0].strip()
                    if is_valid(name) and name not in investors: investors.append(name)
                    
    return _single_line(", ".join(investors[:12]))

# ==========================================================
# 6. 레코드 파싱 매핑
# ==========================================================
def parse_bond_record(dfs, t: Target, corr_after, html_raw, company_market_map) -> dict:
    rec = {k: "" for k in BOND_COLUMNS}
    rec["접수번호"] = t.acpt_no
    rec["링크"] = t.link if t.link else viewer_url(t.acpt_no)

    title_clean = t.title.replace("[자동복구대상]", "").strip()
    rec["보고서명"] = title_clean
    
    t_ns = title_clean.replace(" ", "")
    if "교환" in t_ns: rec["구분"] = "EB"
    elif "신주인수권" in t_ns: rec["구분"] = "BW"
    elif "전환" in t_ns: rec["구분"] = "CB"
    
    comp_cands = ["회사명", "회사 명", "발행회사"]
    table_comp = scan_label_value_preferring_correction(dfs, comp_cands, corr_after)
    rec["회사명"] = table_comp or company_from_title(title_clean) or title_clean
    if rec["회사명"] in ["유", "코", "넥"]: rec["회사명"] = title_clean

    mkt = scan_label_value_preferring_correction(dfs, ["상장시장", "시장구분"], corr_after)
    rec["상장시장"] = mkt or market_from_title(title_clean) or t.market or company_market_map.get(norm_company_name(rec["회사명"]), "")
    if rec["상장시장"]: company_market_map[norm_company_name(rec["회사명"])] = rec["상장시장"]

    rec["최초 이사회결의일"] = _format_date(scan_label_value_preferring_correction(dfs, ["이사회결의일(결정일)", "이사회결의일", "최초이사회결의일"], corr_after))
    rec["납입일"] = _format_date(scan_label_value_preferring_correction(dfs, ["납입일", "납입기일", "청약일"], corr_after))
    rec["만기"] = _format_date(scan_label_value_preferring_correction(dfs, ["사채만기일", "만기일", "상환기일"], corr_after))
    
    rec["모집방식"] = scan_label_value_preferring_correction(dfs, ["사채발행방법", "모집방법", "발행방법"], corr_after)
    
    # [호출] 1. 발행상품
    rec["발행상품"] = extract_product_type(dfs, corr_after)

    def get_corr_num(labels, fallback_keys=[], min_val=-1, as_float=False):
        val = scan_label_value_preferring_correction(dfs, labels, corr_after)
        if as_float:
            num = _to_float(val)
            if num is None and fallback_keys: num = find_row_best_float(dfs, fallback_keys)
            return str(num) if num is not None else ""
        else:
            num = _to_int(val)
            if (num is None or num <= min_val) and fallback_keys:
                num = find_row_best_int(dfs, fallback_keys, min_val)
            if num is not None:
                if num == 0: return "0"
                if num > 50: return f"{num:,}" 
        return ""

    rec["권면총액(원)"] = get_corr_num(["사채의권면(전자등록)총액(원)", "권면(전자등록)총액(원)", "사채의 권면총액", "사채의 총액"], ["권면총액", "원"], 50)
    rec["Coupon"] = get_corr_num(["표면이자율(%)", "표면이자율", "표면금리"], ["표면이자율"], -1, True)
    rec["YTM"] = get_corr_num(["만기이자율(%)", "만기이자율", "만기보장수익률"], ["만기이자율"], -1, True)
    rec["행사(전환)가액(원)"] = get_corr_num(["전환가액(원/주)", "교환가액(원/주)", "행사가액(원/주)", "전환가액", "교환가액", "행사가액"], ["가액", "원"], 50)
    rec["전환주식수"] = get_corr_num(["전환에 따라 발행할 주식수", "교환대상 주식수", "주식수"], ["주식수"], 50)
    rec["주식총수대비 비율"] = scan_label_value_preferring_correction(dfs, ["주식총수 대비 비율(%)", "총수 대비 비율"], corr_after)
    rec["Refixing Floor"] = get_corr_num(["최저 조정가액 (원)", "최저조정가액", "리픽싱하한"], ["최저조정가액", "원"], 50)

    s_date, e_date = extract_period_dates(dfs, corr_after, ["전환청구기간", "교환청구기간", "권리행사기간"])
    rec["전환청구 시작"], rec["전환청구 종료"] = s_date, e_date

    # [호출] 2. Put / Call Option
    rec["Put Option"] = extract_option_details(html_raw, 'put')
    rec["Call Option"] = extract_option_details(html_raw, 'call')
    
    ratio, ytc = extract_call_ratio_and_ytc(rec["Call Option"])
    rec["Call 비율"] = ratio
    rec["YTC"] = ytc

    rec["투자자"] = extract_investors(dfs, corr_after)
    
    # [호출] 3. 자금용도 (라벨만)
    rec["자금용도"] = extract_fund_usage(dfs, corr_after)

    return rec

# ==========================================================
# 7. 구글 시트 연동
# ==========================================================
def gs_open():
    if not GOOGLE_SHEET_ID or not GOOGLE_CREDENTIALS_JSON: raise RuntimeError("구글 시트 연동 정보 누락")
    gc = gspread.service_account_from_dict(json.loads(GOOGLE_CREDENTIALS_JSON))
    sh = gc.open_by_key(GOOGLE_SHEET_ID)
    try: seen_ws = sh.worksheet(SEEN_SHEET_NAME)
    except:
        seen_ws = sh.add_worksheet(title=SEEN_SHEET_NAME, rows=2000, cols=2)
        seen_ws.update("A1:B1", [SEEN_HEADERS])
    try: bond_ws = sh.worksheet(BOND_OUT_SHEET)
    except: bond_ws = sh.add_worksheet(title=BOND_OUT_SHEET, rows=2000, cols=len(BOND_COLUMNS) + 5)
    return sh, bond_ws, seen_ws

def build_indices(values: List[List[str]], headers: List[str]):
    col_acpt = headers.index("접수번호")
    col_comp = headers.index("회사명")
    col_first = headers.index("최초 이사회결의일")
    col_type = headers.index("구분")
    
    r_idx, e_idx = {}, {}
    for r, row in enumerate(values[1:], start=2):
        acpt = row[col_acpt].strip() if col_acpt < len(row) else ""
        if acpt.isdigit(): r_idx[acpt] = r
        
        comp = row[col_comp].strip() if col_comp < len(row) else ""
        first = row[col_first].strip() if col_first < len(row) else ""
        btype = row[col_type].strip() if col_type < len(row) else ""
        
        k = make_event_key(comp, first, btype)
        if k.count("|") == 2: e_idx[k] = (r, acpt)
    return r_idx, e_idx

# ==========================================================
# 8. 메인 실행
# ==========================================================
def run():
    sh, bond_ws, seen_ws = gs_open()

    if not bond_ws.get_all_values() or bond_ws.row_values(1) != BOND_COLUMNS: 
        bond_ws.update(f"A1:{rowcol_to_a1(1, len(BOND_COLUMNS))}", [BOND_COLUMNS])

    values = bond_ws.get_all_values()
    last_row_ref = [len(values)]
    bond_index, event_index = build_indices(values, BOND_COLUMNS)

    seen_values = seen_ws.get_all_values()
    last_seen_row_ref = [len(seen_values)]
    seen_index = {row[0].strip(): r for r, row in enumerate(seen_values[1:], start=2) if row and row[0].strip().isdigit()}

    company_market_map = {}
    for row in values[1:]:
        c_name = row[BOND_COLUMNS.index("회사명")].strip() if len(row) > BOND_COLUMNS.index("회사명") else ""
        c_mkt = row[BOND_COLUMNS.index("상장시장")].strip() if len(row) > BOND_COLUMNS.index("상장시장") else ""
        if c_name and c_mkt in ["코스닥", "유가증권", "코넥스"]:
            company_market_map[norm_company_name(c_name)] = c_mkt

    targets_dict = {t.acpt_no: t for t in parse_rss_targets()}

    # [복구 시스템] 빈칸/오류 감지 시 강제 재파싱
    for row in values[1:]:
        acpt = row[BOND_COLUMNS.index("접수번호")] if len(row) > BOND_COLUMNS.index("접수번호") else ""
        if not acpt.isdigit(): continue
        
        amt = row[BOND_COLUMNS.index("권면총액(원)")] if len(row) > BOND_COLUMNS.index("권면총액(원)") else ""
        price = row[BOND_COLUMNS.index("행사(전환)가액(원)")] if len(row) > BOND_COLUMNS.index("행사(전환)가액(원)") else ""
        prod = row[BOND_COLUMNS.index("발행상품")] if len(row) > BOND_COLUMNS.index("발행상품") else ""
        put_opt = row[BOND_COLUMNS.index("Put Option")] if len(row) > BOND_COLUMNS.index("Put Option") else ""
        
        needs_fix = (not amt or not price or not prod or not put_opt)
        
        if needs_fix and acpt not in targets_dict:
            title = row[BOND_COLUMNS.index("보고서명")] if len(row) > BOND_COLUMNS.index("보고서명") else "[자동복구대상]"
            targets_dict[acpt] = Target(acpt_no=acpt, title=title, link=viewer_url(acpt), market=row[BOND_COLUMNS.index("상장시장")])
            print(f"[INFO] 누락 데이터 복구 재실행: {title} ({acpt})")

    if RUN_ONE_ACPTNO:
        targets = [Target(acpt_no=RUN_ONE_ACPTNO, title=f"[MANUAL]{RUN_ONE_ACPTNO}", link="")]
    else:
        targets = list(targets_dict.values())[:LIMIT] if LIMIT > 0 else list(targets_dict.values())

    if not targets:
        print("[INFO] 처리할 대상이 없습니다.")
        return

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS, args=["--no-sandbox"])
        context = browser.new_context(viewport={"width": 1400, "height": 900})
        
        ok = 0
        for t in targets:
            try:
                dfs, src, html_raw = scrape_one(context, t.acpt_no)
                corr_after = extract_correction_after_map(dfs) if is_correction_title(t.title) else None
                rec = parse_bond_record(dfs, t, corr_after, html_raw, company_market_map)

                evk = make_event_key(rec.get("회사명", ""), rec.get("최초 이사회결의일", ""), rec.get("구분", ""))
                mode = "APPEND"
                row = -1
                
                if evk in event_index:
                    row, old_acpt = event_index[evk]
                    mode = "UPDATE"
                elif rec["접수번호"] in bond_index:
                    row = bond_index[rec["접수번호"]]
                    mode = "UPDATE"

                row_vals = [rec.get(h, "") for h in BOND_COLUMNS]
                if mode == "UPDATE":
                    bond_ws.update(f"A{row}:{rowcol_to_a1(row, len(BOND_COLUMNS))}", [row_vals])
                    bond_index[rec["접수번호"]] = row
                    event_index[evk] = (row, rec["접수번호"])
                else:
                    bond_ws.append_row(row_vals, value_input_option="RAW")
                    last_row_ref[0] += 1
                    row = last_row_ref[0]
                    bond_index[rec["접수번호"]] = row
                    event_index[evk] = (row, rec["접수번호"])

                print(f"[OK] {t.acpt_no} mode={mode} row={row} | {rec['회사명']} {rec['구분']}")
                
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                if t.acpt_no in seen_index: seen_ws.update(f"B{seen_index[t.acpt_no]}", [[ts]])
                else:
                    seen_ws.append_row([t.acpt_no, ts], value_input_option="RAW")
                    last_seen_row_ref[0] += 1
                    seen_index[t.acpt_no] = last_seen_row_ref[0]
                ok += 1
            except Exception as e:
                print(f"[FAIL] {t.acpt_no} {t.title} :: {e}")
            
            time.sleep(0.4)

        context.close()
        browser.close()
        print(f"[DONE] ok={ok}")

if __name__ == "__main__":
    run()
