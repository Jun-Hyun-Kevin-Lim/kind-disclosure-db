# ==========================================================
# #주식연계채권_코드V13.1_PureText_Option_Master (투자자 추출 강화판)
# 1. [강화] 투자자(Investors): 신탁업자(증권사) 괄호 노이즈 제거 및 운용사(투자업자) 추출 로직 추가
# 2. [유지] Put/Call Option: HTML 생텍스트 기반 직독직해 엔진 (V13.0)
# 3. [유지] 전환청구기간/납입일/자금용도/발행상품 등 기존 최적화 엔진 유지
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
# 4. 정정사항 엔진
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
    cand_clean = {_clean_label(x) for x in label_candidates}
    if corr_after:
        for c in cand_clean:
            if c in corr_after and str(corr_after[c]).strip(): return _single_line(str(corr_after[c]))
        for k, v in corr_after.items():
            if str(v).strip() and any(c in k for c in cand_clean): return _single_line(str(v))
            
    for df in reversed(dfs): 
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
    for df in reversed(dfs): 
        arr = df.astype(str).values
        best_in_df = None
        for r in range(arr.shape[0]):
            row = [str(x).strip() for x in arr[r].tolist()]
            if all(k in _norm("".join(row)) for k in keys):
                for cell in row:
                    if any(d in cell for d in ["년", "월", "일", "예정일"]): continue
                    amt = _max_int_in_text(cell)
                    if amt is not None and amt > min_val: 
                        best_in_df = max(best_in_df or 0, amt)
        if best_in_df is not None:
            return best_in_df
    return None

def find_row_best_float(dfs, must_contain) -> Optional[float]:
    keys = [_norm(x) for x in must_contain]
    for df in reversed(dfs): 
        arr = df.astype(str).values
        best_in_df = None
        for r in range(arr.shape[0]):
            row = [str(x).strip() for x in arr[r].tolist()]
            if all(k in _norm("".join(row)) for k in keys):
                vals = [x for x in [_to_float(x) for x in row] if x is not None]
                if vals: best_in_df = max(vals, key=lambda z: abs(z))
        if best_in_df is not None:
            return best_in_df
    return None

# ==========================================================
# 5. 핵심 컬럼 전용 추출기 (강화된 투자자 로직 포함)
# ==========================================================

def extract_product_type(dfs: List[pd.DataFrame], corr_after: Dict) -> str:
    labels = ["1. 사채의 종류", "1.사채의종류", "사채의 종류", "사체의 종류", "사태의 종류", "사케의 종류", "사채종류", "종류"]
    def get_clean_product(text: str) -> str:
        if not text: return ""
        t = re.sub(r'\s+', ' ', text).strip()
        t = re.sub(r'(?:1\.\s*)?(?:사채|사체|사태|사케)의\s*종류', '', t)
        t = re.sub(r'\b종류\b', '', t) 
        t = t.replace('발행결정', '').strip()
        
        match = re.search(r'(전환사채|교환사채|신주인수권부사채|사채)', t)
        if not match: return "" 
        t = t[:match.end()].strip() 
        
        pattern = r'((?:제\s*\d+\s*회차?|회차\s*\d+|제?\d+회차?)?\s*(?:제\s*\d+\s*회차?)?\s*(?:(?:무기명식?|기명식?|이권부|무이권부|보증|무보증|사모|공모|비분리형?|분리형?)\s*)*(?:전환사채|교환사채|신주인수권부사채))'
        matches = re.findall(pattern, t)
        for m in matches:
            res = m.strip()
            res = re.sub(r'^회차\s*(\d+)', r'제\1회차', res)
            if 5 <= len(res) <= 40:
                s_idx = res.find("사채")
                if s_idx != -1: res = res[:s_idx+2].strip()
                return _single_line(res)
        return ""

    if corr_after:
        for k, v in corr_after.items():
            if any(_norm(lb) in _norm(k) for lb in labels):
                cleaned = get_clean_product(v)
                if cleaned: return cleaned

    for df in reversed(dfs):
        arr = df.astype(str).values
        for r in range(arr.shape[0]):
            for c in range(arr.shape[1]):
                if any(_clean_label(lb) in _clean_label(arr[r][c]) for lb in labels):
                    for cc in range(c + 1, min(arr.shape[1], c + 5)):
                        cleaned = get_clean_product(arr[r][cc])
                        if cleaned: return cleaned
                    for rr in range(r + 1, min(arr.shape[0], r + 3)):
                        cleaned = get_clean_product(arr[rr][c])
                        if cleaned: return cleaned
        for r in range(min(10, arr.shape[0])): 
            row_str = " ".join([str(x) for x in arr[r] if str(x).lower() != 'nan'])
            cleaned = get_clean_product(row_str)
            if cleaned: return cleaned
    return ""

def extract_payment_date(dfs: List[pd.DataFrame], corr_after: Dict) -> str:
    if corr_after:
        for k, v in corr_after.items():
            if "납입" in k:
                pay_idx = v.find("납입") if "납입" in v else 0
                dates = re.findall(r'\d{4}[-년\.\s]+\d{1,2}[-월\.\s]+\d{1,2}', v[pay_idx:])
                if dates: return _format_date(dates[-1])
    
    for df in reversed(dfs):
        arr = df.astype(str).values
        R, C = arr.shape
        for r in range(R):
            row_str = " ".join([str(x) for x in arr[r] if str(x).lower() != 'nan'])
            if "납입일" in _norm(row_str) or "납입기일" in _norm(row_str):
                pay_idx = row_str.find("납입")
                dates = re.findall(r'\d{4}[-년\.\s]+\d{1,2}[-월\.\s]+\d{1,2}', row_str[pay_idx:])
                if dates: return _format_date(dates[-1])
                dates = re.findall(r'\d{4}[-년\.\s]+\d{1,2}[-월\.\s]+\d{1,2}', row_str)
                if dates: return _format_date(dates[-1])
                if r + 1 < R:
                    next_row = " ".join([str(x) for x in arr[r+1] if str(x).lower() != 'nan'])
                    dates = re.findall(r'\d{4}[-년\.\s]+\d{1,2}[-월\.\s]+\d{1,2}', next_row)
                    if dates: return _format_date(dates[-1])
    return ""

def extract_fund_usage(dfs: List[pd.DataFrame], corr_after: Dict) -> str:
    target_keys = ["시설자금", "영업양수자금", "운영자금", "채무상환자금", "타법인 증권 취득자금", "타법인증권취득자금", "기타자금"]
    for df in reversed(dfs): 
        found_funds = {}
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
        if found_funds:
            result = [k for k, v in sorted(found_funds.items(), key=lambda x: x[1], reverse=True)]
            return _single_line(", ".join(result))

    if corr_after:
        found_funds = {}
        for k, v in corr_after.items():
            for tk in target_keys:
                if _norm(tk) in _norm(k):
                    amt = _max_int_in_text(v)
                    if amt and amt > 100:
                        std_key = "타법인 증권 취득자금" if "타법인" in tk else tk
                        found_funds[std_key] = amt
        if found_funds:
            result = [k for k, v in sorted(found_funds.items(), key=lambda x: x[1], reverse=True)]
            return _single_line(", ".join(result))
            
    val = scan_label_value_preferring_correction(dfs, ["조달자금의 구체적 사용 목적", "자금용도"], corr_after)
    return _single_line(val)

# [강화된 투자자 추출기]
def extract_investors(dfs: List[pd.DataFrame], corr_after: Dict[str, str]) -> str:
    investors = []
    blacklist = [
        "관계", "지분", "%", "배정", "비고", "합계", "소계", "명", "출자자", "해당사항", 
        "내역", "금액", "주식수", "단위", "이사", "이사회", "총계", "주소", "근거", "선정경위"
    ]
    
    def is_valid_investor_name(sn):
        if not sn: return False
        # (본건펀드... ) 같은 부가 설명 제거하고 순수 이름만 추출
        sn = re.sub(r'\(본건펀드.*?\)', '', sn).strip()
        sn_clean = sn.replace(" ", "")
        if not (2 <= len(sn_clean) <= 40): return False
        if re.fullmatch(r'[\d,\.\s\-]+', sn_clean): return False
        sn_norm = _norm(sn_clean)
        for bw in blacklist:
            if bw in sn_norm: return False
        return True

    # 1. 정정 공시 데이터 우선 확인
    val = scan_label_value_preferring_correction(dfs, ["발행대상자", "배정대상자", "투자자", "성명(법인명)", "인수인", "대상자"], corr_after)
    if val:
        for chunk in re.split(r'[\n,;/]', val):
            cleaned = chunk.strip()
            if is_valid_investor_name(cleaned):
                name = re.sub(r'\(본건펀드.*?\)', '', cleaned).strip()
                if name not in investors: investors.append(name)

    # 2. 표 스캔 로직 강화
    if not investors:
        for df in reversed(dfs):
            df_str = df.to_string()
            if any(kw in _norm(df_str) for kw in ["발행대상사명", "발행대상자명", "대상자명", "성명(법인명)"]):
                arr = df.astype(str).values
                R, C = arr.shape
                name_col_idx = -1
                start_row = 1
                for r in range(min(5, R)):
                    for c in range(C):
                        cell_v = _norm(arr[r][c])
                        if any(kw in cell_v for kw in ["대상자명", "성명", "법인명", "인수인", "대상사명"]):
                            name_col_idx = c
                            start_row = r + 1
                            break
                    if name_col_idx != -1: break
                
                if name_col_idx != -1:
                    for rr in range(start_row, R):
                        raw_name = arr[rr][name_col_idx].split('\n')[0].strip()
                        clean_name = re.sub(r'\(.*?신탁업자.*?\)', '', raw_name).strip()
                        if is_valid_investor_name(clean_name) and clean_name not in investors:
                            investors.append(clean_name)

    # 3. 투자업자(운용사) 표 별도 탐색
    if not investors:
        for df in dfs:
            if "투자업자" in df.to_string() or "집합투자업자" in df.to_string():
                arr = df.astype(str).values
                col_idx = -1
                for c in range(arr.shape[1]):
                    if "투자업자" in _norm(arr[0][c]):
                        col_idx = c; break
                if col_idx != -1:
                    for r in range(1, arr.shape[0]):
                        name = arr[r][col_idx].strip()
                        if is_valid_investor_name(name) and name not in investors:
                            investors.append(name)

    return _single_line(", ".join(investors[:12]))

# ----------------------------------------------------------------------
# [V13.0] Put/Call Option 직독직해 스캐너
# ----------------------------------------------------------------------
def extract_option_details(html_raw: str, option_type: str, corr_after: Dict[str, str]) -> str:
    my_kws = ["조기상환청구권", "put option", "풋옵션"] if option_type == 'put' else ["매도청구권", "call option", "콜옵션"]
    opp_kws = ["매도청구권", "call option", "콜옵션", "【특정인", "미상환 주권", "기타 투자판단", "발행결정 전후", "10. 기타사항", "11. 기타사항", "12. 기타사항", "13. 기타사항", "20. 기타사항", "기타사항"] if option_type == 'put' else ["조기상환청구권", "put option", "풋옵션", "【특정인", "미상환 주권", "기타 투자판단", "발행결정 전후", "10. 기타사항", "11. 기타사항", "12. 기타사항", "13. 기타사항", "20. 기타사항", "기타사항"]
    
    if corr_after:
        for k, v in corr_after.items():
            if any(_norm(kw).lower() in _norm(k).lower() for kw in my_kws) and len(v) > 10:
                val = v
                for kw in my_kws:
                    pattern = r'^.*?' + kw + r'(?:[\s\(\)\[\]a-zA-Z]*(?:에\s*관한\s*사항)?\s*\]?)?'
                    val = re.sub(pattern, '', val, flags=re.IGNORECASE)
                val = re.sub(r'^[:\-\]\]\s]+', '', val.strip())
                return _single_line(val)

    soup = BeautifulSoup(html_raw, 'lxml')
    for tag in soup(['style', 'script']): tag.decompose()
    for tag in soup.find_all(['br', 'p', 'div', 'tr', 'td', 'th', 'li']):
        tag.insert_after('\n')
        
    text = soup.get_text(separator='\n')
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{2,}', '\n', text)
    lines = text.split('\n')
    target_indices = []
    
    for i, line in enumerate(lines):
        line_clean = re.sub(r'\s+', '', line).lower()
        if any(re.sub(r'\s+', '', kw).lower() in line_clean for kw in my_kws):
            if any(re.sub(r'\s+', '', opp).lower() in line_clean for opp in ["매도청구권", "calloption", "콜옵션"] if option_type == 'put'):
                continue
            if any(re.sub(r'\s+', '', opp).lower() in line_clean for opp in ["조기상환청구권", "putoption", "풋옵션"] if option_type == 'call'):
                continue
            score = 1
            if "사항" in line: score += 2
            if line.strip().startswith("[") or line.strip().startswith("【") or re.match(r'^[가-힣0-9]\.', line.strip()): 
                score += 1
            target_indices.append((i, score))
            
    if not target_indices: return ""
    target_indices.sort(key=lambda x: (x[1], x[0]))
    best_start_idx = target_indices[-1][0]
    
    collected_lines = []
    for i in range(best_start_idx + 1, len(lines)):
        line = lines[i].strip()
        if not line: continue
        line_clean = re.sub(r'\s+', '', line).lower()
        is_stop = False
        for kw in opp_kws:
            if re.sub(r'\s+', '', kw).lower() in line_clean:
                is_stop = True; break
        if is_stop: break
        collected_lines.append(line)
        
    header_line = lines[best_start_idx].strip()
    header_line_cleaned = header_line
    for kw in my_kws:
        pattern = r'^.*?' + kw + r'(?:[\s\(\)\[\]a-zA-Z]*(?:에\s*관한\s*사항)?\s*\]?)?'
        header_line_cleaned = re.sub(pattern, '', header_line_cleaned, flags=re.IGNORECASE)
    
    header_line_cleaned = re.sub(r'^[:\-\]\]\s]+', '', header_line_cleaned)
    if header_line_cleaned and len(header_line_cleaned) > 5:
        collected_lines.insert(0, header_line_cleaned)
        
    return _single_line(" ".join(collected_lines))

def extract_call_ratio_and_ytc(call_text: str, html_raw: str) -> Tuple[str, str]:
    ratio, ytc = "", ""
    call_text_clean = re.sub(r'\s+', ' ', str(call_text or ""))
    r_patterns = [
        r'(?:권면총액|발행(?:사채)?총액|발행가액|발행규모|지분율)[^\d]{0,10}?(?:의|중|대비)\s*(\d{1,3}(?:\.\d+)?)\s*(?:%|퍼센트)',
        r'(?:매도청구권|Call Option)[^\d]{0,20}?(?:비율|한도|부여|행사)[^\d]{0,10}?(\d{1,3}(?:\.\d+)?)\s*(?:%|퍼센트)',
        r'(\d{1,3}(?:\.\d+)?)\s*(?:%|퍼센트)\s*(?:를|을)?\s*초과하여\s*행사할\s*수\s*없',
        r'100분의\s*(\d{1,3}(?:\.\d+)?)'
    ]
    for p in r_patterns:
        m = re.search(p, call_text_clean, re.IGNORECASE)
        if m:
            val = float(m.group(1))
            if 5 <= val <= 100: ratio = f"{val:g}%"; break
    y_patterns = [
        r'(?:수익률|이율|적용이율|할증률|복리)[^\d]{0,15}?(?:연|연복리|복리)?\s*(\d{1,2}(?:\.\d+)?)\s*(?:%|퍼센트)',
        r'(?:연|연복리|복리)\s*(\d{1,2}(?:\.\d+)?)\s*(?:%|퍼센트)[^\d]{0,15}?(?:의\s*수익|이율|가산)',
        r'(?:%|퍼센트)[^\d]{0,15}?(?:수익률|이율|복리|연)\s*(\d{1,2}(?:\.\d+)?)'
    ]
    for p in y_patterns:
        m = re.search(p, call_text_clean, re.IGNORECASE)
        if m:
            val = float(m.group(1))
            if 0 <= val <= 20: ytc = f"{val:g}%"; break
    if not ratio or not ytc:
        all_pcts = [float(v) for v in re.findall(r'(\d{1,3}(?:\.\d+)?)\s*(?:%|퍼센트)', call_text_clean) if float(v) > 0]
        if not ratio and all_pcts:
            rands = [v for v in all_pcts if 10 <= v <= 100 and v != float(ytc.replace('%', '') if ytc else -1)]
            if rands: ratio = f"{max(rands):g}%"
        if not ytc and all_pcts:
            yands = [v for v in all_pcts if 0 <= v <= 20 and v != float(ratio.replace('%', '') if ratio else -1)]
            if yands: ytc = f"{yands[0]:g}%"
    return ratio, ytc

def extract_period_dates(dfs, corr_after, period_kws) -> Tuple[str, str]:
    if corr_after:
        for k, v in corr_after.items():
            if any(_norm(p) in _norm(k) for p in period_kws):
                dates = re.findall(r'\d{4}[-년\.\s]+\d{1,2}[-월\.\s]+\d{1,2}', v)
                if len(dates) >= 2: return _format_date(dates[0]), _format_date(dates[-1])

    for df in reversed(dfs): 
        arr = df.astype(str).values
        R, C = arr.shape
        for r in range(R):
            row_str = _norm(" ".join(arr[r]))
            if any(p in row_str for p in period_kws) or "시작일" in row_str or "종료일" in row_str:
                block_text = ""
                for rr in range(r, min(R, r + 5)):
                    block_text += " " + " ".join([str(x) for x in arr[rr] if str(x).lower() != 'nan'])
                block_text = re.sub(r'상기\s*전환.*?시작일.*?발행결정.*?\.', '', block_text)
                dates = re.findall(r'\d{4}[-년\.\s]+\d{1,2}[-월\.\s]+\d{1,2}', block_text)
                unique_dates = []
                for d in dates:
                    fd = _format_date(d)
                    if fd not in unique_dates: unique_dates.append(fd)
                if len(unique_dates) >= 2: return unique_dates[0], unique_dates[1]
                elif len(unique_dates) == 1: return unique_dates[0], ""
    return "", ""

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
    
    table_comp = scan_label_value_preferring_correction(dfs, ["회사명", "발행회사"], corr_after)
    rec["회사명"] = table_comp or company_from_title(title_clean)
    mkt = scan_label_value_preferring_correction(dfs, ["상장시장", "시장구분"], corr_after)
    rec["상장시장"] = mkt or market_from_title(title_clean) or t.market or company_market_map.get(norm_company_name(rec["회사명"]), "")
    
    rec["최초 이사회결의일"] = _format_date(scan_label_value_preferring_correction(dfs, ["이사회결의일(결정일)"], corr_after))
    rec["납입일"] = extract_payment_date(dfs, corr_after)
    rec["만기"] = _format_date(scan_label_value_preferring_correction(dfs, ["사채만기일", "만기일"], corr_after))
    rec["모집방식"] = scan_label_value_preferring_correction(dfs, ["사채발행방법", "모집방법"], corr_after)
    rec["발행상품"] = extract_product_type(dfs, corr_after)

    def get_corr_num(labels, fallback_keys=[], min_val=-1, as_float=False):
        val = scan_label_value_preferring_correction(dfs, labels, corr_after)
        if as_float:
            num = _to_float(val)
            if num is None and fallback_keys: num = find_row_best_float(dfs, fallback_keys)
            return str(num) if num is not None else ""
        else:
            num = _to_int(val)
            if (num is None or num <= min_val) and fallback_keys: num = find_row_best_int(dfs, fallback_keys, min_val)
            return f"{num:,}" if num is not None else ""

    rec["권면총액(원)"] = get_corr_num(["사채의권면(전자등록)총액(원)", "권면총액"], ["권면총액", "원"], 50)
    rec["Coupon"] = get_corr_num(["표면이자율(%)"], ["표면이자율"], -1, True)
    rec["YTM"] = get_corr_num(["만기이자율(%)"], ["만기이자율"], -1, True)
    rec["행사(전환)가액(원)"] = get_corr_num(["전환가액(원/주)", "행사가액"], ["가액", "원"], 50)
    rec["전환주식수"] = get_corr_num(["전환에 따라 발행할 주식수"], ["주식수"], 50)
    rec["주식총수대비 비율"] = scan_label_value_preferring_correction(dfs, ["주식총수 대비 비율(%)"], corr_after)
    rec["Refixing Floor"] = get_corr_num(["최저 조정가액 (원)"], ["최저조정가액", "원"], 50)

    rec["전환청구 시작"], rec["전환청구 종료"] = extract_period_dates(dfs, corr_after, ["전환청구기간", "권리행사기간"])
    rec["Put Option"] = extract_option_details(html_raw, 'put', corr_after)
    rec["Call Option"] = extract_option_details(html_raw, 'call', corr_after)
    ratio, ytc = extract_call_ratio_and_ytc(rec["Call Option"], html_raw)
    rec["Call 비율"], rec["YTC"] = ratio, ytc
    rec["투자자"] = extract_investors(dfs, corr_after)
    rec["자금용도"] = extract_fund_usage(dfs, corr_after)

    return rec

# ==========================================================
# 7. 구글 시트 및 메인 실행
# ==========================================================
def gs_open():
    if not GOOGLE_SHEET_ID or not GOOGLE_CREDENTIALS_JSON: raise RuntimeError("연동 정보 누락")
    gc = gspread.service_account_from_dict(json.loads(GOOGLE_CREDENTIALS_JSON))
    sh = gc.open_by_key(GOOGLE_SHEET_ID)
    try: seen_ws = sh.worksheet(SEEN_SHEET_NAME)
    except: seen_ws = sh.add_worksheet(title=SEEN_SHEET_NAME, rows=2000, cols=2); seen_ws.update("A1:B1", [SEEN_HEADERS])
    try: bond_ws = sh.worksheet(BOND_OUT_SHEET)
    except: bond_ws = sh.add_worksheet(title=BOND_OUT_SHEET, rows=2000, cols=len(BOND_COLUMNS))
    return sh, bond_ws, seen_ws

def run():
    sh, bond_ws, seen_ws = gs_open()
    if not bond_ws.get_all_values(): bond_ws.update("A1", [BOND_COLUMNS])
    
    values = bond_ws.get_all_values()
    bond_index = {row[BOND_COLUMNS.index("접수번호")]: i+1 for i, row in enumerate(values) if i > 0}
    seen_index = {row[0]: i+1 for i, row in enumerate(seen_ws.get_all_values()) if i > 0}
    
    company_market_map = {} # 상장시장 맵핑 생략(원본 유지)
    targets = parse_rss_targets()[:LIMIT] if LIMIT > 0 else parse_rss_targets()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        context = browser.new_context()
        for t in targets:
            try:
                dfs, src, html_raw = scrape_one(context, t.acpt_no)
                corr_after = extract_correction_after_map(dfs) if is_correction_title(t.title) else None
                rec = parse_bond_record(dfs, t, corr_after, html_raw, company_market_map)
                row_vals = [rec.get(h, "") for h in BOND_COLUMNS]
                
                if t.acpt_no in bond_index:
                    idx = bond_index[t.acpt_no]
                    bond_ws.update(f"A{idx}", [row_vals])
                else:
                    bond_ws.append_row(row_vals)
                
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                seen_ws.append_row([t.acpt_no, ts])
                print(f"[OK] {t.acpt_no} | {rec['회사명']}")
            except Exception as e: print(f"[FAIL] {t.acpt_no} :: {e}")
        browser.close()

if __name__ == "__main__":
    run()
