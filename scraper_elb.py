# ==========================================================
# #주식연계채권_코드V13.5_Perfect_Option_Sentence (옵션 첫 문장 완벽 추출 패치)
# 1. [핵심개선] Put/Call Option: "조기상환청구권(Put Option)에" 처럼 제목만 짤리거나 엉뚱한 문장이 나오는 버그 완벽 해결
# 2. [핵심개선] 문맥 스코어링 도입: "본 사채의 사채권자는...", "발행회사 또는 발행회사가 지정하는 자는..." 등 진짜 첫 문장부터 추출
# 3. [유지] V13.3의 모든 기능 (투자자 싹쓸이, Call 비율/YTC 정규식, 발행상품 확장 등) 100% 유지
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
# 5. 핵심 컬럼 전용 추출기 (발행상품, 투자자 등)
# ==========================================================

def extract_product_type(dfs: List[pd.DataFrame], corr_after: Dict) -> str:
    labels = ["1. 사채의 종류", "1.사채의종류", "사채의 종류", "사체의 종류", "사태의 종류", "사케의 종류", "사채종류", "종류"]
    def get_clean_product(text: str) -> str:
        if not text: return ""
        t = re.sub(r'\s+', ' ', text).strip()
        t = re.sub(r'(?:1\.\s*)?(?:사채|사체|사태|사케)의\s*종류', '', t)
        t = re.sub(r'\b종류\b', '', t) 
        t = t.replace('발행결정', '').strip()
        
        pattern = r'((?:제\s*\d+\s*회차?)?[\s\w,()]*(?:무기명식|기명식|이권부|무보증|보증|사모|공모|비분리형|분리형|표면|만기)[\s\w,()]*?(?:전환사채|교환사채|신주인수권부사채|사채))'
        matches = re.findall(pattern, t)
        
        if not matches:
            match = re.search(r'((?:제\s*\d+\s*회차?)?\s*(?:전환사채|교환사채|신주인수권부사채))', t)
            if match: return _single_line(match.group(1))
            return ""

        for m in matches:
            res = m.strip()
            res = re.sub(r'^회차\s*(\d+)', r'제\1회차', res)
            if 5 <= len(res) <= 60:
                s_idx = res.find("사채")
                if s_idx != -1: res = res[:s_idx+2].strip()
                res = re.sub(r'^.*?((?:제\d+회)?\s*(?:무기명|기명))', r'\1', res)
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


def extract_investors(dfs: List[pd.DataFrame], corr_after: Dict[str, str]) -> str:
    investors = []
    
    blacklist = [
        "관계", "배정", "비고", "합계", "소계", "해당사항", "내역", "금액", "주식수", 
        "단위", "이사회", "총계", "주소", "근거", "선정경위", "거래내역", "목적", 
        "취득내역", "잔고", "출자자수", "주요사항"
    ]
    
    def clean_investor_name(sn):
        if not sn or str(sn).lower() == 'nan': return ""
        s = str(sn).replace('\n', ' ').replace('\r', '').strip()
        s = re.sub(r'\([^)]*신탁업자[^)]*\)', '', s)
        s = re.sub(r'\([^)]*본건펀드[^)]*\)', '', s)
        s = re.sub(r'\([^)]*전문투자자[^)]*\)', '', s)
        s = re.sub(r'\([^)]*손익차등[^)]*\)', '', s)
        s = re.sub(r'주\s*\d+\)', '', s)
        return re.sub(r'\s+', ' ', s).strip()

    def is_valid_investor_name(sn):
        if not sn: return False
        sn_clean = sn.replace(" ", "")
        if not (2 <= len(sn_clean) <= 50): return False
        if re.fullmatch(r'[\d,\.\s\-]+', sn_clean): return False
        sn_norm = _norm(sn_clean)
        for bw in blacklist:
            if bw in sn_norm: return False
        return True

    target_col_kws = ["대상자명", "대상사명", "성명", "법인명", "인수인", "투자기구", "투자업자", "발행대상", "투자자"]

    for df in reversed(dfs):
        arr = df.astype(str).values
        R, C = arr.shape
        found_cols = []
        start_row = 1
        
        for r in range(min(5, R)):
            for c in range(C):
                cell_v = _norm(arr[r][c])
                if any(kw in cell_v for kw in target_col_kws):
                    if "최대주주" in cell_v or "대표이사" in cell_v: continue
                    found_cols.append(c)
            if found_cols:
                start_row = r + 1
                break
        
        for col_idx in found_cols:
            for rr in range(start_row, R):
                cell_data = str(arr[rr][col_idx])
                valid_found = False
                for line in cell_data.split('\n'):
                    c_line = clean_investor_name(line)
                    if is_valid_investor_name(c_line):
                        if c_line not in investors: investors.append(c_line)
                        valid_found = True
                
                if not valid_found:
                    c_whole = clean_investor_name(cell_data.replace('\n', ' '))
                    if is_valid_investor_name(c_whole) and c_whole not in investors:
                        investors.append(c_whole)

    if not investors and corr_after:
        for k, v in corr_after.items():
            if any(_norm(kw) in _norm(k) for kw in ["발행대상자", "배정대상자", "투자자", "인수인", "대상자"]):
                for chunk in re.split(r'[,;/]', v.replace('\n', ',')):
                    c_name = clean_investor_name(chunk)
                    if is_valid_investor_name(c_name) and c_name not in investors:
                        investors.append(c_name)

    if not investors:
        val = scan_label_value_preferring_correction(dfs, ["발행대상자", "배정대상자", "투자자", "성명(법인명)", "인수인"], corr_after)
        if val:
            for chunk in re.split(r'[,;/]', val.replace('\n', ',')):
                c_name = clean_investor_name(chunk)
                if is_valid_investor_name(c_name) and c_name not in investors:
                    investors.append(c_name)

    if not investors:
        for df in dfs:
            arr = df.astype(str).values
            for r in range(arr.shape[0]):
                for c in range(arr.shape[1]):
                    cell_val = clean_investor_name(arr[r][c].replace('\n', ' '))
                    if re.search(r'(투자조합|사모투자|펀드|파트너스|인베스트먼트|자산운용|증권)', cell_val):
                        if is_valid_investor_name(cell_val) and cell_val not in investors:
                            investors.append(cell_val)

    final_investors = []
    for inv in investors:
        if inv and inv not in final_investors:
            final_investors.append(inv)

    return _single_line(", ".join(final_investors[:15]))


# ----------------------------------------------------------------------
# ★ [V13.5 최고 성능 패치] Put/Call Option 직독직해 엔진
# - 표 헤더 무시, 줄바꿈 무시, 진짜 "주어(사채권자/발행회사)"가 포함된 문맥 덩어리 탐색
# ----------------------------------------------------------------------
def extract_option_details(html_raw: str, option_type: str, corr_after: Dict[str, str]) -> str:
    my_kws = ["조기상환청구권", "put option", "풋옵션"] if option_type == 'put' else ["매도청구권", "call option", "콜옵션"]
    opp_kws = ["매도청구권", "call option", "콜옵션"] if option_type == 'put' else ["조기상환청구권", "put option", "풋옵션"]
    stop_kws = opp_kws + ["【특정인", "미상환 주권", "기타 투자판단", "발행결정 전후", "10. 기타사항", "11. 기타사항", "12. 기타사항", "13. 기타사항", "20. 기타사항", "합병 관련 사항", "청약일", "납입일"]
    
    # 앞쪽 지저분한 목차 라벨(예: "가. 조기상환청구권에 관한 사항")만 똑똑하게 날리는 함수
    def clean_title_prefix(text, kws):
        for kw in kws:
            pattern = re.compile(r'^([\[【<가-힣0-9\s\.\-]*' + re.escape(kw) + r'(?:\s*\([a-zA-Z\s]+\))?(?:[\s]*(?:에\s*관한\s*사항|관한\s*사항|사항)?\s*\]?)?[\s:\-\.]*)', re.IGNORECASE)
            match = pattern.match(text)
            # 만약 문장 맨 앞부분이 키워드 매칭이면서 길이가 짧다면 (진짜 제목이라는 뜻) 삭제
            if match and len(match.group(1)) < 50:
                text = text[match.end():].strip()
                break
        return text

    if corr_after:
        for k, v in corr_after.items():
            if any(_norm(kw).lower() in _norm(k).lower() for kw in my_kws) and len(v) > 10:
                v_clean = re.sub(r'\s+', ' ', v).strip()
                v_clean = clean_title_prefix(v_clean, my_kws)
                if v_clean: return v_clean[:400] + ("..." if len(v_clean) > 400 else "")

    # HTML 태그를 모두 없애되, 시각적 줄바꿈 구역마다 '|||' 구분자를 넣어 하나의 문자열로 연결
    soup = BeautifulSoup(html_raw, 'lxml')
    for tag in soup(['style', 'script']): tag.decompose()
    for tag in soup.find_all(['br', 'p', 'div', 'tr', 'td', 'th', 'li']):
        tag.insert_after('|||')
    
    text = soup.get_text(separator=' ', strip=True)
    blocks = [b.strip() for b in text.split('|||') if b.strip()]

    candidates = []
    for i, block in enumerate(blocks):
        block_lower = block.lower().replace(' ', '')
        if any(kw.replace(' ', '') in block_lower for kw in my_kws):
            
            # 키워드가 포함된 문단부터 시작하여 아래로 최대 15개 문단을 하나의 글로 합침
            gathered = []
            for j in range(i, min(len(blocks), i + 15)):
                check_lower = blocks[j].lower().replace(' ', '')
                
                # 다른 섹션 제목(상대방 옵션, 기타사항 등)이 나오면 합치기 중단
                if j > i:
                    is_stop = False
                    for skw in stop_kws:
                        if skw.replace(' ', '') in check_lower:
                            # 단순 문장 안의 단어가 아니라, 정말 제목처럼 짧거나 괄호로 시작할 때만 멈춤
                            if len(blocks[j]) < 40 or blocks[j].startswith('[') or blocks[j].startswith('【') or re.match(r'^([0-9가-힣]\.|제\s*\d+\s*조)', blocks[j]):
                                is_stop = True
                                break
                    if is_stop: break
                
                gathered.append(blocks[j])
            
            raw_cand = " ".join(gathered)
            cleaned = clean_title_prefix(raw_cand, my_kws)
            
            # [핵심] 엉뚱한 테이블을 거르기 위한 AI 문맥 스코어링 시스템
            score = 0
            if len(cleaned) > 20: score += 10
            if len(cleaned) > 100: score += 10
            
            if option_type == 'put':
                if "사채권자" in cleaned: score += 30
                if "발행회사에" in cleaned or "발행회사에게" in cleaned or "청구할 수 있다" in cleaned: score += 20
                if "조기상환" in cleaned: score += 10
            else:
                if "발행회사" in cleaned: score += 30
                if "지정하는 자" in cleaned: score += 20
                if "매수" in cleaned or "매도청구" in cleaned: score += 20
                if "콜옵션" in cleaned: score += 10
            
            # 표 헤더처럼 생긴 가짜 텍스트 강력 감점 (Screenshot 1번의 지놈앤컴퍼니 현상 방지)
            if "매매일" in cleaned or "상환율" in cleaned or "from" in cleaned.lower() or "to" in cleaned.lower(): score -= 50
            if "구분" in cleaned and "청구기간" in cleaned: score -= 50

            candidates.append((score, cleaned))

    if not candidates: return "없음"

    # 점수가 가장 높은 텍스트 블록 선택
    candidates.sort(key=lambda x: x[0], reverse=True)
    best_score, best_text = candidates[0]

    # 제대로 된 문장이 아니면 버림
    if not best_text or len(best_text) < 5 or best_score < 0:
        return "없음"

    best_text = re.sub(r'\s+', ' ', best_text).strip()
    
    # 마지막으로 맨 앞의 특수기호 찌꺼기 한 번 더 정리
    best_text = re.sub(r'^[\s\(\)\[\]\]:\-\.]+', '', best_text)
    
    if len(best_text) > 300: best_text = best_text[:300] + "..."
    return best_text


def extract_call_ratio_and_ytc(call_text: str, html_raw: str) -> Tuple[str, str]:
    ratio, ytc = "", ""
    call_text_clean = re.sub(r'\s+', ' ', str(call_text or ""))

    r_patterns = [
        r'(?:권면총액|발행총액|발행가액|발행규모|전자등록총액)[^\d]{0,20}?(?:의|중|대비)\s*(\d{1,3}(?:\.\d+)?)\s*(?:%|퍼센트)',
        r'(?:콜옵션|매도청구권).*?(?:비율|한도|초과하여).*?(\d{1,3}(?:\.\d+)?)\s*(?:%|퍼센트)',
        r'(\d{1,3}(?:\.\d+)?)\s*(?:%|퍼센트)\s*(?:를|을)\s*초과(?:하여)?',
        r'(?:100|백)분의\s*(\d{1,3}(?:\.\d+)?)'
    ]

    for p in r_patterns:
        m = re.search(p, call_text_clean, re.IGNORECASE)
        if m:
            val = float(m.group(1))
            if 5 <= val <= 100: 
                ratio = f"{val:g}%"
                break

    y_patterns = [
        r'(?:수익률|할증률|연복리|복리|이율)[^\d]{0,15}?(?:연|연복리|복리)?\s*(\d{1,2}(?:\.\d+)?)\s*(?:%|퍼센트)',
        r'(?:연|연복리|복리)\s*(\d{1,2}(?:\.\d+)?)\s*(?:%|퍼센트)[^\d]{0,15}?(?:의\s*수익|이율|가산)',
        r'(?:보장수익률|연환산수익률).*?(\d{1,2}(?:\.\d+)?)\s*(?:%|퍼센트)'
    ]

    for p in y_patterns:
        m = re.search(p, call_text_clean, re.IGNORECASE)
        if m:
            val = float(m.group(1))
            if 0 <= val <= 30: 
                ytc = f"{val:g}%"
                break

    if not ratio or not ytc:
        soup = BeautifulSoup(html_raw, 'lxml')
        text = soup.get_text(separator=' ', strip=True)
        call_idx = text.lower().find("매도청구권")
        if call_idx == -1: call_idx = text.lower().find("call option")
        
        if call_idx != -1:
            window = text[call_idx:call_idx+1500]
            if not ratio:
                for p in r_patterns:
                    m = re.search(p, window, re.IGNORECASE)
                    if m and 5 <= float(m.group(1)) <= 100: 
                        ratio = f"{float(m.group(1)):g}%"
                        break
            if not ytc:
                for p in y_patterns:
                    m = re.search(p, window, re.IGNORECASE)
                    if m and 0 <= float(m.group(1)) <= 30: 
                        ytc = f"{float(m.group(1)):g}%"
                        break
                        
    return ratio, ytc


def extract_period_dates(dfs, corr_after, period_kws) -> Tuple[str, str]:
    if corr_after:
        for k, v in corr_after.items():
            if any(_norm(p) in _norm(k) for p in period_kws):
                dates = re.findall(r'\d{4}[-년\.\s]+\d{1,2}[-월\.\s]+\d{1,2}', v)
                if len(dates) >= 2:
                    return _format_date(dates[0]), _format_date(dates[-1])

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
                    if fd not in unique_dates: 
                        unique_dates.append(fd)
                        
                if len(unique_dates) >= 2:
                    return unique_dates[0], unique_dates[1]
                elif len(unique_dates) == 1:
                    return unique_dates[0], ""
                    
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
    
    comp_cands = ["회사명", "회사 명", "발행회사"]
    table_comp = scan_label_value_preferring_correction(dfs, comp_cands, corr_after)
    rec["회사명"] = table_comp or company_from_title(title_clean) or title_clean
    if rec["회사명"] in ["유", "코", "넥"]: rec["회사명"] = title_clean

    mkt = scan_label_value_preferring_correction(dfs, ["상장시장", "시장구분"], corr_after)
    rec["상장시장"] = mkt or market_from_title(title_clean) or t.market or company_market_map.get(norm_company_name(rec["회사명"]), "")
    if rec["상장시장"]: company_market_map[norm_company_name(rec["회사명"])] = rec["상장시장"]

    rec["최초 이사회결의일"] = _format_date(scan_label_value_preferring_correction(dfs, ["이사회결의일(결정일)", "이사회결의일", "최초이사회결의일"], corr_after))
    
    rec["납입일"] = extract_payment_date(dfs, corr_after)
    rec["만기"] = _format_date(scan_label_value_preferring_correction(dfs, ["사채만기일", "만기일", "상환기일"], corr_after))
    
    rec["모집방식"] = scan_label_value_preferring_correction(dfs, ["사채발행방법", "모집방법", "발행방법"], corr_after)
    
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

    rec["Put Option"] = extract_option_details(html_raw, 'put', corr_after)
    rec["Call Option"] = extract_option_details(html_raw, 'call', corr_after)
    
    ratio, ytc = extract_call_ratio_and_ytc(rec["Call Option"], html_raw)
    rec["Call 비율"] = ratio
    rec["YTC"] = ytc

    rec["투자자"] = extract_investors(dfs, corr_after)
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

    for row in values[1:]:
        acpt = row[BOND_COLUMNS.index("접수번호")] if len(row) > BOND_COLUMNS.index("접수번호") else ""
        if not acpt.isdigit(): continue
        
        amt = row[BOND_COLUMNS.index("권면총액(원)")] if len(row) > BOND_COLUMNS.index("권면총액(원)") else ""
        price = row[BOND_COLUMNS.index("행사(전환)가액(원)")] if len(row) > BOND_COLUMNS.index("행사(전환)가액(원)") else ""
        prod = row[BOND_COLUMNS.index("발행상품")] if len(row) > BOND_COLUMNS.index("발행상품") else ""
        put_opt = row[BOND_COLUMNS.index("Put Option")] if len(row) > BOND_COLUMNS.index("Put Option") else ""
        
        investor_val = row[BOND_COLUMNS.index("투자자")] if len(row) > BOND_COLUMNS.index("투자자") else ""
        
        # 누락 복구 조건 발동!
        needs_fix = (not amt or not price or not prod or not put_opt or not investor_val)
        
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
