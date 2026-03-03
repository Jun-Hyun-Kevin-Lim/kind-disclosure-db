# ==========================================================
# #주식연계채권_코드V5.9_Ultra (정확도 극대화 & 한줄 텍스트 렌더링)
# - 타겟 필터링 완벽화: 정확히 '사채권발행결정' 3종만 캡처 (기타 공시 원천 차단)
# - 텍스트 한줄 처리: 줄바꿈(\n)을 띄어쓰기로 변환하여 시트 가독성 최적화
# - 정정공시 덮어쓰기: '정정후' 데이터를 기존 접수번호 혹은 일치하는 이벤트 행에 UPDATE
# - 데이터 정제: 날짜(YYYY-MM-DD) 규격화, 표 순번(1,2,3..) 오인 방지 로직 강화
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
# 설정 (ENV)
# ==========================================================
BASE = "https://kind.krx.co.kr"
DEFAULT_RSS = (
    "http://kind.krx.co.kr:80/disclosure/rsstodaydistribute.do"
    "?method=searchRssTodayDistribute&mktTpCd=0&currentPageSize=100"
)

RSS_URL = os.getenv("RSS_URL", DEFAULT_RSS)

HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
LIMIT = int(os.getenv("LIMIT", "0"))
RUN_ONE_ACPTNO = os.getenv("RUN_ONE_ACPTNO", "").strip()

GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "").strip()
GOOGLE_CREDENTIALS_JSON = (
    os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip() or
    os.environ.get("GOOGLE_CREDS", "").strip()
)

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
# 유틸리티
# ==========================================================
def _norm(s: str) -> str:
    return re.sub(r"\s+", "", str(s or "")).replace(":", "")

def _clean_label(s: str) -> str:
    return re.sub(r"^([①-⑩]|\(\d+\)|\d+\.)+", "", _norm(s))

def _single_line(s: str) -> str:
    """모든 텍스트를 한 줄로 만듭니다. (줄바꿈 제거)"""
    if not s: return ""
    return re.sub(r'\s+', ' ', str(s)).strip()

def _format_date(s: str) -> str:
    """텍스트에서 YYYY-MM-DD 형식의 날짜만 추출"""
    m = re.search(r'(\d{4})[-년\.\s]+(\d{1,2})[-월\.\s]+(\d{1,2})', str(s))
    if m: return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    return _single_line(s)

def _to_int(s: str) -> Optional[int]:
    t = re.sub(r"[^\d\-]", "", str(s or "").replace(",", ""))
    return int(t) if t not in ("", "-") else None

def _to_float(s: str) -> Optional[float]:
    t = re.sub(r"[^\d\.\-]", "", str(s or "").replace(",", ""))
    return float(t) if t not in ("", "-", ".") else None

def norm_company_name(name: str) -> str:
    return _norm(name or "").replace("주식회사", "").replace("(주)", "")

def company_from_title(title: str) -> str:
    t2 = re.sub(r"\[(유|코|넥|코넥|KOSPI|KOSDAQ|KONEX)\]", "", title or "").strip()
    t2 = re.sub(r"\[.*?정정.*?\]", "", t2).strip()
    parts = t2.split()
    if not parts: return ""
    if parts[0] in ("주식회사", "(주)", "㈜") and len(parts) > 1:
        return parts[1]
    return parts[0]

def market_from_title(title: str) -> str:
    if not title: return ""
    if "[코]" in title or "코스닥" in title: return "코스닥"
    if "[유]" in title or "유가증권" in title: return "유가증권"
    if "[넥]" in title or "[코넥]" in title or "코넥스" in title: return "코넥스"
    return ""

def viewer_url(acpt_no: str, docno: str = "") -> str:
    return f"{BASE}/common/disclsviewer.do?method=searchInitInfo&acptNo={acpt_no}&docno={docno}"

def is_correction_title(title: str) -> bool:
    return "정정" in (title or "")

def make_event_key(company: str, first_board_date: str, bond_type: str) -> str:
    return f"{_norm(company)}|{_norm_date(first_board_date)}|{_norm(bond_type)}"

def _norm_date(s: str) -> str:
    return re.sub(r"[^\d]", "", str(s or ""))

# [타겟 필터링 완벽화] 띄어쓰기를 무시하고 오직 3가지 키워드만 타겟팅
def match_strict_keyword(title: str) -> bool:
    if not title: return False
    title_no_space = title.replace(" ", "")
    target_kws = ["전환사채권발행결정", "교환사채권발행결정", "신주인수권부사채권발행결정"]
    return any(kw in title_no_space for kw in target_kws)

# ==========================================================
# 커스텀 HTML 파서
# ==========================================================
def extract_tables_from_html_robust(html: str) -> List[pd.DataFrame]:
    html = (html or "").replace("\x00", "")
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript"]): tag.decompose()
    
    results = []
    for tbl in soup.find_all("table"):
        try:
            one = pd.read_html(str(tbl), header=None)
            if one: results.append(one[0].where(pd.notnull(one[0]), ""))
            continue
        except: pass
        
        rows = [[c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])] for tr in tbl.find_all("tr")]
        rows = [r for r in rows if r]
        if rows:
            max_len = max(len(r) for r in rows)
            results.append(pd.DataFrame([r + [""] * (max_len - len(r)) for r in rows]))
            
    if not results: raise ValueError("표 파싱 실패")
    return results

def parse_rss_targets() -> List[Target]:
    feed = feedparser.parse(RSS_URL)
    targets = []
    for it in getattr(feed, "entries", []):
        title = getattr(it, "title", "") or ""
        link = getattr(it, "link", "") or ""
        
        if not match_strict_keyword(title): continue # 철통 필터링 적용
        
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
            bonus = sum(1 for w in ["권면총액", "표면이자율", "만기", "행사가액", "전환가액", "조기상환", "매도청구", "정정사항"] if w in lower)
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
        if best_html.lower().count("<table") == 0: raise RuntimeError("table 못 찾음")
        return extract_tables_from_html_robust(best_html), url, all_frames_html
    finally:
        try: page.close()
        except: pass

# ==========================================================
# 파싱 엔진 (정정 반영 & 한줄 포맷팅)
# ==========================================================
def extract_correction_after_map(dfs: List[pd.DataFrame]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for df in dfs:
        try: arr = df.astype(str).values
        except: continue
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
                if v and v.lower() != "nan" and _norm(v) not in ("정정후", "정정전", "항목", "변경사유", "-"):
                    after_val = _single_line(v) # 정정값도 한줄로
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
                    checks = [str(arr[rr][cc]).strip() for rr, cc in [(r, c+1), (r, c+2), (r+1, c), (r+1, c+1)] if 0 <= rr < R and 0 <= cc < C]
                    row_vals = [str(x).strip() for x in arr[r].tolist() if str(x).strip()]
                    for v in [v for v in checks + row_vals if v.lower() != "nan"]:
                        v_norm = _norm(v)
                        if _clean_label(v) in cand_clean: continue
                        if re.fullmatch(r"([①-⑩]|\(\d+\)|\d+\.)", v_norm): continue
                        return _single_line(v)
    return ""

def extract_option_details(html_raw: str, option_type: str) -> str:
    """본문에서 옵션 내용 추출 후 완벽한 한줄 텍스트로 반환"""
    soup = BeautifulSoup(html_raw, 'lxml')
    text = soup.get_text(separator=' ', strip=True) # 줄바꿈을 공백으로 치환하며 가져옴
    
    kws = ["조기상환청구권", "Put Option", "PutOption"] if option_type == 'put' else ["매도청구권", "Call Option", "CallOption"]
    idx = -1
    for kw in kws:
        idx = text.find(kw)
        if idx != -1: break
        
    if idx != -1:
        snippet = text[idx:idx+1500]
        # 다음 목차 번호(예: 10., 11., 20.)가 나오면 자름
        match = re.search(r'\s(1[0-9]|2[0-9])\.\s', snippet[50:])
        if match:
            snippet = snippet[:50+match.start()]
        return _single_line(snippet)
    return ""

def extract_call_ratio_and_ytc(call_text: str) -> Tuple[str, str]:
    if not call_text: return "", ""
    ratio, ytc = "", ""
    r_match = re.search(r'(\d{1,3})(?:\s*/\s*100|\s*%)', call_text)
    if r_match:
        val = float(r_match.group(1))
        if 0 < val <= 100: ratio = f"{val:g}%"
    y_match = re.search(r'연\s*(?:복리)?\s*(\d+(?:\.\d+)?)\s*%', call_text)
    if y_match: ytc = f"{y_match.group(1)}%"
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
    """투자자 리스트를 한 줄(A, B, C)로 추출"""
    investors = []
    blacklist = ["관계", "지분", "%", "배정", "비고", "합계", "소계", "명", "출자자"]
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
            if "발행대상자명" in _norm(df.to_string()) or "대상자명" in _norm(df.to_string()):
                arr = df.astype(str).values
                for r in range(1, arr.shape[0]):
                    name = arr[r][0].split('\n')[0].strip()
                    if is_valid(name) and name not in investors: investors.append(name)
                    
    return _single_line(", ".join(investors[:15])) # 한 줄로 결합

def extract_fund_usage(dfs: List[pd.DataFrame], corr_after) -> str:
    """자금조달 목적을 한 줄(운영자금 50억, 시설자금 10억)로 추출"""
    uses_map = {"시설자금":0, "영업양수자금":0, "운영자금":0, "채무상환자금":0, "타법인증권":0, "기타자금":0}
    for df in dfs:
        text = _norm(df.to_string())
        if "자금조달의목적" in text:
            for k in uses_map.keys():
                m = re.search(f"{k}.*?([\d,]{{4,}})", text)
                if m: uses_map[k] += int(m.group(1).replace(",", ""))
                
    result = [f"{k} {v:,}원" for k, v in sorted(uses_map.items(), key=lambda x: x[1], reverse=True) if v > 0]
    if result: return _single_line(", ".join(result))
    
    val = scan_label_value_preferring_correction(dfs, ["조달자금의 구체적 사용 목적", "자금용도"], corr_after)
    return _single_line(val)

# ==========================================================
# 레코드 파싱 (최종 매핑)
# ==========================================================
def parse_bond_record(dfs, t: Target, corr_after, html_raw, company_market_map) -> dict:
    rec = {k: "" for k in BOND_COLUMNS}
    rec["접수번호"] = t.acpt_no
    rec["링크"] = t.link if t.link else viewer_url(t.acpt_no)

    title_clean = t.title.replace("[자동복구대상]", "").strip()
    rec["보고서명"] = title_clean
    
    title_no_space = title_clean.replace(" ", "")
    if "교환사채" in title_no_space: rec["구분"] = "EB"
    elif "신주인수권" in title_no_space: rec["구분"] = "BW"
    elif "전환사채" in title_no_space: rec["구분"] = "CB"
    
    rec["회사명"] = scan_label_value_preferring_correction(dfs, ["회사명", "회사 명"], corr_after) or company_from_title(title_clean)
    rec["상장시장"] = scan_label_value_preferring_correction(dfs, ["상장시장", "시장구분"], corr_after) or market_from_title(title_clean) or company_market_map.get(norm_company_name(rec["회사명"]), "")

    rec["최초 이사회결의일"] = _format_date(scan_label_value_preferring_correction(dfs, ["이사회결의일(결정일)", "이사회결의일", "최초이사회결의일"], corr_after))
    rec["납입일"] = _format_date(scan_label_value_preferring_correction(dfs, ["납입일", "납입기일", "청약일"], corr_after))
    rec["만기"] = _format_date(scan_label_value_preferring_correction(dfs, ["사채만기일", "만기일", "상환기일"], corr_after))
    
    rec["모집방식"] = scan_label_value_preferring_correction(dfs, ["사채발행방법", "모집방법", "발행방법"], corr_after)
    rec["발행상품"] = scan_label_value_preferring_correction(dfs, ["사채의 종류", "종류", "사채종류"], corr_after)

    def get_corr_num(labels, as_float=False):
        val = scan_label_value_preferring_correction(dfs, labels, corr_after)
        if as_float: return str(_to_float(val)) if _to_float(val) is not None else ""
        amt = _to_int(val)
        return f"{amt:,}" if amt and amt > 50 else "" # 항목번호(1,2,3) 오인 방지 (50 초과만 허용)

    rec["권면총액(원)"] = get_corr_num(["사채의권면(전자등록)총액(원)", "권면(전자등록)총액(원)", "사채의 권면총액", "사채의 총액", "발행총액"])
    rec["Coupon"] = get_corr_num(["표면이자율(%)", "표면이자율", "표면금리"], as_float=True)
    rec["YTM"] = get_corr_num(["만기이자율(%)", "만기이자율", "만기보장수익률"], as_float=True)
    rec["행사(전환)가액(원)"] = get_corr_num(["전환가액(원/주)", "교환가액(원/주)", "행사가액(원/주)", "전환가액", "교환가액", "행사가액"])
    rec["전환주식수"] = get_corr_num(["전환에 따라 발행할 주식수", "교환대상 주식수", "주식수"])
    rec["주식총수대비 비율"] = scan_label_value_preferring_correction(dfs, ["주식총수 대비 비율(%)", "총수 대비 비율"], corr_after)
    rec["Refixing Floor"] = get_corr_num(["최저 조정가액 (원)", "최저조정가액", "리픽싱하한"])

    s_date, e_date = extract_period_dates(dfs, corr_after, ["전환청구기간", "교환청구기간", "권리행사기간"])
    rec["전환청구 시작"] = s_date
    rec["전환청구 종료"] = e_date

    rec["Put Option"] = extract_option_details(html_raw, 'put')
    rec["Call Option"] = extract_option_details(html_raw, 'call')
    
    ratio, ytc = extract_call_ratio_and_ytc(rec["Call Option"])
    rec["Call 비율"] = ratio
    rec["YTC"] = ytc

    rec["투자자"] = extract_investors(dfs, corr_after)
    rec["자금용도"] = extract_fund_usage(dfs, corr_after)

    return rec

# ==========================================================
# Google Sheets 연동 및 Update (정정 덮어쓰기 로직)
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
    if not values or len(values) < 2: return {}, {}
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
        bond_type = row[col_type].strip() if col_type < len(row) else ""
        
        # [덮어쓰기 기준키] 회사명 | 결의일 | CB/EB/BW
        k = make_event_key(comp, first, bond_type)
        if k.strip("|") and k not in e_idx: e_idx[k] = (r, acpt)
    return r_idx, e_idx

# ==========================================================
# 메인 실행
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
        
        needs_fix = (not amt or not price)
        if needs_fix and acpt not in targets_dict:
            title = row[BOND_COLUMNS.index("보고서명")] if len(row) > BOND_COLUMNS.index("보고서명") else "[자동복구대상]"
            targets_dict[acpt] = Target(acpt_no=acpt, title=title, link="")
            print(f"[INFO] 누락 데이터 복구 진행: {title} ({acpt})")

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
