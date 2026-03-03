# ==========================================================
# #유상증자_코드V4.4 (Squash Bug & Self-Healing 완전판)
# - 확정발행금액 1,200경 단위 숫자 오류 완벽 수정 (문자열 병합 차단)
# - 과거 비정상 금액(자리수 초과) 강제 재파싱 조건 추가
# - 날짜 텍스트 오류 및 발행가 "6" 오류 방지 포함
# ==========================================================
import os
import re
import json
import time
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
KEYWORDS = [x.strip() for x in os.getenv("KEYWORDS", "유상증자").split(",") if x.strip()]

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
    "회사명", "상장시장", "최초 이사회결의일", "증자방식", "발행상품",
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
    """텍스트 내에서 독립된 숫자 덩어리들을 찾아 그중 최댓값을 반환 (병합 차단)"""
    if not s: return None
    nums = re.findall(r"\d[\d,]*", str(s))
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
    m = re.search(r"\[([^\]]+)\]", title or "")
    return m.group(1).strip() if m else ""

def market_from_title(title: str) -> str:
    if not title: return ""
    if "[코]" in title or "코스닥" in title: return "코스닥"
    if "[유]" in title or "유가증권" in title: return "유가증권"
    if "[넥]" in title or "[코넥]" in title or "코넥스" in title: return "코넥스"
    return ""

def market_from_html(html: str) -> str:
    if not html: return ""
    m = re.search(r'alt="(코스닥|유가증권|코넥스)"', html)
    if m: return m.group(1)
    m = re.search(r'(코스닥|유가증권|코넥스)시장\s*본부', html)
    if m: return m.group(1)
    if "코넥스" in html or "KONEX" in html: return "코넥스"
    if "코스닥" in html or "KOSDAQ" in html: return "코스닥"
    if "유가증권" in html or "KOSPI" in html: return "유가증권"
    return ""

def viewer_url(acpt_no: str, docno: str = "") -> str:
    return f"{BASE}/common/disclsviewer.do?method=searchInitInfo&acptNo={acpt_no}&docno={docno}"

def match_keyword(title: str) -> bool:
    return bool(title) and any(k in title for k in KEYWORDS)

def is_correction_title(title: str) -> bool:
    return bool(title) and title.strip().startswith("정정")

def make_event_key(company: str, first_board_date: str, method: str) -> str:
    return f"{_norm(company)}|{_norm_date(first_board_date)}|{_norm(method)}"

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

def extract_tables_from_html_robust(html: str) -> List[pd.DataFrame]:
    html = (html or "").replace("\x00", "")
    try:
        return [df.where(pd.notnull(df), "") for df in pd.read_html(html, header=None)]
    except Exception: pass

    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript"]): tag.decompose()
    results = []
    for tbl in soup.find_all("table"):
        try:
            one = pd.read_html(str(tbl), header=None)
            if one: results.append(one[0].where(pd.notnull(one[0]), ""))
            continue
        except Exception: pass
        rows = [[c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])] for tr in tbl.find_all("tr")]
        rows = [r for r in rows if r]
        if rows:
            max_len = max(len(r) for r in rows)
            results.append(pd.DataFrame([r + [""] * (max_len - len(r)) for r in rows]))
    if not results: raise ValueError("표 파싱 실패")
    return results

def scrape_one(context, acpt_no: str) -> Tuple[List[pd.DataFrame], str, str]:
    url = viewer_url(acpt_no)
    page = context.new_page()
    try:
        page.goto(url, wait_until="networkidle", timeout=60000)
        page.wait_for_timeout(1500)
        all_frames_html = " ".join([fr.content() for fr in page.frames])
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
        reason_col = -1

        for r in range(R):
            row_norm = [_norm(x) for x in arr[r].tolist()]
            if any("정정전" in x for x in row_norm) and any("정정후" in x for x in row_norm):
                header_r = r
                after_col = next((i for i, x in enumerate(row_norm) if "정정후" in x), None)
                reason_col = next((i for i, x in enumerate(row_norm) if "사유" in x), -1)
                item_col = next((i for i, x in enumerate(row_norm) if ("정정사항" in x or "항목" in x)), 0)
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
                if v and v.lower() != "nan" and _norm(v) not in ("정정후", "정정전", "항목", "변경사유", "정정사유"):
                    after_val = v

            if not after_val:
                for cc in [after_col - 1, after_col + 1]:
                    if 0 <= cc < C and cc != reason_col:
                        v = str(arr[rr][cc]).strip()
                        if v and v.lower() != "nan" and _norm(v) not in ("정정후", "정정전", "항목", "변경사유", "정정사유"):
                            after_val = v
                            break
            if after_val: out[_norm(item)] = after_val
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
                        
                        # 인덱스 번호 오류 방지
                        if v_norm.isdigit() and int(v_norm) <= 30: continue
                            
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

def find_row_best_int(dfs, must_contain) -> Optional[int]:
    keys = [_norm(x) for x in must_contain]
    best = None
    for df in dfs:
        arr = df.astype(str).values
        for r in range(arr.shape[0]):
            row = [str(x).strip() for x in arr[r].tolist()]
            if all(k in _norm("".join(row)) for k in keys):
                # 각 셀마다 _max_int_in_text를 적용하여 Squash(병합) 버그 방지
                row_max = 0
                for cell in row:
                    amt = _max_int_in_text(cell)
                    if amt and amt > 0: row_max = max(row_max, amt)
                if row_max > 0: best = max(best or 0, row_max)
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
                    if amt and amt >= 100: found_amts[std_name] = max(found_amts.get(std_name, 0), amt)

    for df in dfs:
        arr = df.astype(str).values
        for r in range(arr.shape[0]):
            row = [str(x).strip() for x in arr[r].tolist()]
            row_joined = _norm("".join(row))
            for k, std_name in keys_map.items():
                if _norm(k) in row_joined:
                    # 전체 행을 합치지 않고 개별 '셀' 단위로 금액 추출
                    row_max = 0
                    for cell in row:
                        amt = _max_int_in_text(cell)
                        if amt and amt >= 100:
                            row_max = max(row_max, amt)
                    if row_max > 0:
                        found_amts[std_name] = max(found_amts.get(std_name, 0), row_max)

    std_order = ["시설자금", "영업양수자금", "운영자금", "채무상환자금", "타법인 증권 취득자금", "기타자금"]
    uses = [name for name in std_order if found_amts.get(name, 0) > 0]
    total_sum = sum(found_amts.get(name, 0) for name in uses)
    return ", ".join(uses), total_sum

# ==========================================================
# 레코드 파싱 로직 
# ==========================================================
def parse_rights_issue_record(dfs, t: Target, corr_after, html_raw) -> dict:
    rec = {k: "" for k in RIGHTS_COLUMNS}
    rec["접수번호"] = t.acpt_no
    rec["링크"] = t.link

    title_clean = t.title.replace("[자동복구대상]", "").strip()
    rec["회사명"] = (
        scan_label_value_preferring_correction(dfs, ["회사명", "회사 명"], corr_after) 
        or company_from_title(title_clean)
        or title_clean
    )
    
    mkt = scan_label_value_preferring_correction(dfs, ["상장시장", "시장구분"], corr_after)
    if mkt and ("해당사항" in mkt or len(mkt) < 2 or mkt in ("-", ".")): mkt = ""
    
    rec["상장시장"] = (
        mkt 
        or market_from_title(title_clean) 
        or t.market 
        or market_from_html(html_raw)
    )

    def get_valid_date(labels):
        val = scan_label_value_preferring_correction(dfs, labels, corr_after)
        if val and not bool(re.search(r'\d', val)):
            val = scan_label_value(dfs, labels)
        return val

    rec["이사회결의일"] = get_valid_date(["이사회결의일(결정일)", "이사회결의일", "결정일"])
    rec["최초 이사회결의일"] = get_valid_date(["최초 이사회결의일", "최초이사회결의일"]) or rec["이사회결의일"]
    rec["증자방식"] = scan_label_value_preferring_correction(dfs, ["증자방식", "발행방법", "배정방식"], corr_after)

    issue_txt = scan_label_value_preferring_correction(dfs, ["신주의 종류와 수", "신주의종류와수", "발행예정주식수"], corr_after)
    prev_txt = scan_label_value_preferring_correction(dfs, ["증자전발행주식총수", "증자전 발행주식총수", "발행주식총수"], corr_after)

    issue_shares = _to_int(issue_txt) or _max_int_in_text(issue_txt) or find_row_best_int(dfs, ["신주의종류와수", "보통주식"]) or find_row_best_int(dfs, ["발행예정주식수"])
    prev_shares = _to_int(prev_txt) or _max_int_in_text(prev_txt) or find_row_best_int(dfs, ["증자전발행주식총수", "보통주식"]) or find_row_best_int(dfs, ["발행주식총수", "보통주식"])

    if issue_shares:
        rec["발행상품"] = "보통주식"
        rec["신규발행주식수"] = f"{issue_shares:,}"
    if prev_shares: rec["증자전 주식수"] = f"{prev_shares:,}"

    price_txt = scan_label_value_preferring_correction(dfs, ["신주 발행가액", "신주발행가액", "예정발행가액", "예정발행가", "확정발행가액"], corr_after)
    price = _to_int(price_txt) or find_row_best_int(dfs, ["신주발행가액", "보통주식"]) or find_row_best_int(dfs, ["예정발행가액", "보통주식"])
    if price: rec["확정발행가(원)"] = f"{price:,}"
    else: rec["확정발행가(원)"] = price_txt

    base_txt = scan_label_value_preferring_correction(dfs, ["기준주가"], corr_after)
    base_price = _to_int(base_txt) or find_row_best_int(dfs, ["기준주가", "보통주식"]) or find_row_best_int(dfs, ["기준주가"])
    if base_price: rec["기준주가"] = f"{base_price:,}"
    else: rec["기준주가"] = base_txt

    disc_txt = scan_label_value_preferring_correction(dfs, ["할인율", "할인율(%)", "기준주가에 대한 할인율 또는 할증율 (%)"], corr_after)
    disc = _to_float(disc_txt) or find_row_best_float(dfs, ["기준주가에대한할인율또는할증율"]) or find_row_best_float(dfs, ["할인율"])
    if disc is not None: rec["할인(할증률)"] = f"{disc}"
    else: rec["할인(할증률)"] = disc_txt

    rec["납입일"] = get_valid_date(["납입일", "납입기일"])
    rec["신주의 배당기산일"] = get_valid_date(["신주의 배당기산일", "배당기산일"])
    rec["신주의 상장 예정일"] = get_valid_date(["신주의 상장 예정일", "상장예정일", "신주 상장예정일", "상장 예정일"])

    uses_text, total_fund_amt = extract_fund_use_and_amount(dfs, corr_after)
    rec["자금용도"] = uses_text
    rec["투자자"] = scan_label_value_preferring_correction(dfs, ["제3자배정대상자", "제3자배정 대상자", "투자자"], corr_after)

    sh = _to_int(rec["신규발행주식수"])
    pr = _to_int(rec["확정발행가(원)"])
    if total_fund_amt > 0: rec["확정발행금액(억원)"] = f"{total_fund_amt / 100_000_000:,.2f}"
    elif sh and pr: rec["확정발행금액(억원)"] = f"{(sh * pr) / 100_000_000:,.2f}"

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
        
        # 금액 자리수가 비정상적으로 길면 무조건 재파싱하도록 추가!
        needs_fix = (
            not fund or "(원)" in fund or
            not price or (price.isdigit() and len(price) <= 2) or
            len(fund_amt.replace(",", "").replace(".", "")) >= 8 or 
            not market or
            not re.search(r'\d', pay_date) or "정정" in pay_date or "변경" in pay_date or "요청" in pay_date or
            not first_date
        )
        
        if needs_fix and acpt not in targets_dict:
            title = get_val(row, "회사명") or "[자동복구대상]"
            targets_dict[acpt] = Target(acpt_no=acpt, title=title, link="", market=market)
            print(f"[INFO] 빵꾸/오류 감지됨: {title} ({acpt}) -> 강제 재파싱 대기열 추가")

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
                rec = parse_rights_issue_record(dfs, t, corr_after, html_raw)

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
