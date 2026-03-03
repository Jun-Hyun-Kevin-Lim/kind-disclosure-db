# scraper.py
# 유상증자_코드V2 (PDF 케이스 반영 최종)

import os
import re
import json
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple, Set, Dict

import feedparser
import pandas as pd
from bs4 import BeautifulSoup
import gspread
from gspread.utils import rowcol_to_a1
from playwright.sync_api import sync_playwright


# ==========================================================
# Config (ENV)
# ==========================================================
BASE = "https://kind.krx.co.kr"
DEFAULT_RSS = (
    "http://kind.krx.co.kr:80/disclosure/rsstodaydistribute.do"
    "?method=searchRssTodayDistribute&mktTpCd=0&currentPageSize=100"
)

RSS_URL = os.getenv("RSS_URL", DEFAULT_RSS)
KEYWORDS = [x.strip() for x in os.getenv("KEYWORDS", "유상증자").split(",") if x.strip()]

HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
LIMIT = int(os.getenv("LIMIT", "0"))  # 0이면 RSS가 주는 만큼 전부
RUN_ONE_ACPTNO = os.getenv("RUN_ONE_ACPTNO", "").strip()

GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "").strip()
GOOGLE_CREDENTIALS_JSON = (
    os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
    or os.environ.get("GOOGLE_CREDS", "").strip()
)

RIGHTS_OUT_SHEET = os.getenv("RIGHTS_OUT_SHEET", "유상증자")
SEEN_SHEET_NAME = os.getenv("SEEN_SHEET_NAME", "seen")

# Debug
OUTDIR = Path(os.getenv("OUTDIR", "out"))
DEBUGDIR = OUTDIR / "debug"

RIGHTS_COLUMNS = [
    "회사명", "상장시장", "최초 이사회결의일", "증자방식", "발행상품", "신규발행주식수",
    "확정발행가(원)", "기준주가", "확정발행금액(억원)", "할인(할증률)",
    "증자전 주식수", "증자비율", "납입일", "신주의 배당기산일", "신주의 상장 예정일",
    "이사회결의일", "자금용도", "투자자", "링크", "접수번호"
]


@dataclass
class Target:
    acpt_no: str
    title: str
    link: str


# ==========================================================
# Utils
# ==========================================================
def _norm(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", "", s)
    s = s.replace(":", "")
    return s

def _norm_date(s: str) -> str:
    """날짜 매칭용: 숫자만 남김 (예: 2026-02-28 / 2026년2월28일 -> 20260228)"""
    return re.sub(r"[^\d]", "", (s or "").strip())

def _is_date_like(s: str) -> bool:
    d = _norm_date(s)
    return len(d) == 8 and d.startswith("20")

def _to_int(s: str) -> Optional[int]:
    if s is None:
        return None
    t = str(s).replace(",", "")
    t = re.sub(r"[^\d\-]", "", t)
    if t in ("", "-"):
        return None
    try:
        return int(t)
    except:
        return None

def _to_float(s: str) -> Optional[float]:
    if s is None:
        return None
    t = str(s).replace(",", "")
    t = re.sub(r"[^\d\.\-]", "", t)
    if t in ("", "-", "."):
        return None
    try:
        return float(t)
    except:
        return None

def _max_int_in_text(s: str) -> Optional[int]:
    """문장 내 숫자(, 포함) 중 최대값 추출"""
    if not s:
        return None
    nums = re.findall(r"\d[\d,]*", str(s))
    vals = []
    for x in nums:
        v = _to_int(x)
        if v is not None:
            vals.append(v)
    return max(vals) if vals else None

def extract_acpt_no(text: str) -> Optional[str]:
    m = re.search(r"acptNo=(\d{14})", text or "")
    return m.group(1) if m else None

def company_from_title(title: str) -> str:
    m = re.search(r"\[([^\]]+)\]", title or "")
    return m.group(1).strip() if m else ""

def normalize_market(s: str) -> str:
    t = (s or "").strip()
    low = t.lower()
    if "코스닥" in t or "kosdaq" in low:
        return "코스닥"
    if "코넥스" in t or "konex" in low:
        return "코넥스"
    if "유가" in t or "유가증권" in t or "코스피" in t or "kospi" in low:
        return "유가증권"
    return t

def market_from_title(title: str) -> str:
    if not title:
        return ""
    if "코스닥" in title or "[코]" in title:
        return "코스닥"
    if "코넥스" in title or "[넥]" in title or "[코넥]" in title:
        return "코넥스"
    if "유가" in title or "유가증권" in title or "코스피" in title or "[유]" in title:
        return "유가증권"
    return ""

def viewer_url(acpt_no: str, docno: str = "") -> str:
    return f"{BASE}/common/disclsviewer.do?method=searchInitInfo&acptNo={acpt_no}&docno={docno}"

def match_keyword(title: str) -> bool:
    return bool(title) and any(k in title for k in KEYWORDS)

def is_correction_title(title: str) -> bool:
    """요구사항: 제목 '맨 앞'이 정정인 경우만"""
    return bool(title) and title.strip().startswith("정정")

def make_event_key(company: str, first_board_date: str, method: str) -> str:
    """정정 공시가 원공시 행을 찾아 덮어쓰기 위한 이벤트 키"""
    return f"{_norm(company)}|{_norm_date(first_board_date)}|{_norm(method)}"


# ==========================================================
# RSS → targets (single page only)
# ==========================================================
def parse_rss_targets() -> List[Target]:
    feed = feedparser.parse(RSS_URL)
    items = feed.entries or []
    targets: List[Target] = []

    for it in items:
        title = getattr(it, "title", "") or ""
        link = getattr(it, "link", "") or ""
        guid = getattr(it, "guid", "") or ""

        if not match_keyword(title):
            continue

        acpt_no = extract_acpt_no(link) or extract_acpt_no(guid)
        if not acpt_no:
            continue

        targets.append(Target(acpt_no=acpt_no, title=title, link=link))

    uniq: Dict[str, Target] = {}
    for t in targets:
        uniq.setdefault(t.acpt_no, t)
    return list(uniq.values())


# ==========================================================
# Playwright: popup html → dfs
# ==========================================================
def is_block_page(html: str) -> bool:
    if not html:
        return True
    lower = html.lower()
    suspects = ["비정상", "접근", "제한", "차단", "오류", "error", "권한", "잠시 후", "관리자"]
    return any(s in lower for s in suspects) and ("<table" not in lower)

def frame_score(html: str) -> int:
    if not html:
        return -1
    lower = html.lower()
    tcnt = lower.count("<table")
    if tcnt == 0:
        return -1
    bonus_words = ["기준주가", "납입", "이사회", "할인", "할증", "발행", "청약", "증자방식", "자금조달", "정정사항", "신주발행가액"]
    bonus = sum(1 for w in bonus_words if w in lower)
    length_bonus = min(len(lower) // 2000, 50)
    return tcnt * 100 + bonus * 30 + length_bonus

def pick_best_frame_html(page) -> str:
    best_html, best_score = "", -1
    for fr in page.frames:
        try:
            html = fr.content()
            sc = frame_score(html)
            if sc > best_score:
                best_score = sc
                best_html = html
        except Exception:
            continue
    return best_html

def extract_tables_from_html_robust(html: str) -> List[pd.DataFrame]:
    html = (html or "").replace("\x00", "")

    try:
        dfs = pd.read_html(html, header=None)
        return [df.where(pd.notnull(df), "") for df in dfs]
    except Exception:
        pass

    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    tables = soup.find_all("table")
    results: List[pd.DataFrame] = []

    for tbl in tables:
        try:
            one = pd.read_html(str(tbl), header=None)
            if one:
                results.append(one[0].where(pd.notnull(one[0]), ""))
                continue
        except Exception:
            pass

        rows = []
        for tr in tbl.find_all("tr"):
            cells = tr.find_all(["th", "td"])
            row = [c.get_text(" ", strip=True) for c in cells]
            if row:
                rows.append(row)

        if rows:
            max_len = max(len(r) for r in rows)
            normed = [r + [""] * (max_len - len(r)) for r in rows]
            results.append(pd.DataFrame(normed))

    if not results:
        raise ValueError("No tables parsed (robust).")

    return results

def save_debug(acpt_no: str, page, html: str, reason: str):
    try:
        OUTDIR.mkdir(parents=True, exist_ok=True)
        DEBUGDIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        (DEBUGDIR / f"{acpt_no}_{ts}_{reason}.html").write_text(html or "", encoding="utf-8")
        try:
            page.screenshot(path=str(DEBUGDIR / f"{acpt_no}_{ts}_{reason}.png"), full_page=True)
        except Exception:
            pass
    except Exception:
        pass

def scrape_one(context, t: Target) -> Tuple[List[pd.DataFrame], str]:
    url = viewer_url(t.acpt_no)
    page = context.new_page()
    try:
        page.goto(url, wait_until="networkidle", timeout=60000)
        page.wait_for_timeout(1500)

        html = pick_best_frame_html(page) or ""
        if is_block_page(html) or html.lower().count("<table") == 0:
            save_debug(t.acpt_no, page, page.content(), "block_or_table0")
            raise RuntimeError("차단/오류/프레임 문제로 table을 못 찾음")

        dfs = extract_tables_from_html_robust(html)
        return dfs, url
    finally:
        try:
            page.close()
        except Exception:
            pass


# ==========================================================
# Google Sheets
# ==========================================================
def gs_open():
    if not GOOGLE_SHEET_ID or not GOOGLE_CREDENTIALS_JSON:
        raise RuntimeError("GOOGLE_SHEET_ID / GOOGLE_CREDS 가 비어있습니다. Secrets 확인")

    creds = json.loads(GOOGLE_CREDENTIALS_JSON)
    gc = gspread.service_account_from_dict(creds)
    sh = gc.open_by_key(GOOGLE_SHEET_ID)

    # seen
    try:
        seen_ws = sh.worksheet(SEEN_SHEET_NAME)
    except gspread.WorksheetNotFound:
        seen_ws = sh.add_worksheet(title=SEEN_SHEET_NAME, rows=2000, cols=2)
        seen_ws.update("A1:B1", [["acptNo", "ts"]])

    # rights structured
    try:
        rights_ws = sh.worksheet(RIGHTS_OUT_SHEET)
    except gspread.WorksheetNotFound:
        rights_ws = sh.add_worksheet(title=RIGHTS_OUT_SHEET, rows=2000, cols=len(RIGHTS_COLUMNS) + 5)

    return sh, rights_ws, seen_ws

def ensure_headers(ws, headers: List[str]):
    cur = ws.row_values(1)
    if cur != headers:
        end = rowcol_to_a1(1, len(headers))
        ws.update(f"A1:{end}", [headers])

def load_sheet_values(ws, headers: List[str]) -> List[List[str]]:
    ensure_headers(ws, headers)
    vals = ws.get_all_values()
    if not vals:
        end = rowcol_to_a1(1, len(headers))
        ws.update(f"A1:{end}", [headers])
        vals = ws.get_all_values()
    return vals

def build_acpt_index_from_values(values: List[List[str]], headers: List[str], key_field: str) -> Dict[str, int]:
    key_col = headers.index(key_field)
    idx: Dict[str, int] = {}
    for r, row in enumerate(values[1:], start=2):
        key = (row[key_col] if key_col < len(row) else "").strip()
        if key.isdigit():
            idx[key] = r
    return idx

def build_event_index_from_values(values: List[List[str]], headers: List[str]) -> Dict[str, Tuple[int, str]]:
    col_company = headers.index("회사명")
    col_first = headers.index("최초 이사회결의일")
    col_method = headers.index("증자방식")
    col_acpt = headers.index("접수번호")

    idx: Dict[str, Tuple[int, str]] = {}
    for r, row in enumerate(values[1:], start=2):
        company = (row[col_company] if col_company < len(row) else "").strip()
        first = (row[col_first] if col_first < len(row) else "").strip()
        method = (row[col_method] if col_method < len(row) else "").strip()
        acpt = (row[col_acpt] if col_acpt < len(row) else "").strip()

        k = make_event_key(company, first, method)
        if k.strip("|") and k not in idx:
            idx[k] = (r, acpt)
    return idx

def update_row(ws, headers: List[str], row: int, record: dict):
    ensure_headers(ws, headers)
    row_vals = [record.get(h, "") for h in headers]
    end = rowcol_to_a1(row, len(headers))
    ws.update(f"A{row}:{end}", [row_vals])

def upsert(ws, headers: List[str], index: Dict[str, int], record: dict, key_field: str, last_row_ref: Optional[List[int]] = None) -> Tuple[str, int]:
    ensure_headers(ws, headers)
    key = str(record.get(key_field, "")).strip()
    row_vals = [record.get(h, "") for h in headers]

    if key in index:
        r = index[key]
        end = rowcol_to_a1(r, len(headers))
        ws.update(f"A{r}:{end}", [row_vals])
        return "update", r

    ws.append_row(row_vals, value_input_option="RAW")
    if last_row_ref is not None:
        last_row_ref[0] += 1
        r = last_row_ref[0]
    else:
        r = len(ws.get_all_values())
    index[key] = r
    return "append", r

# seen: 스킵용이 아니라 "마지막 처리시각 기록용"
def build_seen_index(seen_ws) -> Dict[str, int]:
    vals = seen_ws.get_all_values()
    if not vals:
        seen_ws.update("A1:B1", [["acptNo", "ts"]])
        vals = seen_ws.get_all_values()
    if vals and vals[0] != ["acptNo", "ts"]:
        seen_ws.update("A1:B1", [["acptNo", "ts"]])
        vals = seen_ws.get_all_values()

    idx: Dict[str, int] = {}
    for r, row in enumerate(vals[1:], start=2):
        acpt = (row[0] if len(row) > 0 else "").strip()
        if acpt.isdigit():
            idx[acpt] = r
    return idx

def upsert_seen(seen_ws, seen_index: Dict[str, int], acpt_no: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if acpt_no in seen_index:
        r = seen_index[acpt_no]
        seen_ws.update(f"B{r}", [[ts]])
    else:
        seen_ws.append_row([acpt_no, ts], value_input_option="RAW")
        seen_index[acpt_no] = len(seen_ws.get_all_values())


# ==========================================================
# Parser helpers
# ==========================================================
def scan_label_value(dfs: List[pd.DataFrame], label_candidates: List[str]) -> str:
    cand = {_norm(x) for x in label_candidates}

    for df in dfs:
        arr = df.astype(str).values
        R, C = arr.shape

        for r in range(R):
            for c in range(C):
                cell = _norm(arr[r][c])
                if cell in cand:
                    checks = []
                    for rr, cc in [(r, c+1), (r, c+2), (r+1, c), (r+1, c+1)]:
                        if 0 <= rr < R and 0 <= cc < C:
                            v = str(arr[rr][cc]).strip()
                            if v and v.lower() != "nan":
                                checks.append(v)

                    row_vals = [str(x).strip() for x in arr[r].tolist()
                                if str(x).strip() and str(x).strip().lower() != "nan"]
                    row_vals = [x for x in row_vals if _norm(x) not in cand]

                    for v in checks + row_vals:
                        # "4." 같은 번호 제거 + "6" 같은 단일 번호도 제거
                        if re.fullmatch(r"\d+\.", v) or re.fullmatch(r"\d+", v):
                            continue
                        return v
    return ""

def extract_correction_after_map(dfs: List[pd.DataFrame]) -> Dict[str, str]:
    """
    '정정사항' 표에서 (항목 -> 정정후) 값 추출
    """
    out: Dict[str, str] = {}

    for df in dfs:
        try:
            arr = df.astype(str).values
        except Exception:
            continue

        R, C = arr.shape
        header_r = None
        after_col = None
        item_col = None

        for r in range(R):
            row_norm = [_norm(x) for x in arr[r].tolist()]
            has_before = any("정정전" in x for x in row_norm)
            has_after = any("정정후" in x for x in row_norm)
            if has_before and has_after:
                header_r = r
                after_col = next((i for i, x in enumerate(row_norm) if "정정후" in x), None)
                item_col = next(
                    (i for i, x in enumerate(row_norm) if ("정정사항" in x or "정정항목" in x or x == "항목")),
                    0
                )
                break

        if header_r is None or after_col is None:
            continue

        last_item = ""
        for rr in range(header_r + 1, R):
            item = str(arr[rr][item_col]).strip() if item_col is not None and item_col < C else ""
            if item and item.lower() != "nan":
                last_item = item
            else:
                item = last_item

            if not item:
                continue

            after_val = ""
            for cc in [after_col, after_col + 1, after_col - 1]:
                if 0 <= cc < C:
                    v = str(arr[rr][cc]).strip()
                    if v and v.lower() != "nan":
                        vn = _norm(v)
                        if vn not in ("정정후", "정정전", "정정사항", "정정항목", "항목"):
                            after_val = v
                            break

            if after_val:
                out[_norm(item)] = after_val

    return out

def scan_label_value_preferring_correction(
    dfs: List[pd.DataFrame],
    label_candidates: List[str],
    corr_after: Optional[Dict[str, str]] = None
) -> str:
    if corr_after:
        cand_norm = [_norm(x) for x in label_candidates]

        for c in cand_norm:
            v = corr_after.get(c, "")
            if str(v).strip():
                return str(v).strip()

        for k, v in corr_after.items():
            if not str(v).strip():
                continue
            if any(c in k for c in cand_norm):
                return str(v).strip()

    return scan_label_value(dfs, label_candidates)

def scan_label_date(
    dfs: List[pd.DataFrame],
    label_candidates: List[str],
    corr_after: Optional[Dict[str, str]] = None
) -> str:
    # 정정후 우선 (단, 날짜처럼 생긴 값만)
    v = scan_label_value_preferring_correction(dfs, label_candidates, corr_after=corr_after)
    if _is_date_like(v):
        return v

    cand = {_norm(x) for x in label_candidates}

    for df in dfs:
        arr = df.astype(str).values
        R, C = arr.shape
        for r in range(R):
            for c in range(C):
                if _norm(arr[r][c]) in cand:
                    checks = []
                    for rr, cc in [(r, c+1), (r, c+2), (r+1, c), (r+1, c+1)]:
                        if 0 <= rr < R and 0 <= cc < C:
                            checks.append(str(arr[rr][cc]).strip())

                    row_vals = [str(x).strip() for x in arr[r].tolist()]
                    for vv in checks + row_vals:
                        if _is_date_like(vv):
                            return vv
    return ""


def find_row_best_int(dfs: List[pd.DataFrame], must_contain: List[str]) -> Optional[int]:
    keys = [_norm(x) for x in must_contain]
    best = None

    for df in dfs:
        arr = df.astype(str).values
        for r in range(arr.shape[0]):
            row = [str(x).strip() for x in arr[r].tolist()]
            joined = _norm("".join(row))
            if all(k in joined for k in keys):
                nums = [_to_int(x) for x in row]
                nums = [x for x in nums if x is not None and x > 0]
                if nums:
                    m = max(nums)
                    if best is None or m > best:
                        best = m
    return best

def find_row_best_float(dfs: List[pd.DataFrame], must_contain: List[str]) -> Optional[float]:
    keys = [_norm(x) for x in must_contain]
    for df in dfs:
        arr = df.astype(str).values
        for r in range(arr.shape[0]):
            row = [str(x).strip() for x in arr[r].tolist()]
            joined = _norm("".join(row))
            if all(k in joined for k in keys):
                vals = [_to_float(x) for x in row]
                vals = [x for x in vals if x is not None]
                if vals:
                    return max(vals, key=lambda z: abs(z))
    return None


# ==========================================================
# 자금조달의 목적 기반
# ==========================================================
FUND_SPECS = [
    ("시설자금", ["시설자금"]),
    ("영업양수자금", ["영업양수자금", "영업양수 자금"]),
    ("운영자금", ["운영자금"]),
    ("채무상환자금", ["채무상환자금", "채무 상환 자금"]),
    ("타법인증권취득자금", ["타법인증권취득자금", "타법인 증권 취득자금", "타법인증권 취득자금"]),
    ("기타자금", ["기타자금"]),
]

def _best_amount_from_row(row: List[str]) -> Optional[int]:
    nums = [_to_int(x) for x in row]
    nums = [x for x in nums if x is not None and x > 0]
    if not nums:
        return None
    for thr in (1_000_000, 10_000, 1):
        cand = [x for x in nums if x >= thr]
        if cand:
            return max(cand)
    return None

def extract_fund_amounts(
    dfs: List[pd.DataFrame],
    corr_after: Optional[Dict[str, str]] = None
) -> Dict[str, int]:
    out: Dict[str, int] = {}

    # 정정후 우선
    if corr_after:
        for disp, cands in FUND_SPECS:
            nk_list = [_norm(x) for x in cands]
            found = None
            for ck, cv in corr_after.items():
                if any(nk in ck for nk in nk_list):
                    found = _max_int_in_text(cv)
                    break
            if found and found > 0:
                out[disp] = found

    # 본문 탐색
    for disp, cands in FUND_SPECS:
        if disp in out:
            continue
        best = None
        nk_list = [_norm(x) for x in cands]
        for df in dfs:
            arr = df.astype(str).values
            for r in range(arr.shape[0]):
                row = [str(x).strip() for x in arr[r].tolist()]
                joined = _norm("".join(row))
                if any(nk in joined for nk in nk_list):
                    amt = _best_amount_from_row(row)
                    if amt is not None:
                        if best is None or amt > best:
                            best = amt
        if best and best > 0:
            out[disp] = best

    return out

def fund_use_from_amounts(amounts: Dict[str, int]) -> str:
    uses = [disp for disp, _ in FUND_SPECS if amounts.get(disp)]
    return "; ".join(uses)

def sum_amounts_to_eok(amounts: Dict[str, int]) -> str:
    total = sum(v for v in amounts.values() if isinstance(v, int) and v > 0)
    if total <= 0:
        return ""
    return f"{total / 100_000_000:,.2f}"


# ==========================================================
# 신주발행가액 표 전용: 확정 없으면 예정 사용 (PDF 케이스 해결)
# ==========================================================
def extract_issue_price_won(
    dfs: List[pd.DataFrame],
    corr_after: Optional[Dict[str, str]] = None
) -> Optional[int]:
    """
    6. 신주 발행가액 표에서 가격을 뽑는다.
    우선순위: (1) 확정발행가(보통주식) 숫자 → (2) 예정발행가(보통주식) 숫자
    """
    # 정정후 표에 가격이 직접 있을 때
    if corr_after:
        for key_hint in ["확정발행가", "예정발행가", "신주발행가액", "신주발행가액(원)", "신주 발행가액"]:
            for k, v in corr_after.items():
                if key_hint in k:
                    n = _max_int_in_text(v)
                    if n and n >= 100:  # '6' 같은 번호 방지
                        return n

    best_confirmed = None
    best_planned = None

    for df in dfs:
        arr = df.astype(str).values
        for r in range(arr.shape[0]):
            row = [str(x).strip() for x in arr[r].tolist()]
            joined = _norm("".join(row))

            if ("확정발행가" in joined) and ("보통주식" in joined):
                n = _best_amount_from_row(row)
                if n and n >= 100:
                    best_confirmed = max(best_confirmed or 0, n)

            if ("예정발행가" in joined) and ("보통주식" in joined):
                n = _best_amount_from_row(row)
                if n and n >= 100:
                    best_planned = max(best_planned or 0, n)

    return best_confirmed or best_planned


def extract_investors(dfs: List[pd.DataFrame], corr_after: Optional[Dict[str, str]] = None) -> str:
    v = scan_label_value_preferring_correction(
        dfs,
        ["제3자배정대상자", "제3자배정 대상자", "대표주관사/투자자", "투자자"],
        corr_after=corr_after
    )
    return v or ""


# ==========================================================
# Rights issue parser (record)
# ==========================================================
def parse_rights_issue_record(
    dfs: List[pd.DataFrame],
    title: str,
    acpt_no: str,
    link: str,
    corr_after: Optional[Dict[str, str]] = None
) -> dict:
    rec = {k: "" for k in RIGHTS_COLUMNS}
    rec["접수번호"] = acpt_no
    rec["링크"] = link

    # 회사명/시장
    rec["회사명"] = scan_label_value_preferring_correction(dfs, ["회 사 명", "회사명", "회사 명"], corr_after) or company_from_title(title)
    mkt_raw = scan_label_value_preferring_correction(dfs, ["상장시장", "시장구분", "시장 구분"], corr_after) or market_from_title(title)
    rec["상장시장"] = normalize_market(mkt_raw)

    # 결의일
    rec["이사회결의일"] = scan_label_date(
        dfs, ["15. 이사회결의일(결정일)", "이사회결의일(결정일)", "이사회결의일", "결정일"], corr_after
    )
    rec["최초 이사회결의일"] = scan_label_date(dfs, ["최초 이사회결의일", "최초이사회결의일"], corr_after) or rec["이사회결의일"]

    # 증자방식
    rec["증자방식"] = scan_label_value_preferring_correction(dfs, ["5. 증자방식", "증자방식", "발행방법", "배정방식"], corr_after)

    # 주식수
    issue_txt = scan_label_value_preferring_correction(
        dfs, ["1. 신주의 종류와 수", "신주의 종류와 수", "신주의종류와수", "신주의 종류 및 수", "신주의종류및수"], corr_after
    )
    prev_txt = scan_label_value_preferring_correction(
        dfs, ["증자전발행주식총수", "증자전 발행주식총수", "증자전발행주식총수(보통주식)", "발행주식총수", "발행주식 총수"], corr_after
    )

    issue_shares = _to_int(issue_txt) or _max_int_in_text(issue_txt) or \
        find_row_best_int(dfs, ["신주의종류와수", "보통주식"]) or find_row_best_int(dfs, ["보통주식", "(주)"])
    prev_shares = _to_int(prev_txt) or _max_int_in_text(prev_txt) or \
        find_row_best_int(dfs, ["증자전발행주식총수", "보통주식"]) or find_row_best_int(dfs, ["발행주식총수", "보통주식"])

    if issue_shares:
        rec["발행상품"] = "보통주식"
        rec["신규발행주식수"] = f"{issue_shares:,}"
    if prev_shares:
        rec["증자전 주식수"] = f"{prev_shares:,}"

    # ✅ 확정발행가(원): 확정 없으면 예정발행가 사용 (PDF 케이스 해결)
    price = extract_issue_price_won(dfs, corr_after=corr_after)
    rec["확정발행가(원)"] = f"{price:,}" if price else ""

    # 기준주가/할인율
    base_txt = scan_label_value_preferring_correction(dfs, ["7. 기준주가", "기준주가"], corr_after)
    base_price = _to_int(base_txt) or find_row_best_int(dfs, ["기준주가", "보통주식"]) or find_row_best_int(dfs, ["기준주가"])
    rec["기준주가"] = f"{base_price:,}" if base_price else (base_txt or "")

    disc_txt = scan_label_value_preferring_correction(
        dfs,
        ["7-2. 기준주가에 대한 할인율 또는 할증율 (%)", "기준주가에 대한 할인율 또는 할증율 (%)", "할인율또는할증율", "기준주가에대한할인율또는할증율"],
        corr_after
    )
    disc = _to_float(disc_txt)
    if disc is None:
        disc = find_row_best_float(dfs, ["기준주가에대한할인율또는할증율"]) or find_row_best_float(dfs, ["할인율또는할증율"])
    rec["할인(할증률)"] = f"{disc}" if disc is not None else (disc_txt or "")

    # 일정(날짜만)
    rec["납입일"] = scan_label_date(dfs, ["9. 납입일", "납입일"], corr_after)
    rec["신주의 배당기산일"] = scan_label_date(dfs, ["10. 신주의 배당기산일", "신주의 배당기산일", "배당기산일"], corr_after)
    rec["신주의 상장 예정일"] = scan_label_date(dfs, ["12. 신주의 상장 예정일", "신주의 상장 예정일", "상장예정일"], corr_after)

    # ✅ 자금조달 목적(6개) 기반: 자금용도 + 확정발행금액(억원)
    fund_amounts = extract_fund_amounts(dfs, corr_after=corr_after)
    rec["자금용도"] = fund_use_from_amounts(fund_amounts)

    sum_eok = sum_amounts_to_eok(fund_amounts)
    if sum_eok:
        rec["확정발행금액(억원)"] = sum_eok
    else:
        # fallback: 합산 실패 시에만 주식수*발행가
        sh = _to_int(rec["신규발행주식수"])
        pr = _to_int(rec["확정발행가(원)"])
        if sh and pr:
            rec["확정발행금액(억원)"] = f"{(sh * pr) / 100_000_000:,.2f}"

    # 투자자
    rec["투자자"] = extract_investors(dfs, corr_after=corr_after)

    # 증자비율
    sh = _to_int(rec["신규발행주식수"])
    pv = _to_int(rec["증자전 주식수"])
    if sh and pv and pv > 0:
        rec["증자비율"] = f"{sh / pv * 100:.2f}%"

    return rec


# ==========================================================
# Main
# ==========================================================
def run():
    _, rights_ws, seen_ws = gs_open()

    # 시트 값 로드 + 인덱스 구성
    values = load_sheet_values(rights_ws, RIGHTS_COLUMNS)
    last_row_ref = [len(values)]

    rights_index = build_acpt_index_from_values(values, RIGHTS_COLUMNS, key_field="접수번호")
    event_index = build_event_index_from_values(values, RIGHTS_COLUMNS)

    # seen은 스킵용이 아니라 기록용
    seen_index = build_seen_index(seen_ws)

    # 대상 선정
    if RUN_ONE_ACPTNO:
        targets = [Target(acpt_no=RUN_ONE_ACPTNO, title=f"[MANUAL]{RUN_ONE_ACPTNO}", link="")]
    else:
        targets = parse_rss_targets()
        targets = targets[:LIMIT] if LIMIT > 0 else targets

    if not targets:
        print("[INFO] 처리할 대상이 없습니다.")
        return

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=HEADLESS,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
        )
        context = browser.new_context(
            locale="ko-KR",
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
            viewport={"width": 1400, "height": 900},
        )

        ok = 0
        for t in targets:
            try:
                dfs, src = scrape_one(context, t)

                if "유상증자" in (t.title or ""):
                    corr_after = None
                    if is_correction_title(t.title):
                        corr_after = extract_correction_after_map(dfs)

                    rec = parse_rights_issue_record(dfs, t.title, t.acpt_no, src, corr_after=corr_after)

                    # 정정 공시: event_key로 기존행 update 우선
                    if is_correction_title(t.title):
                        evk = make_event_key(
                            rec.get("회사명", ""),
                            rec.get("최초 이사회결의일", "") or rec.get("이사회결의일", ""),
                            rec.get("증자방식", "")
                        )

                        target_row = None
                        old_acpt = ""

                        if evk in event_index:
                            target_row, old_acpt = event_index[evk]
                        elif rec["접수번호"] in rights_index:
                            target_row = rights_index[rec["접수번호"]]

                        if target_row:
                            update_row(rights_ws, RIGHTS_COLUMNS, target_row, rec)

                            if old_acpt and old_acpt in rights_index and old_acpt != rec["접수번호"]:
                                del rights_index[old_acpt]
                            rights_index[rec["접수번호"]] = target_row
                            event_index[evk] = (target_row, rec["접수번호"])

                            print(f"[OK] {t.acpt_no} correction=Y mode=UPDATE row={target_row}")
                        else:
                            mode, row = upsert(
                                rights_ws, RIGHTS_COLUMNS, rights_index, rec,
                                key_field="접수번호", last_row_ref=last_row_ref
                            )
                            event_index[evk] = (row, rec["접수번호"])
                            print(f"[OK] {t.acpt_no} correction=Y mode={mode.upper()} row={row}")

                    else:
                        # 일반 공시도 항상 upsert (있으면 update)
                        mode, row = upsert(
                            rights_ws, RIGHTS_COLUMNS, rights_index, rec,
                            key_field="접수번호", last_row_ref=last_row_ref
                        )

                        evk = make_event_key(
                            rec.get("회사명", ""),
                            rec.get("최초 이사회결의일", "") or rec.get("이사회결의일", ""),
                            rec.get("증자방식", "")
                        )
                        event_index[evk] = (row, rec["접수번호"])

                        print(f"[OK] {t.acpt_no} correction=N mode={mode.upper()} row={row}")

                # 처리 시각 기록
                upsert_seen(seen_ws, seen_index, t.acpt_no)
                ok += 1

            except Exception as e:
                print(f"[FAIL] {t.acpt_no} {t.title} :: {e}")

            time.sleep(0.4)

        context.close()
        browser.close()

    print(f"[DONE] ok={ok}")
