# ==========================================================
# #유상증자_코드V2
# - RSS에서 "유상증자" 공시 수집 → KIND 뷰어에서 표 파싱 → Google Sheets에 구조화 저장
# - 정정 공시(제목 맨 앞 "정정")는 "정정사항" 표의 '정정후' 값을 우선 적용
# - 정정 공시는 append가 아니라 기존 행을 찾아 update(덮어쓰기)
# - seen은 더 이상 "스킵"에 쓰지 않음(지속 업데이트). 처리 로그용으로 ts만 갱신
# ==========================================================

import os
import re
import json
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple, Set, Dict, Any

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
    os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
    or os.environ.get("GOOGLE_CREDS", "").strip()
)

RIGHTS_OUT_SHEET = os.getenv("RIGHTS_OUT_SHEET", "유상증자")
SEEN_SHEET_NAME = os.getenv("SEEN_SHEET_NAME", "seen")

# 디버그 저장 (원하면 유지: 실패 시 out/debug에 저장)
OUTDIR = Path(os.getenv("OUTDIR", "out"))
DEBUGDIR = OUTDIR / "debug"

# rights(유상증자) 구조화 컬럼
RIGHTS_COLUMNS = [
    "회사명", "상장시장", "최초 이사회결의일", "증자방식", "발행상품", "신규발행주식수",
    "확정발행가(원)", "기준주가", "확정발행금액(억원)", "할인(할증률)",
    "증자전 주식수", "증자비율", "납입일", "신주의 배당기산일", "신주의 상장 예정일",
    "이사회결의일", "자금용도", "투자자", "링크", "접수번호"
]

# seen(로그) 컬럼: 스킵에 쓰지 않고, 같은 acptNo면 ts만 갱신
SEEN_HEADERS = ["acptNo", "ts"]


@dataclass
class Target:
    acpt_no: str
    title: str
    link: str


# ==========================================================
# 유틸
# ==========================================================
def _norm(s: str) -> str:
    """공백/콜론 제거 등 라벨 매칭용 정규화"""
    s = (s or "").strip()
    s = re.sub(r"\s+", "", s)
    s = s.replace(":", "")
    return s

def _norm_date(s: str) -> str:
    """날짜 매칭용: 숫자만 남김 (예: 2026-02-28 / 2026년2월28일 -> 20260228)"""
    return re.sub(r"[^\d]", "", (s or "").strip())

def _to_int(s: str) -> Optional[int]:
    """문자열을 int로 파싱(콤마/문자 제거)"""
    if s is None:
        return None
    t = str(s).replace(",", "")
    t = re.sub(r"[^\d\-]", "", t)
    if t in ("", "-"):
        return None
    try:
        return int(t)
    except Exception:
        return None

def _to_float(s: str) -> Optional[float]:
    """문자열을 float로 파싱"""
    if s is None:
        return None
    t = str(s).replace(",", "")
    t = re.sub(r"[^\d\.\-]", "", t)
    if t in ("", "-", "."):
        return None
    try:
        return float(t)
    except Exception:
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
    """제목에 [회사명] 형태가 있으면 추출"""
    m = re.search(r"\[([^\]]+)\]", title or "")
    return m.group(1).strip() if m else ""

def market_from_title(title: str) -> str:
    """제목 코드로 시장 추정"""
    if not title:
        return ""
    if "[코]" in title:
        return "코스닥"
    if "[유]" in title:
        return "유가증권"
    if "[넥]" in title or "[코넥]" in title:
        return "코넥스"
    return ""

def viewer_url(acpt_no: str, docno: str = "") -> str:
    return f"{BASE}/common/disclsviewer.do?method=searchInitInfo&acptNo={acpt_no}&docno={docno}"

def match_keyword(title: str) -> bool:
    return bool(title) and any(k in title for k in KEYWORDS)

def is_correction_title(title: str) -> bool:
    """요구사항: 제목 '맨 앞'이 정정인 경우만 정정 공시로 판단"""
    return bool(title) and title.strip().startswith("정정")

def make_event_key(company: str, first_board_date: str, method: str) -> str:
    """정정 공시가 원공시 행을 찾아 덮어쓰기 위한 이벤트 키"""
    return f"{_norm(company)}|{_norm_date(first_board_date)}|{_norm(method)}"


# ==========================================================
# RSS → 대상(Target) 추출
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

    # acptNo 기준 중복 제거
    uniq: Dict[str, Target] = {}
    for t in targets:
        if t.acpt_no not in uniq:
            uniq[t.acpt_no] = t
    return list(uniq.values())


# ==========================================================
# Playwright: 팝업(html) → 표(DataFrame)들 추출
# ==========================================================
def is_block_page(html: str) -> bool:
    """차단/오류 페이지인지 대충 판별"""
    if not html:
        return True
    lower = html.lower()
    suspects = ["비정상", "접근", "제한", "차단", "오류", "error", "권한", "잠시 후", "관리자"]
    return any(s in lower for s in suspects) and ("<table" not in lower)

def frame_score(html: str) -> int:
    """프레임 중 '제일 유력한' 본문을 고르기 위한 간단 스코어"""
    if not html:
        return -1
    lower = html.lower()
    tcnt = lower.count("<table")
    if tcnt == 0:
        return -1
    bonus_words = ["기준주가", "납입", "이사회", "할인", "할증", "발행", "청약", "증자방식", "자금조달", "정정사항"]
    bonus = sum(1 for w in bonus_words if w in lower)
    length_bonus = min(len(lower) // 2000, 50)
    return tcnt * 100 + bonus * 30 + length_bonus

def pick_best_frame_html(page) -> str:
    """페이지 내 프레임들 중 표가 가장 풍부한 프레임 선택"""
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
    """
    표 파싱(안정 버전)
    - 가능한 경우 pd.read_html 사용
    - 실패하면 BeautifulSoup로 수동 파싱
    - header=None로 두고 라벨/값 탐색을 안정화
    """
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
        raise ValueError("표를 파싱하지 못했습니다(robust).")

    return results

def save_debug(acpt_no: str, page, html: str, reason: str):
    """실패 시 디버그 html/png 저장"""
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
    """KIND 뷰어에서 표(DataFrame 리스트) 추출"""
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
# Google Sheets (구조화 저장)
# ==========================================================
def gs_open():
    if not GOOGLE_SHEET_ID or not GOOGLE_CREDENTIALS_JSON:
        raise RuntimeError("GOOGLE_SHEET_ID / GOOGLE_CREDS 가 비어있습니다. Secrets 확인")

    creds = json.loads(GOOGLE_CREDENTIALS_JSON)
    gc = gspread.service_account_from_dict(creds)
    sh = gc.open_by_key(GOOGLE_SHEET_ID)

    # seen(로그)
    try:
        seen_ws = sh.worksheet(SEEN_SHEET_NAME)
    except gspread.WorksheetNotFound:
        seen_ws = sh.add_worksheet(title=SEEN_SHEET_NAME, rows=2000, cols=2)
        seen_ws.update("A1:B1", [SEEN_HEADERS])

    # rights(유상증자 구조화)
    try:
        rights_ws = sh.worksheet(RIGHTS_OUT_SHEET)
    except gspread.WorksheetNotFound:
        rights_ws = sh.add_worksheet(title=RIGHTS_OUT_SHEET, rows=2000, cols=len(RIGHTS_COLUMNS) + 5)

    return sh, rights_ws, seen_ws

def ensure_headers(ws, headers: List[str]):
    """1행 헤더 강제"""
    cur = ws.row_values(1)
    if cur != headers:
        end = rowcol_to_a1(1, len(headers))
        ws.update(f"A1:{end}", [headers])

def load_sheet_values(ws, headers: List[str]) -> List[List[str]]:
    """시트 전체 값을 가져오되, 헤더 보장"""
    ensure_headers(ws, headers)
    vals = ws.get_all_values()
    if not vals:
        end = rowcol_to_a1(1, len(headers))
        ws.update(f"A1:{end}", [headers])
        vals = ws.get_all_values()
    return vals

def build_acpt_index_from_values(values: List[List[str]], headers: List[str], key_field: str) -> Dict[str, int]:
    """접수번호(acptNo) -> row_number"""
    key_col = headers.index(key_field)
    idx: Dict[str, int] = {}
    for r, row in enumerate(values[1:], start=2):
        key = (row[key_col] if key_col < len(row) else "").strip()
        if key.isdigit():
            idx[key] = r
    return idx

def build_event_index_from_values(values: List[List[str]], headers: List[str]) -> Dict[str, Tuple[int, str]]:
    """
    event_key -> (row_number, existing_acpt_no)
    event_key는 회사명|최초이사회결의일|증자방식 기반
    """
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
    """특정 row를 record로 덮어쓰기"""
    ensure_headers(ws, headers)
    row_vals = [record.get(h, "") for h in headers]
    end = rowcol_to_a1(row, len(headers))
    ws.update(f"A{row}:{end}", [row_vals])

def upsert(
    ws,
    headers: List[str],
    index: Dict[str, int],
    record: dict,
    key_field: str,
    last_row_ref: Optional[List[int]] = None
) -> Tuple[str, int]:
    """
    key_field 값이 기존에 있으면 update, 없으면 append
    return: ("update"/"append", row_number)
    """
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


# ==========================================================
# seen(로그): 스킵 제거 + 같은 acptNo면 ts만 갱신
# ==========================================================
def build_seen_index(values: List[List[str]], headers: List[str], key_field: str) -> Dict[str, int]:
    """acptNo -> row_number"""
    key_col = headers.index(key_field)
    idx: Dict[str, int] = {}
    for r, row in enumerate(values[1:], start=2):
        key = (row[key_col] if key_col < len(row) else "").strip()
        if key.isdigit():
            idx[key] = r
    return idx

def touch_seen(seen_ws, seen_headers: List[str], seen_index: Dict[str, int], acpt_no: str, last_row_ref: List[int]):
    """
    같은 acptNo면 B열(ts)만 업데이트, 없으면 append
    """
    ensure_headers(seen_ws, seen_headers)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    key = str(acpt_no).strip()
    if not key.isdigit():
        return

    if key in seen_index:
        r = seen_index[key]
        seen_ws.update(f"B{r}", [[ts]])
    else:
        seen_ws.append_row([key, ts], value_input_option="RAW")
        last_row_ref[0] += 1
        seen_index[key] = last_row_ref[0]


# ==========================================================
# 유상증자 파서 보조 함수들
# ==========================================================
def scan_label_value(dfs: List[pd.DataFrame], label_candidates: List[str]) -> str:
    """
    라벨 매칭 후 값 후보 탐색:
    - 오른쪽/두칸오른쪽/아래/아래오른쪽
    - 같은 행의 나머지 값들도 후보로 본다(라벨 제외)
    """
    cand = {_norm(x) for x in label_candidates}

    for df in dfs:
        arr = df.astype(str).values
        R, C = arr.shape

        for r in range(R):
            for c in range(C):
                cell = _norm(arr[r][c])
                if cell in cand:
                    checks = []
                    for rr, cc in [(r, c + 1), (r, c + 2), (r + 1, c), (r + 1, c + 1)]:
                        if 0 <= rr < R and 0 <= cc < C:
                            v = str(arr[rr][cc]).strip()
                            if v and v.lower() != "nan":
                                checks.append(v)

                    row_vals = [
                        str(x).strip()
                        for x in arr[r].tolist()
                        if str(x).strip() and str(x).strip().lower() != "nan"
                    ]
                    row_vals = [x for x in row_vals if _norm(x) not in cand]

                    for v in checks + row_vals:
                        if re.fullmatch(r"\d+\.", v):
                            continue
                        return v
    return ""

def find_row_best_int(dfs: List[pd.DataFrame], must_contain: List[str]) -> Optional[int]:
    """특정 키워드가 포함된 행에서 가장 큰 정수값을 찾음"""
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
    """특정 키워드가 포함된 행에서 가장 큰(절댓값 기준) float값을 찾음"""
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

def extract_correction_after_map(dfs: List[pd.DataFrame]) -> Dict[str, str]:
    """
    '정정사항' 표에서 (항목 -> 정정후) 값을 뽑아낸다.
    - 정정전/정정후 헤더가 있는 표를 찾는다.
    - 항목 셀이 비어 있으면(병합셀) 직전 항목을 carry-forward.
    - key는 _norm(항목)으로 저장
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

        # 헤더 탐색(정정전/정정후)
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

            # 정정후 값은 after_col 주변도 같이 보면서 채움
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
    """
    정정 공시일 때: '정정사항 표(정정후)' 값을 먼저 반환하고,
    없으면 기존 표에서 라벨 탐색 로직 사용
    """
    if corr_after:
        cand_norm = [_norm(x) for x in label_candidates]

        # 1) 정확 매칭
        for c in cand_norm:
            v = corr_after.get(c, "")
            if str(v).strip():
                return str(v).strip()

        # 2) 포함 매칭(항목명이 약간 다를 때)
        for k, v in corr_after.items():
            if not str(v).strip():
                continue
            if any(c in k for c in cand_norm):
                return str(v).strip()

    return scan_label_value(dfs, label_candidates)

def build_fund_use_text(dfs: List[pd.DataFrame], corr_after: Optional[Dict[str, str]] = None) -> str:
    """
    자금용도(시설/운영 등) 금액을 표에서 찾아 '시설자금:xxx; 운영자금:yyy' 형태로 구성
    - 정정 공시일 경우 corr_after(정정후)에서 먼저 금액을 찾는다
    - 금액은 너무 작은 값(예: 항목 번호 4 등) 오인식 방지 위해 1,000,000 이상만 인정
    """
    keys = [
        "시설자금",
        "영업양수자금",
        "운영자금",
        "채무상환자금",
        "타법인증권취득자금",
        "타법인증권",
        "기타자금",
    ]

    parts = []

    for k in keys:
        best = None

        # 1) 정정후 우선(정정사항 표에서 금액 추출)
        if corr_after:
            kn = _norm(k)
            for itemk, v in corr_after.items():
                if kn in itemk:
                    amt = _max_int_in_text(v) or _to_int(v)
                    if amt is not None and amt >= 1_000_000:
                        if best is None or amt > best:
                            best = amt

        # 2) 기존 표 스캔(정정후가 없거나 못 찾은 경우)
        if best is None:
            for df in dfs:
                arr = df.astype(str).values
                for r in range(arr.shape[0]):
                    row = [str(x).strip() for x in arr[r].tolist()]
                    if _norm(k) in _norm("".join(row)):
                        nums = [_to_int(x) for x in row]
                        nums = [x for x in nums if x is not None and x >= 1_000_000]
                        if nums:
                            m = max(nums)
                            if best is None or m > best:
                                best = m

        if best is not None:
            parts.append(f"{k}:{best:,}")

    return "; ".join(parts)

def extract_investors(dfs: List[pd.DataFrame], corr_after: Optional[Dict[str, str]] = None) -> str:
    """투자자/제3자배정대상자 등 추출(정정후 우선)"""
    v = scan_label_value_preferring_correction(
        dfs,
        ["제3자배정대상자", "제3자배정 대상자", "대표주관사/투자자", "투자자"],
        corr_after=corr_after
    )
    return v or ""


# ==========================================================
# 유상증자 레코드 파싱
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
    rec["회사명"] = (
        scan_label_value_preferring_correction(dfs, ["회 사 명", "회사명", "회사 명"], corr_after)
        or company_from_title(title)
    )
    rec["상장시장"] = (
        scan_label_value_preferring_correction(dfs, ["상장시장", "시장구분", "시장 구분"], corr_after)
        or market_from_title(title)
    )

    # 결의일
    rec["이사회결의일"] = scan_label_value_preferring_correction(
        dfs,
        ["15. 이사회결의일(결정일)", "이사회결의일(결정일)", "이사회결의일", "결정일"],
        corr_after
    )
    rec["최초 이사회결의일"] = (
        scan_label_value_preferring_correction(dfs, ["최초 이사회결의일", "최초이사회결의일"], corr_after)
        or rec["이사회결의일"]
    )

    # 증자방식
    rec["증자방식"] = scan_label_value_preferring_correction(
        dfs,
        ["5. 증자방식", "증자방식", "발행방법", "배정방식"],
        corr_after
    )

    # 주식수(정정후 우선)
    issue_txt = scan_label_value_preferring_correction(
        dfs,
        ["1. 신주의 종류와 수", "신주의 종류와 수", "신주의종류와수", "신주의 종류 및 수", "신주의종류및수"],
        corr_after
    )
    prev_txt = scan_label_value_preferring_correction(
        dfs,
        ["증자전발행주식총수", "증자전 발행주식총수", "증자전발행주식총수(보통주식)", "발행주식총수", "발행주식 총수"],
        corr_after
    )

    issue_shares = (
        _to_int(issue_txt)
        or _max_int_in_text(issue_txt)
        or find_row_best_int(dfs, ["신주의종류와수", "보통주식"])
        or find_row_best_int(dfs, ["보통주식", "(주)"])
    )
    prev_shares = (
        _to_int(prev_txt)
        or _max_int_in_text(prev_txt)
        or find_row_best_int(dfs, ["증자전발행주식총수", "보통주식"])
        or find_row_best_int(dfs, ["발행주식총수", "보통주식"])
    )

    if issue_shares:
        rec["발행상품"] = "보통주식"
        rec["신규발행주식수"] = f"{issue_shares:,}"
    if prev_shares:
        rec["증자전 주식수"] = f"{prev_shares:,}"

    # 발행가/기준주가/할인율(정정후 우선)
    price_txt = scan_label_value_preferring_correction(
        dfs,
        ["6. 신주 발행가액", "신주 발행가액", "신주발행가액", "신주 발행가액(원)", "신주발행가액(원)"],
        corr_after
    )
    price = (
        _to_int(price_txt)
        or find_row_best_int(dfs, ["신주발행가액", "보통주식"])
        or find_row_best_int(dfs, ["신주", "발행가액"])
    )
    if price:
        rec["확정발행가(원)"] = f"{price:,}"
    else:
        rec["확정발행가(원)"] = price_txt

    base_txt = scan_label_value_preferring_correction(dfs, ["7. 기준주가", "기준주가"], corr_after)
    base_price = (
        _to_int(base_txt)
        or find_row_best_int(dfs, ["기준주가", "보통주식"])
        or find_row_best_int(dfs, ["기준주가"])
    )
    if base_price:
        rec["기준주가"] = f"{base_price:,}"
    else:
        rec["기준주가"] = base_txt

    disc_txt = scan_label_value_preferring_correction(
        dfs,
        ["7-2. 기준주가에 대한 할인율 또는 할증율 (%)", "기준주가에 대한 할인율 또는 할증율 (%)", "할인율또는할증율", "기준주가에대한할인율또는할증율"],
        corr_after
    )
    disc = _to_float(disc_txt)
    if disc is None:
        disc = (
            find_row_best_float(dfs, ["기준주가에대한할인율또는할증율"])
            or find_row_best_float(dfs, ["할인율또는할증율"])
        )
    if disc is not None:
        rec["할인(할증률)"] = f"{disc}"
    else:
        rec["할인(할증률)"] = disc_txt

    # 일정(정정후 우선)
    rec["납입일"] = scan_label_value_preferring_correction(dfs, ["9. 납입일", "납입일"], corr_after)
    rec["신주의 배당기산일"] = scan_label_value_preferring_correction(
        dfs,
        ["10. 신주의 배당기산일", "신주의 배당기산일", "배당기산일"],
        corr_after
    )
    rec["신주의 상장 예정일"] = scan_label_value_preferring_correction(
        dfs,
        ["12. 신주의 상장 예정일", "신주의 상장 예정일", "상장예정일"],
        corr_after
    )

    # 자금용도/투자자(정정후 우선)
    rec["자금용도"] = (
        scan_label_value_preferring_correction(dfs, ["4. 자금조달의 목적", "자금조달의 목적", "자금용도"], corr_after)
        or build_fund_use_text(dfs, corr_after=corr_after)
    )
    rec["투자자"] = extract_investors(dfs, corr_after=corr_after)

    # 계산 보강
    sh = _to_int(rec["신규발행주식수"])
    pr = _to_int(rec["확정발행가(원)"])
    if sh and pr:
        rec["확정발행금액(억원)"] = f"{(sh * pr) / 100_000_000:,.2f}"

    pv = _to_int(rec["증자전 주식수"])
    if sh and pv and pv > 0:
        rec["증자비율"] = f"{sh / pv * 100:.2f}%"

    return rec


# ==========================================================
# 실행
# ==========================================================
def run():
    _, rights_ws, seen_ws = gs_open()

    # rights 시트: 한 번 로드해서 인덱스 구성
    values = load_sheet_values(rights_ws, RIGHTS_COLUMNS)
    last_row_ref = [len(values)]  # rights append 시 row 계산용

    rights_index = build_acpt_index_from_values(values, RIGHTS_COLUMNS, key_field="접수번호")
    event_index = build_event_index_from_values(values, RIGHTS_COLUMNS)

    # seen은 스킵에 쓰지 않고 "처리 로그(ts 갱신)"만 유지
    seen_values = load_sheet_values(seen_ws, SEEN_HEADERS)
    last_seen_row_ref = [len(seen_values)]
    seen_index = build_seen_index(seen_values, SEEN_HEADERS, key_field="acptNo")

    # 대상 선정
    if RUN_ONE_ACPTNO:
        targets = [Target(acpt_no=RUN_ONE_ACPTNO, title=f"[MANUAL]{RUN_ONE_ACPTNO}", link="")]
    else:
        targets = parse_rss_targets()
        # ✅ seen 스킵 제거: 매번 RSS에 잡히면 처리(지속 업데이트)
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

                # 유상증자만 구조화 저장
                if "유상증자" in (t.title or ""):
                    corr_after = None
                    if is_correction_title(t.title):
                        corr_after = extract_correction_after_map(dfs)

                    rec = parse_rights_issue_record(
                        dfs,
                        title=t.title,
                        acpt_no=t.acpt_no,
                        link=src,
                        corr_after=corr_after
                    )

                    # 정정 공시: 기존 행 찾아 update(덮어쓰기)
                    if is_correction_title(t.title):
                        evk = make_event_key(
                            rec.get("회사명", ""),
                            rec.get("최초 이사회결의일", "") or rec.get("이사회결의일", ""),
                            rec.get("증자방식", "")
                        )

                        target_row = None
                        old_acpt = ""

                        # 1) event_key로 찾기
                        if evk in event_index:
                            target_row, old_acpt = event_index[evk]
                        # 2) 혹시 같은 접수번호가 이미 있으면 그 행
                        elif rec["접수번호"] in rights_index:
                            target_row = rights_index[rec["접수번호"]]

                        if target_row:
                            update_row(rights_ws, RIGHTS_COLUMNS, target_row, rec)

                            # 인덱스 갱신(접수번호가 바뀌는 케이스 대비)
                            if old_acpt and old_acpt in rights_index and old_acpt != rec["접수번호"]:
                                del rights_index[old_acpt]
                            rights_index[rec["접수번호"]] = target_row
                            event_index[evk] = (target_row, rec["접수번호"])

                            print(f"[OK] {t.acpt_no} correction=Y mode=UPDATE row={target_row}")
                        else:
                            # 못 찾으면 일단 upsert(접수번호 기준)로 저장
                            mode, row = upsert(
                                rights_ws, RIGHTS_COLUMNS, rights_index, rec,
                                key_field="접수번호", last_row_ref=last_row_ref
                            )
                            event_index[evk] = (row, rec["접수번호"])
                            print(f"[OK] {t.acpt_no} correction=Y mode={mode.upper()} row={row}")

                    else:
                        # 일반 공시: 접수번호 기준 upsert(매번 업데이트 가능)
                        mode, row = upsert(
                            rights_ws, RIGHTS_COLUMNS, rights_index, rec,
                            key_field="접수번호", last_row_ref=last_row_ref
                        )

                        # event index도 업데이트(후속 정정 대비)
                        evk = make_event_key(
                            rec.get("회사명", ""),
                            rec.get("최초 이사회결의일", "") or rec.get("이사회결의일", ""),
                            rec.get("증자방식", "")
                        )
                        event_index[evk] = (row, rec["접수번호"])

                        print(f"[OK] {t.acpt_no} correction=N mode={mode.upper()} row={row}")

                # ✅ seen 로그: 스킵용이 아니라 로그용(같은 acptNo면 ts만 갱신)
                touch_seen(seen_ws, SEEN_HEADERS, seen_index, t.acpt_no, last_seen_row_ref)

                ok += 1

            except Exception as e:
                print(f"[FAIL] {t.acpt_no} {t.title} :: {e}")

            time.sleep(0.4)

        context.close()
        browser.close()

    print(f"[DONE] ok={ok}")


if __name__ == "__main__":
    run()
