# ==========================================================
# #유상증자_코드V2 (완성 개선판)
# ----------------------------------------------------------
# 기능 요약
# 1) RSS에서 "유상증자" 공시 수집 → KIND 뷰어에서 표 파싱 → Google Sheets 구조화 저장
# 2) 정정 공시(제목 맨 앞 "정정") 또는 본문에 "정정사항(정정전/정정후)" 표가 있으면:
#    - 정정사항 표의 "정정후" 값을 최우선으로 적용
#    - 기존 행을 찾아 UPDATE(덮어쓰기) (append 방지)
# 3) seen은 스킵 용도가 아니라 "처리 로그(ts 갱신)" 용도 (지속 업데이트)
#
# 이번에 “노란 셀” 문제를 해결하기 위한 핵심 로직
# - 확정발행가/기준주가 등 숫자 필드: "6" 같은 문항번호 오인식 방지(가격 검증)
# - 납입일 등 날짜 필드: "납입일 변경에 따른 정정" 같은 텍스트 유입 방지(날짜 검증)
# - RSS가 '오늘 공시'만 주는 문제 해결: 시트 내 이상값(노란 셀) 행을 찾아 재처리 모드 제공
#
# 실행 모드(ENV)
# - RUN_ONE_ACPTNO=14자리접수번호 : 해당 1건만 강제 재처리
# - RECHECK_BAD=true             : 시트에서 이상값(노란 패턴) 행들을 찾아 재처리
# - MAX_RECHECK=200              : RECHECK_BAD에서 최대 재처리 건수 제한(기본 0=무제한)
# - LIMIT=0                      : RSS 처리 개수 제한(0이면 무제한)
# ==========================================================

import os
import re
import json
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple, Dict, Set

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
LIMIT = int(os.getenv("LIMIT", "0"))  # 0이면 무제한
RUN_ONE_ACPTNO = os.getenv("RUN_ONE_ACPTNO", "").strip()

RECHECK_BAD = os.getenv("RECHECK_BAD", "false").lower() == "true"
MAX_RECHECK = int(os.getenv("MAX_RECHECK", "0"))  # 0이면 무제한

GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "").strip()
GOOGLE_CREDENTIALS_JSON = (
    os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
    or os.environ.get("GOOGLE_CREDS", "").strip()
)

RIGHTS_OUT_SHEET = os.getenv("RIGHTS_OUT_SHEET", "유상증자")
SEEN_SHEET_NAME = os.getenv("SEEN_SHEET_NAME", "seen")

# 디버그 저장
OUTDIR = Path(os.getenv("OUTDIR", "out"))
DEBUGDIR = OUTDIR / "debug"

# rights(유상증자) 구조화 컬럼
RIGHTS_COLUMNS = [
    "회사명", "상장시장", "최초 이사회결의일", "증자방식", "발행상품", "신규발행주식수",
    "확정발행가(원)", "기준주가", "확정발행금액(억원)", "할인(할증률)",
    "증자전 주식수", "증자비율", "납입일", "신주의 배당기산일", "신주의 상장 예정일",
    "이사회결의일", "자금용도", "투자자", "링크", "접수번호"
]

# seen(로그) 컬럼: 스킵이 아니라 "처리 로그(ts 갱신)" 용도
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
    """공백/콜론 제거 등 간단 정규화"""
    s = (s or "").strip()
    s = re.sub(r"\s+", "", s)
    s = s.replace(":", "")
    return s

def _norm_date(s: str) -> str:
    """날짜 비교용: 숫자만 남김(YYYYMMDD)"""
    return re.sub(r"[^\d]", "", (s or "").strip())

def _to_int(s: str) -> Optional[int]:
    """문자열→int (콤마/문자 제거)"""
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
    """문자열→float"""
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
    """문장 내 숫자(, 포함) 중 최댓값 추출"""
    if not s:
        return None
    nums = re.findall(r"\d[\d,]*", str(s))
    vals = []
    for x in nums:
        v = _to_int(x)
        if v is not None:
            vals.append(v)
    return max(vals) if vals else None

def _int_from_text(s: str) -> Optional[int]:
    """텍스트에서 int 후보(여러 숫자면 최대값)"""
    return _max_int_in_text(s) or _to_int(s)

def extract_acpt_no(text: str) -> Optional[str]:
    m = re.search(r"acptNo=(\d{14})", text or "")
    return m.group(1) if m else None

def company_from_title(title: str) -> str:
    """제목 [회사명] 추출"""
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
    """제목 '맨 앞'이 정정인 경우만"""
    return bool(title) and title.strip().startswith("정정")

def make_event_key(company: str, first_board_date: str, method: str) -> str:
    """정정 공시가 원공시 행을 찾아 덮어쓰기 위한 이벤트 키"""
    return f"{_norm(company)}|{_norm_date(first_board_date)}|{_norm(method)}"


# ----------------------------------------------------------
# 값 검증(validator) + 키 정규화
# ----------------------------------------------------------
def _strip_leading_item_no(s: str) -> str:
    """항목/라벨 앞 번호 제거: '15. ', '7-2. ', '(1) ' 등"""
    if not s:
        return ""
    t = str(s).strip()
    t = re.sub(r"^\s*[\(\[]?\s*\d+(?:-\d+)?\s*[\)\].]?\s*", "", t)
    return t.strip()

def _norm_key(s: str) -> str:
    """특수문자 제거(한글/영문/숫자만 남김)"""
    s = (s or "").strip()
    return re.sub(r"[^0-9A-Za-z가-힣]", "", s)

def _label_key_variants(label: str) -> Set[str]:
    """라벨 후보 키 변형(번호 포함/번호 제거 둘 다)"""
    v = set()
    v.add(_norm_key(label))
    v.add(_norm_key(_strip_leading_item_no(label)))
    return {x for x in v if x}

def is_date_like(s: str) -> bool:
    """날짜처럼 생긴 문자열인지"""
    if not s:
        return False
    t = str(s).strip()
    if re.search(r"\d{4}\s*[년\.\-/]\s*\d{1,2}\s*[월\.\-/]\s*\d{1,2}", t):
        return True
    digits = re.sub(r"[^\d]", "", t)
    return len(digits) == 8

def normalize_date_str(s: str) -> str:
    """
    날짜 표기를 YYYY-MM-DD로 정규화(가능한 경우)
    - 실패하면 원문 그대로 반환
    """
    if not s:
        return ""
    t = str(s).strip()
    digits = re.sub(r"[^\d]", "", t)
    if len(digits) == 8:
        y, m, d = digits[:4], digits[4:6], digits[6:8]
        return f"{y}-{m}-{d}"
    return t

def is_price_like(s: str, min_v: int = 10, max_v: int = 2_000_000) -> bool:
    """가격(발행가/기준주가) 범위 검증"""
    v = _int_from_text(s)
    if v is None:
        return False
    return (min_v <= v <= max_v)

def is_text_ok_for_missing(s: str) -> bool:
    """미정/추후확정 같은 값은 '오류'로 보지 않기"""
    if not s:
        return False
    t = str(s).strip()
    keywords = ["미정", "추후", "예정", "미확정", "결정", "확정후", "산정중"]
    return any(k in t for k in keywords)


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
# Playwright: 뷰어(html) → 표(DataFrame)들 추출
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
    bonus_words = ["기준주가", "납입", "이사회", "할인", "할증", "발행", "청약", "증자방식", "자금조달", "정정사항", "정정전", "정정후"]
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
    """
    표 파싱(안정 버전)
    - header=None 유지 (라벨/값 탐색에 유리)
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

    # seen(로그)
    try:
        seen_ws = sh.worksheet(SEEN_SHEET_NAME)
    except gspread.WorksheetNotFound:
        seen_ws = sh.add_worksheet(title=SEEN_SHEET_NAME, rows=2000, cols=2)
        seen_ws.update("A1:B1", [SEEN_HEADERS])

    # rights(유상증자)
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

def upsert(
    ws,
    headers: List[str],
    index: Dict[str, int],
    record: dict,
    key_field: str,
    last_row_ref: Optional[List[int]] = None
) -> Tuple[str, int]:
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

def build_seen_index(values: List[List[str]], headers: List[str], key_field: str) -> Dict[str, int]:
    key_col = headers.index(key_field)
    idx: Dict[str, int] = {}
    for r, row in enumerate(values[1:], start=2):
        key = (row[key_col] if key_col < len(row) else "").strip()
        if key.isdigit():
            idx[key] = r
    return idx

def touch_seen(seen_ws, seen_headers: List[str], seen_index: Dict[str, int], acpt_no: str, last_row_ref: List[int]):
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
# 파서 보조: 라벨/값 스캔 + 정정사항 추출
# ==========================================================
def scan_label_value(dfs: List[pd.DataFrame], label_candidates: List[str], validator=None) -> str:
    """
    라벨 매칭 후 값 후보 탐색(강화)
    - 주변 셀: 오른쪽 1~4칸 + 아래 1~2줄
    - 같은 행 전체에서도 후보 탐색
    - validator가 있으면 통과한 값만 채택(가격/날짜 오류 방지)
    """
    cand: Set[str] = set()
    for x in label_candidates:
        cand |= _label_key_variants(x)

    for df in dfs:
        arr = df.astype(str).values
        R, C = arr.shape

        for r in range(R):
            for c in range(C):
                cell = _norm_key(arr[r][c])
                if cell in cand:
                    checks: List[str] = []

                    neighbor_coords = []
                    for k in range(1, 5):
                        neighbor_coords.append((r, c + k))
                    neighbor_coords += [(r + 1, c), (r + 1, c + 1), (r + 2, c), (r + 2, c + 1)]

                    for rr, cc in neighbor_coords:
                        if 0 <= rr < R and 0 <= cc < C:
                            v = str(arr[rr][cc]).strip()
                            if v and v.lower() != "nan":
                                checks.append(v)

                    row_vals = [
                        str(x).strip()
                        for x in arr[r].tolist()
                        if str(x).strip() and str(x).strip().lower() != "nan"
                    ]
                    row_vals = [x for x in row_vals if _norm_key(x) not in cand]

                    for v in checks + row_vals:
                        # "6", "6." 같은 문항번호/잡값 1차 제거
                        if re.fullmatch(r"\d{1,2}\.?", v.strip()):
                            continue

                        if validator is not None and not validator(v):
                            continue

                        return v
    return ""

def has_correction_table(dfs: List[pd.DataFrame]) -> bool:
    """본문에 정정사항(정정전/정정후) 표가 있는지 감지"""
    for df in dfs:
        try:
            arr = df.astype(str).values
        except Exception:
            continue
        flat = " ".join([str(x) for x in arr.flatten().tolist()])
        if ("정정전" in flat) and ("정정후" in flat):
            return True
    return False

def extract_correction_after_map(dfs: List[pd.DataFrame]) -> Dict[str, str]:
    """
    '정정사항' 표에서 (항목 -> 정정후) 값 추출
    - key는 _norm_key(번호 제거된 항목명)로 저장
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

        # 헤더 탐색
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
                item_clean = _strip_leading_item_no(item)
                out[_norm_key(item_clean)] = after_val

    return out

def scan_label_value_preferring_correction(
    dfs: List[pd.DataFrame],
    label_candidates: List[str],
    corr_after: Optional[Dict[str, str]] = None,
    validator=None
) -> str:
    """
    정정 공시일 때: 정정사항 표(정정후) 우선
    - validator 통과한 값만 채택
    - 없으면 본문 표에서 라벨 스캔(validator 적용)
    """
    if corr_after:
        cand_norm: Set[str] = set()
        for x in label_candidates:
            cand_norm |= _label_key_variants(x)

        # 1) 정확 매칭
        for c in cand_norm:
            v = corr_after.get(c, "")
            if str(v).strip():
                if validator is None or validator(v):
                    return str(v).strip()

        # 2) 포함 매칭(항목명이 조금 다를 때)
        for k, v in corr_after.items():
            if not str(v).strip():
                continue
            if any(c in k for c in cand_norm):
                if validator is None or validator(v):
                    return str(v).strip()

    return scan_label_value(dfs, label_candidates, validator=validator)

def build_fund_use_text(dfs: List[pd.DataFrame], corr_after: Optional[Dict[str, str]] = None) -> str:
    """
    자금용도 금액을 찾아 "시설자금:xxx; 운영자금:yyy"로 구성
    - 정정 공시일 경우 corr_after(정정후)에서 먼저 금액을 찾음
    - 너무 작은 값(문항번호 등) 방지 위해 1,000,000 이상만 인정
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

        # 1) 정정후 우선
        if corr_after:
            kn = _norm_key(k)
            for itemk, v in corr_after.items():
                if kn and (kn in itemk):
                    amt = _int_from_text(v)
                    if amt is not None and amt >= 1_000_000:
                        best = amt if best is None else max(best, amt)

        # 2) 본문 표 스캔
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
                            best = m if best is None else max(best, m)

        if best is not None:
            parts.append(f"{k}:{best:,}")

    return "; ".join(parts)

def extract_investors(dfs: List[pd.DataFrame], corr_after: Optional[Dict[str, str]] = None) -> str:
    return scan_label_value_preferring_correction(
        dfs,
        ["제3자배정대상자", "제3자배정 대상자", "대표주관사/투자자", "투자자"],
        corr_after=corr_after
    ) or ""


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

    # 결의일(날짜 검증 적용)
    rec["이사회결의일"] = normalize_date_str(
        scan_label_value_preferring_correction(
            dfs,
            ["15. 이사회결의일(결정일)", "이사회결의일(결정일)", "이사회결의일", "결정일"],
            corr_after,
            validator=is_date_like
        )
    )
    rec["최초 이사회결의일"] = normalize_date_str(
        scan_label_value_preferring_correction(dfs, ["최초 이사회결의일", "최초이사회결의일"], corr_after, validator=is_date_like)
        or rec["이사회결의일"]
    )

    # 증자방식
    rec["증자방식"] = scan_label_value_preferring_correction(
        dfs,
        ["5. 증자방식", "증자방식", "발행방법", "배정방식"],
        corr_after
    )

    # 주식수
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

    issue_shares = _to_int(issue_txt) or _max_int_in_text(issue_txt)
    prev_shares = _to_int(prev_txt) or _max_int_in_text(prev_txt)

    if issue_shares:
        rec["발행상품"] = "보통주식"
        rec["신규발행주식수"] = f"{issue_shares:,}"
    if prev_shares:
        rec["증자전 주식수"] = f"{prev_shares:,}"

    # 확정발행가(가격 검증 적용)
    price_txt = scan_label_value_preferring_correction(
        dfs,
        ["6. 신주 발행가액", "신주 발행가액", "신주발행가액", "확정 발행가액", "확정발행가액",
         "발행가액", "발행가액(원)", "1주당 발행가액", "1주당신주발행가액"],
        corr_after,
        validator=is_price_like
    )
    price = _int_from_text(price_txt)
    if price is not None:
        rec["확정발행가(원)"] = f"{price:,}"
    else:
        rec["확정발행가(원)"] = price_txt  # 미정 등 텍스트 가능

    # 기준주가(가격 검증 적용)
    base_txt = scan_label_value_preferring_correction(
        dfs,
        ["7. 기준주가", "기준주가", "기준주가(원)"],
        corr_after,
        validator=is_price_like
    )
    base_price = _int_from_text(base_txt)
    if base_price is not None:
        rec["기준주가"] = f"{base_price:,}"
    else:
        rec["기준주가"] = base_txt

    # 할인/할증률
    disc_txt = scan_label_value_preferring_correction(
        dfs,
        ["7-2. 기준주가에 대한 할인율 또는 할증율 (%)",
         "기준주가에 대한 할인율 또는 할증율 (%)",
         "할인율또는할증율",
         "기준주가에대한할인율또는할증율"],
        corr_after
    )
    disc = _to_float(disc_txt)
    if disc is None:
        # 행 기반 fallback
        disc = None
        for df in dfs:
            arr = df.astype(str).values
            for r in range(arr.shape[0]):
                row = [str(x).strip() for x in arr[r].tolist()]
                joined = _norm("".join(row))
                if ("기준주가에대한할인율또는할증율" in joined) or ("할인율또는할증율" in joined):
                    vals = [_to_float(x) for x in row]
                    vals = [x for x in vals if x is not None]
                    if vals:
                        disc = max(vals, key=lambda z: abs(z))
                        break
            if disc is not None:
                break
    rec["할인(할증률)"] = f"{disc}" if disc is not None else disc_txt

    # 일정(날짜 검증 적용)
    rec["납입일"] = normalize_date_str(
        scan_label_value_preferring_correction(dfs, ["9. 납입일", "납입일"], corr_after, validator=is_date_like)
    )
    rec["신주의 배당기산일"] = normalize_date_str(
        scan_label_value_preferring_correction(dfs, ["10. 신주의 배당기산일", "신주의 배당기산일", "배당기산일"], corr_after, validator=is_date_like)
    )
    rec["신주의 상장 예정일"] = normalize_date_str(
        scan_label_value_preferring_correction(dfs, ["12. 신주의 상장 예정일", "신주의 상장 예정일", "상장예정일"], corr_after, validator=is_date_like)
    )

    # 자금용도 / 투자자
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
# 정정 공시 UPDATE 대상 행 찾기(강화)
# - event_key가 바뀌는 케이스(정정으로 최초결의일/방식 변경 등)에도 대응
# ==========================================================
def _yyyymmdd_int(s: str) -> Optional[int]:
    d = _norm_date(s)
    if len(d) != 8:
        return None
    try:
        return int(d)
    except Exception:
        return None

def find_best_row_for_correction(values: List[List[str]], headers: List[str], rec: dict) -> Optional[int]:
    """
    event_key로 못 찾는 경우 대비:
    - 회사명 동일한 행들 중
    - '최초 이사회결의일'이 가까운 행을 우선
    - (가능하면 증자방식도 일치)
    """
    try:
        col_company = headers.index("회사명")
        col_first = headers.index("최초 이사회결의일")
        col_method = headers.index("증자방식")
    except Exception:
        return None

    target_company = _norm(rec.get("회사명", ""))
    if not target_company:
        return None

    target_first = _yyyymmdd_int(rec.get("최초 이사회결의일", "") or rec.get("이사회결의일", ""))
    target_method = _norm(rec.get("증자방식", ""))

    best_row = None
    best_score = None  # 낮을수록 좋음

    for r, row in enumerate(values[1:], start=2):
        company = _norm(row[col_company] if col_company < len(row) else "")
        if company != target_company:
            continue

        first = row[col_first] if col_first < len(row) else ""
        first_i = _yyyymmdd_int(first)

        method = _norm(row[col_method] if col_method < len(row) else "")

        # 날짜 거리(없으면 큰 페널티)
        if target_first is not None and first_i is not None:
            dist = abs(first_i - target_first)
        else:
            dist = 99999999

        # 방식 페널티
        method_penalty = 0 if (target_method and method == target_method) else 50

        score = dist + method_penalty

        # 너무 멀면(예: 60일 이상) 제외(완전 다른 이벤트 방지)
        # yyyymmdd 차이는 실제 날짜 차이가 아니지만 대략적으로 큰 값이면 제외
        if dist != 99999999 and dist > 200:  # 대충 2개월 수준(달/일 단위 차이 고려)
            continue

        if best_score is None or score < best_score:
            best_score = score
            best_row = r

    return best_row


# ==========================================================
# RECHECK_BAD: 시트에서 "이상값(노란 셀 패턴)" 행을 찾아 접수번호 리스트 생성
# ==========================================================
def is_bad_price_cell(s: str) -> bool:
    if not s:
        return True
    if is_text_ok_for_missing(s):
        return False
    v = _to_int(s)
    if v is None:
        return True
    return (v < 10 or v > 2_000_000)

def is_bad_date_cell(s: str) -> bool:
    if not s:
        return True
    if is_text_ok_for_missing(s):
        return False
    return not is_date_like(s)

def collect_bad_acptnos_from_sheet(values: List[List[str]], headers: List[str], limit: int = 0) -> List[str]:
    """
    시트에서 다음 조건이면 '재처리 필요'로 판단:
    - 확정발행가/기준주가 비어있거나 10 미만(문항번호 오인식) / 너무 큰 값
    - 납입일/배당기산일/상장예정일 비어있거나 날짜형식이 아님
    """
    col_acpt = headers.index("접수번호")
    col_price = headers.index("확정발행가(원)")
    col_base = headers.index("기준주가")
    col_pay = headers.index("납입일")
    col_div = headers.index("신주의 배당기산일")
    col_list = headers.index("신주의 상장 예정일")

    out: List[str] = []
    seen: Set[str] = set()

    for row in values[1:]:
        acpt = (row[col_acpt] if col_acpt < len(row) else "").strip()
        if not acpt.isdigit():
            continue

        price = row[col_price] if col_price < len(row) else ""
        base = row[col_base] if col_base < len(row) else ""
        pay = row[col_pay] if col_pay < len(row) else ""
        div = row[col_div] if col_div < len(row) else ""
        lst = row[col_list] if col_list < len(row) else ""

        bad = (
            is_bad_price_cell(price)
            or is_bad_price_cell(base)
            or is_bad_date_cell(pay)
            or is_bad_date_cell(div)
            or is_bad_date_cell(lst)
        )

        if bad and acpt not in seen:
            out.append(acpt)
            seen.add(acpt)

        if limit > 0 and len(out) >= limit:
            break

    return out


# ==========================================================
# 실행
# ==========================================================
def run():
    # 버전 출력(배포/액션에서 "바뀐 코드가 실행됐는지" 확인용)
    print("[VERSION] rights_issue_parser validator-fix + recheck_bad 2026-03-03")

    _, rights_ws, seen_ws = gs_open()

    # rights 시트: 한 번 로드해서 인덱스 구성
    values = load_sheet_values(rights_ws, RIGHTS_COLUMNS)
    last_row_ref = [len(values)]

    rights_index = build_acpt_index_from_values(values, RIGHTS_COLUMNS, key_field="접수번호")
    event_index = build_event_index_from_values(values, RIGHTS_COLUMNS)

    # seen 시트(로그)
    seen_values = load_sheet_values(seen_ws, SEEN_HEADERS)
    last_seen_row_ref = [len(seen_values)]
    seen_index = build_seen_index(seen_values, SEEN_HEADERS, key_field="acptNo")

    # 대상 선정
    targets: List[Target] = []

    if RUN_ONE_ACPTNO:
        targets = [Target(acpt_no=RUN_ONE_ACPTNO, title=f"[MANUAL]{RUN_ONE_ACPTNO}", link="")]

    elif RECHECK_BAD:
        # 시트 이상값 행의 접수번호만 찾아 재처리
        bad_acpts = collect_bad_acptnos_from_sheet(values, RIGHTS_COLUMNS, limit=MAX_RECHECK)
        targets = [Target(acpt_no=a, title=f"[RECHECK]{a}", link="") for a in bad_acpts]
        print(f"[INFO] RECHECK_BAD 대상 {len(targets)}건")

    else:
        # RSS 기준 (오늘 공시)
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

                # 유상증자만 구조화 저장(제목이 없을 수 있어도 본문에서 최대한 추출)
                # - RSS 제목이 없을 수 있는 RECHECK_BAD/RUN_ONE 케이스도 대응
                if ("유상증자" in (t.title or "")) or RECHECK_BAD or RUN_ONE_ACPTNO:
                    # 정정 여부: (제목 맨앞 정정) OR (본문에 정정전/정정후 표 존재)
                    correction_flag = is_correction_title(t.title) or has_correction_table(dfs)

                    corr_after = extract_correction_after_map(dfs) if correction_flag else None

                    rec = parse_rights_issue_record(
                        dfs=dfs,
                        title=t.title,
                        acpt_no=t.acpt_no,
                        link=src,
                        corr_after=corr_after
                    )

                    # 정정 공시: 기존 행 찾아 update(덮어쓰기)
                    if correction_flag:
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

                        # 2) 같은 접수번호가 이미 있으면 그 행
                        elif rec.get("접수번호", "") in rights_index:
                            target_row = rights_index[rec["접수번호"]]

                        # 3) 그래도 없으면(정정으로 key가 바뀐 케이스) 회사명/가까운 날짜로 추정
                        if not target_row:
                            target_row = find_best_row_for_correction(values, RIGHTS_COLUMNS, rec)

                        if target_row:
                            update_row(rights_ws, RIGHTS_COLUMNS, target_row, rec)

                            # 인덱스 갱신(접수번호가 바뀌는 케이스 대비)
                            if old_acpt and old_acpt in rights_index and old_acpt != rec["접수번호"]:
                                del rights_index[old_acpt]
                            rights_index[rec["접수번호"]] = target_row
                            event_index[evk] = (target_row, rec["접수번호"])

                            print(f"[OK] {t.acpt_no} correction=Y mode=UPDATE row={target_row}")

                        else:
                            # 못 찾으면 upsert(접수번호 기준)로 저장
                            mode, rownum = upsert(
                                rights_ws, RIGHTS_COLUMNS, rights_index, rec,
                                key_field="접수번호", last_row_ref=last_row_ref
                            )
                            event_index[evk] = (rownum, rec["접수번호"])
                            print(f"[OK] {t.acpt_no} correction=Y mode={mode.upper()} row={rownum}")

                    else:
                        # 일반 공시: 접수번호 기준 upsert
                        mode, rownum = upsert(
                            rights_ws, RIGHTS_COLUMNS, rights_index, rec,
                            key_field="접수번호", last_row_ref=last_row_ref
                        )

                        # event index도 업데이트(후속 정정 대비)
                        evk = make_event_key(
                            rec.get("회사명", ""),
                            rec.get("최초 이사회결의일", "") or rec.get("이사회결의일", ""),
                            rec.get("증자방식", "")
                        )
                        event_index[evk] = (rownum, rec["접수번호"])
                        print(f"[OK] {t.acpt_no} correction=N mode={mode.upper()} row={rownum}")

                # seen 로그(ts 갱신)
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
