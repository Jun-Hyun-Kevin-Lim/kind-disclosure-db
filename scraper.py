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


# =========================
# Config (ENV)
# =========================
BASE = "https://kind.krx.co.kr"
DEFAULT_RSS = (
    "http://kind.krx.co.kr:80/disclosure/rsstodaydistribute.do"
    "?method=searchRssTodayDistribute&mktTpCd=0&currentPageSize=100"
)

RSS_URL = os.getenv("RSS_URL", DEFAULT_RSS)

# 제목 기준 필터
KEYWORDS = [x.strip() for x in os.getenv("KEYWORDS", "유상증자,전환사채,교환사채,신주인수권부사채").split(",") if x.strip()]

HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
LIMIT = int(os.getenv("LIMIT", "30"))
RUN_ONE_ACPTNO = os.getenv("RUN_ONE_ACPTNO", "").strip()

GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "").strip()
GOOGLE_CREDENTIALS_JSON = (
    os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
    or os.environ.get("GOOGLE_CREDS", "").strip()
)

# RAW 덤프(원하면 유지)
DUMP_SHEET_NAME = os.getenv("DUMP_SHEET_NAME", "RAW_dump")
SAVE_RAW = os.getenv("SAVE_RAW", "false").lower() == "true"

# 중복방지
SEEN_SHEET_NAME = os.getenv("SEEN_SHEET_NAME", "seen")

# 유상증자 “정규 컬럼” 저장 탭
RIGHTS_OUT_SHEET = os.getenv("RIGHTS_OUT_SHEET", "유상증자")

RIGHTS_COLUMNS = [
    "회사명", "상장시장", "최초 이사회결의일", "증자방식", "발행상품", "신규발행주식수",
    "확정발행가(원)", "기준주가", "확정발행금액(억원)", "할인(할증률)",
    "증자전 주식수", "증자비율", "납입일", "신주의 배당기산일", "신주의 상장 예정일",
    "이사회결의일", "자금용도", "투자자", "링크", "접수번호"
]

# Debug
OUTDIR = Path(os.getenv("OUTDIR", "out"))
DEBUGDIR = OUTDIR / "debug"


@dataclass
class Target:
    acpt_no: str
    title: str
    link: str


# =========================
# Generic helpers
# =========================
def extract_acpt_no(text: str) -> Optional[str]:
    m = re.search(r"acptNo=(\d{14})", text or "")
    return m.group(1) if m else None


def match_keyword(title: str) -> bool:
    return bool(title) and any(k in title for k in KEYWORDS)


def viewer_url(acpt_no: str, docno: str = "") -> str:
    return f"{BASE}/common/disclsviewer.do?method=searchInitInfo&acptNo={acpt_no}&docno={docno}"


def ensure_sheet_size(ws, extra_rows_needed: int, min_cols: int):
    if ws.col_count < min_cols:
        ws.add_cols(min_cols - ws.col_count)
    target_rows = ws.row_count + max(extra_rows_needed, 0) + 50
    if ws.row_count < target_rows:
        ws.add_rows(target_rows - ws.row_count)


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

    uniq = {}
    for t in targets:
        if t.acpt_no not in uniq:
            uniq[t.acpt_no] = t
    return list(uniq.values())


def is_block_page(html: str) -> bool:
    if not html:
        return True
    lower = html.lower()
    suspects = ["비정상", "접근이 제한", "차단", "권한", "error", "에러", "오류", "잠시 후", "관리자에게"]
    return any(s in lower for s in suspects) and ("<table" not in lower)


def frame_score(html: str) -> int:
    if not html:
        return -1
    lower = html.lower()
    tcnt = lower.count("<table")
    if tcnt == 0:
        return -1
    bonus_words = ["기준주가", "납입", "이사회", "할인", "할증", "발행", "청약", "사채", "교환", "전환", "유상"]
    bonus = sum(1 for w in bonus_words if w in lower)
    length_bonus = min(len(lower) // 2000, 50)
    return tcnt * 100 + bonus * 30 + length_bonus


def pick_best_frame_html(page) -> str:
    best_html = ""
    best_score = -1
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
        dfs = pd.read_html(html, header=None)  # header=None이 “라벨/값” 추출에 안정적
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
                df = one[0].where(pd.notnull(one[0]), "")
                results.append(df)
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
            norm = [r + [""] * (max_len - len(r)) for r in rows]
            results.append(pd.DataFrame(norm))

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


# =========================
# Google Sheets
# =========================
def gs_open():
    if not GOOGLE_SHEET_ID or not GOOGLE_CREDENTIALS_JSON:
        raise RuntimeError("GOOGLE_SHEET_ID / GOOGLE_CREDS(또는 GOOGLE_CREDENTIALS_JSON)가 비어있습니다.")

    creds = json.loads(GOOGLE_CREDENTIALS_JSON)
    gc = gspread.service_account_from_dict(creds)
    sh = gc.open_by_key(GOOGLE_SHEET_ID)

    # RAW dump
    try:
        dump_ws = sh.worksheet(DUMP_SHEET_NAME)
    except gspread.WorksheetNotFound:
        dump_ws = sh.add_worksheet(title=DUMP_SHEET_NAME, rows=2000, cols=80)

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

    return sh, dump_ws, seen_ws, rights_ws


def load_seen_from_sheet(seen_ws) -> Set[str]:
    col = seen_ws.col_values(1)
    vals = [x.strip() for x in col if x and x.strip().isdigit()]
    return set(vals)


def append_seen(seen_ws, acpt_no: str):
    seen_ws.append_row([acpt_no, datetime.now().strftime("%Y-%m-%d %H:%M:%S")], value_input_option="RAW")


def ensure_headers(ws, headers: List[str]):
    current = ws.row_values(1)
    if current != headers:
        end_a1 = rowcol_to_a1(1, len(headers))
        ws.update(f"A1:{end_a1}", [headers])


def load_key_index(ws, key_field: str) -> Dict[str, int]:
    """
    접수번호(acptNo) -> row 번호 인덱스(1-based)
    """
    ensure_headers(ws, RIGHTS_COLUMNS)
    key_idx = RIGHTS_COLUMNS.index(key_field) + 1
    col = ws.col_values(key_idx)
    idx = {}
    for r, v in enumerate(col, start=1):
        vv = str(v).strip()
        if vv.isdigit() and r > 1:
            idx[vv] = r
    return idx


def upsert_record(ws, index: Dict[str, int], headers: List[str], record: dict, key_field: str = "접수번호"):
    ensure_headers(ws, headers)
    key_val = str(record.get(key_field, "")).strip()
    row_values = [record.get(h, "") for h in headers]

    if key_val in index:
        r = index[key_val]
        end_a1 = rowcol_to_a1(r, len(headers))
        ws.update(f"A{r}:{end_a1}", [row_values])
    else:
        ws.append_row(row_values, value_input_option="RAW")
        # append 후 row 번호는 “현재 데이터 길이”로 근사 업데이트
        index[key_val] = len(ws.col_values(1))  # A열 길이 = 마지막 row


# =========================
# RAW dump (optional)
# =========================
def df_to_rowlists(df: pd.DataFrame) -> Tuple[List[str], List[List[str]]]:
    cols = [str(c) for c in list(df.columns)]
    values = []
    for _, row in df.iterrows():
        values.append([str(x) if x != "" else "" for x in row.tolist()])
    return cols, values


def build_dump_rows(acpt_no: str, title: str, src_url: str, dfs: List[pd.DataFrame], run_ts: str) -> List[List[str]]:
    rows: List[List[str]] = []
    rows.append([acpt_no, "", "META", title, src_url, run_ts])
    rows.append([acpt_no, "", "BLANK"])

    for i, df in enumerate(dfs):
        cols, data_rows = df_to_rowlists(df)
        rows.append([acpt_no, str(i), "TABLE_LABEL", f"tableIndex: {i}"])
        rows.append([acpt_no, str(i), "HEADER"] + cols)

        width = max(len(cols), max((len(r) for r in data_rows), default=0))
        for r in data_rows:
            r = r + [""] * (width - len(r))
            rows.append([acpt_no, str(i), "DATA"] + r)

        rows.append([acpt_no, "", "BLANK"])

    return rows


def append_rows_chunked(ws, rows: List[List[str]], min_cols: int = 220, chunk: int = 200):
    max_len = max((len(r) for r in rows), default=0)
    ensure_sheet_size(ws, extra_rows_needed=len(rows), min_cols=max(min_cols, max_len + 5))
    for i in range(0, len(rows), chunk):
        ws.append_rows(rows[i:i + chunk], value_input_option="RAW")
        time.sleep(0.2)


# =========================
# Rights issue parser (key-value extraction)
# =========================
def _norm(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", "", s)
    s = s.replace(":", "")
    return s


def _to_int(s: str) -> Optional[int]:
    if s is None:
        return None
    t = str(s)
    t = re.sub(r"[^\d\-]", "", t)
    if t in ("", "-"):
        return None
    try:
        return int(t)
    except:
        return None


def market_from_title(title: str) -> str:
    if not title:
        return ""
    if "[코]" in title:
        return "코스닥"
    if "[유]" in title:
        return "유가증권"
    if "[넥]" in title or "[코넥]" in title:
        return "코넥스"
    return ""


def scan_label_value(dfs: List[pd.DataFrame], label_candidates: List[str]) -> str:
    cand = {_norm(x) for x in label_candidates}
    for df in dfs:
        arr = df.astype(str).values
        rows, cols = arr.shape
        for r in range(rows):
            for c in range(cols):
                cell = _norm(arr[r][c])
                if cell in cand:
                    # 오른쪽
                    if c + 1 < cols:
                        v = str(arr[r][c + 1]).strip()
                        if v and v.lower() != "nan":
                            return v
                    # 같은 행 마지막
                    v2 = str(arr[r][cols - 1]).strip()
                    if v2 and v2.lower() != "nan" and _norm(v2) not in cand:
                        return v2
    return ""


def find_best_number_row(dfs: List[pd.DataFrame], must_include: List[str]) -> Tuple[str, Optional[int]]:
    """
    행 텍스트에 must_include들이 포함된 행에서 가장 큰 정수를 찾아 반환
    """
    keys = [_norm(x) for x in must_include]
    best_label = ""
    best_num = None
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
                    if best_num is None or m > best_num:
                        best_num = m
                        best_label = " ".join([x for x in row if x and x.lower() != "nan"])
    return best_label, best_num


def build_fund_use_text(dfs: List[pd.DataFrame]) -> str:
    keys = ["시설자금", "영업양수자금", "운영자금", "채무상환자금", "타법인취득자금", "기타자금"]
    parts = []
    for k in keys:
        _, n = find_best_number_row(dfs, [k])
        if n is not None:
            parts.append(f"{k}:{n:,}")
    return "; ".join(parts)


def parse_rights_issue_record(dfs: List[pd.DataFrame], title: str, acpt_no: str, link: str) -> dict:
    rec = {k: "" for k in RIGHTS_COLUMNS}
    rec["접수번호"] = acpt_no
    rec["링크"] = link

    rec["회사명"] = scan_label_value(dfs, ["회사명", "회사 명"]) or ""
    rec["상장시장"] = scan_label_value(dfs, ["상장시장", "시장구분", "시장 구분"]) or market_from_title(title)

    rec["이사회결의일"] = scan_label_value(dfs, ["이사회결의일", "이사회 결의일"]) or ""
    rec["최초 이사회결의일"] = scan_label_value(dfs, ["최초이사회결의일", "최초 이사회결의일"]) or rec["이사회결의일"]

    rec["증자방식"] = scan_label_value(dfs, ["증자방식", "발행방법", "발행 방법"]) or ""
    rec["투자자"] = (
        scan_label_value(dfs, ["제3자배정대상자", "제3자배정 대상자", "제3자배정대상자(명칭)"])
        or scan_label_value(dfs, ["대표주관사/투자자", "대표주관사", "투자자"])
    )

    # 신규발행주식수: "신주의 종류와 수"+"보통주식" 근처에서 숫자 찾기
    _, issue_shares = find_best_number_row(dfs, ["신주의종류와수", "보통주식"])
    if issue_shares is None:
        _, issue_shares = find_best_number_row(dfs, ["보통주식"])
    if issue_shares is not None:
        rec["발행상품"] = "보통주식"
        rec["신규발행주식수"] = f"{issue_shares:,}"

    # 증자전 주식수: "증자전 발행주식총수"+"보통주식"
    _, prev_shares = find_best_number_row(dfs, ["증자전발행주식총수", "보통주식"])
    if prev_shares is not None:
        rec["증자전 주식수"] = f"{prev_shares:,}"

    # 확정발행가 / 기준주가 / 할인율
    rec["확정발행가(원)"] = (
        scan_label_value(dfs, ["확정발행가", "신주발행가액", "신주 발행가액", "신주발행가", "신주 발행가"])
        or ""
    )
    rec["기준주가"] = scan_label_value(dfs, ["기준주가"]) or ""

    rec["할인(할증률)"] = (
        scan_label_value(dfs, ["할인(할증률)", "할인율", "할증율"])
        or scan_label_value(dfs, ["기준주가에대한할인율또는할증율(%)", "기준주가에 대한 할인율 또는 할증율 (%)"])
        or ""
    )

    # 납입/배당/상장예정
    rec["납입일"] = scan_label_value(dfs, ["납입일"]) or ""
    rec["신주의 배당기산일"] = scan_label_value(dfs, ["신주의배당기산일", "신주의 배당기산일"]) or ""
    rec["신주의 상장 예정일"] = scan_label_value(dfs, ["신주의상장예정일", "신주의 상장 예정일"]) or ""

    # 자금용도
    rec["자금용도"] = (
        scan_label_value(dfs, ["자금용도", "자금조달의목적", "자금조달의 목적"])
        or build_fund_use_text(dfs)
    )

    # 계산 보강: 확정발행금액(억원), 증자비율
    shares = _to_int(rec["신규발행주식수"])
    price = _to_int(rec["확정발행가(원)"])
    if shares and price:
        rec["확정발행금액(억원)"] = f"{(shares * price) / 100_000_000:,.2f}"

    prev = _to_int(rec["증자전 주식수"])
    if shares and prev and prev > 0:
        rec["증자비율"] = f"{shares / prev * 100:.2f}%"
    else:
        rec["증자비율"] = scan_label_value(dfs, ["증자비율"]) or rec["증자비율"]

    return rec


# =========================
# Main run
# =========================
def run():
    _, dump_ws, seen_ws, rights_ws = gs_open()
    seen_set = load_seen_from_sheet(seen_ws)

    # 유상증자 structured upsert용 인덱스 1회 로드
    rights_index = load_key_index(rights_ws, key_field="접수번호")

    if RUN_ONE_ACPTNO:
        targets = [Target(acpt_no=RUN_ONE_ACPTNO, title=f"MANUAL_{RUN_ONE_ACPTNO}", link="")]
    else:
        targets = parse_rss_targets()
        targets = [t for t in targets if t.acpt_no not in seen_set]
        targets = targets[:LIMIT] if LIMIT > 0 else targets

    if not targets:
        print("[INFO] 처리할 대상이 없습니다.")
        return

    run_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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

                # 1) 유상증자면 “정규 컬럼” 저장
                if "유상증자" in (t.title or ""):
                    rec = parse_rights_issue_record(dfs=dfs, title=t.title, acpt_no=t.acpt_no, link=src)
                    upsert_record(rights_ws, rights_index, RIGHTS_COLUMNS, rec, key_field="접수번호")

                # 2) (옵션) RAW_dump도 저장
                if SAVE_RAW:
                    rows = build_dump_rows(acpt_no=t.acpt_no, title=t.title, src_url=src, dfs=dfs, run_ts=run_ts)
                    append_rows_chunked(dump_ws, rows)

                # 3) 중복 방지 기록
                append_seen(seen_ws, t.acpt_no)
                ok += 1
                print(f"[OK] {t.acpt_no} -> rights_struct={'Y' if '유상증자' in (t.title or '') else 'N'} raw={'Y' if SAVE_RAW else 'N'}")

            except Exception as e:
                print(f"[FAIL] {t.acpt_no} {t.title} :: {e}")

            time.sleep(0.5)

        context.close()
        browser.close()

    print(f"[DONE] ok={ok}")
