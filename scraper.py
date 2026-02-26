import os, re, json, time
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Set, Tuple

import feedparser
import pandas as pd
from bs4 import BeautifulSoup
import gspread
from playwright.sync_api import sync_playwright


BASE = "https://kind.krx.co.kr"
DEFAULT_RSS = (
    "http://kind.krx.co.kr:80/disclosure/rsstodaydistribute.do"
    "?method=searchRssTodayDistribute&mktTpCd=0&currentPageSize=100"
)

RSS_URL = os.getenv("RSS_URL", DEFAULT_RSS)
KEYWORDS = [x.strip() for x in os.getenv("KEYWORDS", "유상증자,전환사채,교환사채,신주인수권부사채").split(",") if x.strip()]
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
LIMIT = int(os.getenv("LIMIT", "30"))
RUN_ONE_ACPTNO = os.getenv("RUN_ONE_ACPTNO", "").strip()

GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "").strip()
GOOGLE_CREDENTIALS_JSON = (
    os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
    or os.environ.get("GOOGLE_CREDS", "").strip()   # 네 시크릿명
)

SEEN_SHEET_NAME = os.getenv("SEEN_SHEET_NAME", "seen")

# 보기용 탭 이름 (네 구글시트 탭과 동일)
CATEGORY_SHEET_MAP = {
    "유상증자": "유상증자",
    "전환사채": "전환사채",
    "교환사채": "교환사채",
    "신주인수권부사채": "신주인수권부사채",
}

# “다음에 붙일 row”를 저장할 셀(시트 맨 오른쪽 구석)
NEXT_ROW_CELL = "AA1"


@dataclass
class Target:
    acpt_no: str
    title: str
    link: str


def viewer_url(acpt_no: str, docno: str = "") -> str:
    return f"{BASE}/common/disclsviewer.do?method=searchInitInfo&acptNo={acpt_no}&docno={docno}"


def extract_acpt_no(text: str) -> Optional[str]:
    m = re.search(r"acptNo=(\d{14})", text or "")
    return m.group(1) if m else None


def match_keyword(title: str) -> bool:
    return bool(title) and any(k in title for k in KEYWORDS)


def detect_category_sheet(title: str) -> str:
    for k, sheet in CATEGORY_SHEET_MAP.items():
        if k in (title or ""):
            return sheet
    return CATEGORY_SHEET_MAP.get(KEYWORDS[0], "유상증자")


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

    # 중복 제거
    uniq = {}
    for t in targets:
        if t.acpt_no not in uniq:
            uniq[t.acpt_no] = t
    return list(uniq.values())


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
    보기용(사진처럼)으로 만들려면 header=None이 더 안정적임.
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
                df = one[0].where(pd.notnull(one[0]), "")
                results.append(df)
                continue
        except Exception:
            pass

        # 최후 수동 파싱
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


def gs_open():
    if not GOOGLE_SHEET_ID or not GOOGLE_CREDENTIALS_JSON:
        raise RuntimeError("GOOGLE_SHEET_ID / GOOGLE_CREDS 가 비어있습니다. Secrets 설정 필요")

    creds = json.loads(GOOGLE_CREDENTIALS_JSON)
    gc = gspread.service_account_from_dict(creds)
    sh = gc.open_by_key(GOOGLE_SHEET_ID)

    # seen 탭
    try:
        seen_ws = sh.worksheet(SEEN_SHEET_NAME)
    except gspread.WorksheetNotFound:
        seen_ws = sh.add_worksheet(title=SEEN_SHEET_NAME, rows=2000, cols=2)
        seen_ws.update("A1:B1", [["acptNo", "ts"]])

    return sh, seen_ws


def get_or_create_ws(sh, title: str):
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=2000, cols=30)
    return ws


def load_seen(seen_ws) -> Set[str]:
    col = seen_ws.col_values(1)
    return set([x.strip() for x in col if x and x.strip().isdigit()])


def append_seen(seen_ws, acpt_no: str):
    seen_ws.append_row([acpt_no, datetime.now().strftime("%Y-%m-%d %H:%M:%S")], value_input_option="RAW")


def ensure_sheet_size(ws, extra_rows: int, min_cols: int):
    if ws.col_count < min_cols:
        ws.add_cols(min_cols - ws.col_count)
    target_rows = ws.row_count + extra_rows + 50
    if ws.row_count < target_rows:
        ws.add_rows(target_rows - ws.row_count)


def get_next_row(ws) -> int:
    v = (ws.acell(NEXT_ROW_CELL).value or "").strip()
    if v.isdigit():
        return int(v)
    # 초기화 (처음만)
    # A열 기준 마지막 데이터 다음 줄로 잡음 (가벼운 방식)
    last = len(ws.col_values(1)) + 1
    ws.update(NEXT_ROW_CELL, str(last))
    return last


def bump_next_row(ws, new_next: int):
    ws.update(NEXT_ROW_CELL, str(new_next))


def pad_rows(rows: List[List[str]]) -> List[List[str]]:
    m = max((len(r) for r in rows), default=1)
    return [r + [""] * (m - len(r)) for r in rows]


def build_pretty_rows(acpt_no: str, title: str, dfs: List[pd.DataFrame]) -> Tuple[List[List[str]], List[int]]:
    """
    사진처럼:
    - 제목 1줄
    - 빈줄
    - tableIndex 줄 (굵게 처리)
    - 표 내용 그대로
    - 빈줄
    """
    rows: List[List[str]] = []
    table_label_rel_rows: List[int] = []

    rows.append([f"{title} (acptNo: {acpt_no})"])
    rows.append([""])

    for i, df in enumerate(dfs):
        table_label_rel_rows.append(len(rows))
        rows.append([f"tableIndex: {i}"])

        for r in df.values.tolist():
            rows.append([("" if x is None else str(x)) for x in r])

        rows.append([""])

    return rows, table_label_rel_rows


def append_rows_chunked(ws, rows: List[List[str]], chunk: int = 200):
    rows = pad_rows(rows)
    ensure_sheet_size(ws, extra_rows=len(rows), min_cols=max(30, len(rows[0]) + 2))
    for i in range(0, len(rows), chunk):
        ws.append_rows(rows[i:i + chunk], value_input_option="RAW")
        time.sleep(0.2)


def format_block(ws, start_row: int, table_label_rel_rows: List[int]):
    # 제목 줄 bold
    ws.format(f"A{start_row}:Z{start_row}", {"textFormat": {"bold": True}})
    # tableIndex 줄 bold + 연한 배경
    for rel in table_label_rel_rows:
        r = start_row + rel
        ws.format(
            f"A{r}:Z{r}",
            {
                "textFormat": {"bold": True},
                "backgroundColor": {"red": 0.95, "green": 0.95, "blue": 0.95},
            },
        )


def scrape_one(context, t: Target) -> List[pd.DataFrame]:
    url = viewer_url(t.acpt_no)
    page = context.new_page()
    try:
        page.goto(url, wait_until="networkidle", timeout=60000)
        page.wait_for_timeout(1500)

        html = pick_best_frame_html(page) or ""
        if html.lower().count("<table") == 0:
            raise RuntimeError("table 0개 (차단/프레임 문제 가능)")

        return extract_tables_from_html_robust(html)
    finally:
        try:
            page.close()
        except Exception:
            pass


def run():
    sh, seen_ws = gs_open()
    seen = load_seen(seen_ws)

    # 대상 선정
    if RUN_ONE_ACPTNO:
        targets = [Target(acpt_no=RUN_ONE_ACPTNO, title=f"MANUAL_{RUN_ONE_ACPTNO}", link="")]
    else:
        targets = parse_rss_targets()
        targets = [t for t in targets if t.acpt_no not in seen]
        targets = targets[:LIMIT] if LIMIT > 0 else targets

    if not targets:
        print("[INFO] 처리할 대상이 없습니다.")
        return

    # 카테고리 탭 준비
    ws_map = {name: get_or_create_ws(sh, name) for name in CATEGORY_SHEET_MAP.values()}

    ok = 0
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

        for t in targets:
            try:
                dfs = scrape_one(context, t)

                sheet_name = detect_category_sheet(t.title)
                ws = ws_map[sheet_name]

                start = get_next_row(ws)

                rows, table_label_rel_rows = build_pretty_rows(t.acpt_no, t.title, dfs)
                append_rows_chunked(ws, rows)

                # 포맷(굵게/배경)
                format_block(ws, start, table_label_rel_rows)

                # 다음 row 포인터 업데이트
                bump_next_row(ws, start + len(pad_rows(rows)))

                # 중복방지 기록
                append_seen(seen_ws, t.acpt_no)
                ok += 1
                print(f"[OK] {t.acpt_no} -> {sheet_name} (tables={len(dfs)})")

            except Exception as e:
                print(f"[FAIL] {t.acpt_no} {t.title} :: {e}")

            time.sleep(0.6)

        context.close()
        browser.close()

    print(f"[DONE] ok={ok}")
