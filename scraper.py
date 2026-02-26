import os
import re
import json
import time
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Tuple

import feedparser
import pandas as pd
from bs4 import BeautifulSoup
import gspread
from playwright.sync_api import sync_playwright


# =========================
# Config
# =========================
BASE = "https://kind.krx.co.kr"
DEFAULT_RSS = (
    "http://kind.krx.co.kr:80/disclosure/rsstodaydistribute.do"
    "?method=searchRssTodayDistribute&mktTpCd=0&currentPageSize=100"
)

RSS_URL = os.getenv("RSS_URL", DEFAULT_RSS)
KEYWORDS = [x.strip() for x in os.getenv("KEYWORDS", "유상증자,전환사채,교환사채").split(",") if x.strip()]
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
LIMIT = int(os.getenv("LIMIT", "20"))
RUN_ONE_ACPTNO = os.getenv("RUN_ONE_ACPTNO", "").strip()

SEEN_FILE = os.getenv("SEEN_FILE", "seen.json")

GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "").strip()
GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()

DUMP_SHEET_NAME = os.getenv("DUMP_SHEET_NAME", "dump")  # 탭 이름


@dataclass
class Target:
    acpt_no: str
    title: str
    link: str


def extract_acpt_no(text: str) -> Optional[str]:
    m = re.search(r"acptNo=(\d{14})", text or "")
    return m.group(1) if m else None


def match_keyword(title: str) -> bool:
    if not title:
        return False
    return any(k in title for k in KEYWORDS)


def viewer_url(acpt_no: str, docno: str = "") -> str:
    return f"{BASE}/common/disclsviewer.do?method=searchInitInfo&acptNo={acpt_no}&docno={docno}"


def load_seen() -> set:
    if os.path.exists(SEEN_FILE):
        try:
            with open(SEEN_FILE, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except Exception:
            return set()
    return set()


def save_seen(seen: set) -> None:
    with open(SEEN_FILE, "w", encoding="utf-8") as f:
        json.dump(sorted(list(seen)), f, ensure_ascii=False, indent=2)


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

    # 중복 제거(첫 등장만)
    uniq = {}
    for t in targets:
        if t.acpt_no not in uniq:
            uniq[t.acpt_no] = t
    return list(uniq.values())


def pick_best_frame_html(page) -> str:
    """
    공시뷰어 본문이 frame/iframe에 있을 수 있어 frames를 훑고
    <table>이 가장 많은 frame의 HTML을 선택.
    """
    best_html = ""
    best_cnt = -1
    for fr in page.frames:
        try:
            html = fr.content()
            cnt = html.lower().count("<table")
            if cnt > best_cnt:
                best_cnt = cnt
                best_html = html
        except Exception:
            continue
    return best_html


def extract_tables_from_html_robust(html: str) -> List[pd.DataFrame]:
    """
    ✅ 핵심: read_html이 한 번에 터지는 케이스를 방지
    - 전체 read_html 실패 시 table을 하나씩 분리 파싱
    - 그래도 안 되면 BeautifulSoup로 최후 수동 추출
    """
    html = (html or "").replace("\x00", "")

    # 1) 통째로 시도
    try:
        dfs = pd.read_html(html)
        return [df.where(pd.notnull(df), "") for df in dfs]
    except Exception:
        pass

    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    tables = soup.find_all("table")
    results: List[pd.DataFrame] = []

    for tbl in tables:
        # 2) table 단위로 read_html
        try:
            one = pd.read_html(str(tbl))
            if one:
                df = one[0].where(pd.notnull(one[0]), "")
                results.append(df)
                continue
        except Exception:
            pass

        # 3) 최후: 수동 파싱
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


def gs_client_and_sheet():
    if not GOOGLE_SHEET_ID or not GOOGLE_CREDENTIALS_JSON:
        raise RuntimeError("GOOGLE_SHEET_ID / GOOGLE_CREDENTIALS_JSON 이 비어있습니다. GitHub Secrets 설정 필요")

    creds = json.loads(GOOGLE_CREDENTIALS_JSON)
    gc = gspread.service_account_from_dict(creds)
    sh = gc.open_by_key(GOOGLE_SHEET_ID)

    # dump 탭 없으면 생성
    try:
        ws = sh.worksheet(DUMP_SHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=DUMP_SHEET_NAME, rows=2000, cols=40)

    return sh, ws


def df_to_rowlists(df: pd.DataFrame) -> Tuple[List[str], List[List[str]]]:
    # 컬럼/값을 전부 string으로
    cols = [str(c) for c in list(df.columns)]
    values = []
    for _, row in df.iterrows():
        values.append([str(x) if x != "" else "" for x in row.tolist()])
    return cols, values


def append_dump_to_sheet(ws, acpt_no: str, title: str, src_url: str, dfs: List[pd.DataFrame], run_ts: str):
    """
    dump 시트에 엑셀처럼 덤프하되, 추후 파싱/추적용 메타 컬럼을 앞에 붙임:
    [acptNo, tableIndex, rowType, ...data]
    """
    rows: List[List[str]] = []

    # 타이틀 블록
    rows.append([acpt_no, "", "TITLE", title, src_url, run_ts])
    rows.append([acpt_no, "", "BLANK"])

    for i, df in enumerate(dfs):
        cols, data_rows = df_to_rowlists(df)

        # 테이블 라벨
        rows.append([acpt_no, str(i), "TABLE_LABEL", f"tableIndex: {i}"])
        # 헤더
        rows.append([acpt_no, str(i), "HEADER"] + cols)

        # 데이터
        width = max(len(cols), max((len(r) for r in data_rows), default=0))
        for r in data_rows:
            r = r + [""] * (width - len(r))
            rows.append([acpt_no, str(i), "DATA"] + r)

        rows.append([acpt_no, "", "BLANK"])

    # 한 번에 append (빠름)
    ws.append_rows(rows, value_input_option="RAW")


def scrape_one(context, t: Target) -> Tuple[List[pd.DataFrame], str]:
    url = viewer_url(t.acpt_no)
    page = context.new_page()
    try:
        page.goto(url, wait_until="networkidle", timeout=60000)
        page.wait_for_timeout(1500)
        html = pick_best_frame_html(page)

        if not html or html.lower().count("<table") == 0:
            # 이 케이스는 보통 차단/에러 페이지거나 frame 선택 실패
            raise RuntimeError("table 0개로 보임 (차단/오류/프레임 문제 가능)")

        dfs = extract_tables_from_html_robust(html)
        return dfs, url
    finally:
        try:
            page.close()
        except Exception:
            pass


def run():
    seen = load_seen()

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

    # 구글시트 연결
    _, ws = gs_client_and_sheet()

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
                append_dump_to_sheet(ws, t.acpt_no, t.title, src, dfs, run_ts)
                seen.add(t.acpt_no)
                ok += 1
                print(f"[OK] {t.acpt_no} tables={len(dfs)} -> GoogleSheet:{DUMP_SHEET_NAME}")
            except Exception as e:
                print(f"[FAIL] {t.acpt_no} {t.title} :: {e}")
            time.sleep(0.5)

        context.close()
        browser.close()

    save_seen(seen)
    print(f"[DONE] ok={ok} / seen_total={len(seen)}")


if __name__ == "__main__":
    run()
