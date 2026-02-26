import os, json, time
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs

import requests
import pandas as pd
import feedparser
import gspread
from google.oauth2.service_account import Credentials

# fallback
from playwright.sync_api import sync_playwright


# =========================
# Config
# =========================
DEFAULT_KIND_RSS_URL = (
    "http://kind.krx.co.kr:80/disclosure/rsstodaydistribute.do"
    "?method=searchRssTodayDistribute&mktTpCd=0&currentPageSize=100"
)

# env에 빈 문자열이 들어오는 케이스(Secrets 미설정/빈 값) 방지
_rss_env = os.getenv("KIND_RSS_URL")
KIND_RSS_URL = _rss_env if _rss_env else DEFAULT_KIND_RSS_URL

BASE_POPUP_URL = "https://kind.krx.co.kr/common/disclsviewer.do"

TARGET_KEYWORDS = [
    "유상증자결정",
    "전환사채권발행결정",
    "교환사채권발행결정",
]

SHEET_ID = os.environ["GOOGLE_SHEET_ID"]
CREDS_JSON = os.environ["GOOGLE_CREDENTIALS_JSON"]

TAB_RAW = os.getenv("TAB_RAW", "RAW_POPUP")
TAB_SEEN = os.getenv("TAB_SEEN", "SEEN")
TAB_LOGS = os.getenv("TAB_LOGS", "LOGS")


# =========================
# Utils
# =========================
def now_ts():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def safe_str(x):
    if x is None:
        return ""
    return str(x).strip()


def extract_acptno_from_link(link: str) -> str:
    # link 예: ...disclsviewer.do?method=searchInitInfo&acptNo=202602200001169&docno=
    try:
        q = parse_qs(urlparse(link).query)
        return safe_str(q.get("acptNo", [""])[0])
    except Exception:
        return ""


def is_target_title(title: str) -> bool:
    t = safe_str(title)
    return any(k in t for k in TARGET_KEYWORDS)


# =========================
# Google Sheet
# =========================
def open_sheet():
    creds_dict = json.loads(CREDS_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)
    return sh


def get_or_create_worksheet(sh, title: str, rows: int = 2000, cols: int = 26):
    try:
        return sh.worksheet(title)
    except Exception:
        ws = sh.add_worksheet(title=title, rows=str(rows), cols=str(cols))
        return ws


def ensure_headers(ws_seen, ws_logs):
    # SEEN headers
    seen_vals = ws_seen.get_values("A1:D1")
    if not seen_vals or not seen_vals[0] or safe_str(seen_vals[0][0]).lower() != "id":
        ws_seen.update("A1:D1", [["id", "ts", "title", "link"]])

    # LOGS headers
    logs_vals = ws_logs.get_values("A1:E1")
    if not logs_vals or not logs_vals[0] or safe_str(logs_vals[0][0]).lower() != "ts":
        ws_logs.update("A1:E1", [["ts", "status", "id", "title", "error"]])


def read_seen_ids(ws_seen):
    col = ws_seen.col_values(1)
    ids = set(x.strip() for x in col[1:] if x.strip())
    return ids


def log_append(ws_logs, status, _id, title, error=""):
    ws_logs.append_row([now_ts(), status, _id, title, error], value_input_option="RAW")


# =========================
# Fetch popup HTML
# =========================
def fetch_popup_html_requests(acpt_no: str, docno: str = "") -> str:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://kind.krx.co.kr/",
    }
    params = {"method": "searchInitInfo", "acptNo": acpt_no, "docno": docno}
    r = requests.get(BASE_POPUP_URL, params=params, headers=headers, timeout=30)
    r.raise_for_status()
    r.encoding = r.apparent_encoding
    return r.text


def fetch_popup_html_playwright(acpt_no: str, docno: str = "") -> str:
    params = f"method=searchInitInfo&acptNo={acpt_no}&docno={docno}"
    url = f"{BASE_POPUP_URL}?{params}"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, wait_until="networkidle", timeout=60000)
        html = page.content()
        browser.close()
        return html


# =========================
# Parse tables
# =========================
def parse_tables_from_html(html: str) -> list[pd.DataFrame]:
    try:
        tables = pd.read_html(html)  # list of dfs
    except ValueError:
        return []

    cleaned = []
    for df in tables:
        df = df.fillna("").astype(str)
        cleaned.append(df)
    return cleaned


# =========================
# Dump rows (like screenshot)
# =========================
def build_dump_rows(title: str, link: str, acpt_no: str, tables: list[pd.DataFrame]):
    rows = []
    rows.append([f"{title} (acptNo: {acpt_no})"])
    rows.append([link])
    rows.append([""])

    for i, df in enumerate(tables):
        rows.append([f"tableIndex: {i}"])
        cols = [safe_str(c) for c in df.columns.tolist()]
        rows.append(cols)
        for r in df.values.tolist():
            rows.append([safe_str(x) for x in r])
        rows.append([""])
        rows.append([""])
    return rows


def append_dump(ws_raw, rows):
    ws_raw.append_rows(rows, value_input_option="RAW")


# =========================
# Main
# =========================
def run():
    sh = open_sheet()

    ws_raw = get_or_create_worksheet(sh, TAB_RAW, rows=5000, cols=30)
    ws_seen = get_or_create_worksheet(sh, TAB_SEEN, rows=5000, cols=10)
    ws_logs = get_or_create_worksheet(sh, TAB_LOGS, rows=5000, cols=10)

    ensure_headers(ws_seen, ws_logs)

    seen = read_seen_ids(ws_seen)

    feed = feedparser.parse(KIND_RSS_URL)
    items = feed.entries if hasattr(feed, "entries") else []

    processed = 0

    for it in items:
        title = safe_str(getattr(it, "title", ""))
        link = safe_str(getattr(it, "link", ""))
        guid = safe_str(getattr(it, "id", "")) or safe_str(getattr(it, "guid", ""))

        if not title or not link:
            continue
        if not is_target_title(title):
            continue

        acpt_no = extract_acptno_from_link(link)
        # acptNo가 없으면 guid라도 id로 사용 (최소 중복방지)
        _id = f"KIND:{acpt_no}" if acpt_no else f"KINDGUID:{guid}" if guid else f"KINDLINK:{link}"

        if _id in seen:
            continue

        try:
            if not acpt_no:
                raise RuntimeError("acptNo not found in link (cannot open popup reliably).")

            # 1) requests 시도
            html = fetch_popup_html_requests(acpt_no)
            tables = parse_tables_from_html(html)

            # 2) 표가 0개면 playwright 재시도
            if len(tables) == 0:
                html = fetch_popup_html_playwright(acpt_no)
                tables = parse_tables_from_html(html)

            if len(tables) == 0:
                raise RuntimeError("No tables found in popup HTML (requests+playwright).")

            rows = build_dump_rows(title, link, acpt_no, tables)
            append_dump(ws_raw, rows)

            # SEEN 기록
            ws_seen.append_row([_id, now_ts(), title, link], value_input_option="RAW")
            log_append(ws_logs, "OK", _id, title, "")

            seen.add(_id)
            processed += 1

            time.sleep(1.0)  # KIND 차단/부하 방지

        except Exception as e:
            log_append(ws_logs, "ERROR", _id, title, repr(e))

    print(f"done. processed={processed}")


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print("FATAL:", repr(e))
        raise
