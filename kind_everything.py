import os, json, re, time
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs

import requests
import pandas as pd
import feedparser
import gspread
from google.oauth2.service_account import Credentials

# (fallback) playwright
from playwright.sync_api import sync_playwright


# =========================
# 0) Config
# =========================
KIND_RSS_URL = os.getenv(
    "KIND_RSS_URL",
    "http://kind.krx.co.kr:80/disclosure/rsstodaydistribute.do?method=searchRssTodayDistribute&mktTpCd=0&currentPageSize=100"
)
BASE_POPUP_URL = "https://kind.krx.co.kr/common/disclsviewer.do"

TARGET_KEYWORDS = [
    "유상증자결정",
    "전환사채권발행결정",
    "교환사채권발행결정",
]

SHEET_ID = os.environ["GOOGLE_SHEET_ID"]
CREDS_JSON = os.environ["GOOGLE_CREDENTIALS_JSON"]

TAB_RAW = "RAW_POPUP"
TAB_SEEN = "SEEN"
TAB_LOGS = "LOGS"


# =========================
# 1) Google Sheet connect
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


# =========================
# 2) Helpers
# =========================
def now_ts():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def safe_str(x):
    if x is None:
        return ""
    return str(x).strip()


def extract_acptno(link: str) -> str:
    # link 예: ...disclsviewer.do?method=searchInitInfo&acptNo=202602200001169&docno=
    q = parse_qs(urlparse(link).query)
    return safe_str(q.get("acptNo", [""])[0])


def is_target_title(title: str) -> bool:
    t = safe_str(title)
    return any(k in t for k in TARGET_KEYWORDS)


def read_seen_ids(ws_seen):
    # SEEN 탭 A열에 id가 쌓인다고 가정
    col = ws_seen.col_values(1)
    # 첫 행 헤더 제거
    ids = set(x.strip() for x in col[1:] if x.strip())
    return ids


def log_append(ws_logs, status, _id, title, error=""):
    ws_logs.append_row([now_ts(), status, _id, title, error], value_input_option="RAW")


# =========================
# 3) Fetch popup HTML (requests)
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


# =========================
# 4) Fetch popup HTML (playwright fallback)
# =========================
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
# 5) Parse all tables
# =========================
def parse_tables_from_html(html: str) -> list[pd.DataFrame]:
    tables = pd.read_html(html)  # list of dfs
    cleaned = []
    for df in tables:
        df = df.fillna("").astype(str)
        cleaned.append(df)
    return cleaned


# =========================
# 6) Dump like screenshot (tableIndex)
# =========================
def build_dump_rows(title: str, link: str, acpt_no: str, tables: list[pd.DataFrame]):
    rows = []
    rows.append([f"{title} (acptNo: {acpt_no})"])
    rows.append([link])
    rows.append([""])

    for i, df in enumerate(tables):
        rows.append([f"tableIndex: {i}"])
        # 컬럼이 숫자/NaN일 수도 있으니 문자열로
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
# 7) Main: RSS → filter → dedup → dump
# =========================
def run():
    sh = open_sheet()
    ws_raw = sh.worksheet(TAB_RAW)
    ws_seen = sh.worksheet(TAB_SEEN)
    ws_logs = sh.worksheet(TAB_LOGS)

    seen = read_seen_ids(ws_seen)

    feed = feedparser.parse(KIND_RSS_URL)
    items = feed.entries if hasattr(feed, "entries") else []

    # 최신부터 처리하고 싶으면 reverse 조정
    processed = 0

    for it in items:
        title = safe_str(getattr(it, "title", ""))
        link = safe_str(getattr(it, "link", ""))
        if not title or not link:
            continue
        if not is_target_title(title):
            continue

        acpt_no = extract_acptno(link)
        if not acpt_no:
            continue

        _id = f"KIND:{acpt_no}"
        if _id in seen:
            continue

        try:
            # 1) requests 시도
            html = fetch_popup_html_requests(acpt_no)
            tables = parse_tables_from_html(html)

            # 표가 너무 적거나 0개면 playwright로 재시도
            if len(tables) == 0:
                html = fetch_popup_html_playwright(acpt_no)
                tables = parse_tables_from_html(html)

            rows = build_dump_rows(title, link, acpt_no, tables)
            append_dump(ws_raw, rows)

            # SEEN 기록
            ws_seen.append_row([_id, now_ts(), title, link], value_input_option="RAW")
            log_append(ws_logs, "OK", _id, title, "")

            seen.add(_id)
            processed += 1

            time.sleep(1.0)  # KIND 부하/차단 방지용

        except Exception as e:
            log_append(ws_logs, "ERROR", _id, title, repr(e))

    print(f"done. processed={processed}")


if __name__ == "__main__":
    run()    try:
        r = requests.get(header_url, timeout=10)
        for m in re.finditer(r'<option\s+value="(\d+)\|([^"]+)"', r.text):
            dl_url = f"https://kind.krx.co.kr/common/applcmn.do?method=download&apndNo={m.group(1)}"
            if ".pdf" in m.group(2).lower(): pdf_link = dl_url
            elif ".xls" in m.group(2).lower() or "excel" in m.group(2).lower(): excel_link = dl_url
    except: pass
    return excel_link, pdf_link

def run_macro():
    tabs, existing_acptnos = connect_gs()
    seen_list = set(load_json(SEEN_FILE, []))
    
    to_date = datetime.now().strftime("%Y-%m-%d")
    from_date = (datetime.now() - timedelta(days=SEARCH_DAYS_AGO)).strftime("%Y-%m-%d")
    print(f"\n🚀 KIND 매크로 로봇 시동... [{from_date} ~ {to_date}]")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(locale="ko-KR")
        page = context.new_page()

        for kw in KEYWORDS:
            print(f"\n▶️ [{kw}] 탭 검색 시작...")
            
            # 1. 사이트 접속
            page.goto("https://kind.krx.co.kr/disclosure/details.do", wait_until="networkidle")
            
            # 2. 날짜 입력 (자바스크립트 강제 입력 방식)
            page.evaluate(f"document.getElementById('fromDate').value = '{from_date}';")
            page.evaluate(f"document.getElementById('toDate').value = '{to_date}';")
            
            # 3. 사람처럼 검색어 입력 후 버튼 클릭
            page.fill("#reportNm", kw)
            page.click("a.btn1:has-text('검색')")
            
            # 4. 결과창 로딩 대기
            page.wait_for_timeout(3000)
            
            # 5. 표 데이터 긁어오기
            rows = page.locator("table.list tbody tr").all()
            for row in rows:
                cols = row.locator("td").all()
                if len(cols) < 4: continue
                
                # 데이터 파싱
                pub_time = cols[0].inner_text().strip()
                company = cols[1].inner_text().strip()
                
                # 상장시장 아이콘 파싱 (유가, 코스닥 등)
                market = ""
                img_loc = cols[1].locator("img")
                if img_loc.count() > 0:
                    market = img_loc.first.get_attribute("alt") or ""
                
                title_el = cols[2].locator("a").first
                title = title_el.inner_text().strip()
                onclick_text = title_el.get_attribute("onclick") or ""
                
                acptno_m = re.search(r"openDisclsViewer\('(\d+)'", onclick_text)
                if not acptno_m: continue
                acptno = acptno_m.group(1)
                
                if acptno in seen_list or acptno in existing_acptnos:
                    continue
                    
                # 공시 링크 및 Excel, PDF 링크 조립
                link = f"https://kind.krx.co.kr/common/disclsviewer.do?method=search&acptno={acptno}"
                excel_link, pdf_link = get_attachment_links(acptno)
                
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                # 구글 시트 저장
                ws = tabs[kw]
                try:
                    row_id = get_next_id(ws)
                    final_row = [row_id, now, pub_time, company, market, title, excel_link, pdf_link, link]
                    ws.append_row(final_row, value_input_option="USER_ENTERED")
                    print(f" ✔️ [{company}] '{kw}' 시트에 추가 완료!")
                except Exception as e:
                    print(f" ❌ 시트 저장 실패: {e}")
                    continue
                
                seen_list.add(acptno)
                existing_acptnos.add(acptno)
                
    save_json(SEEN_FILE, list(seen_list))
    print("\n✅ 모든 매크로 작업이 성공적으로 완료되었습니다!")

if __name__ == "__main__":
    run_macro()
