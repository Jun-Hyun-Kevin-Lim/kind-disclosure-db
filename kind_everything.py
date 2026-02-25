import os, json, time, re
from datetime import datetime
from urllib.parse import urlparse, parse_qs
import feedparser
import requests
import gspread
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# =========================
# Config
# =========================
BOT_VERSION = os.getenv("BOT_VERSION", "kind-bot-v3")
RSS_URL = "https://kind.krx.co.kr/disclosure/rsstodaydistribute.do?method=searchRssTodayDistribute&repIsuSrtCd=&mktTpCd=0&searchCorpName=&currentPageSize=200"

KEYWORDS = ["유상증자", "전환사채", "신주인수권부사채", "교환사채"]
SHEET_NAME = "KIND_대경"
SEEN_FILE = "seen.json"
RETRY_FILE = "retry_queue.json"
BASE = "https://kind.krx.co.kr"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
}

SLEEP_SECONDS = float(os.getenv("SLEEP_SECONDS", "1"))
PW_NAV_TIMEOUT_MS = int(os.getenv("PW_NAV_TIMEOUT_MS", "20000"))

# =========================
# Utils
# =========================
def load_json(filepath, default_val):
    if os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            pass
    return default_val

def save_json(filepath, data):
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def fetch(session: requests.Session, url: str, referer: str | None = None, timeout=25):
    headers = dict(DEFAULT_HEADERS)
    if referer:
        headers["Referer"] = referer
    r = session.get(url, headers=headers, timeout=timeout, allow_redirects=True)
    if not r.encoding or r.encoding.lower() == "iso-8859-1":
        r.encoding = r.apparent_encoding or "utf-8"
    return r

def connect_gs():
    creds_dict = json.loads(os.environ["GOOGLE_CREDS"])
    creds = Credentials.from_service_account_info(
        creds_dict,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    client = gspread.authorize(creds)
    sh = client.open(SHEET_NAME)

    tabs = {}
    for kw in KEYWORDS:
        try:
            ws = sh.worksheet(kw)
        except gspread.exceptions.WorksheetNotFound:
            print(f"[GS] Worksheet '{kw}' not found. Creating new one.")
            ws = sh.add_worksheet(title=kw, rows="1000", cols="10")
            ws.append_row(["ID", "수집시간", "공시일시", "회사명", "상장시장", "공시제목", "Excel Link", "PDF Link", "공시링크"])
        tabs[kw] = ws

    print(f"[BOT] {BOT_VERSION} | Opened spreadsheet='{sh.title}'")
    return tabs

def get_next_id(ws):
    col = ws.col_values(1)
    if len(col) <= 1:
        return 1
    last = str(col[-1]).strip()
    if last.isdigit():
        return int(last) + 1
    mx = 0
    for v in col[1:]:
        if str(v).strip().isdigit():
            mx = max(mx, int(v))
    return mx + 1

def fetch_rss(session):
    r = fetch(session, RSS_URL, referer=f"{BASE}/")
    feed = feedparser.parse(r.content)
    print(f"[RSS] status={r.status_code} bytes={len(r.content)} entries={len(feed.entries)}")
    return feed

def extract_company_from_title(title: str):
    m = re.match(r"^\[([^\]]+)\]\s*([^\s]+)", (title or "").strip())
    return m.group(2) if m else ""

def extract_market_code_from_title(title: str):
    m = re.match(r"^\[([^\]]+)\]", (title or "").strip())
    return m.group(1).strip() if m else ""

def extract_acptno_from_link(link: str, html_text: str):
    qs = parse_qs(urlparse(link).query)
    acpt = (qs.get("acptno") or qs.get("acptNo") or [None])[0]
    if acpt: return acpt
    
    m = re.search(r"(acptno|acptNo)=(\d{8,14})", html_text, re.I)
    if m: return m.group(2)
    return None

def get_attachment_links_via_playwright(acptno: str) -> tuple[str, str]:
    """
    Playwright를 사용하여 KIND 공시 뷰어를 열고 첨부파일 링크를 추출합니다.
    """
    viewer_url = f"{BASE}/common/disclsviewer.do?method=search&acptno={acptno}"
    excel_link, pdf_link = "", ""
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(locale="ko-KR")
        page = context.new_page()
        page.set_default_navigation_timeout(PW_NAV_TIMEOUT_MS)
        
        try:
            page.goto(viewer_url, wait_until="networkidle")
            page.wait_for_timeout(2500)  # 프레임 안의 데이터가 로드될 시간 확보
            
            # 모든 프레임을 돌면서 첨부파일 링크(a 태그나 option 태그)를 찾음
            for fr in page.frames:
                elements = fr.query_selector_all("a, option")
                for el in elements:
                    text = (el.inner_text() or "").strip().lower()
                    onclick = el.get_attribute("onclick") or ""
                    href = el.get_attribute("href") or ""
                    value = el.get_attribute("value") or ""
                    
                    apnd_no = None
                    
                    # 다운로드 번호 추출 패턴 확인
                    if "apndno=" in href.lower():
                        m = re.search(r"apndno=(\d+)", href, re.I)
                        if m: apnd_no = m.group(1)
                    elif "download" in onclick.lower() or "file" in onclick.lower():
                        m = re.search(r"['\"](\d{4,})['\"]", onclick)
                        if m: apnd_no = m.group(1)
                    elif value.isdigit() and len(value) > 3:
                        apnd_no = value
                        
                    # 파일 번호를 찾았을 경우 다이렉트 링크 조립
                    if apnd_no:
                        download_url = f"https://kind.krx.co.kr/common/applcmn.do?method=download&apndNo={apnd_no}"
                        
                        if ".pdf" in text or "pdf" in text:
                            if not pdf_link: pdf_link = download_url
                        elif any(ext in text for ext in [".xls", ".xlsx", "excel", "엑셀"]):
                            if not excel_link: excel_link = download_url
        except PWTimeout:
            print(" [PW Error] 페이지 로딩 타임아웃")
        except Exception as e:
            print(f" [PW Error] {e}")
        finally:
            browser.close()
            
    return excel_link, pdf_link

def main():
    tabs = connect_gs()
    seen_list = load_json(SEEN_FILE, [])
    retry_queue = load_json(RETRY_FILE, [])
    session = requests.Session()
    feed = fetch_rss(session)
    
    items = []
    for entry in feed.entries:
        title = entry.get("title", "") or ""
        link = entry.get("link", "") or ""
        guid = entry.get("id") or link
        pub = entry.get("published", "") or ""
        
        if not guid: continue
        if not any(k in title for k in KEYWORDS): continue
        if guid in seen_list: continue
        
        items.append({"title": title, "link": link, "guid": guid, "pub": pub})
    
    items.extend(retry_queue)
    
    uniq = {}
    for it in items: uniq[it["guid"]] = it
    items = list(uniq.values())
    
    print(f"[QUEUE] to_process={len(items)} seen={len(seen_list)} retry={len(retry_queue)}")
    if not items:
        print("✅ 모든 작업 완료! (새로운 공시 없음)")
        return

    new_retry = []
    for item in items:
        title, link, guid, pub = item["title"], item["link"], item["guid"], item.get("pub", "")
        company = extract_company_from_title(title)
        market_code = extract_market_code_from_title(title)
        print(f"\nProcessing: {title}")
        
        matched_kws = [k for k in KEYWORDS if k in title]
        if not matched_kws:
            continue
        
        link_res = fetch(session, link, referer=f"{BASE}/")
        acptno = extract_acptno_from_link(link, link_res.text)
        
        if not acptno:
            print(" [FAIL] acptNo not found")
            new_retry.append(item)
            continue

        # Playwright를 이용해 다운로드 링크 확보
        excel_link, pdf_link = get_attachment_links_via_playwright(acptno)

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        is_success = True
        
        # 스크린샷에 맞춘 9개 컬럼 배열 구성
        for kw in matched_kws:
            ws = tabs[kw]
            try:
                row_id = get_next_id(ws)
                row_data = [
                    row_id,         # A: ID
                    now,            # B: 수집시간
                    pub,            # C: 공시일시
                    company,        # D: 회사명
                    market_code,    # E: 상장시장
                    title,          # F: 공시제목
                    excel_link,     # G: Excel Link
                    pdf_link,       # H: PDF Link
                    link            # I: 공시링크
                ]
                ws.append_row(row_data, value_input_option="USER_ENTERED")
                print(f" -> Saved to '{kw}' | Excel: {'O' if excel_link else 'X'} | PDF: {'O' if pdf_link else 'X'}")
            except Exception as e:
                print(f" -> [Google Sheets Error for {kw}] {e}")
                is_success = False
        
        if is_success:
            if guid not in seen_list:
                seen_list.append(guid)
        else:
            new_retry.append(item)
        
        time.sleep(SLEEP_SECONDS)

    save_json(SEEN_FILE, seen_list)
    save_json(RETRY_FILE, new_retry)
    print("\n✅ 모든 작업 완료!")

if __name__ == "__main__":
    main()
