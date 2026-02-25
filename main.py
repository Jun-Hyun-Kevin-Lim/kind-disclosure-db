import os, json, time, re
from datetime import datetime
from urllib.parse import urlparse, parse_qs
import feedparser
import requests
import gspread
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials

# =========================
# Config
# =========================
BOT_VERSION = os.getenv("BOT_VERSION", "kind-bot-v2")
RSS_URL = "https://kind.krx.co.kr/disclosure/rsstodaydistribute.do?method=searchRssTodayDistribute&repIsuSrtCd=&mktTpCd=0&searchCorpName=&currentPageSize=200"

# 새로운 탭 이름이자 검색 키워드
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
    # 설정된 키워드대로 각각의 탭(Worksheet)을 연결하거나 생성합니다.
    for kw in KEYWORDS:
        try:
            ws = sh.worksheet(kw)
        except gspread.exceptions.WorksheetNotFound:
            print(f"[GS] Worksheet '{kw}' not found. Creating new one.")
            ws = sh.add_worksheet(title=kw, rows="1000", cols="10")
            # 새 시트 생성 시 헤더 추가
            ws.append_row(["ID", "수집시간", "공시일시", "공시제목", "공시링크", "Excel", "PDF"])
        tabs[kw] = ws

    print(f"[BOT] {BOT_VERSION} | Opened spreadsheet='{sh.title}'")
    return tabs

def get_next_id(ws):
    col = ws.col_values(1)
    if len(col) <= 1:
        return 1  # 1행이 헤더이므로 첫 데이터는 1
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

def extract_acptno_from_link(link: str, html_text: str):
    qs = parse_qs(urlparse(link).query)
    acpt = (qs.get("acptno") or qs.get("acptNo") or [None])[0]
    if acpt: return acpt
    
    m = re.search(r"(acptno|acptNo)=(\d{8,14})", html_text, re.I)
    if m: return m.group(2)
    return None

def extract_attachments(html_text: str):
    """
    공시 뷰어의 헤더 프레임에서 첨부파일(Excel, PDF) 다운로드 링크를 추출합니다.
    """
    excel_link, pdf_link = "", ""
    soup = BeautifulSoup(html_text, "lxml")
    
    # 첨부파일은 보통 <option> 태그나 <a> 태그 안에 존재합니다.
    for tag in soup.find_all(["a", "option"]):
        text = tag.get_text(strip=True).lower()
        value = tag.get("value", "")
        onclick = tag.get("onclick", "")
        href = tag.get("href", "")
        
        apnd_no = None
        
        # 다운로드 apndNo 추출 로직
        if "apndno=" in href.lower():
            m = re.search(r"apndno=(\d+)", href, re.I)
            if m: apnd_no = m.group(1)
        elif "download" in onclick.lower() or "file" in onclick.lower():
            m = re.search(r"['\"](\d{4,})['\"]", onclick)
            if m: apnd_no = m.group(1)
        elif value.isdigit() and len(value) > 3:
            apnd_no = value
            
        if apnd_no:
            # KIND 실제 파일 다운로드 URL 조립
            download_url = f"https://kind.krx.co.kr/common/applcmn.do?method=download&apndNo={apnd_no}"
            
            if ".pdf" in text or "pdf" in text:
                if not pdf_link: pdf_link = download_url
            elif any(ext in text for ext in [".xls", ".xlsx", "excel", "엑셀"]):
                if not excel_link: excel_link = download_url
                
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
        # 지정된 4개 키워드 중 하나라도 제목에 포함되어 있는지 확인
        if not any(k in title for k in KEYWORDS): continue
        if guid in seen_list: continue
        
        items.append({"title": title, "link": link, "guid": guid, "pub": pub})
    
    items.extend(retry_queue)
    
    # 중복 제거
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
        print(f"\nProcessing: {title}")
        
        # 제목에 포함된 키워드 찾기 (예: "유상증자"와 "전환사채"가 동시에 있다면 둘 다 해당)
        matched_kws = [k for k in KEYWORDS if k in title]
        if not matched_kws:
            continue
        
        link_res = fetch(session, link, referer=f"{BASE}/")
        acptno = extract_acptno_from_link(link, link_res.text)
        
        if not acptno:
            print(" [FAIL] acptNo not found")
            new_retry.append(item)
            continue

        # 첨부파일 정보가 들어있는 헤더 프레임 직접 호출
        header_url = f"{BASE}/common/disclsviewer.do?method=header&acptno={acptno}"
        header_res = fetch(session, header_url, referer=link)
        
        # 파일 링크 추출
        excel_link, pdf_link = extract_attachments(header_res.text)

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        is_success = True
        
        # 매칭된 모든 탭(시트)에 데이터 추가
        for kw in matched_kws:
            ws = tabs[kw]
            try:
                row_id = get_next_id(ws)
                # 요청하신 7개 컬럼 매핑
                row_data = [
                    row_id,        # ID
                    now,           # 수집시간
                    pub,           # 공시일시
                    title,         # 공시제목
                    link,          # 공시링크
                    excel_link,    # Excel 링크
                    pdf_link       # PDF 링크
                ]
                ws.append_row(row_data, value_input_option="USER_ENTERED")
                print(f" -> Saved to '{kw}' 시트 | Excel: {'O' if excel_link else 'X'} | PDF: {'O' if pdf_link else 'X'}")
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
