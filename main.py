import os, json, time, re
from datetime import datetime
from urllib.parse import urlparse, parse_qs

import feedparser
import requests
import gspread
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# =========================
# 1. Config & Constants
# =========================
BOT_VERSION = "kind-bot-v4.0-clean"
RSS_URL = "http://kind.krx.co.kr:80/disclosure/rsstodaydistribute.do?method=searchRssTodayDistribute&mktTpCd=0&currentPageSize=100"
BASE_URL = "https://kind.krx.co.kr"

# 수집 대상 공시 명확화 (결정 공시만)
TARGET_KEYWORDS = ["유상증자결정", "전환사채권발행결정", "교환사채권발행결정"]

SHEET_NAME = os.getenv("SHEET_NAME", "KIND_대경")
SEEN_FILE = os.getenv("SEEN_FILE", "seen.json")
RETRY_FILE = os.getenv("RETRY_FILE", "retry_queue.json")

# 시트 헤더 필드
ISSUE_FIELDS = [
    "회사명", "상장시장", "최초 이사회결의일", "증자방식", "발행상품", "신규발행주식수", 
    "확정발행가(원)", "기준주가", "확정발행금액(억원)", "할인(할증률)", "증자전 주식수", 
    "증자비율", "청약일", "납입일", "주관사", "자금용도", "투자자", "증자금액"
]

# 파싱용 키워드 사전 (정규화된 형태로 매칭하기 위해 공백 제거 버전을 내부적으로 사용)
ALIASES = {
    "최초 이사회결의일": ["이사회결의일", "결의일", "결정일"],
    "증자방식": ["증자방식", "발행방식", "배정방법", "사채발행방법"],
    "발행상품": ["발행상품", "신주의종류", "주식의종류", "증권종류", "사채의종류"],
    "신규발행주식수": ["신규발행주식수", "발행주식수", "신주수", "전환에따라발행할주식", "교환에따라발행할주식"],
    "확정발행가(원)": ["신주발행가액", "발행가액", "전환가액", "교환가액", "1주당발행가액"],
    "기준주가": ["기준주가", "기준주가액"],
    "확정발행금액(억원)": ["모집총액", "발행총액", "사채의권면총액", "권면총액"],
    "할인(할증률)": ["할인율", "할증률"],
    "증자전 주식수": ["증자전발행주식총수", "기발행주식총수", "발행주식총수"],
    "증자비율": ["증자비율", "주식총수대비비율"],
    "청약일": ["청약일", "청약기간", "청약시작일"],
    "납입일": ["납입일", "대금납입일", "납입기일"],
    "주관사": ["주관사", "대표주관회사", "인수회사", "인수단"],
    "자금용도": ["자금용도", "자금조달의목적", "자금사용목적"],
    "투자자": ["투자자", "제3자배정대상자", "배정대상자", "인수인", "발행대상자"],
    "증자금액": ["증자금액", "발행규모", "조달금액", "모집금액"]
}

# =========================
# 2. Utility Functions
# =========================
def clean_text(text):
    """HTML 텍스트의 공백과 줄바꿈을 깔끔하게 정리합니다."""
    if not text: return ""
    return re.sub(r'\s+', ' ', text).strip()

def is_valid_value(val):
    """추출된 값이 유효한지(빈칸이나 대시가 아닌지) 검사합니다."""
    v = val.replace(" ", "")
    if not v or v in ("-", "—", ".", "0", "해당사항없음"): return False
    return True

def extract_acptno(url):
    qs = parse_qs(urlparse(url).query)
    if 'acptno' in qs: return qs['acptno'][0]
    if 'acptNo' in qs: return qs['acptNo'][0]
    return None

def save_json(filepath, data):
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def load_json(filepath):
    if os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)
        except: pass
    return []

# =========================
# 3. Core Parsing Logic (완전 재작성)
# =========================
def parse_disclosure_html(html_content):
    """
    HTML 문서의 모든 테이블을 순회하며 키-값 쌍을 찾아냅니다.
    """
    soup = BeautifulSoup(html_content, 'lxml')
    result = {k: "" for k in ISSUE_FIELDS}
    
    # 문서 내의 모든 테이블 행(tr) 탐색
    for tr in soup.find_all('tr'):
        cells = tr.find_all(['th', 'td'])
        if len(cells) < 2: continue
        
        for i, cell in enumerate(cells):
            cell_txt_raw = cell.get_text(strip=True)
            cell_txt_norm = cell_txt_raw.replace(" ", "").replace("\n", "")
            
            # 항목명(Key) 매칭 확인
            for field, aliases in ALIASES.items():
                if result[field]: continue # 이미 찾은 값이면 패스
                
                if any(alias in cell_txt_norm for alias in aliases):
                    # 키워드를 찾았으면 '바로 다음 칸'의 데이터를 가져옴
                    if i + 1 < len(cells):
                        next_cell_val = clean_text(cells[i+1].get_text(" ", strip=True))
                        if is_valid_value(next_cell_val):
                            result[field] = next_cell_val
                            break
    return result

def get_full_html_via_playwright(viewer_url):
    """
    Playwright를 사용해 공시 뷰어에 접속한 뒤, 데이터가 있는 프레임의 HTML을 가져옵니다.
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        try:
            page.goto(viewer_url, wait_until="networkidle", timeout=20000)
            page.wait_for_timeout(2000) # 렌더링 대기
            
            combined_html = ""
            # 모든 프레임의 내용을 하나로 합쳐서 파싱 확률을 높임
            for frame in page.frames:
                try:
                    content = frame.content()
                    if "<table" in content.lower():
                        combined_html += content
                except:
                    continue
                    
            return combined_html
        except Exception as e:
            print(f"Playwright 로드 에러: {e}")
            return ""
        finally:
            browser.close()

# =========================
# 4. Google Sheets Logic
# =========================
def setup_sheets():
    creds_dict = json.loads(os.environ["GOOGLE_CREDS"])
    creds = Credentials.from_service_account_info(
        creds_dict, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    client = gspread.authorize(creds)
    sh = client.open(SHEET_NAME)
    
    sheets = {}
    for title in ["RAW", "유상증자", "전환사채", "교환사채"]:
        try:
            sheets[title] = sh.worksheet(title)
        except gspread.exceptions.WorksheetNotFound:
            sheets[title] = sh.add_worksheet(title=title, rows="1000", cols="30")
            
    # 헤더 세팅
    issue_header = ["ID", "수집시간", "공시일시", "회사명", "상장시장", "공시제목", "공시링크"] + ISSUE_FIELDS + ["상태"]
    for title in ["유상증자", "전환사채", "교환사채"]:
        if not sheets[title].row_values(1):
            sheets[title].update("A1", [issue_header])
            
    return sheets

def get_next_id(ws):
    col = ws.col_values(1)
    if len(col) > 1 and col[-1].isdigit():
        return int(col[-1]) + 1
    return 1

# =========================
# 5. Main Execution
# =========================
def main():
    print(f"🚀 {BOT_VERSION} 시작")
    sheets = setup_sheets()
    seen_list = load_json(SEEN_FILE)
    
    session = requests.Session()
    feed = feedparser.parse(session.get(RSS_URL).content)
    
    # 1. 타겟 공시 필터링
    target_items = []
    for entry in feed.entries:
        title = entry.title
        link = entry.link
        guid = entry.id or link
        
        if guid in seen_list: continue
        if not any(k in title for k in TARGET_KEYWORDS): continue
        
        # 회사명, 시장 추출 (예: [코]아미코젠 -> 코, 아미코젠)
        m = re.match(r"^\[([^\]]+)\]\s*([^\s]+)", title.strip())
        market = m.group(1) if m else ""
        company = m.group(2) if m else ""
        
        target_items.append({
            "title": title, "link": link, "guid": guid, 
            "pub": entry.published, "company": company, "market": market
        })

    print(f"📌 파싱 대상 공시: {len(target_items)}건")

    # 2. 파싱 및 시트 기록
    for item in target_items:
        print(f"\n[분석중] {item['title']}")
        
        acptno = extract_acptno(item['link'])
        if not acptno: continue
        
        viewer_url = f"{BASE_URL}/common/disclsviewer.do?method=search&acptno={acptno}"
        html_content = get_full_html_via_playwright(viewer_url)
        
        if not html_content:
            print(" ❌ 페이지 로드 실패")
            continue
            
        # 데이터 추출
        parsed_data = parse_disclosure_html(html_content)
        filled_count = sum(1 for v in parsed_data.values() if v)
        
        # 데이터가 5개 이상 채워지면 성공으로 간주
        status = "SUCCESS" if filled_count >= 5 else "INCOMPLETE"
        
        # 타겟 시트 결정
        target_ws_name = "유상증자"
        if "전환사채" in item['title']: target_ws_name = "전환사채"
        elif "교환사채" in item['title']: target_ws_name = "교환사채"
        
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        target_ws = sheets[target_ws_name]
        rid = get_next_id(target_ws)
        
        row_data = [
            rid, now, item['pub'], item['company'], item['market'], item['title'], item['link']
        ] + [parsed_data[k] for k in ISSUE_FIELDS] + [status]
        
        try:
            target_ws.append_row(row_data, value_input_option="USER_ENTERED")
            print(f" ✅ 시트 저장 완료 ({target_ws_name}, {filled_count}개 필드 채워짐)")
            seen_list.append(item['guid'])
        except Exception as e:
            print(f" ❌ 시트 저장 실패: {e}")
            
        time.sleep(1)

    save_json(SEEN_FILE, seen_list)
    print("\n🎉 모든 작업 완료!")

if __name__ == "__main__":
    main()
