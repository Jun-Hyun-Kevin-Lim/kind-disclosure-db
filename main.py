import os
import json
import time
import re
import html
import requests
import feedparser
import gspread
from datetime import datetime
from urllib.parse import urlparse, parse_qs, urlencode
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# =========================
# 1. 환경 설정 및 상수
# =========================
class Config:
    BOT_VERSION = os.getenv("BOT_VERSION", "kind-bot-v1.1")
    DEBUG = os.getenv("DEBUG_HTML", "0") == "1"
    RSS_URL = "https://kind.krx.co.kr/disclosure/rsstodaydistribute.do?method=searchRssTodayDistribute&repIsuSrtCd=&mktTpCd=0&searchCorpName=&currentPageSize=200"
    BASE_URL = "https://kind.krx.co.kr"
    
    KEYWORDS = ["전환사채", "유상증자", "교환사채"]
    TARGET_KEYS = [
        "최초 이사회결의일", "증자방식", "발행상품", "신규발행주식수", "확정발행가(원)", 
        "기준주가", "확정발행금액(억원)", "할인(할증률)", "증자전 주식수", "증자비율", 
        "청약일", "납입일", "주관사", "자금용도", "투자자", "증자금액"
    ]
    
    # 별칭 맵핑 (데이터 추출용)
    ALIASES = {
        "최초 이사회결의일": ["이사회결의일", "결의일", "결정일", "이사회 결의일"],
        "증자방식": ["증자방식", "발행방식", "배정방법", "배정방식"],
        "발행상품": ["신주의 종류", "주식의 종류", "증권종류", "발행상품"],
        "신규발행주식수": ["신규발행주식수", "발행주식수", "발행할 주식의 수", "신주수", "증자할 주식수"],
        "확정발행가(원)": ["확정발행가", "신주발행가액", "발행가", "발행가액", "1주당 발행가액"],
        "기준주가": ["기준주가", "기준주가액"],
        "확정발행금액(억원)": ["확정발행금액", "모집총액", "발행총액", "발행금액", "모집금액", "조달금액"],
        "할인(할증률)": ["할인율", "할증률", "할인율(%)"],
        "증자전 주식수": ["증자전 주식수", "증자전 발행주식총수", "발행주식총수", "기발행주식총수"],
        "증자비율": ["증자비율", "증자비율(%)"],
        "청약일": ["청약일", "청약기간", "청약시작일"],
        "납입일": ["납입일", "대금납입일"],
        "주관사": ["주관사", "대표주관회사", "공동주관회사", "인수회사", "인수단"],
        "자금용도": ["자금용도", "자금조달의 목적", "자금사용 목적", "자금조달 목적"],
        "투자자": ["투자자", "제3자배정대상자", "배정대상자", "발행대상자", "대상자", "인수인"],
        "증자금액": ["증자금액", "발행규모", "조달금액", "모집금액", "총 조달금액"],
    }

# =========================
# 2. 크롤링 엔진 클래스
# =========================
class KindBot:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept-Language": "ko-KR,ko;q=0.9"
        })
        self.raw_ws, self.issue_ws = self._connect_google_sheets()

    def _connect_google_sheets(self):
        creds_dict = json.loads(os.environ["GOOGLE_CREDS"])
        creds = Credentials.from_service_account_info(
            creds_dict, 
            scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        )
        client = gspread.authorize(creds)
        sh = client.open(os.getenv("SHEET_NAME", "KIND_대경"))
        return sh.worksheet("RAW"), sh.worksheet("ISSUE")

    def _norm(self, s):
        return re.sub(r"\s+", "", str(s or "")).lower()

    def get_next_id(self):
        col = self.raw_ws.col_values(1)
        if len(col) <= 1: return 1
        nums = [int(v) for v in col if str(v).isdigit()]
        return max(nums) + 1 if nums else 1

    # --- 데이터 파싱 로직 ---
    def parse_table_to_matrix(self, table):
        matrix = []
        span_map = {}
        for r_idx, tr in enumerate(table.find_all("tr")):
            row = []
            c_idx = 0
            while True:
                while (r_idx, c_idx) in span_map:
                    row.append(span_map.pop((r_idx, c_idx)))
                    c_idx += 1
                
                cells = tr.find_all(["th", "td"])
                if not cells: break # 빈 줄 방지
                
                for cell in cells:
                    while (r_idx, c_idx) in span_map:
                        row.append(span_map.pop((r_idx, c_idx)))
                        c_idx += 1
                        
                    txt = cell.get_text(" ", strip=True)
                    rs = int(cell.get("rowspan", 1))
                    cs = int(cell.get("colspan", 1))
                    
                    for _ in range(cs):
                        row.append(txt)
                        for r_offset in range(1, rs):
                            span_map[(r_idx + r_offset, c_idx)] = txt
                        c_idx += 1
                break
            matrix.append(row)
        return matrix

    def extract_values(self, matrices):
        results = {k: "" for k in Config.TARGET_KEYS}
        for key in Config.TARGET_KEYS:
            aliases = [self._norm(a) for a in Config.ALIASES[key]]
            for mtx in matrices:
                for row in mtx:
                    for i, cell in enumerate(row):
                        cell_norm = self._norm(re.sub(r"^\d+\.\s*", "", cell))
                        if any(a in cell_norm for a in aliases):
                            # 다음 열에서 값 찾기
                            for next_val in row[i+1:]:
                                if next_val and next_val not in ("-", "—"):
                                    results[key] = next_val
                                    break
                        if results[key]: break
                    if results[key]: break
        return results

    # --- 브라우저 자동화 ---
    def get_dynamic_content(self, url):
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            try:
                page.goto(url, wait_until="networkidle", timeout=20000)
                page.wait_for_timeout(3000)
                
                best_html = ""
                max_tables = -1
                
                for frame in page.frames:
                    try:
                        c = frame.content()
                        t_count = c.lower().count("<table") + c.lower().count("&lt;table")
                        if t_count > max_tables:
                            max_tables = t_count
                            best_html = c
                    except: continue
                return best_html
            finally:
                browser.close()

# =========================
# 3. 메인 실행 루프
# =========================
def main():
    bot = KindBot()
    seen_path = "seen.json"
    seen_list = []
    if os.path.exists(seen_path):
        with open(seen_path, "r") as f: seen_list = json.load(f)

    print(f"🚀 KIND Bot Starting... (Version: {Config.BOT_VERSION})")
    
    # RSS 가져오기
    resp = bot.session.get(Config.RSS_URL)
    feed = feedparser.parse(resp.content)
    
    for entry in feed.entries:
        if entry.id in seen_list: continue
        if not any(k in entry.title for k in Config.KEYWORDS): continue
        
        print(f"🔍 처리 중: {entry.title}")
        
        try:
            # 1. acptNo 추출
            acpt_match = re.search(r"acptno=(\d+)", entry.link)
            if not acpt_match: continue
            acpt_no = acpt_match.group(1)
            
            # 2. Viewer 접근 및 docNo 선택 (간략화)
            viewer_url = f"{Config.BASE_URL}/common/disclsviewer.do?method=search&acptno={acpt_no}"
            html_content = bot.get_dynamic_content(viewer_url)
            
            if not html_content or "<title>창 닫기</title>" in html_content:
                print(f"⚠️ 본문을 불러오지 못했습니다: {entry.title}")
                continue

            # 3. 파싱
            soup = BeautifulSoup(html.unescape(html_content), "lxml")
            tables = soup.find_all("table")
            matrices = [bot.parse_table_to_matrix(t) for t in tables]
            fields = bot.extract_values(matrices)
            
            # 4. 저장
            filled_count = sum(1 for v in fields.values() if v)
            status = "SUCCESS" if filled_count >= 10 else "INCOMPLETE"
            
            row_id = bot.get_next_id()
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # RAW 탭
            bot.raw_ws.append_row([row_id, now, entry.published, entry.title, entry.link, entry.id, status])
            
            # ISSUE 탭
            company = re.sub(r"\[.*?\]", "", entry.title).split()[0]
            issue_data = [row_id, now, entry.published, entry.title, entry.link, entry.id, company, "KIND"]
            issue_data += [fields[k] for k in Config.TARGET_KEYS]
            bot.issue_ws.append_row(issue_data)
            
            seen_list.append(entry.id)
            print(f"✅ 저장 완료 (Status: {status}, Filled: {filled_count})")
            
        except Exception as e:
            print(f"❌ 에러 발생 ({entry.title}): {e}")
        
        time.sleep(1)

    with open(seen_path, "w") as f:
        json.dump(seen_list[-500:], f) # 최근 500개만 유지

if __name__ == "__main__":
    main()
