import os, json, time, re
from datetime import datetime, timedelta
import requests
import gspread
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright

# =========================
# 설정 (사진에 나온 4개 탭 및 9개 컬럼)
# =========================
SHEET_NAME = "KIND_대경"
SEEN_FILE = "seen.json"
BASE = "https://kind.krx.co.kr"

KEYWORDS = ["유상증자", "전환사채", "신주인수권부사채", "교환사채"]
COLS = ["ID", "수집시간", "공시일시", "회사명", "상장시장", "공시제목", "Excel Link", "PDF Link", "공시링크"]

# 테스트/실전을 위해 무조건 최근 7일 동안의 공시를 검색
SEARCH_DAYS_AGO = 7 

def load_json(filepath, default_val):
    if os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f: return json.load(f)
        except: pass
    return default_val

def save_json(filepath, data):
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def connect_gs():
    creds_dict = json.loads(os.environ["GOOGLE_CREDS"])
    creds = Credentials.from_service_account_info(
        creds_dict, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    )
    client = gspread.authorize(creds)
    sh = client.open(SHEET_NAME)
    tabs, existing_acptnos = {}, set()

    for kw in KEYWORDS:
        try:
            ws = sh.worksheet(kw)
            acpts = ws.col_values(9)[1:] # I열(공시링크) 가져오기
            for acpt in acpts:
                m = re.search(r"acptno=(\d+)", str(acpt), re.I)
                if m: existing_acptnos.add(m.group(1))
        except gspread.exceptions.WorksheetNotFound:
            print(f"[GS] '{kw}' 시트 생성 중...")
            ws = sh.add_worksheet(title=kw, rows="1000", cols="10")
            ws.append_row(COLS)
        tabs[kw] = ws
    return tabs, existing_acptnos

def get_next_id(ws):
    col = ws.col_values(1)
    if len(col) <= 1: return 1
    return max([int(v) for v in col[1:] if str(v).strip().isdigit()] + [0]) + 1

# 첨부파일 링크(Excel, PDF) 확보 로직
def get_attachment_links(acptno):
    header_url = f"{BASE}/common/disclsviewer.do?method=header&acptno={acptno}"
    excel_link, pdf_link = "", ""
    try:
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
