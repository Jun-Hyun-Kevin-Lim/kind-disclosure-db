import os, json, time, re
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs
import requests
import gspread
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials

# =========================
# Config & Setup
# =========================
BOT_VERSION = os.getenv("BOT_VERSION", "kind-bot-v13-post-search")
SHEET_NAME = "KIND_대경"
SEEN_FILE = "seen.json"
RETRY_FILE = "retry_queue.json"
BASE = "https://kind.krx.co.kr"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept-Language": "ko-KR,ko;q=0.9",
}

SLEEP_SECONDS = float(os.getenv("SLEEP_SECONDS", "1"))

# ✨ 며칠 치 데이터를 검색할지 설정 (기본 1일=오늘. 만약 데이터가 없으면 3, 7 등으로 바꿔서 테스트해보세요!)
SEARCH_DAYS_AGO = 5 

COLS_YU = [
    "ID", "수집시간", "Excel Link", "PDF Link", 
    "회사명", "보고서명", "상장시장", "최초 이사회결의일", "증자방식", "발행상품", 
    "신규발행주식수", "확정발행가(원)", "기준주가", "확정발행금액(억원)", "할인(할증률)", 
    "증자전 주식수", "증자비율", "납입일", "신주의 배당기산일", "신주의 상장 예정일", 
    "이사회결의일", "자금용도", "투자자", "링크", "접수번호"
]

COLS_BOND = [
    "ID", "수집시간", "Excel Link", "PDF Link", 
    "회사명", "상장시장", "최초 이사회결의일", "권면총액(원)", "Coupon", "YTM", 
    "만기", "전환청구 시작", "전환청구 종료", "Put Option", "Call Option", 
    "Call 비율", "YTC", "모집방식", "발행상품", "행사(전환)가액(원)", 
    "전환주식수", "주식총수대비 비율", "Refixing Floor", "납입일", 
    "자금용도", "투자자", "링크", "접수번호"
]

KEYWORDS_YU = ["유상증자"]
KEYWORDS_BOND = ["전환사채", "신주인수권부사채", "교환사채"]
ALL_KEYWORDS = KEYWORDS_YU + KEYWORDS_BOND

ALIASES = {
    "최초 이사회결의일": ["최초 이사회결의일", "이사회결의일", "결의일", "결정일"],
    "이사회결의일": ["이사회결의일", "결의일", "결정일"],
    "증자방식": ["증자방식", "배정방식", "배정방법"],
    "발행상품": ["발행상품", "신주의 종류", "주식의 종류", "사채의 종류"],
    "신규발행주식수": ["신규발행주식수", "발행신주수", "신주수", "발행할 주식의 수"],
    "확정발행가(원)": ["확정발행가", "신주발행가액", "발행가액", "1주당 발행가액", "예정발행가"],
    "기준주가": ["기준주가", "기준주가액"],
    "확정발행금액(억원)": ["확정발행금액", "모집총액", "자금조달목적", "조달금액", "모집금액"],
    "할인(할증률)": ["할인율", "할증률", "할인율(%)"],
    "증자전 주식수": ["증자전 주식수", "기발행주식총수", "증자전 발행주식총수"],
    "증자비율": ["증자비율", "증자비율(%)"],
    "납입일": ["납입일", "대금납입일", "청약기일"],
    "신주의 배당기산일": ["신주의 배당기산일", "배당기산일"],
    "신주의 상장 예정일": ["신주의 상장 예정일", "상장예정일", "상장 예정일"],
    "자금용도": ["자금용도", "조달목적", "자금조달의 목적", "자금사용 목적"],
    "투자자": ["투자자", "배정대상자", "제3자배정 대상자", "인수인", "제3자 배정대상자"],
    "권면총액(원)": ["권면총액", "사채의 권면총액", "발행총액"],
    "Coupon": ["표면이자율", "표면 이자율", "표면금리"],
    "YTM": ["만기보장수익률", "만기수익률", "만기이자율"],
    "만기": ["사채만기일", "만기일", "상환기일"],
    "전환청구 시작": ["전환청구기간 시작일", "권리행사기간 시작일", "행사기간 시작일"],
    "전환청구 종료": ["전환청구기간 종료일", "권리행사기간 종료일", "행사기간 종료일"],
    "Put Option": ["조기상환청구권", "풋옵션", "Put Option"],
    "Call Option": ["매도청구권", "콜옵션", "Call Option"],
    "Call 비율": ["매도청구권 비율", "콜옵션 비율"],
    "YTC": ["YTC", "조기상환수익률"],
    "모집방식": ["사채발행방법", "모집방법", "발행방법", "모집방식"],
    "행사(전환)가액(원)": ["전환가액", "행사가액", "교환가액"],
    "전환주식수": ["전환할 주식 수", "행사할 주식 수", "교환할 주식 수"],
    "주식총수대비 비율": ["주식총수 대비 비율", "발행주식총수 대비 비율", "주식총수대비"],
    "Refixing Floor": ["최저조정가액", "조정 한도", "리픽싱 한도", "최저 조정가액"]
}

def norm(s: str) -> str:
    return re.sub(r"\s+", "", str(s or "")).lower()

def load_json(filepath, default_val):
    if os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f: return json.load(f)
        except: pass
    return default_val

def save_json(filepath, data):
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def connect_gs_and_setup_tabs():
    creds_dict = json.loads(os.environ["GOOGLE_CREDS"])
    creds = Credentials.from_service_account_info(
        creds_dict, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    )
    client = gspread.authorize(creds)
    sh = client.open(SHEET_NAME)
    tabs = {}
    existing_acptnos = set()

    for kw in ALL_KEYWORDS:
        try:
            ws = sh.worksheet(kw)
            acpt_col_idx = len(COLS_YU) if kw in KEYWORDS_YU else len(COLS_BOND)
            acpts = ws.col_values(acpt_col_idx)[1:] 
            for acpt in acpts:
                if str(acpt).strip().isdigit(): existing_acptnos.add(str(acpt).strip())
        except gspread.exceptions.WorksheetNotFound:
            print(f"[GS] '{kw}' 시트 생성 중...")
            cols_to_use = COLS_YU if kw in KEYWORDS_YU else COLS_BOND
            ws = sh.add_worksheet(title=kw, rows="1000", cols=str(len(cols_to_use) + 5))
            ws.append_row(cols_to_use)
        tabs[kw] = ws
    return tabs, existing_acptnos

def get_next_id(ws):
    col = ws.col_values(1)
    if len(col) <= 1: return 1
    last = str(col[-1]).strip()
    if last.isdigit(): return int(last) + 1
    mx = 0
    for v in col[1:]:
        if str(v).strip().isdigit(): mx = max(mx, int(v))
    return mx + 1

# ✨ 직접 찾아주신 "POST 검색" 로직 완벽 적용!
def search_kind_by_keyword(session, keyword, from_date, to_date):
    url = "https://kind.krx.co.kr/disclosure/details.do"
    payload = {
        "method": "searchDetailsMain",
        "fromDate": from_date,
        "toDate": to_date,
        "reportNm": keyword,  # <- 이 부분! 직접 보고서명에 키워드를 넣어 검색합니다.
        "currentPageSize": "100",
        "pageIndex": "1"
    }
    
    r = session.post(url, data=payload, headers=DEFAULT_HEADERS)
    soup = BeautifulSoup(r.text, 'html.parser')
    
    results = []
    for tr in soup.find_all('tr'):
        tds = tr.find_all('td')
        if len(tds) >= 4:
            company = tds[1].get_text(strip=True)
            title_a = tds[2].find('a')
            
            if title_a and 'openDisclsViewer' in title_a.get('onclick', ''):
                title = title_a.get_text(strip=True)
                acptno_match = re.search(r"openDisclsViewer\('(\d+)'", title_a['onclick'])
                if acptno_match:
                    acptno = acptno_match.group(1)
                    link = f"https://kind.krx.co.kr/common/disclsviewer.do?method=search&acptno={acptno}"
                    results.append({
                        "title": title,
                        "company": company,
                        "link": link,
                        "acptno": acptno,
                        "keyword_matched": keyword
                    })
    return results

# ✨ HTML 본문 파싱 (다운로드 없이 화면에서 바로 값 추출)
def extract_data_from_html(session, acptno):
    extracted = {k: "" for k in ALIASES.keys()}
    excel_link, pdf_link = "", ""
    
    try:
        viewer_url = f"{BASE}/common/disclsviewer.do?method=search&acptno={acptno}"
        vr = session.get(viewer_url, headers=DEFAULT_HEADERS)
        
        for m in re.finditer(r'<option\s+value="(\d+)\|([^"]+)"', vr.text):
            dl_url = f"https://kind.krx.co.kr/common/applcmn.do?method=download&apndNo={m.group(1)}"
            if ".pdf" in m.group(2).lower(): pdf_link = dl_url
            elif ".xls" in m.group(2).lower() or "excel" in m.group(2).lower(): excel_link = dl_url

        docnos = re.findall(r"docno=(\d+)", vr.text) or re.findall(r"(\d{10,14})\|", vr.text)
        if not docnos: return excel_link, pdf_link, extracted
        
        doc_url = f"{BASE}/common/disclsviewer.do?method=docdisclsviewer&acptno={acptno}&docno={docnos[0]}"
        r = session.get(doc_url, headers=DEFAULT_HEADERS)
        soup = BeautifulSoup(r.text, 'html.parser')
        
        for table in soup.find_all('table'):
            for tr in table.find_all('tr'):
                cells = tr.find_all(['th', 'td'])
                for i, cell in enumerate(cells):
                    raw_text = cell.get_text(separator=" ", strip=True)
                    text_norm = norm(raw_text)
                    
                    for key, aliases in ALIASES.items():
                        if not extracted[key]:
                            if any(a in text_norm for a in aliases):
                                if ":" in raw_text or "：" in raw_text:
                                    parts = re.split(r'[:|：]', raw_text)
                                    val = parts[-1].strip()
                                    if val and val not in ["-", "—", ""]:
                                        extracted[key] = val
                                        continue
                                
                                for offset in range(1, 3):
                                    if i + offset < len(cells):
                                        val = cells[i + offset].get_text(separator=" ", strip=True)
                                        if val and val not in ["-", "—", ""]:
                                            extracted[key] = val
                                            break
    except Exception as e:
        pass
        
    return excel_link, pdf_link, extracted

def main():
    tabs, existing_acptnos = connect_gs_and_setup_tabs()
    seen_list = set(load_json(SEEN_FILE, []))
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    
    # 검색할 날짜 계산 (기본 0 = 오늘만. 만약 테스트하고 싶다면 SEARCH_DAYS_AGO를 3 정도로 올리세요)
    to_date = datetime.now().strftime("%Y-%m-%d")
    from_date = (datetime.now() - timedelta(days=SEARCH_DAYS_AGO)).strftime("%Y-%m-%d")
    
    print(f"\n[{from_date} ~ {to_date}] 직접 검색 수집 시작...")
    
    all_items = []
    
    # 1. 각각의 키워드를 하나씩 KIND 서버에 직접 검색 (POST)
    for kw in ALL_KEYWORDS:
        print(f" -> '{kw}' 검색 중...")
        results = search_kind_by_keyword(session, kw, from_date, to_date)
        all_items.extend(results)
        time.sleep(1) # 서버에 무리 가지 않게 1초 대기
        
    # 중복 공시 제거
    unique_items = {item["acptno"]: item for item in all_items}.values()
    
    print(f"\n[QUEUE] 대상 공시: {len(unique_items)}건 발견!")
    if not unique_items:
        print(f"✅ 설정된 기간({from_date} ~ {to_date})에 올라온 공시가 없습니다.")
        return
    
    new_retry = []
    for item in unique_items:
        title, link, company, acptno, matched_kw = item["title"], item["link"], item["company"], item["acptno"], item["keyword_matched"]
        
        if acptno in existing_acptnos or acptno in seen_list:
            continue
            
        print(f"\nProcessing: [{company}] {title}")
        
        # 2. 본문 HTML 파싱하여 데이터 쏙쏙 빼오기
        excel_link, pdf_link, ex_data = extract_data_from_html(session, acptno)

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        is_success = True
        
        # 3. 구글 시트에 저장
        ws = tabs[matched_kw]
        target_cols = COLS_YU if matched_kw in KEYWORDS_YU else COLS_BOND
        try:
            row_id = get_next_id(ws)
            row_dict = {
                "ID": row_id, "수집시간": now, "Excel Link": excel_link, "PDF Link": pdf_link,
                "회사명": company, "보고서명": title, "상장시장": "", 
                "링크": link, "접수번호": acptno
            }
            row_dict.update(ex_data)
            
            final_row = [row_dict.get(col_name, "") for col_name in target_cols]
            ws.append_row(final_row, value_input_option="USER_ENTERED")
            print(f" -> '{matched_kw}' 시트 저장 완료 (데이터 {sum(1 for v in final_row[4:-2] if v)}개 획득)")
        except Exception as e:
            print(f" -> [GS Error for {matched_kw}] {e}")
            is_success = False
        
        if is_success:
            seen_list.add(acptno)
            existing_acptnos.add(acptno)
        else:
            new_retry.append(item)
        
        time.sleep(SLEEP_SECONDS)

    save_json(SEEN_FILE, list(seen_list))
    print("\n✅ 오늘치 작업 완료!")

if __name__ == "__main__":
    main()
