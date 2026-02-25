import os, json, time, re
from datetime import datetime
import requests
import gspread
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials

# =========================
# Config & Setup
# =========================
BOT_VERSION = os.getenv("BOT_VERSION", "kind-bot-v11-details-search")
SHEET_NAME = "KIND_대경"
SEEN_FILE = "seen.json"
RETRY_FILE = "retry_queue.json"
BASE = "https://kind.krx.co.kr"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept-Language": "ko-KR,ko;q=0.9",
}

SLEEP_SECONDS = float(os.getenv("SLEEP_SECONDS", "1"))

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
    return max([int(v) for v in col[1:] if str(v).strip().isdigit()] + [0]) + 1

# ✨ 제안해주신 상세검색(오늘 날짜) 기반으로 공시 목록 가져오기
def get_todays_disclosures(session):
    today_str = datetime.now().strftime("%Y-%m-%d")
    url = "https://kind.krx.co.kr/disclosure/details.do"
    payload = {
        "method": "searchDetailsMain",
        "fromDate": today_str,
        "toDate": today_str,
        "pageIndex": 1,
        "currentPageSize": 200
    }
    
    r = session.post(url, data=payload, headers=DEFAULT_HEADERS)
    soup = BeautifulSoup(r.text, 'html.parser')
    
    items = []
    # 검색 결과 테이블 분석
    for tr in soup.find_all('tr'):
        tds = tr.find_all('td')
        if len(tds) >= 4:
            time_str = tds[0].get_text(strip=True)
            company = tds[1].get_text(strip=True)
            title_a = tds[2].find('a')
            
            if title_a and 'openDisclsViewer' in title_a.get('onclick', ''):
                title = title_a.get_text(strip=True)
                # 제목에서 "[기재정정]", "유가증권" 등 껍데기 제거
                clean_title = re.sub(r"^\[.*?\]\s*", "", title)
                
                # 유상증자, 전환사채 등 키워드 필터링
                if any(kw in clean_title for kw in ALL_KEYWORDS):
                    acptno_match = re.search(r"openDisclsViewer\('(\d+)'", title_a['onclick'])
                    if acptno_match:
                        acptno = acptno_match.group(1)
                        link = f"https://kind.krx.co.kr/common/disclsviewer.do?method=search&acptno={acptno}"
                        items.append({
                            "title": title,
                            "company": company,
                            "link": link,
                            "acptno": acptno,
                            "pub": f"{today_str} {time_str}"
                        })
    return items

# ✨ 공시 뷰어 HTML 직접 파싱 (가장 강력하고 정확한 방법)
def extract_data_from_html(session, acptno):
    extracted = {k: "" for k in ALIASES.keys()}
    excel_link, pdf_link = "", ""
    
    try:
        # 1. 뷰어 메인 접속 및 docno(실제 문서번호) 찾기
        viewer_url = f"{BASE}/common/disclsviewer.do?method=search&acptno={acptno}"
        vr = session.get(viewer_url, headers=DEFAULT_HEADERS)
        
        # 첨부파일 링크 추출
        for m in re.finditer(r'<option\s+value="(\d+)\|([^"]+)"', vr.text):
            dl_url = f"https://kind.krx.co.kr/common/applcmn.do?method=download&apndNo={m.group(1)}"
            if ".pdf" in m.group(2).lower(): pdf_link = dl_url
            elif ".xls" in m.group(2).lower() or "excel" in m.group(2).lower(): excel_link = dl_url

        docnos = re.findall(r"docno=(\d+)", vr.text) or re.findall(r"(\d{10,14})\|", vr.text)
        if not docnos: return excel_link, pdf_link, extracted
        
        # 2. 본문(HTML) 문서 직접 접근
        doc_url = f"{BASE}/common/disclsviewer.do?method=docdisclsviewer&acptno={acptno}&docno={docnos[0]}"
        r = session.get(doc_url, headers=DEFAULT_HEADERS)
        soup = BeautifulSoup(r.text, 'html.parser')
        
        # 3. 본문 내의 모든 <table> 스캔
        for table in soup.find_all('table'):
            for tr in table.find_all('tr'):
                cells = tr.find_all(['th', 'td'])
                for i, cell in enumerate(cells):
                    raw_text = cell.get_text(separator=" ", strip=True)
                    text_norm = norm(raw_text)
                    
                    for key, aliases in ALIASES.items():
                        if not extracted[key]:
                            if any(a in text_norm for a in aliases):
                                # 3-1. 키워드와 값이 같은 셀 안에 있는 경우 (예: "발행가액 : 500원")
                                if ":" in raw_text or "：" in raw_text:
                                    parts = re.split(r'[:|：]', raw_text)
                                    val = parts[-1].strip()
                                    if val and val not in ["-", "—", ""]:
                                        extracted[key] = val
                                        continue
                                
                                # 3-2. 값이 오른쪽 칸에 있는 경우
                                for offset in range(1, 3):
                                    if i + offset < len(cells):
                                        val = cells[i + offset].get_text(separator=" ", strip=True)
                                        if val and val not in ["-", "—", ""]:
                                            extracted[key] = val
                                            break
    except Exception as e:
        print(f" [HTML 파싱 에러] {e}")
        
    return excel_link, pdf_link, extracted

def main():
    tabs, existing_acptnos = connect_gs_and_setup_tabs()
    seen_list = set(load_json(SEEN_FILE, []))
    session = requests.Session()
    
    print(f"\n[{datetime.now().strftime('%Y-%m-%d')}] KIND 상세검색 수집 시작...")
    items = get_todays_disclosures(session)
    
    print(f"[QUEUE] 오늘 올라온 대상 공시: {len(items)}건")
    if not items:
        print("✅ 새로 올라온 공시가 없습니다.")
        return

    for item in items:
        title, link, company, acptno = item["title"], item["link"], item["company"], item["acptno"]
        
        matched_kws = [k for k in ALL_KEYWORDS if k in title]
        if not matched_kws: continue
        if acptno in existing_acptnos or acptno in seen_list: continue
            
        print(f"\nProcessing: [{company}] {title}")
        
        # 🚀 본문 HTML 직접 파싱 (엑셀 다운로드 필요 없음!)
        excel_link, pdf_link, ex_data = extract_data_from_html(session, acptno)

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        is_success = True
        
        for kw in matched_kws:
            ws = tabs[kw]
            target_cols = COLS_YU if kw in KEYWORDS_YU else COLS_BOND
            try:
                row_id = get_next_id(ws)
                row_dict = {
                    "ID": row_id, "수집시간": now, "Excel Link": excel_link, "PDF Link": pdf_link,
                    "회사명": company, "보고서명": title, "상장시장": "", # 상세검색에서는 시장구분이 별도 제공되지 않음
                    "링크": link, "접수번호": acptno
                }
                row_dict.update(ex_data)
                
                final_row = [row_dict.get(col_name, "") for col_name in target_cols]
                ws.append_row(final_row, value_input_option="USER_ENTERED")
                print(f" -> '{kw}' 시트 저장 완료 (데이터 {sum(1 for v in final_row[4:-2] if v)}개 획득)")
            except Exception as e:
                print(f" -> [GS Error for {kw}] {e}")
                is_success = False
        
        if is_success:
            seen_list.add(acptno)
            existing_acptnos.add(acptno)
        
        time.sleep(SLEEP_SECONDS)

    save_json(SEEN_FILE, list(seen_list))
    print("\n✅ 오늘치 작업 완료!")

if __name__ == "__main__":
    main()
