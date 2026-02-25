import os, json, time, re, io
from datetime import datetime
from urllib.parse import urlparse, parse_qs
import feedparser
import requests
import gspread
import pandas as pd
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright

# =========================
# Config & Setup
# =========================
BOT_VERSION = os.getenv("BOT_VERSION", "kind-bot-v10-bs4-html")
RSS_URL = "https://kind.krx.co.kr/disclosure/rsstodaydistribute.do?method=searchRssTodayDistribute&repIsuSrtCd=&mktTpCd=0&searchCorpName=&currentPageSize=200"

SHEET_NAME = "KIND_대경"
SEEN_FILE = "seen.json"
RETRY_FILE = "retry_queue.json"
BASE = "https://kind.krx.co.kr"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9",
}

SLEEP_SECONDS = float(os.getenv("SLEEP_SECONDS", "2"))

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

ALL_KEYWORDS = ["유상증자", "전환사채", "신주인수권부사채", "교환사채"]

ALIASES = {
    "최초 이사회결의일": ["최초 이사회결의일", "이사회결의일", "결의일", "결정일"],
    "이사회결의일": ["이사회결의일", "결의일", "결정일"],
    "증자방식": ["증자방식", "배정방식", "배정방법"],
    "발행상품": ["발행상품", "신주의 종류", "주식의 종류", "사채의 종류"],
    "신규발행주식수": ["신규발행주식수", "발행신주수", "신주수", "발행할 주식의 수"],
    "확정발행가(원)": ["확정발행가", "신주발행가액", "발행가액", "1주당 발행가액"],
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

def extract_acptno_from_link(link: str, html_text: str = ""):
    qs = parse_qs(urlparse(link).query)
    acpt = (qs.get("acptno") or qs.get("acptNo") or [None])[0]
    if acpt: return acpt
    m = re.search(r"(acptno|acptNo)=(\d{8,14})", html_text, re.I)
    if m: return m.group(2)
    return None

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
            acpt_col_idx = len(COLS_YU) if kw == "유상증자" else len(COLS_BOND)
            acpts = ws.col_values(acpt_col_idx)[1:] 
            for acpt in acpts:
                if str(acpt).strip().isdigit(): existing_acptnos.add(str(acpt).strip())
        except gspread.exceptions.WorksheetNotFound:
            cols_to_use = COLS_YU if kw == "유상증자" else COLS_BOND
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

# ✨ 직접 찾아주신 BeautifulSoup + Selenium(Playwright) 방식 적용!
def process_disclosure_via_html_parsing(link: str):
    excel_link, pdf_link = "", ""
    extracted = {k: "" for k in ALIASES.keys()}
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(locale="ko-KR")
        page = context.new_page()
        
        try:
            page.goto(link, wait_until="networkidle")
            page.wait_for_timeout(3000)
            
            # 1. 첨부파일(Excel, PDF) 링크 확보 (이건 화면에 있는 버튼 속성을 그대로 읽음)
            for fr in page.frames:
                for el in fr.query_selector_all("option, a, button"):
                    text = (el.inner_text() or "").lower()
                    val = (el.get_attribute("value") or "").lower()
                    href = (el.get_attribute("href") or "").lower()
                    
                    apnd_no = None
                    if "|" in val and val.split("|")[0].isdigit():
                        apnd_no = val.split("|")[0]
                        text += val
                    elif "apndno=" in href:
                        m = re.search(r"apndno=(\d+)", href)
                        if m: apnd_no = m.group(1)
                    
                    if apnd_no:
                        dl_url = f"https://kind.krx.co.kr/common/applcmn.do?method=download&apndNo={apnd_no}"
                        if "pdf" in text or ".pdf" in text: pdf_link = dl_url
                        elif "xls" in text or "excel" in text or "엑셀" in text: excel_link = dl_url

            # 2. BeautifulSoup으로 화면 내의 "본문 표(Table)"를 직접 파싱!!
            # KIND 공시 뷰어에서 실제 내용이 들어있는 프레임 찾기
            target_frame = None
            for fr in page.frames:
                if "docdisclsviewer" in fr.url or "body" in fr.name.lower():
                    target_frame = fr
                    break
            
            if not target_frame:
                target_frame = page.main_frame
                
            # 프레임의 HTML 소스를 통째로 가져옴
            html_content = target_frame.content()
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Pandas로 HTML 안에 있는 모든 표(table)를 한 번에 읽기
            try:
                dfs = pd.read_html(io.StringIO(str(soup)))
                for df in dfs:
                    for _, row in df.iterrows():
                        row_list = [str(x) for x in row.tolist() if pd.notna(x)]
                        for col_idx, cell_value in enumerate(row_list):
                            cleaned_cell = norm(cell_value)
                            for key, aliases in ALIASES.items():
                                if not extracted[key]:
                                    if any(a in cleaned_cell for a in aliases):
                                        # 키워드를 찾으면 그 오른쪽 칸들에 값이 있는지 확인
                                        for offset in range(1, 3):
                                            if col_idx + offset < len(row_list):
                                                val = str(row_list[col_idx + offset]).strip()
                                                if val and val.lower() not in ["-", "—", "nan", "none", ""]: 
                                                    extracted[key] = val
                                                    break
            except Exception as e:
                print(f"   [HTML 표 파싱 실패] {e}")

        except Exception as e:
            print(f" [Playwright Error] {e}")
        finally:
            browser.close()
            
    return excel_link, pdf_link, extracted

def main():
    tabs, existing_acptnos = connect_gs_and_setup_tabs()
    seen_list = set(load_json(SEEN_FILE, []))
    retry_queue = load_json(RETRY_FILE, [])
    
    feed = requests.get(RSS_URL, headers=DEFAULT_HEADERS)
    feed.encoding = "utf-8"
    parsed_feed = feedparser.parse(feed.text)
    
    items = []
    for entry in parsed_feed.entries:
        title, link, guid = entry.get("title", ""), entry.get("link", ""), entry.get("id", "")
        if not any(k in title for k in ALL_KEYWORDS): continue
        items.append({"title": title, "link": link, "guid": guid})
    
    items.extend(retry_queue)
    items = list({it["guid"]: it for it in items}.values())
    
    print(f"[QUEUE] 처리 대기: {len(items)}건")
    
    new_retry = []
    for item in items:
        title, link, guid = item["title"], item["link"], item["guid"]
        m_comp = re.match(r"^\[([^\]]+)\]\s*([^\s]+)", title.strip())
        market_code, company = (m_comp.group(1).strip(), m_comp.group(2).strip()) if m_comp else ("", "")
        
        matched_kws = [k for k in ALL_KEYWORDS if k in title]
        if not matched_kws: continue
        
        link_res = requests.get(link, headers=DEFAULT_HEADERS)
        acptno = extract_acptno_from_link(link, link_res.text)
        
        if not acptno:
            new_retry.append(item)
            continue

        if acptno in existing_acptnos or acptno in seen_list:
            continue
            
        print(f"\nProcessing: [{company}] {title}")
        
        # 다운로드를 포기하고, 브라우저 화면의 HTML을 바로 읽어버리는 함수 실행!
        excel_link, pdf_link, ex_data = process_disclosure_via_html_parsing(link)

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        is_success = True
        
        for kw in matched_kws:
            ws = tabs[kw]
            target_cols = COLS_YU if kw == "유상증자" else COLS_BOND
            try:
                row_id = get_next_id(ws)
                row_dict = {
                    "ID": row_id, "수집시간": now, "Excel Link": excel_link, "PDF Link": pdf_link,
                    "회사명": company, "보고서명": title, "상장시장": market_code, 
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
        else:
            new_retry.append(item)
        
        time.sleep(SLEEP_SECONDS)

    save_json(SEEN_FILE, list(seen_list))
    save_json(RETRY_FILE, new_retry)
    print("\n✅ 작업 완료!")

if __name__ == "__main__":
    main()
