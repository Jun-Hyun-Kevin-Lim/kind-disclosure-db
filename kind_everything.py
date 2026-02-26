import os, json, time, re, io
from datetime import datetime
import requests
import feedparser
import gspread
import pandas as pd
import tabula
import pdfplumber
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials
from tenacity import retry, stop_after_attempt, wait_fixed

# =========================
# Config & Setup
# =========================
BOT_VERSION = os.getenv("BOT_VERSION", "kind-bot-v15-enterprise")
SHEET_NAME = "KIND_대경"
SEEN_FILE = "seen.json"
RETRY_FILE = "retry_queue.json"
BASE = "https://kind.krx.co.kr"
RSS_URL = "https://kind.krx.co.kr/disclosure/rsstodaydistribute.do?method=searchRssTodayDistribute&repIsuSrtCd=&mktTpCd=0&searchCorpName=&currentPageSize=200"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9",
}

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

# =========================
# Utilities & Retry Network
# =========================
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

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def fetch_url(session, url, stream=False):
    """Tenacity를 이용해 네트워크 오류 시 최대 3번 자동 재시도합니다."""
    r = session.get(url, headers=DEFAULT_HEADERS, stream=stream, timeout=15)
    r.raise_for_status()
    return r

def connect_gs_and_setup_tabs():
    creds_dict = json.loads(os.environ["GOOGLE_CREDS"])
    creds = Credentials.from_service_account_info(
        creds_dict, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    )
    client = gspread.authorize(creds)
    sh = client.open(SHEET_NAME)
    tabs, existing_acptnos = {}, set()

    for kw in ALL_KEYWORDS:
        try:
            ws = sh.worksheet(kw)
            idx = len(COLS_YU) if kw in ["유상증자"] else len(COLS_BOND)
            acpts = ws.col_values(idx)[1:] 
            for acpt in acpts:
                if str(acpt).strip().isdigit(): existing_acptnos.add(str(acpt).strip())
        except gspread.exceptions.WorksheetNotFound:
            cols = COLS_YU if kw in ["유상증자"] else COLS_BOND
            ws = sh.add_worksheet(title=kw, rows="1000", cols=str(len(cols) + 5))
            ws.append_row(cols)
        tabs[kw] = ws
    return tabs, existing_acptnos

def get_next_id(ws):
    col = ws.col_values(1)
    if len(col) <= 1: return 1
    return max([int(v) for v in col[1:] if str(v).strip().isdigit()] + [0]) + 1

# =========================
# Data Extraction Engine
# =========================
def extract_from_dataframe(df, extracted):
    """Pandas DataFrame(표)에서 키워드와 값을 정밀하게 매핑합니다."""
    for _, row in df.iterrows():
        row_list = [str(x) for x in row.tolist() if pd.notna(x)]
        for i, cell in enumerate(row_list):
            cell_norm = norm(cell)
            for key, aliases in ALIASES.items():
                if not extracted[key]:
                    if any(a in cell_norm for a in aliases):
                        # 1. 같은 셀에 있는 경우 (발행가액: 500)
                        if ":" in cell or "：" in cell:
                            val = re.split(r'[:|：]', cell)[-1].strip()
                            if val and val not in ["-", "—"]: extracted[key] = val
                            continue
                        # 2. 오른쪽 칸에 있는 경우
                        for offset in range(1, 3):
                            if i + offset < len(row_list):
                                val = str(row_list[i + offset]).strip()
                                if val and val.lower() not in ["-", "—", "nan", "none"]:
                                    extracted[key] = val
                                    break
    return extracted

def get_attachment_links(session, acptno):
    """뷰어에 접속하여 첨부파일(Excel/PDF) 다운로드 링크와 실제 문서번호(docno)를 가져옵니다."""
    viewer_url = f"{BASE}/common/disclsviewer.do?method=search&acptno={acptno}"
    r = fetch_url(session, viewer_url)
    vr_text = r.content.decode('euc-kr', 'replace')
    
    excel_link, pdf_link = "", ""
    for m in re.finditer(r'<option\s+value="(\d+)\|([^"]+)"', vr_text):
        dl_url = f"https://kind.krx.co.kr/common/applcmn.do?method=download&apndNo={m.group(1)}"
        if ".pdf" in m.group(2).lower(): pdf_link = dl_url
        elif ".xls" in m.group(2).lower() or "excel" in m.group(2).lower(): excel_link = dl_url
        
    docnos = re.findall(r"docno=(\d+)", vr_text) or re.findall(r"(\d{10,14})\|", vr_text)
    docno = docnos[0] if docnos else None
    return excel_link, pdf_link, docno

def build_data_pipeline(session, acptno):
    """[핵심] 3단계 폭포수 파이프라인: HTML -> Excel -> PDF(Tabula/Plumber)"""
    extracted = {k: "" for k in ALIASES.keys()}
    excel_link, pdf_link, docno = get_attachment_links(session, acptno)
    
    # 1순위: HTML 본문 표 추출 (가장 빠르고 정확함)
    if docno:
        try:
            doc_url = f"{BASE}/common/disclsviewer.do?method=docdisclsviewer&acptno={acptno}&docno={docno}"
            r = fetch_url(session, doc_url)
            soup = BeautifulSoup(r.content, 'html.parser', from_encoding='euc-kr')
            dfs = pd.read_html(io.StringIO(str(soup)))
            for df in dfs: extracted = extract_from_dataframe(df, extracted)
        except Exception as e:
            print(f"   [1. HTML 파싱 실패] {e}")

    # 2순위: 엑셀 파싱 (HTML에서 못 뽑은 데이터가 많을 때만 실행)
    filled = sum(1 for v in extracted.values() if v)
    if filled < 5 and excel_link:
        try:
            r = fetch_url(session, excel_link, stream=True)
            tmp_xls = f"temp_{int(time.time())}.xls"
            with open(tmp_xls, "wb") as f: f.write(r.content)
            
            try: df = pd.read_excel(tmp_xls, header=None)
            except: 
                dfs = pd.read_html(tmp_xls, encoding='euc-kr')
                df = pd.concat(dfs, ignore_index=True)
                
            extracted = extract_from_dataframe(df, extracted)
            if os.path.exists(tmp_xls): os.remove(tmp_xls)
        except Exception as e:
            print(f"   [2. Excel 파싱 실패] {e}")

    # 3순위: PDF 파싱 (강력한 Tabula-py -> pdfplumber 백업)
    filled = sum(1 for v in extracted.values() if v)
    if filled < 5 and pdf_link:
        try:
            r = fetch_url(session, pdf_link, stream=True)
            tmp_pdf = f"temp_{int(time.time())}.pdf"
            with open(tmp_pdf, "wb") as f: f.write(r.content)
            
            # 3-1. Tabula로 표를 완벽하게 DataFrame으로 뽑아보기
            try:
                dfs = tabula.read_pdf(tmp_pdf, pages='all', multiple_tables=True, pandas_options={'header': None})
                for df in dfs: extracted = extract_from_dataframe(df, extracted)
            except Exception as tabula_e:
                print(f"   [3-1. Tabula 실패, Plumber로 전환] {tabula_e}")
                
            # 3-2. Tabula 실패 시 텍스트 기반 Plumber 시도
            filled_check = sum(1 for v in extracted.values() if v)
            if filled_check < 5:
                with pdfplumber.open(tmp_pdf) as pdf:
                    for page in pdf.pages:
                        text = page.extract_text()
                        if not text: continue
                        for line in text.split('\n'):
                            text_norm = norm(line)
                            for key, aliases in ALIASES.items():
                                if not extracted[key] and any(a in text_norm for a in aliases):
                                    val = re.split(r'[:|：]', line)[-1].strip() if ":" in line or "：" in line else line.strip()
                                    if val: extracted[key] = val

            if os.path.exists(tmp_pdf): os.remove(tmp_pdf)
        except Exception as e:
            print(f"   [3. PDF 파싱 최종 실패] {e}")

    return excel_link, pdf_link, extracted

# =========================
# Main Routine
# =========================
def main():
    tabs, existing_acptnos = connect_gs_and_setup_tabs()
    seen_list = set(load_json(SEEN_FILE, []))
    retry_queue = load_json(RETRY_FILE, [])
    session = requests.Session()
    
    print(f"\n[{datetime.now().strftime('%Y-%m-%d')}] 엔터프라이즈 봇 수집 시작...")
    
    # RSS를 통한 실시간 당일 공시 감지
    try:
        feed = fetch_url(session, RSS_URL)
        parsed_feed = feedparser.parse(feed.content.decode('utf-8'))
    except Exception as e:
        print(f"RSS 로드 실패: {e}")
        return
        
    items = []
    for entry in parsed_feed.entries:
        title, link, guid = entry.get("title", ""), entry.get("link", ""), entry.get("id", "")
        if not any(k in title for k in ALL_KEYWORDS): continue
        items.append({"title": title, "link": link, "guid": guid})
    
    items.extend(retry_queue)
    unique_items = list({it["guid"]: it for it in items}.values())
    
    print(f"[QUEUE] 처리 대기: {len(unique_items)}건")
    if not unique_items:
        print("✅ 새로 올라온 공시가 없습니다.")
        return
    
    new_retry = []
    for item in unique_items:
        title, link = item["title"], item["link"]
        m_comp = re.match(r"^\[([^\]]+)\]\s*([^\s]+)", title.strip())
        market_code, company = (m_comp.group(1).strip(), m_comp.group(2).strip()) if m_comp else ("", "")
        matched_kws = [k for k in ALL_KEYWORDS if k in title]
        
        acptno = re.search(r"(acptno|acptNo)=(\d+)", link, re.I)
        acptno = acptno.group(2) if acptno else None
        
        if not acptno or acptno in existing_acptnos or acptno in seen_list:
            continue
            
        print(f"\nProcessing: [{company}] {title}")
        
        # 🚀 3단 콤보 데이터 추출기 실행
        excel_link, pdf_link, ex_data = build_data_pipeline(session, acptno)

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        is_success = True
        
        for matched_kw in matched_kws:
            ws = tabs[matched_kw]
            target_cols = COLS_YU if matched_kw in ["유상증자"] else COLS_BOND
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
                print(f" -> '{matched_kw}' 시트 저장 완료 (추출 항목: {sum(1 for v in final_row[4:-2] if v)}개)")
            except Exception as e:
                print(f" -> [GS Error] {e}")
                is_success = False
        
        if is_success:
            seen_list.add(acptno)
            existing_acptnos.add(acptno)
        else:
            new_retry.append(item)
        
        time.sleep(1)

    save_json(SEEN_FILE, list(seen_list))
    save_json(RETRY_FILE, new_retry)
    print("\n✅ 작업 완료!")

if __name__ == "__main__":
    main()
