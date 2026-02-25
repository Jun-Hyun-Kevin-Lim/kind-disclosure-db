import os, json, time, re
from datetime import datetime
from urllib.parse import urlparse, parse_qs
import feedparser
import requests
import gspread
import pandas as pd
import pdfplumber
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# =========================
# Config & Setup
# =========================
BOT_VERSION = os.getenv("BOT_VERSION", "kind-bot-v6-dynamic-cols")
RSS_URL = "https://kind.krx.co.kr/disclosure/rsstodaydistribute.do?method=searchRssTodayDistribute&repIsuSrtCd=&mktTpCd=0&searchCorpName=&currentPageSize=200"

SHEET_NAME = "KIND_대경"
SEEN_FILE = "seen.json"
RETRY_FILE = "retry_queue.json"
BASE = "https://kind.krx.co.kr"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
}

SLEEP_SECONDS = float(os.getenv("SLEEP_SECONDS", "1"))
PW_NAV_TIMEOUT_MS = int(os.getenv("PW_NAV_TIMEOUT_MS", "30000"))

# =========================
# Columns Definition
# =========================
# 1. 유상증자 전용 컬럼
COLS_YU = [
    "ID", "수집시간", "Excel Link", "PDF Link", 
    "회사명", "보고서명", "상장시장", "최초 이사회결의일", "증자방식", "발행상품", 
    "신규발행주식수", "확정발행가(원)", "기준주가", "확정발행금액(억원)", "할인(할증률)", 
    "증자전 주식수", "증자비율", "납입일", "신주의 배당기산일", "신주의 상장 예정일", 
    "이사회결의일", "자금용도", "투자자", "링크", "접수번호"
]

# 2. 사채 (전환사채, 신주인수권부사채, 교환사채) 전용 컬럼
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

# =========================
# Extraction Aliases (엑셀/PDF에서 찾을 단어들)
# =========================
ALIASES = {
    # 공통 및 유상증자
    "최초 이사회결의일": ["최초 이사회결의일", "이사회결의일", "결의일", "결정일"],
    "이사회결의일": ["이사회결의일", "결의일", "결정일"],
    "증자방식": ["증자방식", "배정방식", "배정방법"],
    "발행상품": ["발행상품", "신주의 종류", "주식의 종류", "사채의 종류"],
    "신규발행주식수": ["신규발행주식수", "발행신주수", "신주수", "발행할 주식의 수"],
    "확정발행가(원)": ["확정발행가", "신주발행가액", "발행가액", "1주당 발행가액"],
    "기준주가": ["기준주가", "기준주가액"],
    "확정발행금액(억원)": ["확정발행금액", "모집총액", "자금조달목적"],
    "할인(할증률)": ["할인율", "할증률", "할인율(%)"],
    "증자전 주식수": ["증자전 주식수", "기발행주식총수", "증자전 발행주식총수"],
    "증자비율": ["증자비율", "증자비율(%)"],
    "납입일": ["납입일", "대금납입일", "청약기일"],
    "신주의 배당기산일": ["신주의 배당기산일", "배당기산일"],
    "신주의 상장 예정일": ["신주의 상장 예정일", "상장예정일", "상장 예정일"],
    "자금용도": ["자금용도", "조달목적", "자금조달의 목적", "자금사용 목적"],
    "투자자": ["투자자", "배정대상자", "제3자배정 대상자", "인수인"],
    
    # 사채 전용 (CB, BW, EB)
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
# Utils
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
            # 접수번호 컬럼(맨 마지막)을 읽어 중복 체크용으로 사용
            acpt_col_idx = len(COLS_YU) if kw in KEYWORDS_YU else len(COLS_BOND)
            acpts = ws.col_values(acpt_col_idx)[1:] 
            for acpt in acpts:
                if str(acpt).strip().isdigit(): existing_acptnos.add(str(acpt).strip())
        except gspread.exceptions.WorksheetNotFound:
            print(f"[GS] Worksheet '{kw}' not found. Creating new one.")
            cols_to_use = COLS_YU if kw in KEYWORDS_YU else COLS_BOND
            ws = sh.add_worksheet(title=kw, rows="1000", cols=str(len(cols_to_use) + 5))
            ws.append_row(cols_to_use) # 맞춤형 헤더 삽입
        tabs[kw] = ws

    print(f"[BOT] Opened spreadsheet='{sh.title}' | 기존 데이터: {len(existing_acptnos)}건")
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

# =========================
# File Extraction 
# =========================
def get_attachment_links_via_playwright(acptno: str) -> tuple[str, str]:
    viewer_url = f"{BASE}/common/disclsviewer.do?method=search&acptno={acptno}"
    excel_link, pdf_link = "", ""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(locale="ko-KR")
        page = context.new_page()
        page.set_default_navigation_timeout(PW_NAV_TIMEOUT_MS)
        try:
            page.goto(viewer_url, wait_until="networkidle")
            page.wait_for_timeout(3500) 
            for fr in page.frames:
                for el in fr.query_selector_all("a, option, button, span"):
                    text = (el.inner_text() or "").replace(" ", "").lower()
                    href, onclick, value = (el.get_attribute("href") or "").lower(), (el.get_attribute("onclick") or "").lower(), (el.get_attribute("value") or "").lower()
                    apnd_no = None
                    if "apndno=" in href:
                        m = re.search(r"apndno=(\d+)", href)
                        if m: apnd_no = m.group(1)
                    elif "download" in onclick and ("apndno" in onclick or "file" in onclick):
                        m = re.search(r"['\"](\d{4,})['\"]", onclick)
                        if m: apnd_no = m.group(1)
                    elif "|" in value and value.split("|")[0].isdigit():
                        apnd_no = value.split("|")[0]
                        text += value
                    
                    if apnd_no:
                        download_url = f"https://kind.krx.co.kr/common/applcmn.do?method=download&apndNo={apnd_no}"
                        if any(x in text for x in ["pdf", ".pdf"]): pdf_link = pdf_link or download_url
                        elif any(x in text for x in ["xls", "xlsx", "excel", "엑셀"]): excel_link = excel_link or download_url
        except: pass
        finally: browser.close()
    return excel_link, pdf_link

def extract_data_from_excel(excel_url: str) -> dict:
    extracted = {k: "" for k in ALIASES.keys()}
    if not excel_url: return extracted
    try:
        r = requests.get(excel_url, stream=True, headers=DEFAULT_HEADERS)
        temp_file = f"temp_{int(time.time())}.xlsx" if "xlsx" in excel_url else f"temp_{int(time.time())}.xls"
        with open(temp_file, "wb") as f: f.write(r.content)
            
        df = pd.read_excel(temp_file, header=None)
        for _, row in df.iterrows():
            row_list = [str(x) for x in row.tolist() if pd.notna(x)]
            for col_idx, cell_value in enumerate(row_list):
                cleaned_cell = norm(cell_value)
                for key, aliases in ALIASES.items():
                    if not extracted[key]:
                        if any(a in cleaned_cell for a in aliases):
                            if col_idx + 1 < len(row_list):
                                val = str(row_list[col_idx + 1]).strip()
                                if val and val not in ["-", "—", "nan"]: extracted[key] = val
        if os.path.exists(temp_file): os.remove(temp_file)
    except: pass
    return extracted

def extract_data_from_pdf(pdf_url: str) -> dict:
    extracted = {k: "" for k in ALIASES.keys()}
    if not pdf_url: return extracted
    try:
        r = requests.get(pdf_url, stream=True, headers=DEFAULT_HEADERS)
        temp_file = f"temp_{int(time.time())}.pdf"
        with open(temp_file, "wb") as f: f.write(r.content)
            
        with pdfplumber.open(temp_file) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text: continue
                for line in text.split('\n'):
                    cleaned_line = norm(line)
                    for key, aliases in ALIASES.items():
                        if not extracted[key]:
                            if any(a in cleaned_line for a in aliases):
                                parts = re.split(r'[:|：]', line)
                                val = parts[-1].strip() if len(parts) > 1 else line.strip()
                                if val: extracted[key] = val
        if os.path.exists(temp_file): os.remove(temp_file)
    except: pass
    return extracted

# =========================
# Main Logic
# =========================
def main():
    tabs, existing_acptnos = connect_gs_and_setup_tabs()
    seen_list = set(load_json(SEEN_FILE, []))
    retry_queue = load_json(RETRY_FILE, [])
    session = requests.Session()
    
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
    
    print(f"[QUEUE] to_process={len(items)}")
    
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
        excel_link, pdf_link = get_attachment_links_via_playwright(acptno)
        
        # 데이터 추출 병합 (엑셀 우선)
        ex_data = extract_data_from_excel(excel_link)
        if sum(1 for v in ex_data.values() if v) < 3 and pdf_link:
            pdf_data = extract_data_from_pdf(pdf_link)
            for k, v in pdf_data.items():
                if not ex_data[k] and v: ex_data[k] = v

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        is_success = True
        
        # 시트별로 매핑하여 입력
        for kw in matched_kws:
            ws = tabs[kw]
            target_cols = COLS_YU if kw in KEYWORDS_YU else COLS_BOND
            
            try:
                row_id = get_next_id(ws)
                
                # 수집된 모든 정보(메타데이터 + 추출데이터)를 담은 마스터 딕셔너리
                row_dict = {
                    "ID": row_id, "수집시간": now, "Excel Link": excel_link, "PDF Link": pdf_link,
                    "회사명": company, "보고서명": title, "상장시장": market_code, 
                    "링크": link, "접수번호": acptno
                }
                row_dict.update(ex_data) # 추출한 표 데이터 병합
                
                # 시트의 헤더 순서(target_cols)에 맞게 배열(list) 생성!
                final_row = [row_dict.get(col_name, "") for col_name in target_cols]
                
                ws.append_row(final_row, value_input_option="USER_ENTERED")
                print(f" -> '{kw}' 시트 저장 완료 (추출된 항목: {sum(1 for v in final_row[4:-2] if v)}개)")
                
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
