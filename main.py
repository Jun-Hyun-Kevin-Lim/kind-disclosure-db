import os, json, time, re
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs, urlencode
from io import StringIO

import feedparser
import requests
import gspread
import pandas as pd
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ==========================================
# 1. Config (환경 설정)
# ==========================================
BOT_VERSION = "kind-bot-v10-final"

RSS_URL = "http://kind.krx.co.kr:80/disclosure/rsstodaydistribute.do?method=searchRssTodayDistribute&repIsuSrtCd=&mktTpCd=0&searchCorpName=&currentPageSize=200"
BASE = "https://kind.krx.co.kr"
KEYWORDS = ["유상증자", "전환사채", "교환사채"]

SHEET_NAME = os.getenv("SHEET_NAME", "KIND_대경")
RAW_TAB, TAB_YUSANG, TAB_JEONHWAN, TAB_GYOHWAN = "RAW", "유상증자", "전환사채", "교환사채"
SEEN_FILE, RETRY_FILE = "seen.json", "retry_queue.json"

SUCCESS_FILLED_MIN = 8
SLEEP_SECONDS = 1.0
PW_NAV_TIMEOUT_MS, PW_WAIT_MS = 25000, 3000

ISSUE_FIELDS = [
    "회사명","상장시장","최초 이사회결의일","증자방식","발행상품","신규발행주식수","확정발행가(원)","기준주가","확정발행금액(억원)","할인(할증률)",
    "증자전 주식수","증자비율","청약일","납입일","주관사","자금용도","투자자","증자금액"
]

# ==========================================
# 2. Google Sheets & Utils (에러 해결 핵심)
# ==========================================
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
    """구글 시트 연결 및 워크시트 확보"""
    creds_dict = json.loads(os.environ["GOOGLE_CREDS"])
    creds = Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    )
    client = gspread.authorize(creds)
    sh = client.open(SHEET_NAME)
    
    def get_ws(title):
        try: return sh.worksheet(title)
        except gspread.exceptions.WorksheetNotFound: 
            return sh.add_worksheet(title=title, rows="1000", cols="35")
            
    return get_ws(RAW_TAB), get_ws(TAB_YUSANG), get_ws(TAB_JEONHWAN), get_ws(TAB_GYOHWAN)

def ensure_headers(raw_ws, issue_sheets):
    raw_h = ["ID","수집시간","공시일시","공시제목","공시링크","GUID","처리상태","ACPTNO","DOCNO","FILLED","TABLES","VERSION"]
    if raw_ws.row_values(1)[:len(raw_h)] != raw_h: raw_ws.update("A1", [raw_h])
    
    iss_h = ["ID","수집시간","공시일시","회사명","상장시장","공시제목","공시링크","GUID"] + ISSUE_FIELDS + ["VERSION","처리상태","FILLED","TABLES","ACPTNO","DOCNO"]
    for ws in issue_sheets:
        if ws.row_values(1)[:len(iss_h)] != iss_h: ws.update("A1", [iss_h])

def get_next_id(ws):
    col = ws.col_values(1)[1:]
    ids = [int(v) for v in col if str(v).strip().isdigit()]
    return max(ids) + 1 if ids else 1

# ==========================================
# 3. 정밀 데이터 추출 로직
# ==========================================
def extract_date(text):
    s = str(text).replace('nan', '').strip()
    m = re.search(r"(20[2-3]\d)\s*[\-\.\/년]\s*(\d{1,2})\s*[\-\.\/월]\s*(\d{1,2})", s)
    return f"{m.group(1)}-{m.group(2).zfill(2)}-{m.group(3).zfill(2)}" if m else ""

def extract_number(text, to_eok=False):
    s = str(text).replace('nan', '').replace(',', '').strip()
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    if m:
        val = float(m.group(1))
        if to_eok:
            if "백만원" in s: val /= 100.0
            elif val >= 10000000: val /= 100000000.0
        return str(int(val)) if val.is_integer() else str(round(val, 2))
    return ""

def search_in_dfs(dfs, keywords, ext_func, **kwargs):
    kw_pattern = "|".join([k.replace(" ", "") for k in keywords])
    for df in dfs:
        df = df.astype(str).replace('nan', '')
        for r in range(len(df)):
            for c in range(len(df.columns)):
                cell_norm = df.iloc[r, c].replace(" ", "")
                if re.search(kw_pattern, cell_norm):
                    # 1. 우측 탐색
                    for nc in range(c + 1, len(df.columns)):
                        res = ext_func(df.iloc[r, nc], **kwargs) if kwargs else ext_func(df.iloc[r, nc])
                        if res: return res
                    # 2. 하단 탐색
                    if r + 1 < len(df):
                        res = ext_func(df.iloc[r+1, c], **kwargs) if kwargs else ext_func(df.iloc[r+1, c])
                        if res: return res
    return ""

def parse_with_pandas(html_str):
    fields = {k: "" for k in ISSUE_FIELDS}
    if not html_str: return fields, 0, 0
    html_str = html_str.replace("<br>", " ").replace("<br/>", " ")
    try:
        # 
        dfs = pd.read_html(StringIO(html_str))
        dfs = [df for df in dfs if df.shape[0] > 1]
    except: return fields, 0, 0

    fields["최초 이사회결의일"] = search_in_dfs(dfs, ["최초이사회결의일", "이사회결의일", "결정일"], extract_date)
    fields["납입일"] = search_in_dfs(dfs, ["납입일", "대금납입일"], extract_date)
    fields["신규발행주식수"] = search_in_dfs(dfs, ["신규발행주식수", "발행할주식의수"], extract_number)
    fields["확정발행가(원)"] = search_in_dfs(dfs, ["발행가액", "확정발행가", "전환가액", "교환가격"], extract_number)
    fields["확정발행금액(억원)"] = search_in_dfs(dfs, ["확정발행금액", "모집총액", "권면총액"], extract_number, to_eok=True)
    fields["투자자"] = search_in_dfs(dfs, ["제3자배정대상자", "배정대상자", "대상자"], lambda x: str(x)[:100] if str(x)!='nan' else "")
    
    filled = sum(1 for v in fields.values() if str(v).strip())
    return fields, len(dfs), filled

# ==========================================
# 4. Playwright & Main
# ==========================================
def get_best_html(viewer_url):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        try:
            page.goto(viewer_url, wait_until="networkidle", timeout=PW_NAV_TIMEOUT_MS)
        except: pass
        page.wait_for_timeout(PW_WAIT_MS)
        best_html, max_score = "", -1
        for fr in page.frames:
            try:
                html = fr.content()
                score = html.lower().count("<table") * 2 + sum(1 for k in ["발행가", "납입일", "주식수"] if k in html) * 10
                if score > max_score: max_score, best_html = score, html
            except: continue
        browser.close()
        return best_html

def main():
    raw_ws, ws_yu, ws_jeon, ws_gyo = connect_gs()
    ensure_headers(raw_ws, [ws_yu, ws_jeon, ws_gyo])
    
    seen_list = load_json(SEEN_FILE, [])
    retry_queue = load_json(RETRY_FILE, [])
    
    session = requests.Session()
    feed = feedparser.parse(requests.get(RSS_URL).text)
    
    items = {it["guid"]: it for it in retry_queue}
    for entry in feed.entries:
        title, link = entry.get("title", ""), entry.get("link", "")
        guid = entry.get("id") or link
        if guid not in seen_list and any(k in title for k in KEYWORDS):
            items[guid] = {"title": title, "link": link, "guid": guid, "pub": entry.get("published", "")}

    items_list = list(items.values())
    print(f"📦 처리 시작: {len(items_list)}건")

    new_retry = []
    for item in items_list:
        title, link, guid = item["title"], item["link"], item["guid"]
        print(f"🔍 분석: {title}")
        
        # ACPTNO 추출
        acptno = (parse_qs(urlparse(link).query).get("acptno") or [None])[0]
        if not acptno:
            res = requests.get(link); m = re.search(r'acptNo"\s*value="(\d+)"', res.text)
            acptno = m.group(1) if m else None
        
        if not acptno:
            new_retry.append(item); continue

        viewer_url = f"{BASE}/common/disclsviewer.do?method=search&acptno={acptno}&viewerhost="
        vr_shell = requests.get(viewer_url); soup = BeautifulSoup(vr_shell.text, "lxml")
        options = [opt.get("value").split("|")[0] for opt in soup.find_all("option") if "|" in opt.get("value", "")]

        best_cand = None
        for docno in options[:3]: # 상위 3개 문서만 확인
            doc_url = f"{viewer_url}&docno={docno}"
            html = get_best_html(doc_url)
            if "<title>창 닫기</title>" in html: continue
            
            fields, t_cnt, filled = parse_with_pandas(html)
            if not best_cand or filled > best_cand[0]:
                best_cand = (filled, t_cnt, docno, fields)
            if filled >= SUCCESS_FILLED_MIN: break

        if not best_cand:
            new_retry.append(item); continue

        filled, t_cnt, docno, fields = best_cand
        status = "SUCCESS" if filled >= SUCCESS_FILLED_MIN else "INCOMPLETE"
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        rid = get_next_id(raw_ws)
        raw_ws.append_row([rid, now, item["pub"], title, link, guid, status, acptno, docno, filled, t_cnt, f"{acptno}-{docno}"])
        
        target_ws = ws_jeon if "전환사채" in title else ws_gyo if "교환사채" in title else ws_yu
        
        m = re.match(r"^\[([^\]]+)\]\s*([^\s]+)", title.strip())
        market, corp = (m.group(1), m.group(2)) if m else ("", "")
        
        iss_row = [rid, now, item["pub"], corp, market, title, link, guid] + [fields.get(k, "") for k in ISSUE_FIELDS] + [f"{acptno}-{docno}", status, filled, t_cnt, acptno, docno]
        target_ws.append_row(iss_row)
        
        if status == "SUCCESS": seen_list.append(guid)
        else: new_retry.append(item)
        time.sleep(SLEEP_SECONDS)

    save_json(SEEN_FILE, seen_list)
    save_json(RETRY_FILE, new_retry)
    print("✅ 완료")

if __name__ == "__main__":
    main()
