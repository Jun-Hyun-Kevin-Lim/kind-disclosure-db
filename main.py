import os, json, time, re, html as ihtml
from datetime import datetime
from urllib.parse import urlparse, parse_qs, urlencode

import feedparser
import requests
import gspread
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ==========================================
# 1. Config (환경 설정)
# ==========================================
BOT_VERSION = "kind-bot-v6-heavyweight"

RSS_URL = "http://kind.krx.co.kr:80/disclosure/rsstodaydistribute.do?method=searchRssTodayDistribute&repIsuSrtCd=&mktTpCd=0&searchCorpName=&currentPageSize=200"
BASE = "https://kind.krx.co.kr"
KEYWORDS = ["유상증자", "전환사채", "교환사채"]

SHEET_NAME = os.getenv("SHEET_NAME", "KIND_대경")
RAW_TAB = "RAW"
TAB_YUSANG = "유상증자"
TAB_JEONHWAN = "전환사채"
TAB_GYOHWAN = "교환사채"

SEEN_FILE = "seen.json"
RETRY_FILE = "retry_queue.json"

SUCCESS_FILLED_MIN = 8
SLEEP_SECONDS = 1.0
PW_NAV_TIMEOUT_MS = 25000
PW_WAIT_MS = 3000

ISSUE_FIELDS = [
    "회사명","상장시장","최초 이사회결의일","증자방식","발행상품","신규발행주식수","확정발행가(원)","기준주가","확정발행금액(억원)","할인(할증률)",
    "증자전 주식수","증자비율","청약일","납입일","주관사","자금용도","투자자","증자금액"
]

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
}

# ==========================================
# 2. Google Sheets & Utils
# ==========================================
def load_json(filepath, default_val):
    if os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f: return json.load(f)
        except Exception: pass
    return default_val

def save_json(filepath, data):
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def fetch(session, url, referer=None):
    headers = dict(DEFAULT_HEADERS)
    if referer: headers["Referer"] = referer
    r = session.get(url, headers=headers, timeout=25)
    r.encoding = "utf-8" if not r.encoding or r.encoding.lower() == "iso-8859-1" else r.apparent_encoding
    return r

def get_or_create_worksheet(sh, title):
    try: return sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound: return sh.add_worksheet(title=title, rows="1000", cols="30")

def connect_gs():
    creds_dict = json.loads(os.environ["GOOGLE_CREDS"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
    client = gspread.authorize(creds)
    sh = client.open(SHEET_NAME)
    return get_or_create_worksheet(sh, RAW_TAB), get_or_create_worksheet(sh, TAB_YUSANG), get_or_create_worksheet(sh, TAB_JEONHWAN), get_or_create_worksheet(sh, TAB_GYOHWAN)

def ensure_headers(raw_ws, issue_sheets):
    raw_h = ["ID","수집시간","공시일시","공시제목","공시링크","GUID","처리상태","ACPTNO","DOCNO","FILLED","TABLES","VERSION"]
    if raw_ws.row_values(1)[:len(raw_h)] != raw_h: raw_ws.update("A1", [raw_h])
    
    iss_h = ["ID","수집시간","공시일시","회사명","상장시장","공시제목","공시링크","GUID"] + ISSUE_FIELDS + ["VERSION","처리상태","FILLED","TABLES","ACPTNO","DOCNO"]
    for ws in issue_sheets:
        if ws.row_values(1)[:len(iss_h)] != iss_h: ws.update("A1", [iss_h])

def get_next_id(ws):
    col = ws.col_values(1)[1:]
    return max([int(v) for v in col if str(v).strip().isdigit()] + [0]) + 1

# ==========================================
# 3. 데이터 추출 정밀 로직 (Sniper Regex)
# ==========================================
def parse_date(text: str) -> str:
    """날짜 포맷만 핀셋 추출"""
    if len(text) > 100 or "해당사항" in text.replace(" ", ""): return ""
    m = re.search(r"(20[1-3]\d)\s*[\-\.\/년]\s*(\d{1,2})\s*[\-\.\/월]\s*(\d{1,2})", text)
    if m: return f"{m.group(1)}-{m.group(2).zfill(2)}-{m.group(3).zfill(2)}"
    return ""

def parse_price(text: str) -> str:
    """발행가 전용: 액면가(500원 등)를 피해 진짜 가격만 추출"""
    if len(text) > 150 or "해당사항" in text.replace(" ", ""): return ""
    clean_t = re.sub(r"\([^)]*\)", "", text) # 괄호 안 텍스트 무시
    matches = re.findall(r"(\d{1,3}(?:,\d{3})+|\d{4,})\s*원?", clean_t)
    
    for num_str in reversed(matches): # 뒤쪽에 적힌 숫자가 최종 발행가일 확률이 높음
        val = int(num_str.replace(",", ""))
        if val not in [100, 200, 500, 1000, 2500, 5000]: # 액면가 제외
            return str(val)
    return ""

def parse_number(text: str, to_eok=False) -> str:
    """숫자 전용: 주식수, 권면총액 (억원 변환 지원)"""
    if len(text) > 200 or "해당사항" in text.replace(" ", ""): return ""
    clean_t = re.sub(r"\([^)]*\)", "", text)
    matches = re.findall(r"(\d{1,3}(?:,\d{3})*(?:\.\d+)?)", clean_t)
    
    for num_str in matches:
        val = float(num_str.replace(",", ""))
        if val == 0: continue
        
        if to_eok:
            if "백만원" in text: val = val / 100.0
            elif "억원" in text or "억" in text: pass
            elif val >= 10000000: val = val / 100000000.0 # 1천만 이상은 원 단위로 간주
            
        return str(int(val)) if val.is_integer() else str(round(val, 2))
    return ""

def parse_ratio(text: str) -> str:
    if len(text) > 100: return ""
    m = re.search(r"(\d+(?:\.\d+)?)\s*%", text)
    if m: return m.group(1)
    return ""

def parse_text_short(text: str, max_len=60) -> str:
    """설명충 약관 배제용 텍스트 추출"""
    clean_t = re.sub(r"\s+", " ", str(text)).strip()
    if not clean_t or clean_t in ["-", ".", "해당사항 없음", "해당사항없음"]: return ""
    if len(clean_t) <= max_len: return clean_t
    return ""

# ==========================================
# 4. 표 병합 해제 (Matrix Unroller) & 검색 코어
# ==========================================
def table_to_matrix(table_tag) -> list[list[str]]:
    """가장 중요한 로직: HTML 표의 Rowspan/Colspan을 풀어 2차원 배열로 완벽 복원"""
    rows = table_tag.find_all("tr")
    grid = {}
    max_r, max_c = 0, 0
    
    for r_idx, tr in enumerate(rows):
        c_idx = 0
        for td in tr.find_all(["th", "td"]):
            while grid.get((r_idx, c_idx)) is not None:
                c_idx += 1
            rowspan = int(td.get("rowspan", 1))
            colspan = int(td.get("colspan", 1))
            text = td.get_text(" ", strip=True)
            for i in range(rowspan):
                for j in range(colspan):
                    grid[(r_idx + i, c_idx + j)] = text
            c_idx += colspan
        max_r = max(max_r, r_idx)
        
    matrix = []
    for r in range(max_r + 1):
        c_max = max([c for (row, c) in grid.keys() if row == r] + [-1])
        row_data = [grid.get((r, c), "") for c in range(c_max + 1)]
        if any(cell.strip() for cell in row_data):
            matrix.append(row_data)
    return matrix

def find_in_matrix(matrix, headers: list, extract_func, **kwargs):
    """
    행렬에서 목표 헤더(목차)를 찾은 뒤, 
    우측 셀 -> 아래 셀 -> 대각선 우측아래 셀 순서로 엄격하게 탐색하여 값을 추출합니다.
    """
    norm_headers = [re.sub(r"[\d\.\-\s\(\)\[\]]", "", h).lower() for h in headers]
    
    for r_idx, row in enumerate(matrix):
        for c_idx, cell in enumerate(row):
            clean_cell = re.sub(r"[\d\.\-\s\(\)\[\]]", "", cell).lower()
            if not clean_cell: continue
            
            # 항목명 일치 확인
            if any(h in clean_cell for h in norm_headers):
                
                # 1. 같은 행의 우측 셀들 탐색
                for right_cell in row[c_idx+1:]:
                    val = extract_func(right_cell, **kwargs) if kwargs else extract_func(right_cell)
                    if val: return val
                
                # 2. 아래 행의 셀 탐색 (병합으로 인해 밀린 경우)
                if r_idx + 1 < len(matrix):
                    down_cell = matrix[r_idx+1][c_idx]
                    val = extract_func(down_cell, **kwargs) if kwargs else extract_func(down_cell)
                    if val: return val
                    
                    # 3. 우측 대각선 아래 탐색
                    if c_idx + 1 < len(matrix[r_idx+1]):
                        diag_cell = matrix[r_idx+1][c_idx+1]
                        val = extract_func(diag_cell, **kwargs) if kwargs else extract_func(diag_cell)
                        if val: return val
    return ""

def parse_contents_heavyweight(html_str: str):
    """모든 HTML 테이블을 2차원 배열로 복원한 뒤, 18개 필드를 핀셋 탐색"""
    fields = {k: "" for k in ISSUE_FIELDS}
    if not html_str: return fields, 0, 0

    soup = BeautifulSoup(html_str, "lxml")
    if "&lt;table" in html_str.lower():
        soup = BeautifulSoup(ihtml.unescape(html_str), "lxml")
        
    matrices = [table_to_matrix(t) for t in soup.find_all("table")]
    
    for matrix in matrices:
        # [날짜 필드]
        if not fields["최초 이사회결의일"]: fields["최초 이사회결의일"] = find_in_matrix(matrix, ["최초이사회결의일", "이사회결의일", "결의일"], parse_date)
        if not fields["청약일"]: fields["청약일"] = find_in_matrix(matrix, ["청약일", "청약기간", "청약기일"], parse_date)
        if not fields["납입일"]: fields["납입일"] = find_in_matrix(matrix, ["납입일", "대금납입일", "납입기일"], parse_date)
        
        # [금액/주식수 필드]
        if not fields["신규발행주식수"]: fields["신규발행주식수"] = find_in_matrix(matrix, ["신규발행주식수", "발행할주식의수", "신주의종류와수", "전환에따라발행할주식", "교환에따라발행할주식"], parse_number, to_eok=False)
        if not fields["증자전 주식수"]: fields["증자전 주식수"] = find_in_matrix(matrix, ["증자전발행주식총수", "기발행주식총수", "증자전주식수"], parse_number, to_eok=False)
        
        if not fields["확정발행가(원)"]: fields["확정발행가(원)"] = find_in_matrix(matrix, ["1주당확정발행가액", "신주발행가액", "확정발행가", "전환가액", "교환가액", "교환가격"], parse_price)
        if not fields["기준주가"]: fields["기준주가"] = find_in_matrix(matrix, ["기준주가", "기준주가액"], parse_price)
        
        if not fields["확정발행금액(억원)"]: fields["확정발행금액(억원)"] = find_in_matrix(matrix, ["확정발행금액", "모집총액", "사채의권면총액", "권면총액"], parse_number, to_eok=True)
        
        # [비율 필드]
        if not fields["할인(할증률)"]: fields["할인(할증률)"] = find_in_matrix(matrix, ["할인율", "할증률", "할인할증률"], parse_ratio)
        if not fields["증자비율"]: fields["증자비율"] = find_in_matrix(matrix, ["증자비율"], parse_ratio)
        
        # [텍스트 필드]
        if not fields["증자방식"]: fields["증자방식"] = find_in_matrix(matrix, ["증자방식", "발행방식", "사채발행방법", "배정방법"], parse_text_short, max_len=40)
        if not fields["발행상품"]: fields["발행상품"] = find_in_matrix(matrix, ["발행상품", "사채의종류", "신주의종류", "증권종류"], parse_text_short, max_len=40)
        if not fields["주관사"]: fields["주관사"] = find_in_matrix(matrix, ["주관사", "대표주관회사", "인수회사"], parse_text_short, max_len=40)
        if not fields["투자자"]: fields["투자자"] = find_in_matrix(matrix, ["투자자", "제3자배정대상자", "배정대상자", "사채발행대상자"], parse_text_short, max_len=80)
        if not fields["자금용도"]: fields["자금용도"] = find_in_matrix(matrix, ["자금용도", "자금조달의목적", "자금사용목적"], parse_text_short, max_len=80)

    # 파생 동기화
    if fields["확정발행금액(억원)"] and not fields["증자금액"]:
        fields["증자금액"] = fields["확정발행금액(억원)"]

    filled = sum(1 for v in fields.values() if str(v).strip())
    return fields, len(matrices), filled

# ==========================================
# 5. Playwright & Main 로직
# ==========================================
def extract_acptno(link: str, html_text: str):
    qs = parse_qs(urlparse(link).query)
    acpt = (qs.get("acptno") or qs.get("acptNo") or [None])[0]
    if acpt: return acpt
    m = re.search(r'acptNo"\s*value="(\d+)"', html_text)
    return m.group(1) if m else None

def get_best_html(viewer_url):
    """Playwright를 이용해 숨겨진 Frame 내의 실제 공시 테이블 HTML을 획득합니다."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_default_navigation_timeout(PW_NAV_TIMEOUT_MS)
        try: page.goto(viewer_url, wait_until="networkidle")
        except PWTimeout: pass
        page.wait_for_timeout(PW_WAIT_MS)

        best_html, best_score = "", -1
        for fr in page.frames:
            try:
                html = fr.content()
                t_cnt = html.lower().count("<table") + html.lower().count("&lt;table")
                text_norm = re.sub(r"\s+", "", BeautifulSoup(html, "lxml").get_text(" ", strip=True)).lower()
                k_hits = sum(1 for key in ["발행가", "주식수", "이사회결의일", "납입일", "권면총액"] if key in text_norm)
                score = t_cnt * 2 + k_hits * 10
                
                if score > best_score:
                    best_score, best_html = score, html
            except: continue
        browser.close()
        return best_html

def main():
    raw_ws, ws_yusang, ws_jeonhwan, ws_gyohwan = connect_gs()
    ensure_headers(raw_ws, [ws_yusang, ws_jeonhwan, ws_gyohwan])

    seen_list = load_json(SEEN_FILE, [])
    retry_queue = load_json(RETRY_FILE, [])

    session = requests.Session()
    feed = fetch(session, RSS_URL, referer=f"{BASE}/")
    parsed_feed = feedparser.parse(feed.content)

    items = {it["guid"]: it for it in retry_queue}
    for entry in parsed_feed.entries:
        link = entry.get("link", "")
        guid = entry.get("id") or link
        title = entry.get("title", "")
        if guid and any(k in title for k in KEYWORDS) and guid not in seen_list:
            items[guid] = {"title": title, "link": link, "guid": guid, "pub": entry.get("published", "")}

    items = list(items.values())
    print(f"[QUEUE] 처리대상={len(items)} 완료={len(seen_list)}")
    if not items: return print("✅ 업데이트할 새 공시가 없습니다.")

    new_retry = []

    for item in items:
        title, link, guid, pub = item["title"], item["link"], item["guid"], item["pub"]
        print(f"\n[ITEM] {title}")

        m = re.match(r"^\[([^\]]+)\]\s*([^\s]+)", title.strip())
        market, company = (m.group(1), m.group(2)) if m else ("", "")

        link_res = fetch(session, link, referer=f"{BASE}/")
        acptno = extract_acptno(link, link_res.text)
        if not acptno:
            new_retry.append(item)
            continue

        viewer_url = f"{BASE}/common/disclsviewer.do?method=search&acptno={acptno}&viewerhost="
        vr_shell = fetch(session, viewer_url, referer=link)
        
        options = []
        for opt in BeautifulSoup(vr_shell.text, "lxml").find_all("option"):
            v = opt.get("value", "")
            if re.match(r"^(\d{10,14})\|", v): options.append(v.split("|")[0])

        best_cand = None
        for docno in options[:5]:
            doc_url = f"{viewer_url}&docno={docno}"
            html = get_best_html(doc_url)
            if "<title>창 닫기</title>" in html: continue
            
            fields, tables_cnt, filled = parse_contents_heavyweight(html)
            cand = (filled, tables_cnt, docno, fields)
            if not best_cand or cand[0] > best_cand[0]: best_cand = cand
            if filled >= SUCCESS_FILLED_MIN: break

        if not best_cand:
            print("   [FAIL] 파싱할 수 있는 유효한 데이터를 찾지 못했습니다.")
            new_retry.append(item)
            continue

        filled, tables_cnt, docno, fields = best_cand
        status = "SUCCESS" if filled >= SUCCESS_FILLED_MIN else "INCOMPLETE"
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        version = f"{acptno}-{docno}"

        try:
            rid = get_next_id(raw_ws)
            raw_ws.append_row([rid, now, pub, title, link, guid, status, acptno, docno, filled, tables_cnt, version])

            target_ws = ws_jeonhwan if "전환사채" in title else ws_gyohwan if "교환사채" in title else ws_yusang
            iss_row = [rid, now, pub, company, market, title, link, guid] + [fields.get(k, "") for k in ISSUE_FIELDS] + [version, status, filled, tables_cnt, acptno, docno]
            target_ws.append_row(iss_row)

            if status == "SUCCESS":
                seen_list.append(guid)
                print(f"   -> [SUCCESS] 시트명:{target_ws.title} | 채움:{filled}/18")
            else:
                new_retry.append(item)
                print(f"   -> [INCOMPLETE] 시트명:{target_ws.title} | 채움:{filled}/18 (다음 사이클에 재시도)")

        except Exception as e:
            print(f"   -> [Error] 구글 시트 저장 실패: {e}")
            new_retry.append(item)

        time.sleep(SLEEP_SECONDS)

    save_json(SEEN_FILE, seen_list)
    save_json(RETRY_FILE, new_retry)
    print("\n✅ 전체 작업 완료!")

if __name__ == "__main__":
    main()
