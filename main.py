import os, json, time, re, html
from datetime import datetime
from urllib.parse import urlparse, parse_qs, urlencode

import feedparser
import requests
import gspread
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# =========================
# Config
# =========================
BOT_VERSION = os.getenv("BOT_VERSION", "kind-bot-v1")
DEBUG_HTML = os.getenv("DEBUG_HTML", "0") == "1"
DUMP_FAIL_HTML = os.getenv("DUMP_FAIL_HTML", "0") == "1"

SUCCESS_FILLED_MIN = int(os.getenv("SUCCESS_FILLED_MIN", "10"))

RSS_URL = "http://kind.krx.co.kr:80/disclosure/rsstodaydistribute.do?method=searchRssTodayDistribute&repIsuSrtCd=&mktTpCd=0&searchCorpName=&currentPageSize=100"
KEYWORDS = ["전환사채","유상증자","교환사채"]

SHEET_NAME = "KIND_대경"
RAW_TAB = "RAW"
ISSUE_TAB = "ISSUE"

SEEN_FILE = "seen.json"
RETRY_FILE = "retry_queue.json"

BASE = "https://kind.krx.co.kr"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
}

TARGET_KEYS = [
    "최초 이사회결의일","증자방식","발행상품","신규발행주식수","확정발행가(원)","기준주가","확정발행금액(억원)",
    "할인(할증률)","증자전 주식수","증자비율","청약일","납입일","주관사","자금용도","투자자","증자금액"
]

ALIASES = {
    "최초 이사회결의일": ["이사회결의일","결의일","결정일","이사회 결의일"],
    "증자방식": ["증자방식","발행방식","배정방법","배정방식"],
    "발행상품": ["신주의 종류","주식의 종류","증권종류","발행상품"],
    "신규발행주식수": ["신규발행주식수","발행주식수","발행할 주식의 수","신주수","증자할 주식수"],
    "확정발행가(원)": ["확정발행가","신주발행가액","발행가","발행가액","1주당 발행가액"],
    "기준주가": ["기준주가","기준주가액"],
    "확정발행금액(억원)": ["확정발행금액","모집총액","발행총액","발행금액","모집금액","조달금액"],
    "할인(할증률)": ["할인율","할증률","할인율(%)"],
    "증자전 주식수": ["증자전 주식수","증자전 발행주식총수","발행주식총수","기발행주식총수"],
    "증자비율": ["증자비율","증자비율(%)"],
    "청약일": ["청약일","청약기간","청약시작일"],
    "납입일": ["납입일","대금납입일"],
    "주관사": ["주관사","대표주관회사","공동주관회사","인수회사","인수단"],
    "자금용도": ["자금용도","자금조달의 목적","자금사용 목적","자금조달 목적"],
    "투자자": ["투자자","제3자배정대상자","배정대상자","발행대상자","대상자","인수인"],
    "증자금액": ["증자금액","발행규모","조달금액","모집금액","총 조달금액"],
}

SLEEP_SECONDS = float(os.getenv("SLEEP_SECONDS", "1"))

# Playwright timeouts
PW_NAV_TIMEOUT_MS = int(os.getenv("PW_NAV_TIMEOUT_MS", "20000"))
PW_FRAME_TIMEOUT_MS = int(os.getenv("PW_FRAME_TIMEOUT_MS", "20000"))

# =========================
# Utils
# =========================
def norm(s: str) -> str:
    return re.sub(r"\s+", "", str(s or "")).lower()

def load_json(filepath, default_val):
    if os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            pass
    return default_val

def save_json(filepath, data):
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def fetch(session: requests.Session, url: str, referer: str | None = None, timeout=25):
    headers = dict(DEFAULT_HEADERS)
    if referer:
        headers["Referer"] = referer
    r = session.get(url, headers=headers, timeout=timeout, allow_redirects=True)
    if not r.encoding or r.encoding.lower() == "iso-8859-1":
        r.encoding = r.apparent_encoding or "utf-8"
    return r

def connect_gs():
    creds_dict = json.loads(os.environ["GOOGLE_CREDS"])
    creds = Credentials.from_service_account_info(
        creds_dict,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    client = gspread.authorize(creds)
    sh = client.open(SHEET_NAME)
    raw_ws = sh.worksheet(RAW_TAB)
    issue_ws = sh.worksheet(ISSUE_TAB)
    print(f"[BOT] {BOT_VERSION}")
    print(f"[GS] Opened spreadsheet='{sh.title}' RAW='{raw_ws.title}' ISSUE='{issue_ws.title}'")
    return raw_ws, issue_ws

def get_next_id(ws):
    col = ws.col_values(1)
    if len(col) <= 1:
        return 0
    last = str(col[-1]).strip()
    if last.isdigit():
        return int(last) + 1
    mx = -1
    for v in col:
        v = str(v).strip()
        if v.isdigit():
            mx = max(mx, int(v))
    return mx + 1

def fetch_rss(session):
    r = fetch(session, RSS_URL, referer=f"{BASE}/")
    feed = feedparser.parse(r.content)
    print(f"[RSS] status={r.status_code} bytes={len(r.content)} entries={len(feed.entries)}")
    return feed

def extract_company_from_title(title: str):
    m = re.match(r"^\[([^\]]+)\]\s*([^\s]+)", (title or "").strip())
    return m.group(2) if m else ""

def extract_market_code_from_title(title: str):
    m = re.match(r"^\[([^\]]+)\]", (title or "").strip())
    return m.group(1).strip() if m else ""

def clean_report_title(title: str):
    t = (title or "").strip()
    t = re.sub(r"^\[[^\]]+\]\s*", "", t)        # [코] 제거
    t = re.sub(r"^[^\s]+\s+", "", t, count=1)   # 회사명 제거
    t = t.replace("[정정]", "").strip()
    return t

def extract_acptno_from_link(link: str, html_text: str):
    qs = parse_qs(urlparse(link).query)
    acpt = (qs.get("acptno") or qs.get("acptNo") or [None])[0]
    if acpt:
        return acpt
    m = re.search(r'acptNo"\s*value="(\d+)"', html_text)
    if m:
        return m.group(1)
    m = re.search(r'_TRK_PN\s*=\s*"(\d+)"', html_text)
    if m:
        return m.group(1)
    m = re.search(r"(acptno|acptNo)=(\d{8,14})", html_text)
    if m:
        return m.group(2)
    return None

def build_viewer_url(acptno: str, docno: str | None):
    base = f"{BASE}/common/disclsviewer.do"
    params = {
        "method": "search",
        "acptno": acptno,
        "docno": docno or "",
        "viewerhost": "",
    }
    return base + "?" + urlencode(params, doseq=True)

def parse_docno_options(viewer_html: str):
    soup = BeautifulSoup(viewer_html, "lxml")
    options = []
    for opt in soup.find_all("option"):
        v = (opt.get("value") or "").strip()
        txt = opt.get_text(" ", strip=True)
        # value가 "2026...|..." 형태가 많음 → 앞 숫자만 docno로 사용
        m = re.match(r"^(\d{10,14})\|", v)
        if m:
            options.append((m.group(1), txt))
    # fallback: option이 없으면 docno 형태만이라도 긁기
    if not options:
        for m in re.finditer(r'(\d{10,14})\|', viewer_html or ""):
            options.append((m.group(1), ""))
    # uniq
    uniq = {}
    for d, t in options:
        uniq[d] = t
    return [(d, uniq[d]) for d in uniq.keys()]

def choose_best_docno(options, report_hint: str):
    hint = norm(report_hint)
    best_doc, best_score = None, -1
    for docno, txt in options:
        tn = norm(txt)
        score = 0
        if hint and tn and hint in tn:
            score += 100
        if "유상증자" in tn:
            score += 10
        if "결정" in tn:
            score += 5
        if txt and len(txt) < 5:
            score -= 3
        if not txt:
            score += 1
        if score > best_score:
            best_score = score
            best_doc = docno
    return best_doc

# =========================
# Table parse (수정된 추출 로직: 정정 후 데이터 및 다중 행 구조 대응)
# =========================
def pick_value_from_row(row, key_idx):
    """
    변경: '정정 후' 값은 항상 표의 가장 오른쪽(마지막)에 위치하므로, 
          뒤에서부터(reversed) 우선적으로 탐색합니다.
    """
    # 1. 뒤에서부터 탐색 (정정 후 수치 우선 확보)
    for j in range(len(row) - 1, key_idx, -1):
        v = (row[j] or "").strip()
        
        # 비어있거나 무의미한 하이픈, 0 등은 건너뜀
        if not v or v in ("-", "—", ".", "0", "0원"):
            continue
            
        # 200자가 넘는 너무 긴 텍스트(법적 조항 등)는 실제 데이터가 아닐 확률이 높으므로 패스
        if len(v) > 200:
            continue
            
        return v
    
    # 2. 뒤에서 찾았는데 없으면(긴 조항만 있었던 경우 등), 앞에서부터 다시 탐색 (최후의 보루)
    for j in range(key_idx + 1, len(row)):
        v = (row[j] or "").strip()
        if not v or v in ("-", "—"):
            continue
        if len(v) > 200:
            continue
        return v
        
    return ""

def extract_from_matrix(matrix, aliases):
    als = [norm(a) for a in aliases]
    for r_idx, r in enumerate(matrix):
        for i, cell in enumerate(r):
            c = (cell or "").strip()
            if not c:
                continue
            cn = norm(re.sub(r"^\d+\.\s*", "", c))
            
            # 매칭되는 키워드(Alias)를 찾았을 때
            if any(a and a in cn for a in als):
                
                # [특수 케이스 방어] '자금조달의 목적' 표는 제목 바로 아랫줄에 실제 금액이 기재됨
                if "자금" in cn or "목적" in cn:
                    if r_idx + 1 < len(matrix):
                        next_row = matrix[r_idx + 1]
                        # 아랫줄을 우측부터 역순 탐색하여 진짜 기재된 금액 찾기
                        for j in range(len(next_row) - 1, -1, -1):
                            nv = (next_row[j] or "").strip()
                            if nv and nv not in ("-", "—", ".", "0", "0원") and len(nv) < 50:
                                # 금액과 해당 열의 헤더(예: 운영자금)를 합쳐서 반환 ("운영자금 6,000,000,000" 형태)
                                header = (r[j] or "").strip() if j < len(r) else ""
                                return f"{header} {nv}".strip()

                # 일반적인 표 구조인 경우 (역순 추출 함수 호출)
                v = pick_value_from_row(r, i)
                if v:
                    return v
    return ""

# =========================
# HTML 파싱 및 상태 판별 함수 (복구된 부분!)
# =========================
def parse_contents_html(html_text: str):
    fields = {k: "" for k in TARGET_KEYS}
    if not html_text:
        return fields, 0

    soup = BeautifulSoup(html_text, "lxml")
    tables = soup.find_all("table")

    for table in tables:
        matrix = []
        for tr in table.find_all("tr"):
            row = []
            for td in tr.find_all(["td", "th"]):
                text = td.get_text(" ", strip=True)
                row.append(text)
            if row:
                matrix.append(row)

        for key in TARGET_KEYS:
            if not fields[key]:
                val = extract_from_matrix(matrix, ALIASES.get(key, []))
                if val:
                    fields[key] = val

    return fields, len(tables)

def decide_status(fields):
    filled = sum(1 for v in fields.values() if str(v).strip())
    if filled >= SUCCESS_FILLED_MIN:
        return "SUCCESS"
    return "INCOMPLETE"

# =========================
# Playwright: fetch viewer contents HTML
# =========================
def get_kind_contents_html_by_playwright(viewer_url: str) -> tuple[str, str]:
    """
    모든 프레임을 탐색하여 <table> 태그가 가장 많은 프레임의 HTML을 반환합니다.
    URL 이름 기반의 부정확한 매칭을 피하고 실제 데이터가 로드될 때까지 대기합니다.
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(locale="ko-KR")
        page = context.new_page()
        page.set_default_navigation_timeout(PW_NAV_TIMEOUT_MS)

        try:
            # domcontentloaded 대신 networkidle을 사용하여 내부 iframe의 네트워크 요청까지 대기
            page.goto(viewer_url, wait_until="networkidle")
        except PWTimeout:
            pass

        # 자바스크립트로 동적 생성되는 테이블이 렌더링될 수 있도록 3초간 명시적 대기
        page.wait_for_timeout(3000)

        best_html = ""
        best_label = "NONE"
        max_tables = -1

        # 현재 로드된 모든 프레임을 순회하며 진짜 본문(테이블이 많은 곳)을 찾음
        for fr in page.frames:
            try:
                html_content = fr.content()
                lower_html = html_content.lower()
                
                # 정규 HTML 테이블과 이스케이프된(&lt;table) 테이블 모두 카운트
                table_count = lower_html.count("<table") + lower_html.count("&lt;table")
                
                # 테이블 개수가 가장 많은 프레임을 본문으로 낙점
                if table_count > max_tables:
                    max_tables = table_count
                    best_html = html_content
                    best_label = f"FRAME_WITH_{table_count}_TABLES"
            except Exception:
                continue

        browser.close()
        return best_html, best_label

# =========================
# Main
# =========================
def main():
    raw_ws, issue_ws = connect_gs()
    seen_list = load_json(SEEN_FILE, [])
    retry_queue = load_json(RETRY_FILE, [])

    session = requests.Session()
    feed = fetch_rss(session)

    items = []
    for entry in feed.entries:
        title = entry.get("title", "") or ""
        link = entry.get("link", "") or ""
        guid = entry.get("id") or link
        pub = entry.get("published", "") or ""
        if not guid:
            continue
        if KEYWORDS and not any(k in title for k in KEYWORDS):
            continue
        if guid in seen_list:
            continue
        items.append({"title": title, "link": link, "guid": guid, "pub": pub})

    items.extend(retry_queue)

    uniq = {}
    for it in items:
        uniq[it["guid"]] = it
    items = list(uniq.values())

    print(f"[QUEUE] to_process={len(items)} seen={len(seen_list)} retry={len(retry_queue)}")
    if not items:
        print("✅ 모든 작업 완료!")
        return

    new_retry = []

    for item in items:
        title = item["title"]
        link = item["link"]
        guid = item["guid"]
        pub = item.get("pub", "")

        company = extract_company_from_title(title)
        market_code = extract_market_code_from_title(title)
        report_hint = clean_report_title(title)

        print(f"\nProcessing: {title}")

        # 1) RSS link → acptno
        link_res = fetch(session, link, referer=f"{BASE}/")
        acptno = extract_acptno_from_link(link, link_res.text)
        if not acptno:
            print("   [FAIL] acptNo not found")
            new_retry.append(item)
            continue

        # 2) viewer shell → docno options
        viewer_shell = build_viewer_url(acptno, None)
        vr_shell = fetch(session, viewer_shell, referer=link)
        options = parse_docno_options(vr_shell.text)

        if DEBUG_HTML:
            print(f"   [VIEWER SHELL] bytes={len(vr_shell.content)} options={len(options)}")

        if not options:
            print("   [FAIL] docNo options not found")
            if DUMP_FAIL_HTML:
                print("   [DUMP] viewer_shell (first 2000 chars)")
                print(vr_shell.text[:2000])
            new_retry.append(item)
            continue

        docno = choose_best_docno(options, report_hint)
        if not docno:
            print("   [FAIL] docNo choose failed")
            new_retry.append(item)
            continue

        # 3) viewer doc url
        viewer_doc = build_viewer_url(acptno, docno)

        if DEBUG_HTML:
            print(f"   [VIEWER DOC] url={viewer_doc}")

        # ✅ 4) Playwright로 본문 HTML 확보
        contents_html, path = get_kind_contents_html_by_playwright(viewer_doc)

        # “창 닫기”면 실패로 처리(재시도 큐)
        if "<title>창 닫기</title>" in (contents_html or ""):
            print(f"   [FAIL] CLOSE-WINDOW even with Playwright. path={path}")
            if DUMP_FAIL_HTML:
                print("   [DUMP] contents_html (first 2000 chars)")
                print((contents_html or "")[:2000])
            fields = {k: "" for k in TARGET_KEYS}
            tables_cnt = 0
            status = "INCOMPLETE"
        else:
            fields, tables_cnt = parse_contents_html(contents_html)
            status = decide_status(fields)

        filled = sum(1 for v in fields.values() if str(v).strip())
        version = f"{acptno}-{docno}"

        if DEBUG_HTML:
            print(f"   [PARSE] path={path} tables={tables_cnt} filled={filled}/{len(TARGET_KEYS)} status={status} VERSION={version}")

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        try:
            raw_id = get_next_id(raw_ws)

            raw_ws.append_row(
                [raw_id, now, pub, title, link, guid, status],
                value_input_option="USER_ENTERED"
            )

            issue_row = (
                [raw_id, now, pub, title, link, guid, company, market_code]
                + [fields.get(k, "") for k in TARGET_KEYS]
                + [version]
            )

            issue_ws.append_row(issue_row, value_input_option="USER_ENTERED")

            # --- 여기서부터 수정 ---
            if status == "SUCCESS":
                if guid not in seen_list:
                    seen_list.append(guid)
                print(f"-> [SUCCESS] 데이터 확보 완료. seen에 추가됨.")
            else:
                # 데이터가 부족하면 retry_queue에 넣어 다음 턴에 다시 시도
                if item not in new_retry:
                    new_retry.append(item)
                print(f"-> [INCOMPLETE] 데이터 부족 ({filled}/{len(TARGET_KEYS)}). 다음 실행 시 재시도합니다.")
            # --- 여기까지 수정 ---

            print(f"-> Saved ({status}) path={path} tables={tables_cnt} VERSION={version}")

        except Exception as e:
            print(f"-> [Google Sheets Error] {e}")
            new_retry.append(item)

        time.sleep(SLEEP_SECONDS)

    save_json(SEEN_FILE, seen_list)
    save_json(RETRY_FILE, new_retry)
    print("\n✅ 모든 작업 완료!")

if __name__ == "__main__":
    main()
