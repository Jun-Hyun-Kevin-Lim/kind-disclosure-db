import os, json, time, re, html as ihtml
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
BOT_VERSION = os.getenv("BOT_VERSION", "kind-bot-v4")

RSS_URL = os.getenv(
    "RSS_URL",
    "http://kind.krx.co.kr:80/disclosure/rsstodaydistribute.do"
    "?method=searchRssTodayDistribute&repIsuSrtCd=&mktTpCd=0&searchCorpName=&currentPageSize=200"
)
BASE = "https://kind.krx.co.kr"

KEYWORDS = ["유상증자", "전환사채", "교환사채"]

SHEET_NAME = os.getenv("SHEET_NAME", "KIND_대경")
RAW_TAB = "RAW"
TAB_YUSANG = "유상증자"
TAB_JEONHWAN = "전환사채"
TAB_GYOHWAN = "교환사채"

SEEN_FILE = os.getenv("SEEN_FILE", "seen.json")
RETRY_FILE = os.getenv("RETRY_FILE", "retry_queue.json")

DEBUG_HTML = os.getenv("DEBUG_HTML", "0") == "1"
SUCCESS_FILLED_MIN = int(os.getenv("SUCCESS_FILLED_MIN", "8")) # 기준을 약간 낮춤 (정밀도 향상으로 빈칸이 생길 수 있음)
SLEEP_SECONDS = float(os.getenv("SLEEP_SECONDS", "0.8"))

PW_NAV_TIMEOUT_MS = int(os.getenv("PW_NAV_TIMEOUT_MS", "25000"))
PW_WAIT_MS = int(os.getenv("PW_WAIT_MS", "3000"))

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
}

ISSUE_FIELDS = [
    "회사명","상장시장","최초 이사회결의일","증자방식","발행상품","신규발행주식수","확정발행가(원)","기준주가","확정발행금액(억원)","할인(할증률)",
    "증자전 주식수","증자비율","청약일","납입일","주관사","자금용도","투자자","증자금액",
]

ALIASES = {
    "최초 이사회결의일": ["최초 이사회결의일","이사회결의일","결의일","결정일","이사회 결의일","이사회결의"],
    "증자방식": ["증자방식","발행방식","배정방법","배정방식","사채발행방법","발행방법"],
    "발행상품": ["발행상품","신주의 종류","주식의 종류","증권종류","사채의 종류","사채 종류"],
    "신규발행주식수": ["신규발행주식수","발행주식수","발행할 주식의 수","신주수","증자할 주식수","전환에 따라 발행할 주식","교환에 따라 발행할 주식"],
    "확정발행가(원)": ["확정발행가","신주발행가액","발행가","발행가액","1주당 발행가액","전환가액","교환가액","발행가(원)","교환가격"],
    "기준주가": ["기준주가","기준주가액"],
    "확정발행금액(억원)": ["확정발행금액","모집총액","발행총액","발행금액","모집금액","조달금액","사채의 권면총액","권면(전자등록)총액","권면총액"],
    "할인(할증률)": ["할인(할증률)","할인율","할증률","할인율(%)","할증률(%)"],
    "증자전 주식수": ["증자전 주식수","증자전 발행주식총수","발행주식총수","기발행주식총수","발행주식 총수"],
    "증자비율": ["증자비율","증자비율(%)","주식총수 대비 비율","발행주식총수 대비","증자비율 %"],
    "청약일": ["청약일","청약기간","청약시작일","청약 개시일","청약 종료일","청약일자"],
    "납입일": ["납입일","대금납입일","납입기일","납입일자"],
    "주관사": ["주관사","대표주관회사","공동주관회사","인수회사","인수단","대표주관"],
    "자금용도": ["자금용도","자금조달의 목적","자금사용 목적","자금조달 목적","자금의 사용 목적","자금사용의 목적"],
    "투자자": ["투자자","제3자배정대상자","배정대상자","발행대상자","대상자","인수인","사채발행대상자","상대방","배정상대방"],
    "증자금액": ["증자금액","발행규모","조달금액","모집금액","총 조달금액","발행금액(원)"],
}

# =========================
# Utils & Sheets
# =========================
def norm(s: str) -> str:
    return re.sub(r"\s+", "", str(s or "")).lower()

def load_json(filepath, default_val):
    if os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f: return json.load(f)
        except Exception: pass
    return default_val

def save_json(filepath, data):
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def fetch(session: requests.Session, url: str, referer: str | None = None, timeout=25):
    headers = dict(DEFAULT_HEADERS)
    if referer: headers["Referer"] = referer
    r = session.get(url, headers=headers, timeout=timeout, allow_redirects=True)
    if not r.encoding or r.encoding.lower() == "iso-8859-1":
        r.encoding = r.apparent_encoding or "utf-8"
    return r

def get_or_create_worksheet(sh, title, rows="1000", cols="30"):
    try: return sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound: return sh.add_worksheet(title=title, rows=rows, cols=cols)

def connect_gs():
    creds_dict = json.loads(os.environ["GOOGLE_CREDS"])
    creds = Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
    )
    client = gspread.authorize(creds)
    sh = client.open(SHEET_NAME)
    
    raw_ws = get_or_create_worksheet(sh, RAW_TAB)
    ws_yusang = get_or_create_worksheet(sh, TAB_YUSANG)
    ws_jeonhwan = get_or_create_worksheet(sh, TAB_JEONHWAN)
    ws_gyohwan = get_or_create_worksheet(sh, TAB_GYOHWAN)
    
    return raw_ws, ws_yusang, ws_jeonhwan, ws_gyohwan

def ensure_headers(raw_ws, issue_sheets: list):
    raw_header = ["ID","수집시간","공시일시","공시제목","공시링크","GUID","처리상태","ACPTNO","DOCNO","FILLED","TABLES","VERSION"]
    if (raw_ws.row_values(1) or [])[:len(raw_header)] != raw_header:
        raw_ws.resize(rows=max(raw_ws.row_count, 1), cols=max(raw_ws.col_count, len(raw_header)))
        raw_ws.update("A1", [raw_header])

    issue_header = ["ID","수집시간","공시일시","회사명","상장시장","공시제목","공시링크","GUID"] + ISSUE_FIELDS + ["VERSION","처리상태","FILLED","TABLES","ACPTNO","DOCNO"]
    for ws in issue_sheets:
        if (ws.row_values(1) or [])[:len(issue_header)] != issue_header:
            ws.resize(rows=max(ws.row_count, 1), cols=max(ws.col_count, len(issue_header)))
            ws.update("A1", [issue_header])

def get_next_id(ws):
    col = ws.col_values(1)
    mx = 0
    for v in col[1:]:
        v = str(v).strip()
        if v.isdigit(): mx = max(mx, int(v))
    return mx + 1

# =========================
# ★ 정밀 추출기 (Extractors) ★
# =========================
def table_to_matrix(table_tag) -> list[list[str]]:
    rows = table_tag.find_all("tr")
    grid = {}
    max_r, max_c = 0, 0
    for r_idx, tr in enumerate(rows):
        c_idx = 0
        for td in tr.find_all(["th", "td"]):
            while grid.get((r_idx, c_idx)) is not None: c_idx += 1
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
        if any(cell.strip() for cell in row_data): matrix.append(row_data)
    return matrix

def get_candidate_cells(matrix, aliases):
    """항목명 우측 또는 아래쪽의 후보 셀들을 반환"""
    for r_idx, row in enumerate(matrix):
        for c_idx, cell in enumerate(row):
            cell_clean = norm(re.sub(r"^[\d\.\-]+\s*", "", cell))
            if any(a == cell_clean or a in cell_clean for a in aliases):
                # 우측 셀
                for val in row[c_idx+1:]:
                    if val.strip(): yield val
                # 아래쪽 셀
                if r_idx + 1 < len(matrix):
                    next_row = matrix[r_idx+1]
                    if c_idx < len(next_row) and next_row[c_idx].strip(): yield next_row[c_idx]
                    elif c_idx + 1 < len(next_row) and next_row[c_idx+1].strip(): yield next_row[c_idx+1]

def extract_date(matrix, key):
    aliases = [norm(a) for a in ALIASES.get(key, []) if a]
    for cand in get_candidate_cells(matrix, aliases):
        if len(cand) > 100: continue # 너무 긴 약관 텍스트 무시
        # 정확히 날짜 포맷만 뽑아냄 (예: 2026-02-11)
        m = re.search(r"(20[1-3]\d)\s*[\-\.\/년]\s*(\d{1,2})\s*[\-\.\/월]\s*(\d{1,2})", cand)
        if m: return f"{m.group(1)}-{m.group(2).zfill(2)}-{m.group(3).zfill(2)}"
    return ""

def extract_number(matrix, key, to_eok=False):
    aliases = [norm(a) for a in ALIASES.get(key, []) if a]
    for cand in get_candidate_cells(matrix, aliases):
        if len(cand) > 150: continue # 계산식이 적힌 긴 텍스트 무시
        if "해당사항" in cand.replace(" ", ""): continue
        
        # 숫자(콤마 포함)만 핀셋 추출
        m = re.search(r"(\d{1,3}(?:,\d{3})*(?:\.\d+)?|\d+(?:\.\d+)?)", cand)
        if m:
            raw_num_str = m.group(1).replace(",", "")
            try:
                num = float(raw_num_str)
                if num == 0: continue
                if to_eok:
                    if "백만원" in cand: num = num / 100.0
                    elif "원" in cand and "억원" not in cand and "억" not in cand:
                        num = num / 100000000.0
                    elif num >= 10000000: # 단위가 생략됐어도 1천만이 넘어가면 원 단위로 간주하고 억원으로 변환
                        num = num / 100000000.0
                return str(int(num)) if num.is_integer() else str(round(num, 2))
            except ValueError: continue
    return ""

def extract_ratio(matrix, key):
    aliases = [norm(a) for a in ALIASES.get(key, []) if a]
    for cand in get_candidate_cells(matrix, aliases):
        if len(cand) > 100: continue
        m = re.search(r"(\d+(?:\.\d+)?)\s*%", cand)
        if m: return m.group(1)
        m2 = re.search(r"(\d+(?:\.\d+)?)", cand)
        if m2: return m2.group(1)
    return ""

def extract_text(matrix, key):
    aliases = [norm(a) for a in ALIASES.get(key, []) if a]
    for cand in get_candidate_cells(matrix, aliases):
        if len(cand) > 150: continue # 150자가 넘어가는 글은 약관/설명서로 간주하고 버림
        if "해당사항" in cand.replace(" ", ""): return ""
        clean_text = re.sub(r"\s+", " ", cand).strip()
        return clean_text
    return ""

def parse_contents_html(contents_html: str):
    fields = {k: "" for k in ISSUE_FIELDS}
    if not contents_html: return fields, 0, 0

    soup = BeautifulSoup(contents_html, "lxml")
    matrices = [table_to_matrix(t) for t in soup.find_all("table")]
    
    if "&lt;table" in contents_html.lower():
        unesc = ihtml.unescape(contents_html)
        soup2 = BeautifulSoup(unesc, "lxml")
        matrices.extend([table_to_matrix(t) for t in soup2.find_all("table")])

    t_cnt = len(matrices)

    # 각 필드의 성격에 맞는 전용 함수를 호출
    for key in ISSUE_FIELDS:
        val = ""
        for matrix in matrices:
            if key in ["최초 이사회결의일", "청약일", "납입일"]:
                val = extract_date(matrix, key)
            elif key in ["확정발행금액(억원)", "증자금액"]:
                val = extract_number(matrix, key, to_eok=True)
            elif key in ["신규발행주식수", "확정발행가(원)", "기준주가", "증자전 주식수"]:
                val = extract_number(matrix, key, to_eok=False)
            elif key in ["할인(할증률)", "증자비율"]:
                val = extract_ratio(matrix, key)
            else:
                val = extract_text(matrix, key)
            
            if val: break # 값을 찾았으면 다음 행렬 탐색 중지
        
        fields[key] = val

    filled = sum(1 for v in fields.values() if str(v).strip())
    return fields, t_cnt, filled

# =========================
# Sub Utils
# =========================
def fetch_rss(session):
    r = fetch(session, RSS_URL, referer=f"{BASE}/")
    return feedparser.parse(r.content)

def extract_company_from_title(title: str):
    m = re.match(r"^\[([^\]]+)\]\s*([^\s]+)", (title or "").strip())
    return m.group(2) if m else ""

def extract_market_code_from_title(title: str):
    m = re.match(r"^\[([^\]]+)\]", (title or "").strip())
    return m.group(1).strip() if m else ""

def clean_report_title(title: str):
    t = (title or "").strip()
    t = re.sub(r"^\[[^\]]+\]\s*", "", t)
    t = re.sub(r"^[^\s]+\s+", "", t, count=1)
    return t.replace("[정정]", "").strip()

def extract_acptno(link: str, html_text: str):
    qs = parse_qs(urlparse(link).query)
    acpt = (qs.get("acptno") or qs.get("acptNo") or [None])[0]
    if acpt: return acpt
    m = re.search(r'acptNo"\s*value="(\d+)"', html_text or "")
    if m: return m.group(1)
    m = re.search(r"(acptno|acptNo)=(\d{8,14})", html_text or "")
    if m: return m.group(2)
    return None

def build_viewer_url(acptno: str, docno: str | None):
    base = f"{BASE}/common/disclsviewer.do"
    params = {"method": "search", "acptno": acptno, "docno": docno or "", "viewerhost": ""}
    return base + "?" + urlencode(params, doseq=True)

def parse_docno_options(viewer_html: str):
    soup = BeautifulSoup(viewer_html or "", "lxml")
    options = []
    for opt in soup.find_all("option"):
        v = (opt.get("value") or "").strip()
        txt = opt.get_text(" ", strip=True)
        m = re.match(r"^(\d{10,14})\|", v)
        if m: options.append((m.group(1), txt))
    seen = set()
    out = []
    for d, t in options:
        if d not in seen:
            out.append((d, t))
            seen.add(d)
    return out

def heuristic_docno_rank(options, report_hint: str):
    hint = norm(report_hint)
    ranked = []
    for docno, txt in options:
        tn = norm(txt)
        score = 0
        if hint and tn and hint in tn: score += 100
        for kw in KEYWORDS:
            if kw and (kw in report_hint) and (kw in txt): score += 20
            if kw and (kw in txt): score += 5
        if "정정" in txt: score += 8
        if "결정" in txt: score += 5
        if txt and len(txt) < 4: score -= 3
        ranked.append((score, docno, txt))
    ranked.sort(reverse=True, key=lambda x: x[0])
    return ranked

def score_frame(html_content: str) -> tuple[int,int,int]:
    lower = (html_content or "").lower()
    table_count = lower.count("<table") + lower.count("&lt;table")
    text_norm = norm(BeautifulSoup(html_content or "", "lxml").get_text(" ", strip=True))
    key_hits = sum(1 for key in ISSUE_FIELDS for a in ALIASES.get(key, []) if a and norm(a) in text_norm)
    return table_count * 2 + key_hits * 8, key_hits, table_count

def get_kind_contents_html_by_playwright(viewer_url: str):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(locale="ko-KR")
        page = context.new_page()
        page.set_default_navigation_timeout(PW_NAV_TIMEOUT_MS)

        try: page.goto(viewer_url, wait_until="networkidle")
        except PWTimeout: pass
        page.wait_for_timeout(PW_WAIT_MS)

        best_html, best_label, best_score = "", "NONE", -1
        best_keyhits, best_tables = 0, 0

        for idx, fr in enumerate(page.frames):
            try:
                html_content = fr.content()
                total, key_hits, table_count = score_frame(html_content)
                if total > best_score:
                    best_score, best_html, best_keyhits, best_tables = total, html_content, key_hits, table_count
                    best_label = f"frame#{idx} key_hits={key_hits} tables={table_count}"
            except Exception: continue
        browser.close()
        return best_html, best_label, best_keyhits, best_tables

# =========================
# Main
# =========================
def main():
    raw_ws, ws_yusang, ws_jeonhwan, ws_gyohwan = connect_gs()
    ensure_headers(raw_ws, [ws_yusang, ws_jeonhwan, ws_gyohwan])

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
        
        if not guid: continue
        if KEYWORDS and not any(k in title for k in KEYWORDS): continue
        if guid in seen_list: continue
            
        items.append({"title": title, "link": link, "guid": guid, "pub": pub})

    items.extend(retry_queue)
    uniq = {it["guid"]: it for it in items}
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

        print(f"\n[ITEM] {title}")

        link_res = fetch(session, link, referer=f"{BASE}/")
        acptno = extract_acptno(link, link_res.text)
        if not acptno:
            new_retry.append(item)
            continue

        viewer_shell = build_viewer_url(acptno, None)
        vr_shell = fetch(session, viewer_shell, referer=link)
        options = parse_docno_options(vr_shell.text)

        if not options:
            new_retry.append(item)
            continue

        ranked = heuristic_docno_rank(options, report_hint)
        best = None
        candidates = ranked[: min(6, len(ranked))]

        for _, docno, txt in candidates:
            viewer_doc = build_viewer_url(acptno, docno)
            contents_html, frame_label, key_hits, table_cnt_raw = get_kind_contents_html_by_playwright(viewer_doc)

            if "<title>창 닫기</title>" in (contents_html or ""): continue

            fields, tables_cnt, filled = parse_contents_html(contents_html)
            cand = (filled, tables_cnt, docno, frame_label, fields)
            if best is None or cand[:2] > best[:2]:
                best = cand

            if filled >= SUCCESS_FILLED_MIN:
                break

        if best is None:
            new_retry.append(item)
            continue

        filled, tables_cnt, docno, frame_label, fields = best
        status = "SUCCESS" if filled >= SUCCESS_FILLED_MIN else "INCOMPLETE"
        version = f"{acptno}-{docno}"
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        try:
            rid = get_next_id(raw_ws)
            raw_ws.append_row(
                [rid, now, pub, title, link, guid, status, acptno, docno, filled, tables_cnt, version],
                value_input_option="USER_ENTERED"
            )

            target_ws = ws_yusang
            if "전환사채" in title: target_ws = ws_jeonhwan
            elif "교환사채" in title: target_ws = ws_gyohwan

            issue_row = (
                [rid, now, pub, company, market_code, title, link, guid]
                + [fields.get(k, "") for k in ISSUE_FIELDS]
                + [version, status, filled, tables_cnt, acptno, docno]
            )
            
            target_ws.append_row(issue_row, value_input_option="USER_ENTERED")

            if status == "SUCCESS":
                if guid not in seen_list: seen_list.append(guid)
                print(f"   -> [SUCCESS] target_sheet={target_ws.title} filled={filled} version={version}")
            else:
                if item not in new_retry: new_retry.append(item)
                print(f"   -> [INCOMPLETE] target_sheet={target_ws.title} filled={filled} (retry)")

        except Exception as e:
            print(f"   -> [Google Sheets Error] {e}")
            new_retry.append(item)

        time.sleep(SLEEP_SECONDS)

    save_json(SEEN_FILE, seen_list)
    save_json(RETRY_FILE, new_retry)
    print("\n✅ 모든 작업 완료!")

if __name__ == "__main__":
    main()
