# ====== KIND Disclosure Bot (v2: get docNo from "본문선택" then fetch real searchContents tables) ======
import os, json, time, re, html
from datetime import datetime
from urllib.parse import urlparse, parse_qs, urlencode, urljoin

import feedparser
import requests
import gspread
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials

BOT_VERSION = "kind-bot-v1"
DEBUG_HTML = os.getenv("DEBUG_HTML", "0") == "1"

RSS_URL = "https://kind.krx.co.kr/disclosure/rsstodaydistribute.do?method=searchRssTodayDistribute&repIsuSrtCd=&mktTpCd=0&searchCorpName=&currentPageSize=200"
KEYWORDS = ["유상증자"]

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

# 네가 원하는 16개 + VERSION
TARGET_KEYS = {
    "최초 이사회결의일": ["이사회결의일", "결의일", "결정일", "이사회 결의일"],
    "증자방식": ["증자방식", "발행방식", "배정방법", "배정방식"],
    "발행상품": ["신주의 종류", "주식의 종류", "증권종류", "발행상품"],
    "신규발행주식수": ["신규발행주식수", "발행주식수", "발행할 주식의 수", "신주수", "증자할 주식수"],
    "확정발행가(원)": ["확정발행가", "신주발행가액", "발행가", "발행가액", "1주당 발행가액"],
    "기준주가": ["기준주가", "기준주가액"],
    "확정발행금액(억원)": ["확정발행금액", "모집총액", "발행총액", "발행금액", "모집금액", "조달금액"],
    "할인(할증률)": ["할인율", "할증률", "할인율(%)"],
    "증자전 주식수": ["증자전 주식수", "증자전 발행주식총수", "발행주식총수", "기발행주식총수"],
    "증자비율": ["증자비율", "증자비율(%)"],
    "청약일": ["청약일", "청약기간", "청약시작일"],
    "납입일": ["납입일", "대금납입일"],
    "주관사": ["주관사", "대표주관회사", "공동주관회사", "인수회사", "인수단"],
    "자금용도": ["자금용도", "자금조달의 목적", "자금사용 목적", "자금조달 목적"],
    "투자자": ["투자자", "제3자배정대상자", "배정대상자", "발행대상자", "대상자", "인수인"],
    "증자금액": ["증자금액", "발행규모", "조달금액", "모집금액", "총 조달금액"],
}

SLEEP_SECONDS = 1


# ---------------- utils ----------------
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

def connect_gs():
    creds_dict = json.loads(os.environ["GOOGLE_CREDS"])
    creds = Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
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

def fetch(session: requests.Session, url: str, referer: str | None = None, timeout=25):
    headers = dict(DEFAULT_HEADERS)
    if referer:
        headers["Referer"] = referer
    r = session.get(url, headers=headers, timeout=timeout, allow_redirects=True)
    if not r.encoding or r.encoding.lower() == "iso-8859-1":
        r.encoding = r.apparent_encoding or "utf-8"
    return r

def fetch_rss(session):
    r = fetch(session, RSS_URL)
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
    """
    RSS 제목에서 회사/시장 prefix 제거하고 보고서명만 남김
    예: "[코]유티아이 유상증자결정(종속회사의 주요경영사항)" -> "유상증자결정(종속회사의 주요경영사항)"
    """
    t = (title or "").strip()
    t = re.sub(r"^\[[^\]]+\]\s*", "", t)        # [코] 제거
    t = re.sub(r"^[^\s]+\s+", "", t, count=1)   # 회사명 1단어 제거
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

def build_viewer_shell_url(acptno: str):
    """
    ✅ 핵심: docNo를 넣지 말고 공시뷰어 '껍데기'를 먼저 연다.
    (브라우저도 이렇게 열고, 본문선택에서 docNo를 고름)
    """
    base = f"{BASE}/common/disclsviewer.do"
    params = {
        "method": "search",
        "acptno": acptno,
        "docno": "",
        "viewerhost": "",   # 실제 페이지들도 viewerhost= 형태가 많음
    }
    return base + "?" + urlencode(params, doseq=True)

def parse_body_docno_from_viewer(viewer_html: str, report_hint: str):
    """
    viewer 페이지에서 '본문선택' 드롭다운의 option value에서 docNo를 찾는다.
    report_hint(보고서명)과 option 텍스트 매칭으로 가장 맞는 docNo를 선택.
    """
    soup = BeautifulSoup(viewer_html, "lxml")

    # 후보 option들 수집: value="12345|..." 형태
    options = []
    for opt in soup.find_all("option"):
        v = (opt.get("value") or "").strip()
        txt = opt.get_text(" ", strip=True)
        m = re.match(r"^(\d{6,14})\|", v)
        if m:
            docno = m.group(1)
            options.append((docno, txt))

    if not options:
        return None

    # 보고서명 힌트로 best match
    hint_n = norm(report_hint)
    best = None
    best_score = -1
    for docno, txt in options:
        tn = norm(txt)
        score = 0
        # 힌트가 옵션에 포함되면 큰 가점
        if hint_n and hint_n in tn:
            score += 100
        # "유상증자결정" 같은 핵심 단어 가점
        if "유상증자" in tn:
            score += 10
        if "결정" in tn:
            score += 5
        # 텍스트 길이도 너무 짧으면 감점
        if len(txt) < 5:
            score -= 5

        if score > best_score:
            best_score = score
            best = docno

    return best

def build_search_contents_url(acptno: str, docno: str):
    base = f"{BASE}/common/disclsviewer.do"
    params = {
        "method": "searchContents",
        "acptno": acptno,
        "docno": docno,
    }
    return base + "?" + urlencode(params)

def extract_embedded_html(raw: str) -> str:
    # table이 escape되어 있는 케이스 복원
    if "<table" in raw.lower():
        return raw
    if "&lt;table" in raw.lower():
        decoded = html.unescape(raw)
        if "<table" in decoded.lower():
            return decoded
    return raw


# ---------------- table parsing (rowspan/colspan -> matrix) ----------------
def table_to_matrix(table):
    matrix = []
    span_map = {}
    for tr in table.find_all("tr"):
        row = []
        col = 0
        while col in span_map:
            txt, remain = span_map[col]
            row.append(txt)
            remain -= 1
            if remain <= 0:
                del span_map[col]
            else:
                span_map[col] = (txt, remain)
            col += 1

        for cell in tr.find_all(["th", "td"]):
            while col in span_map:
                txt, remain = span_map[col]
                row.append(txt)
                remain -= 1
                if remain <= 0:
                    del span_map[col]
                else:
                    span_map[col] = (txt, remain)
                col += 1

            text = cell.get_text(" ", strip=True)
            rowspan = int(cell.get("rowspan", "1") or "1")
            colspan = int(cell.get("colspan", "1") or "1")

            for _ in range(colspan):
                row.append(text)
                if rowspan > 1:
                    span_map[col] = (text, rowspan - 1)
                col += 1

        matrix.append(row)
    return matrix

def pick_value_from_row(row, key_idx):
    for j in range(key_idx + 1, len(row)):
        v = (row[j] or "").strip()
        if not v or v in ("-", "—"):
            continue
        return v
    return ""

def extract_from_matrix(matrix, aliases):
    als = [norm(a) for a in aliases]
    for r in matrix:
        for i, cell in enumerate(r):
            c = (cell or "").strip()
            if not c:
                continue
            cn = norm(re.sub(r"^\d+\.\s*", "", c))
            if any(a and a in cn for a in als):
                v = pick_value_from_row(r, i)
                if v:
                    return v
    return ""

def parse_html_to_fields(html_text: str):
    soup = BeautifulSoup(html_text, "lxml")
    tables = soup.find_all("table")
    matrices = [table_to_matrix(t) for t in tables]

    out = {k: "" for k in TARGET_KEYS.keys()}
    for target, aliases in TARGET_KEYS.items():
        for mtx in matrices:
            v = extract_from_matrix(mtx, aliases)
            if v:
                out[target] = v
                break

    return out, len(tables)

def decide_status(fields: dict):
    filled = sum(1 for v in fields.values() if str(v).strip())
    return "SUCCESS" if filled >= 3 else "INCOMPLETE"


# ---------------- main ----------------
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

    # retry 합치기
    items.extend(retry_queue)

    # dedupe
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

        print(f"\nProcessing: {title}")

        company = extract_company_from_title(title)
        market_code = extract_market_code_from_title(title)
        report_hint = clean_report_title(title)

        # 1) RSS 링크 페이지에서 acptNo 확보
        link_res = fetch(session, link)
        acptno = extract_acptno_from_link(link, link_res.text)

        if not acptno:
            print("   [FAIL] acptNo 추출 실패")
            new_retry.append(item)
            continue

        # 2) viewer shell(본문선택 있는 페이지) 열기 — docno 비움
        viewer_shell_url = build_viewer_shell_url(acptno)
        vr = fetch(session, viewer_shell_url, referer=link)

        if DEBUG_HTML:
            print(f"   [VIEWER] status={vr.status_code} bytes={len(vr.content)} url={viewer_shell_url}")
            print(f"   [VIEWER preview] {vr.text[:200].replace(chr(10),' ')}")

        # 3) viewer에서 본문선택 docNo 추출 (보고서명 힌트로 매칭)
        docno = parse_body_docno_from_viewer(vr.text, report_hint)
        if not docno:
            print("   [FAIL] 본문선택 docNo 추출 실패 (viewer에 option 없음)")
            new_retry.append(item)
            continue

        # 4) searchContents로 '진짜 본문 표' 가져오기
        contents_url = build_search_contents_url(acptno, docno)
        cr = fetch(session, contents_url, referer=viewer_shell_url)
        real_html = extract_embedded_html(cr.text)

        fields, tables_cnt = parse_html_to_fields(real_html)
        filled = sum(1 for v in fields.values() if str(v).strip())

        if DEBUG_HTML:
            print(f"   [CONTENTS] status={cr.status_code} bytes={len(cr.content)} tables={tables_cnt} filled={filled}")
            if tables_cnt == 0:
                print(f"   [CONTENTS preview] {real_html[:220].replace(chr(10),' ')}")

        version = f"{acptno}-{docno}"
        status = decide_status(fields)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        try:
            raw_id = get_next_id(raw_ws)

            # RAW
            raw_ws.append_row([raw_id, now, pub, title, link, guid, status], value_input_option="USER_ENTERED")

            # ISSUE (회사명/상장시장 + 16필드 + VERSION)
            issue_row = [
                raw_id, now, pub, title, link, guid,
                company, market_code,
            ] + [fields.get(k, "") for k in TARGET_KEYS.keys()] + [version]

            issue_ws.append_row(issue_row, value_input_option="USER_ENTERED")

            seen_list.append(guid)
            if status != "SUCCESS":
                new_retry.append(item)

            print(f"-> Saved ({status}) tables={tables_cnt} filled={filled} VERSION={version}")

        except Exception as e:
            print(f"-> [Google Sheets Error] {e}")
            new_retry.append(item)

        time.sleep(SLEEP_SECONDS)

    save_json(SEEN_FILE, seen_list)
    save_json(RETRY_FILE, new_retry)
    print("\n✅ 모든 작업 완료!")


if __name__ == "__main__":
    main()
