# ====== KIND Disclosure Bot (Root-cause fix: resolve real HTML with tables + VERSION) ======
import os, json, time, re
from datetime import datetime
from urllib.parse import urlparse, parse_qs, urljoin, urlencode, unquote
import html as htmllib

import feedparser
import requests
import gspread
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials

BOT_VERSION = "2026-02-23-resolver-v1"
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

# 네가 원하는 필드 + VERSION
TARGET_KEYS = {
    "최초 이사회결의일": ["이사회결의일", "결의일", "결정일", "이사회 결의일"],
    "증자방식": ["증자방식", "발행방식", "배정방법", "배정방식"],
    "발행상품": ["신주의 종류", "주식의 종류", "증권종류"],
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


# -----------------
# helpers
# -----------------
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
    # "[코]유티아이 ..." -> 유티아이
    m = re.match(r"^\[([^\]]+)\]\s*([^\s]+)", (title or "").strip())
    return m.group(2) if m else ""

def extract_market_code_from_title(title: str):
    # 시장은 매핑하지 않고 코드 그대로 저장 (요구 반영)
    m = re.match(r"^\[([^\]]+)\]", (title or "").strip())
    return m.group(1).strip() if m else ""

def extract_acptno(link: str, html: str):
    qs = parse_qs(urlparse(link).query)
    acpt = (qs.get("acptno") or qs.get("acptNo") or [None])[0]
    if acpt:
        return acpt
    m = re.search(r'acptNo"\s*value="(\d+)"', html)
    if m:
        return m.group(1)
    m = re.search(r'_TRK_PN\s*=\s*"(\d+)"', html)
    if m:
        return m.group(1)
    m = re.search(r"(acptno|acptNo)=(\d{8,14})", html)
    if m:
        return m.group(2)
    return None

def extract_docno_candidates(html: str):
    soup = BeautifulSoup(html, "lxml")
    vals = []
    for opt in soup.find_all("option"):
        v = (opt.get("value") or "").strip()
        m = re.match(r"^(\d{6,14})\|", v)
        if m:
            vals.append(m.group(1))
    # fallback regex
    vals += re.findall(r"option\s+value=['\"](\d{6,14})\|", html)
    uniq, seen = [], set()
    for x in vals:
        if x.isdigit() and x not in seen:
            uniq.append(x)
            seen.add(x)
    return uniq[:30]

def build_urls(acptno: str, docno: str):
    # ✅ 케이스를 브라우저와 동일하게(acptNo/docNo/orgId/rcpNo) 맞춤
    base = f"{BASE}/common/disclsviewer.do"
    params = {
        "acptNo": acptno,
        "docNo": docno,
        "langTpCd": "0",
        "orgId": "K",
        "tran": "Y",
        "rcpNo": acptno,
    }
    viewer = base + "?" + urlencode({"method": "search", **params})
    contents = base + "?" + urlencode({"method": "searchContents", **params})
    excel = base + "?" + urlencode({"method": "downloadExcel", **params})
    pdf = base + "?" + urlencode({"method": "downloadPdf", **params})  # 있으면 쓰고, 없으면 HTML로 올 수 있음
    return viewer, contents, excel, pdf

def extract_embedded_html(raw: str) -> str:
    """
    table이 HTML 문자열로 인코딩되어 있을 때(escape/unescape/textarea 등) 복원
    """
    if "<table" in raw.lower():
        return raw

    # &lt;table 형태
    if "&lt;table" in raw.lower():
        decoded = htmllib.unescape(raw)
        if "<table" in decoded.lower():
            return decoded

    # %3Ctable 형태
    if "%3ctable" in raw.lower():
        decoded = unquote(raw)
        if "<table" in decoded.lower():
            return decoded

    # unescape('...') 패턴
    m = re.search(r"unescape\(['\"]([^'\"]+)['\"]\)", raw, re.I)
    if m:
        decoded = unquote(m.group(1))
        decoded = htmllib.unescape(decoded)
        if "<table" in decoded.lower():
            return decoded

    # textarea에 HTML이 들어있는 케이스
    soup = BeautifulSoup(raw, "lxml")
    ta = soup.find("textarea")
    if ta:
        decoded = ta.get_text()
        decoded = htmllib.unescape(decoded)
        decoded = unquote(decoded)
        if "<table" in decoded.lower():
            return decoded

    return raw

def find_next_urls(html: str):
    soup = BeautifulSoup(html, "lxml")
    urls = []

    for tag in soup.find_all(["iframe", "frame"]):
        src = tag.get("src")
        if src:
            urls.append(urljoin(BASE, src))

    # searchContents 링크가 스크립트/문자열로 박혀 있는 경우
    for m in re.findall(r"(\/common\/disclsviewer\.do\?method=searchContents[^\"'\s]+)", html):
        urls.append(urljoin(BASE, m))

    # downloadExcel/Pdf 링크도 혹시 박혀 있으면 저장
    for m in re.findall(r"(\/common\/disclsviewer\.do\?method=download(?:Excel|Pdf)[^\"'\s]+)", html, flags=re.I):
        urls.append(urljoin(BASE, m))

    # uniq
    out, seen = [], set()
    for u in urls:
        if u not in seen:
            out.append(u)
            seen.add(u)
    return out

def resolve_real_table_html(session, start_url: str, referer: str | None):
    """
    ✅ 핵심: viewer/search → iframe/frame → 실제 table이 있는 HTML까지 BFS로 따라감
    """
    queue = [(start_url, referer, 0)]
    visited = set()

    while queue:
        url, ref, depth = queue.pop(0)
        if depth > 6:
            continue
        if url in visited:
            continue
        visited.add(url)

        r = fetch(session, url, referer=ref)
        raw = r.text
        raw = extract_embedded_html(raw)

        soup = BeautifulSoup(raw, "lxml")
        tables = soup.find_all("table")

        if DEBUG_HTML:
            ct = (r.headers.get("Content-Type") or "").lower()
            print(f"   [RESOLVE] depth={depth} status={r.status_code} ct={ct} bytes={len(r.content)} tables={len(tables)} url={url}")

        # ✅ table을 찾으면 종료
        if tables:
            return raw, url

        # 다음 URL 후보 추가
        nxts = find_next_urls(r.text)  # 원문 기준으로 링크 탐색
        for n in nxts:
            queue.append((n, url, depth + 1))

    # 끝까지 못 찾으면 마지막으로 start 응답 반환
    return extract_embedded_html(fetch(session, start_url, referer=referer).text), start_url

# ----- table -> matrix (rowspan/colspan)
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
            # "5. 증자방식" 번호 제거
            cn = norm(re.sub(r"^\d+\.\s*", "", c))
            if any(a and a in cn for a in als):
                v = pick_value_from_row(r, i)
                if v:
                    return v
    return ""

def parse_html_to_fields(html: str):
    soup = BeautifulSoup(html, "lxml")
    matrices = [table_to_matrix(t) for t in soup.find_all("table")]
    out = {k: "" for k in TARGET_KEYS.keys()}

    for target, aliases in TARGET_KEYS.items():
        for mtx in matrices:
            v = extract_from_matrix(mtx, aliases)
            if v:
                out[target] = v
                break

    return out

def decide_status(fields: dict):
    # 값이 3개 이상 채워지면 성공
    filled = [k for k, v in fields.items() if str(v).strip()]
    return "SUCCESS" if len(filled) >= 3 else "INCOMPLETE"

# -----------------
# main
# -----------------
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

        print(f"\nProcessing: {title}")

        title_company = extract_company_from_title(title)
        title_market_code = extract_market_code_from_title(title)

        link_res = fetch(session, link)
        acptno = extract_acptno(link, link_res.text)
        docnos = extract_docno_candidates(link_res.text)

        print(f"   [FOUND] acptNo={acptno} docNo_candidates={len(docnos)}")

        best = None  # (filled_count, version, fields)

        if acptno and docnos:
            for idx, docno in enumerate(docnos, 1):
                viewer, contents, excel, pdf = build_urls(acptno, docno)

                # viewer 먼저 열어서 세션 체인 생성
                fetch(session, viewer, referer=link)

                # ✅ 본문(표) HTML resolve
                real_html, real_url = resolve_real_table_html(session, viewer, referer=link)
                soup = BeautifulSoup(real_html, "lxml")
                if not soup.find_all("table"):
                    # viewer에서 못 찾으면 contents도 시도
                    real_html, real_url = resolve_real_table_html(session, contents, referer=viewer)

                tables_cnt = len(BeautifulSoup(real_html, "lxml").find_all("table"))
                if DEBUG_HTML:
                    print(f"   [TRY {idx}] docNo={docno} tables={tables_cnt} real_url={real_url}")

                fields = parse_html_to_fields(real_html)

                # VERSION: 가장 안전하게 acptNo-docNo
                version = f"{acptno}-{docno}"

                filled = sum(1 for v in fields.values() if str(v).strip())
                print(f"   [TRY {idx}] docNo={docno} filled={filled} tables={tables_cnt}")

                if best is None or filled > best[0]:
                    best = (filled, version, fields)

                if filled >= 8:
                    break
        else:
            # fallback: 링크 자체 resolve
            real_html, _ = resolve_real_table_html(session, link, referer=None)
            fields = parse_html_to_fields(real_html)
            best = (sum(1 for v in fields.values() if str(v).strip()), "N/A", fields)

        filled, version, fields = best

        # 회사명/상장시장(코드)은 본문에서 못 나오면 제목 fallback
        # (너가 말한 “정해놓지 말라” 반영: 코드 그대로 저장)
        company = title_company
        market = title_market_code

        status = decide_status(fields)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        try:
            raw_id = get_next_id(raw_ws)

            # RAW
            raw_ws.append_row([raw_id, now, pub, title, link, guid, status], value_input_option="USER_ENTERED")

            # ISSUE: 네 컬럼 순서에 맞춰서 쓴다
            # (ID, 수집시간, 공시일시, 공시제목, 공시링크, GUID, 회사명, 상장시장, <타겟필드들>, VERSION)
            issue_row = [
                raw_id, now, pub, title, link, guid,
                company, market,
            ] + [fields.get(k, "") for k in TARGET_KEYS.keys()] + [version]

            issue_ws.append_row(issue_row, value_input_option="USER_ENTERED")

            seen_list.append(guid)
            if status != "SUCCESS":
                new_retry.append(item)

            print(f"-> Saved ({status}) filled={filled} VERSION={version}")
        except Exception as e:
            print(f"-> [Google Sheets Error] {e}")
            new_retry.append(item)

        time.sleep(SLEEP_SECONDS)

    save_json(SEEN_FILE, seen_list)
    save_json(RETRY_FILE, new_retry)
    print("\n✅ 모든 작업 완료!")


if __name__ == "__main__":
    main()
