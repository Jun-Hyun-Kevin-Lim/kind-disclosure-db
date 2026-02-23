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
# Table parse (rowspan/colspan -> matrix)
# =========================
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

def parse_contents_html(html_text: str):
    if "&lt;table" in (html_text or "").lower():
        html_text = html.unescape(html_text)

    soup = BeautifulSoup(html_text or "", "lxml")
    tables = soup.find_all("table")
    matrices = [table_to_matrix(t) for t in tables]

    out = {k: "" for k in TARGET_KEYS}
    for key in TARGET_KEYS:
        for mtx in matrices:
            v = extract_from_matrix(mtx, ALIASES[key])
            if v:
                out[key] = v
                break
    return out, len(tables)

def decide_status(fields: dict):
    filled = sum(1 for v in fields.values() if str(v).strip())
    return "SUCCESS" if filled >= SUCCESS_FILLED_MIN else "INCOMPLETE"

# =========================
# Playwright: fetch viewer contents HTML
# =========================
def get_kind_contents_html_by_playwright(viewer_url: str) -> tuple[str, str]:
    """
    viewer_url(=disclsviewer.do?method=search&acptno=...&docno=...)을 실제 브라우저로 열고,
    iframe(검색 본문 또는 external 본문)의 HTML을 가져온다.
    반환: (contents_html, path_label)
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(locale="ko-KR")
        page = context.new_page()
        page.set_default_navigation_timeout(PW_NAV_TIMEOUT_MS)

        try:
            page.goto(viewer_url, wait_until="domcontentloaded")
        except PWTimeout:
            # navigation timeout이어도 iframe 로딩은 될 수 있어 계속 진행
            pass

        # iframe/frame 중 본문일 가능성이 높은 frame 찾기
        # 우선순위: /external/*.htm → method=searchContents → 그 외
        best_frame = None
        best_label = "NONE"

        t0 = time.time()
        while (time.time() - t0) * 1000 < PW_FRAME_TIMEOUT_MS:
            frames = page.frames
            # main frame 제외, child frame 위주로 탐색
            cand = []
            for fr in frames:
                if fr == page.main_frame:
                    continue
                u = fr.url or ""
                if "/external/" in u and (u.endswith(".htm") or ".htm?" in u or u.endswith(".html") or ".html?" in u):
                    cand.append((3, "EXTERNAL", fr))
                elif "disclsviewer.do" in u and "method=searchContents" in u:
                    cand.append((2, "SEARCHCONTENTS", fr))
                elif u.startswith("https://kind.krx.co.kr/"):
                    cand.append((1, "OTHER_KIND_FRAME", fr))

            if cand:
                cand.sort(key=lambda x: x[0], reverse=True)
                best_frame = cand[0][2]
                best_label = cand[0][1]
                break

            # 아직 frame이 안 잡히면 조금 대기
            page.wait_for_timeout(300)

        if not best_frame:
            # 마지막 fallback: 페이지 HTML 자체라도 반환
            html_main = page.content()
            browser.close()
            return html_main, "PAGE_FALLBACK"

        # frame content 가져오기
        try:
            contents_html = best_frame.content()
        except Exception:
            contents_html = ""
        browser.close()
        return contents_html, best_label

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

            if guid not in seen_list:
                seen_list.append(guid)

            if status != "SUCCESS":
                new_retry.append(item)

            print(f"-> Saved ({status}) path={path} tables={tables_cnt} filled={filled} VERSION={version}")

        except Exception as e:
            print(f"-> [Google Sheets Error] {e}")
            new_retry.append(item)

        time.sleep(SLEEP_SECONDS)

    save_json(SEEN_FILE, seen_list)
    save_json(RETRY_FILE, new_retry)
    print("\n✅ 모든 작업 완료!")


if __name__ == "__main__":
    main()
