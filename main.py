import os, json, time, re, html
from datetime import datetime
from urllib.parse import urlparse, parse_qs, urlencode, urljoin

import feedparser
import requests
import gspread
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials

# =========================
# Config
# =========================
BOT_VERSION = os.getenv("BOT_VERSION", "kind-bot-v1")
DEBUG_HTML = os.getenv("DEBUG_HTML", "0") == "1"
DUMP_FAIL_HTML = os.getenv("DUMP_FAIL_HTML", "0") == "1"

# ✅ 성공 기준(기본 10개 이상 채워지면 SUCCESS) - 필요시 env로 조정
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

# KIND viewer context defaults (없으면 fallback)
VIEWERHOST_DEFAULT = os.getenv("VIEWERHOST_DEFAULT", "kind.krx.co.kr")
VIEWERPORT_DEFAULT = os.getenv("VIEWERPORT_DEFAULT", "443")
SCRNMODE_DEFAULT = os.getenv("SCRNMODE_DEFAULT", "1")

# 네가 원하는 16개 + VERSION
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

def warmup_kind(session: requests.Session):
    """
    ✅ KIND는 종종 쿠키(예: WMONID/JSESSIONID)가 선행 세팅되어야 iframe/contents가 열림
    """
    try:
        r = fetch(session, f"{BASE}/main.do?method=loadInitPage&scrnmode=1", referer=None)
        if DEBUG_HTML:
            print(f"[WARMUP] {r.status_code} cookies={session.cookies.get_dict()}")
    except Exception as e:
        print(f"[WARMUP] failed: {e}")

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
        m = re.match(r"^(\d{6,14})\|", v)
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
        # option text가 빈 경우라도 docno는 선택 가능하게
        if not txt:
            score += 1
        if score > best_score:
            best_score = score
            best_doc = docno
    return best_doc

# =========================
# Viewer -> Contents URL finders
# =========================
def parse_hidden_context(viewer_html: str) -> dict:
    t = html.unescape(viewer_html or "")
    soup = BeautifulSoup(t, "lxml")
    ctx = {}
    for inp in soup.find_all("input"):
        name = (inp.get("name") or inp.get("id") or "").strip().lower()
        if not name:
            continue
        val = (inp.get("value") or "").strip()
        if val:
            ctx[name] = val
    return ctx

def find_external_htm_url(viewer_html: str) -> str | None:
    """
    ✅ KIND는 본문이 /external/.../*.htm 으로 분리되어 iframe으로 붙는 경우가 많음.
    이걸 찾으면 searchContents 우회 가능.
    """
    t = html.unescape(viewer_html or "")
    soup = BeautifulSoup(t, "lxml")

    for tag in soup.find_all(["iframe", "frame"]):
        src = (tag.get("src") or "").strip()
        if not src:
            continue
        if "/external/" in src and (".htm" in src or ".html" in src):
            return urljoin(BASE, src)

    # regex fallback
    m = re.search(r'(["\'])(/external/[^"\']+\.(?:htm|html)[^"\']*)\1', t)
    if m:
        return urljoin(BASE, m.group(2))
    m = re.search(r'(/external/[^\s<>"\']+\.(?:htm|html)[^\s<>"\']*)', t)
    if m:
        return urljoin(BASE, m.group(1))

    return None

def find_contents_url_strict(viewer_html: str) -> str | None:
    """
    ✅ iframe/src 뿐 아니라 JS 문자열 안의 searchContents URL도 최대한 찾아냄
    """
    t = html.unescape(viewer_html or "")

    # 1) iframe/frame src
    soup = BeautifulSoup(t, "lxml")
    for tag in soup.find_all(["iframe", "frame"]):
        src = (tag.get("src") or "").strip()
        if "disclsviewer.do" in src and "method=searchContents" in src:
            return urljoin(BASE, src)

    # 2) JS/HTML 안 따옴표 포함
    m = re.search(r'(["\'])(/common/disclsviewer\.do\?method=searchContents[^"\']+)\1', t)
    if m:
        return urljoin(BASE, m.group(2))

    # 3) 따옴표 없이
    m = re.search(r'(/common/disclsviewer\.do\?method=searchContents[^\s<>"\']+)', t)
    if m:
        return urljoin(BASE, m.group(1))

    return None

def build_contents_url_with_context(acptno: str, docno: str, viewer_html: str) -> str:
    ctx = parse_hidden_context(viewer_html)
    viewerhost = ctx.get("viewerhost") or VIEWERHOST_DEFAULT
    viewerport = ctx.get("viewerport") or VIEWERPORT_DEFAULT
    scrnmode = ctx.get("scrnmode") or SCRNMODE_DEFAULT

    params = {
        "method": "searchContents",
        "acptno": acptno,
        "docno": docno,
        "viewerhost": viewerhost,
        "viewerport": viewerport,
        "scrnmode": scrnmode,
    }
    return f"{BASE}/common/disclsviewer.do?" + urlencode(params, doseq=True)

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
    # table이 escape 되어있으면 복원
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
# Main
# =========================
def main():
    raw_ws, issue_ws = connect_gs()
    seen_list = load_json(SEEN_FILE, [])
    retry_queue = load_json(RETRY_FILE, [])

    session = requests.Session()
    warmup_kind(session)

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

    # uniq by guid
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

        # 1) RSS 링크 → acptno
        link_res = fetch(session, link, referer=f"{BASE}/")
        acptno = extract_acptno_from_link(link, link_res.text)
        if not acptno:
            print("   [FAIL] acptNo not found")
            new_retry.append(item)
            continue

        # 2) viewer shell(docno empty)
        viewer_shell = build_viewer_url(acptno, None)
        vr_shell = fetch(session, viewer_shell, referer=link)
        options = parse_docno_options(vr_shell.text)

        if DEBUG_HTML:
            print(f"   [VIEWER SHELL] bytes={len(vr_shell.content)} options={len(options)}")

        if not options:
            print("   [FAIL] docNo options not found in viewer shell")
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

        # 3) viewer를 docno 포함해서 다시 열기
        viewer_doc = build_viewer_url(acptno, docno)
        vr_doc = fetch(session, viewer_doc, referer=viewer_shell)

        if DEBUG_HTML:
            print(f"   [VIEWER DOC] docno={docno} bytes={len(vr_doc.content)}")
            print(f"   [VIEWER DOC preview] {vr_doc.text[:200].replace(chr(10),' ')}")

        # =========================
        # ✅ 핵심: 본문 획득 우선순위
        #  A) /external/...htm iframe을 찾으면 그걸 먼저 fetch (가장 안정적)
        #  B) searchContents iframe/src 찾기
        #  C) 컨텍스트 포함해서 searchContents URL 조립
        # =========================
        fields = {k: "" for k in TARGET_KEYS}
        tables_cnt = 0
        content_path = "NONE"
        contents_html = ""

        # A) external htm 우선
        external_url = find_external_htm_url(vr_doc.text)
        if external_url:
            exr = fetch(session, external_url, referer=viewer_doc)
            contents_html = exr.text
            # external도 "창 닫기"를 줄 수 있어서 체크
            if "<title>창 닫기</title>" not in contents_html:
                fields, tables_cnt = parse_contents_html(contents_html)
                content_path = "EXTERNAL"
            else:
                if DEBUG_HTML:
                    print(f"   [EXTERNAL] CLOSE-WINDOW url={external_url} bytes={len(exr.content)}")

        # B/C) external 실패 시 searchContents
        if content_path == "NONE":
            contents_url = find_contents_url_strict(vr_doc.text)
            if not contents_url:
                contents_url = build_contents_url_with_context(acptno, docno, vr_doc.text)

            cr = fetch(session, contents_url, referer=viewer_doc)
            contents_html = cr.text

            if "<title>창 닫기</title>" in contents_html:
                content_path = "CLOSE_WINDOW"
                if DEBUG_HTML:
                    print(f"   [CONTENTS] CLOSE-WINDOW returned. url={contents_url}")
                    print(f"   [COOKIES] {session.cookies.get_dict()}")
                # fallback: viewer_doc 자체 파싱 시도(가끔 표가 들어있음)
                fields, tables_cnt = parse_contents_html(vr_doc.text)
                if sum(1 for v in fields.values() if str(v).strip()) > 0:
                    content_path = "VIEWER_FALLBACK"
            else:
                fields, tables_cnt = parse_contents_html(contents_html)
                content_path = "SEARCHCONTENTS"

            if DEBUG_HTML:
                print(f"   [CONTENTS] status={cr.status_code} bytes={len(cr.content)} path={content_path}")
                print(f"   [CONTENTS URL] {contents_url}")

        # 덤프(분석용)
        if content_path in ("CLOSE_WINDOW", "NONE") and DUMP_FAIL_HTML:
            print("   [DUMP] viewer_doc (first 2000 chars)")
            print(vr_doc.text[:2000])
            print("   [DUMP] contents_html (first 2000 chars)")
            print((contents_html or "")[:2000])

        filled = sum(1 for v in fields.values() if str(v).strip())
        status = decide_status(fields)
        version = f"{acptno}-{docno}"

        if DEBUG_HTML:
            print(f"   [PARSE] tables={tables_cnt} filled={filled}/{len(TARGET_KEYS)} status={status} VERSION={version} via={content_path}")

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        try:
            raw_id = get_next_id(raw_ws)

            # RAW: ID | 수집시간 | 공시일(published) | 제목 | 링크 | GUID | 처리상태
            raw_ws.append_row(
                [raw_id, now, pub, title, link, guid, status],
                value_input_option="USER_ENTERED"
            )

            # ISSUE: ID | 수집시간 | 공시일 | 제목 | 링크 | GUID | 회사명 | 상장시장 | 16필드 | VERSION
            issue_row = (
                [raw_id, now, pub, title, link, guid, company, market_code]
                + [fields.get(k, "") for k in TARGET_KEYS]
                + [version]
            )

            issue_ws.append_row(issue_row, value_input_option="USER_ENTERED")

            # 성공/실패 기록
            if guid not in seen_list:
                seen_list.append(guid)

            if status != "SUCCESS":
                new_retry.append(item)

            print(f"-> Saved ({status}) via={content_path} tables={tables_cnt} filled={filled} VERSION={version}")

        except Exception as e:
            print(f"-> [Google Sheets Error] {e}")
            new_retry.append(item)

        time.sleep(SLEEP_SECONDS)

    save_json(SEEN_FILE, seen_list)
    save_json(RETRY_FILE, new_retry)
    print("\n✅ 모든 작업 완료!")


if __name__ == "__main__":
    main()
