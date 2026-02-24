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
BOT_VERSION = os.getenv("BOT_VERSION", "kind-bot-v2")

RSS_URL = os.getenv(
    "RSS_URL",
    "http://kind.krx.co.kr:80/disclosure/rsstodaydistribute.do"
    "?method=searchRssTodayDistribute&repIsuSrtCd=&mktTpCd=0&searchCorpName=&currentPageSize=100"
)
BASE = "https://kind.krx.co.kr"

KEYWORDS = [k.strip() for k in os.getenv("KEYWORDS", "전환사채,유상증자,교환사채").split(",") if k.strip()]

SHEET_NAME = os.getenv("SHEET_NAME", "KIND_대경")
RAW_TAB = os.getenv("RAW_TAB", "RAW")
ISSUE_TAB = os.getenv("ISSUE_TAB", "ISSUE")

SEEN_FILE = os.getenv("SEEN_FILE", "seen.json")
RETRY_FILE = os.getenv("RETRY_FILE", "retry_queue.json")

DEBUG_HTML = os.getenv("DEBUG_HTML", "0") == "1"
DUMP_FAIL_HTML = os.getenv("DUMP_FAIL_HTML", "0") == "1"

SUCCESS_FILLED_MIN = int(os.getenv("SUCCESS_FILLED_MIN", "10"))
SLEEP_SECONDS = float(os.getenv("SLEEP_SECONDS", "0.8"))

PW_NAV_TIMEOUT_MS = int(os.getenv("PW_NAV_TIMEOUT_MS", "25000"))
PW_WAIT_MS = int(os.getenv("PW_WAIT_MS", "3000"))

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
}

# (PPT 기준) 최종 추출 필드
ISSUE_FIELDS = [
    "회사명","상장시장","최초 이사회결의일","증자방식","발행상품","신규발행주식수","확정발행가(원)","기준주가","확정발행금액(억원)","할인(할증률)",
    "증자전 주식수","증자비율","청약일","납입일","주관사","자금용도","투자자","증자금액",
]

ALIASES = {
    "최초 이사회결의일": ["최초 이사회결의일","이사회결의일","결의일","결정일","이사회 결의일","이사회결의"],
    "증자방식": ["증자방식","발행방식","배정방법","배정방식","사채발행방법","발행방법"],
    "발행상품": ["발행상품","신주의 종류","주식의 종류","증권종류","사채의 종류","사채 종류"],
    "신규발행주식수": ["신규발행주식수","발행주식수","발행할 주식의 수","신주수","증자할 주식수","전환에 따라 발행할 주식","교환에 따라 발행할 주식"],
    "확정발행가(원)": ["확정발행가","신주발행가액","발행가","발행가액","1주당 발행가액","전환가액","교환가액","발행가(원)"],
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
# Utils
# =========================
def norm(s: str) -> str:
    return re.sub(r"\s+", "", str(s or "")).lower()

def load_json(filepath, default_val):
    if os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return default_val
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

def ensure_headers(raw_ws, issue_ws):
    raw_header = ["ID","수집시간","공시일시","공시제목","공시링크","GUID","처리상태","ACPTNO","DOCNO","FILLED","TABLES","VERSION"]
    if (raw_ws.row_values(1) or [])[:len(raw_header)] != raw_header:
        raw_ws.resize(rows=max(raw_ws.row_count, 1), cols=max(raw_ws.col_count, len(raw_header)))
        raw_ws.update("A1", [raw_header])

    issue_header = ["ID","수집시간","공시일시","회사명","상장시장","공시제목","공시링크","GUID"] + ISSUE_FIELDS + ["VERSION","처리상태","FILLED","TABLES","ACPTNO","DOCNO"]
    if (issue_ws.row_values(1) or [])[:len(issue_header)] != issue_header:
        issue_ws.resize(rows=max(issue_ws.row_count, 1), cols=max(issue_ws.col_count, len(issue_header)))
        issue_ws.update("A1", [issue_header])

def get_next_id(ws):
    col = ws.col_values(1)
    mx = 0
    for v in col[1:]:
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

def extract_acptno(link: str, html_text: str):
    qs = parse_qs(urlparse(link).query)
    acpt = (qs.get("acptno") or qs.get("acptNo") or [None])[0]
    if acpt:
        return acpt
    m = re.search(r'acptNo"\s*value="(\d+)"', html_text or "")
    if m:
        return m.group(1)
    m = re.search(r"(acptno|acptNo)=(\d{8,14})", html_text or "")
    if m:
        return m.group(2)
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
        if m:
            options.append((m.group(1), txt))
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
        if hint and tn and hint in tn:
            score += 100
        for kw in KEYWORDS:
            if kw and (kw in report_hint) and (kw in txt):
                score += 20
            if kw and (kw in txt):
                score += 5
        if "정정" in txt:
            score += 8
        if "결정" in txt:
            score += 5
        if txt and len(txt) < 4:
            score -= 3
        ranked.append((score, docno, txt))
    ranked.sort(reverse=True, key=lambda x: x[0])
    return ranked

# =========================
# Parsing
# =========================
def is_date_like(text: str) -> bool:
    return bool(re.search(r"\d{4}\s*[\-\.\/년]\s*\d{1,2}\s*[\-\.\/월]\s*\d{1,2}", text or ""))

def is_number_like(text: str) -> bool:
    return bool(re.search(r"\d", text or ""))

def pick_best_value(values: list[str], key_name: str) -> str:
    needs_date = ("일" in key_name) or ("기간" in key_name)
    needs_num = any(x in key_name for x in ["수", "가", "금액", "비율", "할인", "할증"])
    for v in values:
        v = (v or "").strip()
        if not v or v in ("-", "—", ".", "0", "0원"):
            continue
        if len(v) > 200:
            continue
        if needs_date and not is_date_like(v):
            continue
        if needs_num and not is_number_like(v):
            continue
        return v
    return ""

def normalize_amount_to_eok(raw: str) -> str:
    if not raw:
        return ""
    s = raw.replace(",", "").strip()
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    if not m:
        return raw.strip()
    num = float(m.group(1))
    if "백만원" in s:
        num = num / 100.0
        return str(int(num) if abs(num - int(num)) < 1e-9 else round(num, 2))
    if "억원" in s or ("억" in s and "원" not in s):
        return str(int(num) if abs(num - int(num)) < 1e-9 else round(num, 2))
    if "원" in s or num >= 1e7:
        num = num / 1e8
        return str(int(num) if abs(num - int(num)) < 1e-9 else round(num, 2))
    return str(int(num) if abs(num - int(num)) < 1e-9 else round(num, 2))

def table_to_matrix(table_tag) -> list[list[str]]:
    matrix = []
    for tr in table_tag.find_all("tr"):
        row = []
        for td in tr.find_all(["th", "td"]):
            row.append(td.get_text(" ", strip=True))
        if row:
            matrix.append(row)
    return matrix

def extract_from_matrix(matrix: list[list[str]], key_name: str) -> str:
    aliases = [norm(a) for a in ALIASES.get(key_name, []) if a]
    if not aliases:
        return ""
    for r_idx, row in enumerate(matrix):
        for c_idx, cell in enumerate(row):
            cell_txt = (cell or "").strip()
            if not cell_txt:
                continue
            cn = norm(re.sub(r"^\d+\.\s*", "", cell_txt))
            if not any(a in cn for a in aliases):
                continue

            right = [row[j] for j in range(len(row)-1, c_idx, -1)]
            v = pick_best_value(right, key_name)
            if v:
                return v

            if r_idx + 1 < len(matrix):
                next_row = matrix[r_idx + 1]
                candidates = []
                if c_idx < len(next_row):
                    candidates.append(next_row[c_idx])
                candidates += next_row[max(0, c_idx-1):min(len(next_row), c_idx+3)]
                v2 = pick_best_value(candidates, key_name)
                if v2:
                    return v2
    return ""

def parse_contents_html(contents_html: str):
    fields = {k: "" for k in ISSUE_FIELDS}
    if not contents_html:
        return fields, 0, 0

    def _parse_one(html_text: str):
        soup = BeautifulSoup(html_text, "lxml")
        tables = soup.find_all("table")
        hit = 0
        for table in tables:
            matrix = table_to_matrix(table)
            for key in ISSUE_FIELDS:
                val = extract_from_matrix(matrix, key)
                if val:
                    fields[key] = val  # 아래쪽 테이블이 최신일 때 덮어쓰기
            joined = norm(" ".join([" ".join(r) for r in matrix]))
            for key in ISSUE_FIELDS:
                for a in ALIASES.get(key, []):
                    if a and norm(a) in joined:
                        hit += 1
                        break
        return len(tables), hit

    t_cnt, hit1 = _parse_one(contents_html)

    if "&lt;table" in contents_html.lower():
        unesc = ihtml.unescape(contents_html)
        t2, hit2 = _parse_one(unesc)
        t_cnt = max(t_cnt, t2)
        hit1 = max(hit1, hit2)

    amt_key = "확정발행금액(억원)"
    if fields.get(amt_key):
        fields[amt_key] = normalize_amount_to_eok(fields[amt_key])

    filled = sum(1 for v in fields.values() if str(v).strip())
    return fields, t_cnt, filled

def decide_status(filled: int) -> str:
    return "SUCCESS" if filled >= SUCCESS_FILLED_MIN else "INCOMPLETE"

# =========================
# Playwright: best frame 선택
# =========================
def score_frame(html_content: str) -> tuple[int,int,int]:
    lower = (html_content or "").lower()
    table_count = lower.count("<table") + lower.count("&lt;table")
    text_norm = norm(BeautifulSoup(html_content or "", "lxml").get_text(" ", strip=True))
    key_hits = 0
    for key in ISSUE_FIELDS:
        for a in ALIASES.get(key, []):
            if a and norm(a) in text_norm:
                key_hits += 1
                break
    total = table_count * 2 + key_hits * 8
    return total, key_hits, table_count

def get_kind_contents_html_by_playwright(viewer_url: str) -> tuple[str, str, int, int]:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(locale="ko-KR")
        page = context.new_page()
        page.set_default_navigation_timeout(PW_NAV_TIMEOUT_MS)

        try:
            page.goto(viewer_url, wait_until="networkidle")
        except PWTimeout:
            pass

        page.wait_for_timeout(PW_WAIT_MS)

        best_html = ""
        best_label = "NONE"
        best_score = -1
        best_keyhits = 0
        best_tables = 0

        for idx, fr in enumerate(page.frames):
            try:
                html_content = fr.content()
                total, key_hits, table_count = score_frame(html_content)
                if total > best_score:
                    best_score = total
                    best_html = html_content
                    best_keyhits = key_hits
                    best_tables = table_count
                    best_label = f"frame#{idx} key_hits={key_hits} tables={table_count}"
            except Exception:
                continue

        browser.close()
        return best_html, best_label, best_keyhits, best_tables

# =========================
# Main
# =========================
def main():
    raw_ws, issue_ws = connect_gs()
    ensure_headers(raw_ws, issue_ws)

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

        print(f"\n[ITEM] {title}")

        link_res = fetch(session, link, referer=f"{BASE}/")
        acptno = extract_acptno(link, link_res.text)
        if not acptno:
            print("   [FAIL] acptNo not found")
            new_retry.append(item)
            continue

        viewer_shell = build_viewer_url(acptno, None)
        vr_shell = fetch(session, viewer_shell, referer=link)
        options = parse_docno_options(vr_shell.text)

        if not options:
            print("   [FAIL] docNo options not found")
            if DUMP_FAIL_HTML:
                print(vr_shell.text[:2000])
            new_retry.append(item)
            continue

        ranked = heuristic_docno_rank(options, report_hint)

        # ✅ 핵심: 후보 docno 여러개를 직접 파싱해보고 "가장 많이 채워지는 docno" 채택
        best = None  # (filled, tables, docno, label, fields)
        candidates = ranked[: min(6, len(ranked))]

        if DEBUG_HTML:
            print("   [DOCNO CANDIDATES]")
            for sc, dn, tx in candidates:
                print(f"     - score={sc} docno={dn} txt={tx}")

        for _, docno, txt in candidates:
            viewer_doc = build_viewer_url(acptno, docno)
            contents_html, frame_label, key_hits, table_cnt_raw = get_kind_contents_html_by_playwright(viewer_doc)

            if "<title>창 닫기</title>" in (contents_html or ""):
                if DEBUG_HTML:
                    print(f"   [CAND] docno={docno} CLOSE-WINDOW label={frame_label}")
                continue

            fields, tables_cnt, filled = parse_contents_html(contents_html)

            if DEBUG_HTML:
                print(f"   [CAND] docno={docno} filled={filled} tables={tables_cnt} keyHits={key_hits} label={frame_label} txt={txt}")

            cand = (filled, tables_cnt, docno, frame_label, fields)
            if best is None or cand[:2] > best[:2]:
                best = cand

            if filled >= SUCCESS_FILLED_MIN:
                break

        if best is None:
            print("   [FAIL] no parsable docno candidates")
            new_retry.append(item)
            continue

        filled, tables_cnt, docno, frame_label, fields = best
        status = decide_status(filled)
        version = f"{acptno}-{docno}"
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        try:
            rid = get_next_id(raw_ws)

            raw_ws.append_row(
                [rid, now, pub, title, link, guid, status, acptno, docno, filled, tables_cnt, version],
                value_input_option="USER_ENTERED"
            )

            issue_row = (
                [rid, now, pub, company, market_code, title, link, guid]
                + [fields.get(k, "") for k in ISSUE_FIELDS]
                + [version, status, filled, tables_cnt, acptno, docno]
            )
            issue_ws.append_row(issue_row, value_input_option="USER_ENTERED")

            if status == "SUCCESS":
                if guid not in seen_list:
                    seen_list.append(guid)
                print(f"   -> [SUCCESS] filled={filled} version={version} frame={frame_label}")
            else:
                if item not in new_retry:
                    new_retry.append(item)
                print(f"   -> [INCOMPLETE] filled={filled} version={version} frame={frame_label} (retry)")

        except Exception as e:
            print(f"   -> [Google Sheets Error] {e}")
            new_retry.append(item)

        time.sleep(SLEEP_SECONDS)

    save_json(SEEN_FILE, seen_list)
    save_json(RETRY_FILE, new_retry)
    print("\n✅ 모든 작업 완료!")

if __name__ == "__main__":
    main()
