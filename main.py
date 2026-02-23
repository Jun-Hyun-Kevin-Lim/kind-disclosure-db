# ====== KIND Disclosure Bot (Stable Contents URL + Excel Wrapper Handling) ======
import os, json, time, re, io
from datetime import datetime
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse, urljoin

import feedparser
import pandas as pd
import requests
import gspread
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials

# =====================
# Config
# =====================
RSS_URL = "https://kind.krx.co.kr/disclosure/rsstodaydistribute.do?method=searchRssTodayDistribute&repIsuSrtCd=&mktTpCd=0&searchCorpName=&currentPageSize=200"
KEYWORDS = ["유상증자"]  # 테스트할 땐 [] 로 두면 전체 저장됨

SHEET_NAME = "KIND_대경"
RAW_TAB = "RAW"
ISSUE_TAB = "ISSUE"

SEEN_FILE = "seen.json"
RETRY_FILE = "retry_queue.json"

REQUIRED_FIELDS = ["회사명", "확정발행가(원)", "증자금액"]

TARGET_KEYS = {
    "회사명": ["회사명", "발행회사", "상호", "명칭"],
    "상장시장": ["상장시장", "시장구분", "시장"],
    "최초 이사회결의일": ["이사회결의일", "결의일", "사채발행결정일"],
    "증자방식": ["증자방식", "발행방식"],
    "발행상품": ["발행상품", "증권종류", "사채의 종류"],
    "신규발행주식수": ["신규발행주식수", "발행주식수", "발행할 주식의 수"],
    "확정발행가(원)": ["확정발행가", "발행가", "발행가액", "전환가액", "교환가액"],
    "기준주가": ["기준주가", "기준주가액"],
    "확정발행금액(억원)": ["확정발행금액", "사채의 권면총액", "발행총액"],
    "할인(할증률)": ["할인율", "할증률", "할인율(%)"],
    "증자전 주식수": ["증자전 주식수", "발행주식총수"],
    "증자비율": ["증자비율", "증자비율(%)"],
    "청약일": ["청약일", "청약시작일"],
    "납입일": ["납입일", "대금납입일"],
    "주관사": ["주관사", "대표주관회사"],
    "자금용도": ["자금용도", "자금조달의 목적"],
    "투자자": ["투자자", "배정대상자", "발행대상자", "대상자"],
    "증자금액": ["증자금액", "발행규모"],
}

BASE = "https://kind.krx.co.kr"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
    "Accept": "application/xml,text/xml;q=0.9,text/html;q=0.8,*/*;q=0.7",
    "Connection": "keep-alive",
}

SLEEP_SECONDS = 1


# =====================
# State
# =====================
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


# =====================
# Google Sheets
# =====================
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
    print(f"[GS] Opened spreadsheet='{sh.title}' RAW='{raw_ws.title}' ISSUE='{issue_ws.title}'")
    return raw_ws, issue_ws


def get_next_id(ws):
    col = ws.col_values(1)
    max_id = -1
    for v in col:
        v = str(v).strip()
        if v.isdigit():
            max_id = max(max_id, int(v))
    return max_id + 1


# =====================
# HTTP / RSS
# =====================
def fetch(session: requests.Session, url: str, timeout=25, headers=None, method="GET", data=None):
    h = dict(DEFAULT_HEADERS)
    if headers:
        h.update(headers)

    if method == "POST":
        r = session.post(url, headers=h, timeout=timeout, allow_redirects=True, data=data)
    else:
        r = session.get(url, headers=h, timeout=timeout, allow_redirects=True)

    if not r.encoding or r.encoding.lower() == "iso-8859-1":
        r.encoding = r.apparent_encoding or "utf-8"
    return r


def fetch_rss_feed(session: requests.Session):
    r = fetch(session, RSS_URL, timeout=25)
    ct = (r.headers.get("Content-Type") or "").lower()
    print(f"[RSS] status={r.status_code} ct={ct} bytes={len(r.content)} final_url={r.url}")
    feed = feedparser.parse(r.content)
    print(f"[RSS] entries={len(feed.entries)} bozo={getattr(feed,'bozo',0)}")
    for i, e in enumerate(feed.entries[:5]):
        print(f"[RSS sample {i+1}] {e.get('title','')}")
    return feed


# =====================
# URL helpers
# =====================
def _qs_get(qs: dict, *keys):
    for k in keys:
        if k in qs and qs[k]:
            return qs[k][0]
    return None


def extract_params(url: str):
    qs = parse_qs(urlparse(url).query)
    acptno = _qs_get(qs, "acptno", "acptNo")
    docno = _qs_get(qs, "docno", "docNo")
    rcpno = _qs_get(qs, "rcpno", "rcpNo")
    orgid = _qs_get(qs, "orgid", "orgId")
    lang = _qs_get(qs, "langTpCd")
    tran = _qs_get(qs, "tran")
    viewerhost = _qs_get(qs, "viewerhost")
    return {
        "acptno": acptno,
        "docno": docno,
        "rcpno": rcpno,
        "orgid": orgid,
        "langTpCd": lang,
        "tran": tran,
        "viewerhost": viewerhost,
    }


def ensure_kind_defaults(url: str):
    """
    KIND 뷰어에서 종종 필요한 기본 파라미터를 강제로 보강:
    - langTpCd=0
    - orgid=K
    - rcpno=acptno
    - tran=Y
    """
    u = urlparse(url)
    qs = parse_qs(u.query)

    # 키를 lower로 통일하면서 값 유지
    def get_any(*keys):
        for k in keys:
            if k in qs and qs[k]:
                return qs[k][0]
        return None

    acptno = get_any("acptno", "acptNo")
    docno = get_any("docno", "docNo")

    # 기존 값 있으면 유지, 없으면 기본값
    if "langTpCd" not in qs:
        qs["langTpCd"] = ["0"]
    if "orgid" not in qs and "orgId" not in qs:
        qs["orgid"] = ["K"]
    if ("rcpno" not in qs and "rcpNo" not in qs) and acptno:
        qs["rcpno"] = [acptno]
    if "tran" not in qs:
        qs["tran"] = ["Y"]

    # acptno/docno는 있으면 그대로 두고, 없으면 건드리지 않음
    new_query = urlencode({k: v[0] for k, v in qs.items()}, doseq=False)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_query, u.fragment))


def replace_method(url: str, new_method: str):
    u = urlparse(url)
    qs = parse_qs(u.query)
    qs["method"] = [new_method]
    new_query = urlencode({k: v[0] for k, v in qs.items()}, doseq=False)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_query, u.fragment))


# =====================
# Find real contents URL from HTML
# =====================
def find_search_contents_url(html: str):
    # 1) 직접 링크 형태
    m = re.search(r"(\/common\/disclsviewer\.do\?method=searchContents[^\"'\s]+)", html)
    if m:
        return urljoin(BASE, m.group(1))

    m = re.search(r"(https?:\/\/kind\.krx\.co\.kr\/common\/disclsviewer\.do\?method=searchContents[^\"'\s]+)", html)
    if m:
        return m.group(1)

    # 2) iframe src 안에 있을 수도
    soup = BeautifulSoup(html, "lxml")
    iframe = soup.find("iframe")
    if iframe and iframe.get("src") and "searchContents" in iframe["src"]:
        return urljoin(BASE, iframe["src"])

    return None


def build_urls_from_original(link: str, session: requests.Session):
    """
    ✅ 가장 강력한 방식:
    - RSS link 페이지를 먼저 열고
    - 그 HTML 안에서 '진짜 searchContents URL'을 찾아서 사용
    """
    # 1) 원본 링크 먼저 열기
    r = fetch(session, link, timeout=25)
    html = r.text

    # 2) HTML에서 contents url 추출
    contents_url = find_search_contents_url(html)

    # 3) 못 찾으면: 파라미터 기반으로 구성
    if not contents_url:
        p = extract_params(link)
        if not p["acptno"] or not p["docno"]:
            # link HTML에서 숫자도 최대한 추출
            # (docno 패턴 다양해서 여러 개 시도)
            m = re.search(r"(acptno|acptNo)=(\d{8,14})", html)
            if m and not p["acptno"]:
                p["acptno"] = m.group(2)

            m = re.search(r"(docno|docNo)=(\d{1,14})", html)
            if m and not p["docno"]:
                p["docno"] = m.group(2)

            if not p["docno"]:
                m = re.search(r"option\s+value=['\"](\d+)\|", html)
                if m:
                    p["docno"] = m.group(1)

        if p["acptno"] and p["docno"]:
            base = f"{BASE}/common/disclsviewer.do"
            qs = {
                "method": "searchContents",
                "acptno": p["acptno"],
                "docno": p["docno"],
            }
            contents_url = base + "?" + urlencode(qs)
        else:
            # 최악: link 자체로 파싱
            contents_url = link

    # 4) 기본 파라미터 보강
    contents_url = ensure_kind_defaults(contents_url)
    viewer_url = ensure_kind_defaults(replace_method(contents_url, "search"))
    excel_url = ensure_kind_defaults(replace_method(contents_url, "downloadExcel"))

    return viewer_url, contents_url, excel_url


# =====================
# Table flatten helpers (HTML / Soup)
# =====================
def flatten_df(df: pd.DataFrame):
    bag = {}
    df = df.fillna("").astype(str)
    for r in range(len(df)):
        row = [str(x).strip() for x in df.iloc[r].tolist()]
        row = [x for x in row if x != ""]
        if len(row) < 2:
            continue

        # (1) 2칸씩 페어로 (0,1), (2,3)...
        for i in range(0, len(row) - 1, 2):
            k = row[i].strip()
            v = row[i + 1].strip() if i + 1 < len(row) else ""
            if k and v and len(k) < 60:
                bag[k] = v

        # (2) 첫 칸이 key, 나머지가 value인 형태도 커버
        k0 = row[0].strip()
        v0 = " ".join(row[1:]).strip()
        if k0 and v0 and len(k0) < 60:
            bag.setdefault(k0, v0)

    return bag


def flatten_tables_from_html(html: str):
    bag = {}
    # pandas read_html 우선
    try:
        tables = pd.read_html(io.StringIO(html))
        for df in tables:
            bag.update(flatten_df(df))
        if bag:
            return bag
    except:
        pass

    # soup table fallback
    soup = BeautifulSoup(html, "lxml")
    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            cells = []
            for cell in tr.find_all(["th", "td"]):
                txt = cell.get_text(" ", strip=True)
                if txt:
                    cells.append(txt)
            if len(cells) < 2:
                continue
            # 2칸 페어
            for i in range(0, len(cells) - 1, 2):
                k = cells[i].strip()
                v = cells[i + 1].strip()
                if k and v and len(k) < 60:
                    bag[k] = v
            # 첫칸 key + 나머지 value
            k0 = cells[0].strip()
            v0 = " ".join(cells[1:]).strip()
            if k0 and v0 and len(k0) < 60:
                bag.setdefault(k0, v0)

    return bag


# =====================
# Parse HTML (iframe/frame follow)
# =====================
def parse_html_tables(url: str, session: requests.Session, depth: int = 0):
    try:
        r = fetch(session, url, timeout=25)
        html = r.text

        bag = flatten_tables_from_html(html)
        if bag:
            return bag

        # iframe/frame 추적
        soup = BeautifulSoup(html, "lxml")
        iframe = soup.find("iframe")
        if iframe and iframe.get("src") and depth < 3:
            return parse_html_tables(urljoin(BASE, iframe["src"]), session, depth + 1)

        frame = soup.find("frame")
        if frame and frame.get("src") and depth < 3:
            return parse_html_tables(urljoin(BASE, frame["src"]), session, depth + 1)

        # searchContents 링크 재추적
        cu = find_search_contents_url(html)
        if cu and depth < 3:
            cu = ensure_kind_defaults(cu)
            return parse_html_tables(cu, session, depth + 1)

        return {}
    except:
        return {}


# =====================
# Excel fallback: handle HTML wrapper (form/redirect)
# =====================
def _extract_redirect_or_form(html: str):
    """
    downloadExcel이 HTML로 내려올 때,
    - meta refresh
    - location.href
    - form(action + hidden inputs)
    를 찾아서 반환
    """
    soup = BeautifulSoup(html, "lxml")

    # meta refresh
    meta = soup.find("meta", attrs={"http-equiv": re.compile("refresh", re.I)})
    if meta and meta.get("content"):
        m = re.search(r"url=(.+)$", meta["content"], re.I)
        if m:
            return ("GET", m.group(1).strip(), None)

    # location.href / document.location
    m = re.search(r"(location\.href|document\.location)\s*=\s*['\"]([^'\"]+)['\"]", html)
    if m:
        return ("GET", m.group(2).strip(), None)

    # form submit
    form = soup.find("form")
    if form and form.get("action"):
        action = form["action"]
        data = {}
        for inp in form.find_all("input"):
            name = inp.get("name")
            if not name:
                continue
            data[name] = inp.get("value", "")
        return ("POST", action, data)

    return (None, None, None)


def parse_excel_fallback(excel_url: str, session: requests.Session, referer: str):
    """
    1) downloadExcel 요청
    2) 만약 HTML이면 -> redirect/form 따라가서 1번 더 시도
    3) 진짜 엑셀이면 read_excel
    """
    headers = {"Referer": referer}
    r = fetch(session, excel_url, timeout=25, headers=headers)
    ct = (r.headers.get("Content-Type") or "").lower()
    cd = (r.headers.get("Content-Disposition") or "").lower()
    print(f"   [Excel HTTP] status={r.status_code} ct={ct} cd={cd} bytes={len(r.content)}")

    if r.status_code != 200 or len(r.content) < 200:
        return {}

    head = r.content[:400].lstrip().lower()

    # HTML이면 wrapper 가능성 → redirect/form 따라가기
    if ("text/html" in ct) or head.startswith(b"<!doctype html") or head.startswith(b"<html") or (b"<table" in head):
        preview = (r.text[:350]).replace("\n", " ")
        print(f"   [Excel HTML Preview] {preview}")

        method, nxt, data = _extract_redirect_or_form(r.text)
        if method and nxt:
            nxt_url = urljoin(BASE, nxt)
            nxt_url = ensure_kind_defaults(nxt_url)
            print(f"   [Excel Wrapper] follow {method} -> {nxt_url}")

            if method == "POST":
                rr = fetch(session, nxt_url, timeout=25, headers={"Referer": referer}, method="POST", data=data)
            else:
                rr = fetch(session, nxt_url, timeout=25, headers={"Referer": referer})

            ct2 = (rr.headers.get("Content-Type") or "").lower()
            print(f"   [Excel Follow] status={rr.status_code} ct={ct2} bytes={len(rr.content)}")
            if rr.status_code == 200 and len(rr.content) > 200:
                # follow 결과가 또 HTML이면 table만이라도 시도
                head2 = rr.content[:200].lstrip().lower()
                if ("text/html" in ct2) or head2.startswith(b"<html") or b"<table" in head2:
                    try:
                        return flatten_tables_from_html(rr.text)
                    except:
                        return {}
                # 엑셀이면 파싱
                try:
                    df = pd.read_excel(io.BytesIO(rr.content))
                    return flatten_df(df)
                except:
                    return {}

        # wrapper에서 table 있으면 그걸로라도
        try:
            return flatten_tables_from_html(r.text)
        except:
            return {}

    # 진짜 엑셀이면
    try:
        df = pd.read_excel(io.BytesIO(r.content))
        return flatten_df(df)
    except Exception as e:
        print(f"   [Excel Parse Error] {e}")
        return {}


# =====================
# Mapping & completeness
# =====================
def _norm(s: str):
    return re.sub(r"\s+", "", str(s or "")).lower()


def map_to_target(bag: dict):
    out = {}
    norm_map = {_norm(k): k for k in bag.keys()}
    for target, aliases in TARGET_KEYS.items():
        val = ""
        for a in aliases:
            na = _norm(a)
            matched = None
            for nk, orig_k in norm_map.items():
                if na and na in nk:
                    matched = orig_k
                    break
            if matched:
                val = bag.get(matched, "")
                break
        out[target] = val
    return out


def check_completeness(mapped_data: dict):
    return all(mapped_data.get(f) for f in REQUIRED_FIELDS)


# =====================
# Main
# =====================
def main():
    raw_ws, issue_ws = connect_gs()
    seen_list = load_json(SEEN_FILE, [])
    retry_queue = load_json(RETRY_FILE, [])

    print(f"[STATE] seen={len(seen_list)} retry_queue={len(retry_queue)} keywords={KEYWORDS}")

    session = requests.Session()
    feed = fetch_rss_feed(session)

    total_entries = len(feed.entries)
    kw_match = 0
    new_items = []

    for entry in feed.entries:
        title = entry.get("title", "") or ""
        link = entry.get("link", "") or ""
        guid = entry.get("id") or link
        pub = entry.get("published", "") or ""

        if not guid:
            continue

        if KEYWORDS:
            if not any(k in title for k in KEYWORDS):
                continue

        kw_match += 1
        if guid in seen_list:
            continue

        new_items.append({"title": title, "link": link, "guid": guid, "pub": pub})

    print(f"[FILTER] total_entries={total_entries} keyword_matched={kw_match} new_items={len(new_items)}")

    items_to_process = new_items + retry_queue
    uniq = {}
    for it in items_to_process:
        uniq[it["guid"]] = it
    items_to_process = list(uniq.values())
    print(f"[QUEUE] to_process={len(items_to_process)}")

    if not items_to_process:
        print("[INFO] 처리할 항목이 0개라서 시트에 기록되지 않았습니다.")
        print("✅ 모든 작업 완료!")
        return

    new_retry_queue = []

    for item in items_to_process:
        title = item["title"]
        link = item["link"]
        guid = item["guid"]
        pub = item.get("pub", "")

        print(f"\nProcessing: {title}")
        is_correction = 1 if "[정정]" in title else 0

        viewer_url, contents_url, excel_url = build_urls_from_original(link, session)
        print(f"   [URLs] viewer={viewer_url}")
        print(f"   [URLs] contents={contents_url}")
        print(f"   [URLs] excel={excel_url}")

        # viewer 먼저 열어 세션/쿠키 확보
        try:
            fetch(session, viewer_url, timeout=20)
        except:
            pass

        # 본문 HTML 파싱
        bag = parse_html_tables(contents_url, session)
        print(f"   [HTML bag] keys={len(bag)}")
        mapped = map_to_target(bag)
        is_complete = check_completeness(mapped)

        # Excel fallback
        if (not is_complete) and excel_url:
            print("-> [Fallback] 엑셀 데이터 다운로드 시도...")
            fb_bag = parse_excel_fallback(excel_url, session, referer=viewer_url)
            print(f"   [Excel bag] keys={len(fb_bag)}")
            fb_mapped = map_to_target(fb_bag)

            for k, v in fb_mapped.items():
                if (not mapped.get(k)) and v:
                    mapped[k] = v

            is_complete = check_completeness(mapped)

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        status = "SUCCESS" if is_complete else "INCOMPLETE"

        # 실패해도 한 줄은 남김
        try:
            raw_id = get_next_id(raw_ws)
            raw_ws.append_row([raw_id, now, pub, title, link, guid, status], value_input_option="USER_ENTERED")

            issue_row = [raw_id, now, pub, title, link, guid, is_correction] + [mapped[k] for k in TARGET_KEYS.keys()]
            issue_ws.append_row(issue_row, value_input_option="USER_ENTERED")

            if guid not in seen_list:
                seen_list.append(guid)

            if not is_complete:
                new_retry_queue.append(item)

            print(f"-> Saved to Sheets ({status})")

        except Exception as e:
            print(f"-> [Google Sheets Error] {e}")
            new_retry_queue.append(item)

        time.sleep(SLEEP_SECONDS)

    save_json(SEEN_FILE, seen_list)
    save_json(RETRY_FILE, new_retry_queue)
    print("\n✅ 모든 작업 완료!")


if __name__ == "__main__":
    main()
