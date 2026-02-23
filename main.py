# ====== KIND Disclosure Bot (DocNo 탐색 + HTML 표 숫자 추출 강화) ======
import os, json, time, re, io
from datetime import datetime
from urllib.parse import urlparse, parse_qs, urlencode, urljoin

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
KEYWORDS = ["유상증자"]  # 테스트: [] 로 두면 전체 가져옴

SHEET_NAME = "KIND_대경"
RAW_TAB = "RAW"
ISSUE_TAB = "ISSUE"

SEEN_FILE = "seen.json"
RETRY_FILE = "retry_queue.json"

# ✅ 너무 빡세게 잡지 말고, "회사명 + (증자금액 or 확정발행금액 or 자금용도)" 중 하나라도 있으면 COMPLETE로 처리
MIN_COMPLETE_FIELDS = ["회사명"]

TARGET_KEYS = {
    "회사명": ["회사명", "발행회사", "상호", "명칭", "종속회사인", "종속회사"],
    "상장시장": ["상장시장", "시장구분", "시장"],
    "최초 이사회결의일": ["이사회결의일", "결의일", "사채발행결정일"],
    "증자방식": ["증자방식", "발행방식"],
    "발행상품": ["발행상품", "증권종류", "사채의 종류"],
    "신규발행주식수": ["신규발행주식수", "발행주식수", "발행할 주식의 수", "증자전 발행주식총수"],
    "확정발행가(원)": ["확정발행가", "발행가", "발행가액", "전환가액", "교환가액", "1주당 발행가액"],
    "기준주가": ["기준주가", "기준주가액"],
    "확정발행금액(억원)": ["확정발행금액", "사채의 권면총액", "발행총액", "발행금액", "모집총액"],
    "할인(할증률)": ["할인율", "할증률", "할인율(%)", "할인율(%)"],
    "증자전 주식수": ["증자전 주식수", "발행주식총수", "기발행주식총수"],
    "증자비율": ["증자비율", "증자비율(%)"],
    "청약일": ["청약일", "청약시작일", "청약기간"],
    "납입일": ["납입일", "대금납입일"],
    "주관사": ["주관사", "대표주관회사", "인수회사"],
    "자금용도": ["자금용도", "자금조달의 목적", "자금사용 목적"],
    "투자자": ["투자자", "배정대상자", "발행대상자", "대상자", "인수인"],
    "증자금액": ["증자금액", "발행규모", "조달금액", "자금조달금액", "모집금액"],
}

BASE = "https://kind.krx.co.kr"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
    "Accept": "application/xml,text/xml;q=0.9,text/html;q=0.8,*/*;q=0.7",
    "Connection": "keep-alive",
}

SLEEP_SECONDS = 1

PURPOSE_KEYS = [
    "시설자금", "운영자금", "영업양수자금", "채무상환자금", "타법인 증권 취득자금", "기타자금"
]


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
# Helpers
# =====================
def norm(s: str):
    return re.sub(r"\s+", "", str(s or "")).lower()


def extract_company_from_title(title: str):
    # 예: "[코]유티아이 유상증자결정(...)" -> "유티아이"
    m = re.match(r"^\[(코|유)\]([^\s]+)\s", title.strip())
    if m:
        return m.group(2)
    return ""


def extract_market_from_title(title: str):
    if title.strip().startswith("[코]"):
        return "코스닥"
    if title.strip().startswith("[유]"):
        return "유가증권"
    return ""


def parse_int_like(s: str):
    if not s:
        return None
    digits = re.sub(r"[^\d]", "", str(s))
    if not digits:
        return None
    try:
        return int(digits)
    except:
        return None


def fmt_int(n: int):
    return f"{n:,}"


# =====================
# 1) RSS link 페이지에서 acptNo / docNo 후보 전부 뽑기
# =====================
def extract_acptno_from_url_or_html(link: str, html: str):
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
    # select option value="12345|..." 형태가 가장 흔함
    cands = re.findall(r"option\s+value=['\"](\d+)\|", html)
    # 혹시 docNo=12345 형태도
    cands += re.findall(r"(?:docno|docNo)=(\d+)", html)
    # 유니크 + 정렬
    uniq = []
    seen = set()
    for x in cands:
        if x not in seen:
            uniq.append(x)
            seen.add(x)
    return uniq[:20]  # 너무 많으면 속도 느려짐 방지


def ensure_kind_defaults(params: dict):
    # KIND에서 자주 필요한 기본 파라미터
    if "langTpCd" not in params:
        params["langTpCd"] = "0"
    if "orgid" not in params and "orgId" not in params:
        params["orgid"] = "K"
    if "tran" not in params:
        params["tran"] = "Y"
    if "rcpno" not in params and "rcpNo" not in params:
        # rcpno를 acptno로 보강(보통 이 형태가 잘 됨)
        if params.get("acptno"):
            params["rcpno"] = params["acptno"]
    return params


def build_urls(acptno: str, docno: str):
    base = f"{BASE}/common/disclsviewer.do"
    params = ensure_kind_defaults({
        "acptno": acptno,
        "docno": docno,
    })
    viewer = base + "?" + urlencode({"method": "search", **params})
    contents = base + "?" + urlencode({"method": "searchContents", **params})
    excel = base + "?" + urlencode({"method": "downloadExcel", **params})
    return viewer, contents, excel


# =====================
# 2) HTML 표 파싱 (rowspan/다열 표도 숫자 잡기)
# =====================
def flatten_df(df: pd.DataFrame):
    bag = {}
    df = df.fillna("").astype(str)

    for r in range(len(df)):
        row = [str(x).strip() for x in df.iloc[r].tolist()]
        # 중요: 빈칸도 유지해야 rowspan 구조가 덜 깨짐 → 대신 앞의 빈칸 제거만 최소
        # (완전 제거하면 key/value가 엇갈릴 때가 있음)
        # 여기서는 "연속 빈칸"만 줄이고 남김
        cleaned = []
        for x in row:
            cleaned.append(x)

        # 2칸씩 (k,v) 페어
        for i in range(0, len(cleaned) - 1):
            k = cleaned[i].strip()
            v = cleaned[i + 1].strip()
            if k and v and k != "-" and v != "-" and len(k) < 80:
                bag.setdefault(k, v)

        # row 안에 (원) + 숫자 형태가 있으면 강제 추출
        joined = " ".join([x for x in cleaned if x and x != "-"])
        m = re.findall(r"([가-힣A-Za-z\s]+?\(원\))\s*([\d,]+)", joined)
        for k, v in m:
            k = k.strip()
            v = v.strip()
            if k and v:
                bag.setdefault(k, v)

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

    # soup fallback
    soup = BeautifulSoup(html, "lxml")
    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            cells = [c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])]
            cells = [c for c in cells if c and c != "-"]
            if len(cells) < 2:
                continue

            for i in range(0, len(cells) - 1):
                k = cells[i].strip()
                v = cells[i + 1].strip()
                if k and v and len(k) < 80:
                    bag.setdefault(k, v)

            joined = " ".join(cells)
            m = re.findall(r"([가-힣A-Za-z\s]+?\(원\))\s*([\d,]+)", joined)
            for k, v in m:
                bag.setdefault(k.strip(), v.strip())

    return bag


def parse_html_tables(url: str, session: requests.Session, depth: int = 0):
    try:
        r = fetch(session, url, timeout=25)
        html = r.text

        bag = flatten_tables_from_html(html)
        if bag:
            return bag

        soup = BeautifulSoup(html, "lxml")

        iframe = soup.find("iframe")
        if iframe and iframe.get("src") and depth < 3:
            return parse_html_tables(urljoin(BASE, iframe["src"]), session, depth + 1)

        frame = soup.find("frame")
        if frame and frame.get("src") and depth < 3:
            return parse_html_tables(urljoin(BASE, frame["src"]), session, depth + 1)

        return {}
    except:
        return {}


# =====================
# 3) Excel (docNo 맞으면 진짜 xls 내려옴 / 틀리면 HTML(1650bytes))
# =====================
def parse_excel_download(excel_url: str, session: requests.Session, referer: str):
    headers = {"Referer": referer}
    r = fetch(session, excel_url, timeout=25, headers=headers)
    ct = (r.headers.get("Content-Type") or "").lower()
    size = len(r.content)

    # 진짜 엑셀 여부 간단 판별: HTML이면 버림
    head = r.content[:200].lstrip().lower()
    if ("text/html" in ct) or head.startswith(b"<html") or head.startswith(b"<!doctype html") or size < 5000:
        return {}

    try:
        df = pd.read_excel(io.BytesIO(r.content))
        return flatten_df(df)
    except:
        return {}


# =====================
# 4) Map + 후처리(시설자금 등으로 증자금액/자금용도 만들기)
# =====================
def map_to_target(bag: dict):
    out = {}
    norm_map = {norm(k): k for k in bag.keys()}

    for target, aliases in TARGET_KEYS.items():
        val = ""
        for a in aliases:
            na = norm(a)
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


def enrich_from_purpose_rows(mapped: dict, bag: dict):
    """
    UTI처럼 '자금조달의 목적' 아래에
    시설자금(원) 14,487,000,000 이런 식으로 있을 때
    -> 자금용도/증자금액을 자동 채움
    """
    purposes = []
    total = 0

    for k, v in bag.items():
        kk = str(k)
        vv = str(v)

        # 시설자금(원) 같은 키 직접 잡기
        if "(원)" in kk:
            for p in PURPOSE_KEYS:
                if p in kk:
                    n = parse_int_like(vv)
                    if n:
                        purposes.append(p)
                        total += n

        # value 쪽에 "시설자금(원) 14,487..." 같이 붙어있는 케이스도 잡기
        for p in PURPOSE_KEYS:
            if p in vv and "(원)" in vv:
                mm = re.findall(rf"{re.escape(p)}\s*\(원\)\s*([\d,]+)", vv)
                for amt in mm:
                    n = parse_int_like(amt)
                    if n:
                        purposes.append(p)
                        total += n

    purposes = list(dict.fromkeys(purposes))  # unique preserve order

    if not mapped.get("자금용도") and purposes:
        mapped["자금용도"] = ", ".join(purposes)

    if (not mapped.get("증자금액")) and total > 0:
        mapped["증자금액"] = fmt_int(total)

    return mapped


def is_complete(mapped: dict):
    for f in MIN_COMPLETE_FIELDS:
        if not mapped.get(f):
            return False
    # 회사명만 있으면 너무 느슨하니까, 숫자/자금용도 중 하나라도 있으면 complete
    if mapped.get("증자금액") or mapped.get("확정발행금액(억원)") or mapped.get("자금용도") or mapped.get("확정발행가(원)"):
        return True
    return False


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

    # --- collect items ---
    items = []
    kw_match = 0

    for entry in feed.entries:
        title = entry.get("title", "") or ""
        link = entry.get("link", "") or ""
        guid = entry.get("id") or link
        pub = entry.get("published", "") or ""

        if not guid:
            continue

        if KEYWORDS and not any(k in title for k in KEYWORDS):
            continue

        kw_match += 1
        if guid in seen_list:
            continue

        items.append({"title": title, "link": link, "guid": guid, "pub": pub})

    # retry 추가
    items.extend(retry_queue)

    # dedupe
    uniq = {}
    for it in items:
        uniq[it["guid"]] = it
    items = list(uniq.values())

    print(f"[FILTER] keyword_matched={kw_match} to_process={len(items)}")

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

        # 기본값(제목에서라도 채우기)
        fallback_company = extract_company_from_title(title)
        fallback_market = extract_market_from_title(title)

        # 1) RSS link 페이지 열어서 acptNo/docNo 후보 추출
        r = fetch(session, link, timeout=25)
        html = r.text

        acptno = extract_acptno_from_url_or_html(link, html)
        docnos = extract_docno_candidates(html)

        if not acptno:
            print("   [WARN] acptNo 추출 실패 → 링크 자체로만 진행")
            docnos = []

        print(f"   [FOUND] acptNo={acptno} docNo_candidates={len(docnos)}")

        best = {"score": -1, "bag": {}, "mapped": {}, "viewer": "", "contents": "", "excel": ""}

        # 2) docNo 후보들을 하나씩 시도해서 "표가 가장 많이 나오는 docNo" 선택
        if acptno and docnos:
            for idx, docno in enumerate(docnos, 1):
                viewer_url, contents_url, excel_url = build_urls(acptno, docno)

                # viewer 먼저 열기(세션)
                try:
                    fetch(session, viewer_url, timeout=15)
                except:
                    pass

                bag = parse_html_tables(contents_url, session)
                # html에서 너무 적으면 viewer도 한 번
                if len(bag) < 5:
                    bag2 = parse_html_tables(viewer_url, session)
                    if len(bag2) > len(bag):
                        bag = bag2

                # excel은 docNo 맞는지 확인용으로만(맞으면 bag 확 늘어남)
                if len(bag) < 10:
                    ex_bag = parse_excel_download(excel_url, session, referer=viewer_url)
                    if len(ex_bag) > len(bag):
                        bag.update(ex_bag)

                mapped = map_to_target(bag)
                mapped = enrich_from_purpose_rows(mapped, bag)

                # score: key 수 + 숫자 포함 value 개수
                numeric_cnt = sum(1 for v in bag.values() if re.search(r"\d", str(v)))
                score = len(bag) + numeric_cnt

                print(f"   [TRY {idx}] docNo={docno} bag_keys={len(bag)} numeric_vals={numeric_cnt} score={score}")

                if score > best["score"]:
                    best = {
                        "score": score,
                        "bag": bag,
                        "mapped": mapped,
                        "viewer": viewer_url,
                        "contents": contents_url,
                        "excel": excel_url,
                    }

                # 충분히 좋은 docNo면 조기 종료(속도)
                if score >= 60:
                    break
        else:
            # docNo 후보가 없으면: 그냥 link에서 표를 시도
            bag = parse_html_tables(link, session)
            mapped = map_to_target(bag)
            mapped = enrich_from_purpose_rows(mapped, bag)
            best = {"score": len(bag), "bag": bag, "mapped": mapped, "viewer": link, "contents": link, "excel": ""}

        mapped = best["mapped"]

        # 3) 제목 기반 fallback 채우기(회사명/시장)
        if not mapped.get("회사명") and fallback_company:
            mapped["회사명"] = fallback_company
        if not mapped.get("상장시장") and fallback_market:
            mapped["상장시장"] = fallback_market

        complete = is_complete(mapped)
        status = "SUCCESS" if complete else "INCOMPLETE"

        print(f"   [BEST] score={best['score']} status={status}")
        print(f"   [URL] contents={best['contents']}")

        # 4) Sheets 저장(실패여도 한 줄은 남김)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        is_correction = 1 if "[정정]" in title else 0

        try:
            raw_id = get_next_id(raw_ws)
            raw_ws.append_row([raw_id, now, pub, title, link, guid, status], value_input_option="USER_ENTERED")

            issue_row = [raw_id, now, pub, title, link, guid, is_correction] + [mapped.get(k, "") for k in TARGET_KEYS.keys()]
            issue_ws.append_row(issue_row, value_input_option="USER_ENTERED")

            if guid not in seen_list:
                seen_list.append(guid)

            # complete 아니면 retry에 남김
            if not complete:
                new_retry.append(item)

            print(f"-> Saved to Sheets ({status})")
        except Exception as e:
            print(f"-> [Google Sheets Error] {e}")
            new_retry.append(item)

        time.sleep(SLEEP_SECONDS)

    save_json(SEEN_FILE, seen_list)
    save_json(RETRY_FILE, new_retry)
    print("\n✅ 모든 작업 완료!")


if __name__ == "__main__":
    main()
