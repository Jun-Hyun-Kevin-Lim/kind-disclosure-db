# ====== KIND Disclosure Bot (Fix Column Shift + Better Table Parse, No PURPOSE_KEYS) ======
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
KEYWORDS = ["유상증자"]  # 테스트: []로 두면 전체

SHEET_NAME = "KIND_대경"
RAW_TAB = "RAW"
ISSUE_TAB = "ISSUE"

SEEN_FILE = "seen.json"
RETRY_FILE = "retry_queue.json"

TARGET_KEYS = {
  "회사명": ["회사명", "발행회사", "발행인", "상호", "명칭"],
  "상장시장": ["상장시장", "시장구분", "시장"],

  "최초 이사회결의일": ["이사회결의일", "결의일", "결정일"],

  "증자방식": ["증자방식", "발행방식", "배정방법", "배정방식"],
  "발행상품": ["신주의 종류", "주식의 종류", "증권종류"],

  "신규발행주식수": ["신규발행주식수", "발행주식수", "발행할 주식의 수", "신주수"],
  "증자전 주식수": ["증자전 주식수", "증자전 발행주식총수", "발행주식총수", "기발행주식총수"],

  "확정발행가(원)": ["확정발행가", "신주발행가액", "발행가", "발행가액", "1주당 발행가액"],
  "기준주가": ["기준주가", "기준주가액"],

  "확정발행금액(억원)": ["확정발행금액", "모집총액", "발행총액", "발행금액", "모집금액"],
  "할인(할증률)": ["할인율", "할증률", "할인율(%)"],

  "증자비율": ["증자비율", "증자비율(%)"],

  "청약일": ["청약일", "청약기간", "청약시작일"],
  "납입일": ["납입일", "대금납입일"],

  "주관사": ["주관사", "대표주관회사", "공동주관회사", "인수회사", "인수단"],

  "자금용도": ["자금용도", "자금조달의 목적", "자금사용 목적"],
  "투자자": ["투자자", "제3자배정대상자", "배정대상자", "발행대상자", "대상자", "인수인"],
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
# Utils
# =====================
def norm(s: str):
    return re.sub(r"\s+", "", str(s or "")).lower()

def parse_int_like(s: str):
    digits = re.sub(r"[^\d]", "", str(s or ""))
    if not digits:
        return None
    try:
        return int(digits)
    except:
        return None

def fmt_int(n: int):
    return f"{n:,}"

def extract_company_from_title(title: str):
    # [코]유티아이 ... / [유]포스코인터내셔널 [정정]...
    t = title.strip()
    m = re.match(r"^\[(코|유)\]([^\s]+)", t)
    return m.group(2) if m else ""

def extract_market_from_title(title: str):
    t = title.strip()
    if t.startswith("[코]"):
        return "코스닥"
    if t.startswith("[유]"):
        return "유가증권"
    return ""


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
    # ✅ 가장 안정적인 방식: A열의 마지막 값 + 1
    col = ws.col_values(1)
    if len(col) <= 1:
        return 0
    last = col[-1]
    if str(last).strip().isdigit():
        return int(str(last).strip()) + 1

    # fallback: 숫자만 훑어서 max+1
    mx = -1
    for v in col:
        v = str(v).strip()
        if v.isdigit():
            mx = max(mx, int(v))
    return mx + 1


# =====================
# HTTP / RSS
# =====================
def fetch(session: requests.Session, url: str, timeout=25, headers=None):
    h = dict(DEFAULT_HEADERS)
    if headers:
        h.update(headers)
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
# KIND: acptNo / docNo candidates
# =====================
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
    # ✅ soup로 option value를 우선 파싱 (정규식보다 안정적)
    soup = BeautifulSoup(html, "lxml")
    vals = []
    for opt in soup.find_all("option"):
        v = (opt.get("value") or "").strip()
        if not v:
            continue
        # "20260223003083|..." 형태
        m = re.match(r"^(\d{6,14})\|", v)
        if m:
            vals.append(m.group(1))
        # docNo=숫자 형태도
        m2 = re.search(r"(?:docno|docNo)=(\d{6,14})", v)
        if m2:
            vals.append(m2.group(1))

    # fallback regex
    vals += re.findall(r"option\s+value=['\"](\d{6,14})\|", html)
    vals += re.findall(r"(?:docno|docNo)=(\d{6,14})", html)

    uniq = []
    seen = set()
    for x in vals:
        if x not in seen:
            uniq.append(x)
            seen.add(x)
    return uniq[:20]

def build_urls(acptno: str, docno: str):
    base = f"{BASE}/common/disclsviewer.do"
    params = {
        "acptno": acptno,
        "docno": docno,
        "langTpCd": "0",
        "orgid": "K",
        "tran": "Y",
        "rcpno": acptno,
    }
    viewer = base + "?" + urlencode({"method": "search", **params})
    contents = base + "?" + urlencode({"method": "searchContents", **params})
    excel = base + "?" + urlencode({"method": "downloadExcel", **params})
    return viewer, contents, excel


# =====================
# Table parsing (merged cells 대응)
# =====================
def extract_pairs_from_row(cells):
    # cells: ["5. 증자방식", "", "주주배정증자"] 같은 케이스를 잡기
    cleaned = [c.strip() for c in cells]
    nonempty = [c for c in cleaned if c and c != "-"]
    pairs = {}

    # 1) 인접 페어
    for i in range(len(cleaned) - 1):
        a, b = cleaned[i], cleaned[i + 1]
        if a and b and a != "-" and b != "-" and a != b:
            pairs.setdefault(a, b)

    # 2) 첫 nonempty -> 마지막 nonempty
    if len(nonempty) >= 2:
        pairs.setdefault(nonempty[0], nonempty[-1])

    # 3) "키(원) 숫자" 패턴
    joined = " ".join(nonempty)
    m = re.findall(r"([가-힣A-Za-z\s]+?\(원\))\s*([\d,]+)", joined)
    for k, v in m:
        pairs.setdefault(k.strip(), v.strip())

    return pairs

def flatten_tables_from_html(html: str):
    bag = {}

    # pandas read_html
    try:
        tables = pd.read_html(io.StringIO(html))
        for df in tables:
            df = df.fillna("").astype(str)
            for r in range(len(df)):
                row = [str(x) for x in df.iloc[r].tolist()]
                bag.update(extract_pairs_from_row(row))
        if bag:
            return bag
    except:
        pass

    # soup fallback
    soup = BeautifulSoup(html, "lxml")
    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            row = [td.get_text(" ", strip=True) for td in tr.find_all(["th", "td"])]
            if len(row) < 2:
                continue
            bag.update(extract_pairs_from_row(row))

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
# Mapping + 자동 합산(원) → 증자금액
# =====================
def map_to_target(bag: dict):
    out = {k: "" for k in TARGET_KEYS.keys()}
    norm_map = {norm(k): k for k in bag.keys()}

    for target, aliases in TARGET_KEYS.items():
        for a in aliases:
            na = norm(a)
            matched = None
            for nk, orig_k in norm_map.items():
                if na and na in nk:
                    matched = orig_k
                    break
            if matched:
                out[target] = str(bag.get(matched, "")).strip()
                break
    return out

def compute_total_won_from_bag(bag: dict):
    total = 0
    for k, v in bag.items():
        kk = str(k)
        if "(원)" in kk:
            n = parse_int_like(v)
            # 너무 작은 값(액면가 등) 제외하려고 1,000,000원 이상만 합산
            if n and n >= 1_000_000:
                total += n
    return total

def decide_status(mapped: dict):
    # 회사명은 항상 들어가게 만들 거라, 실제 의미 있는 값이 1개라도 있으면 SUCCESS
    meaningful = [
        mapped.get("증자방식"), mapped.get("신규발행주식수"), mapped.get("확정발행가(원)"),
        mapped.get("증자금액"), mapped.get("확정발행금액(억원)"), mapped.get("자금용도")
    ]
    return "SUCCESS" if any(x for x in meaningful if str(x).strip()) else "INCOMPLETE"


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

        # ✅ 제목에서 회사/시장 먼저 확보 (시트에 항상 정확히 들어가게)
        title_company = extract_company_from_title(title)
        title_market = extract_market_from_title(title)

        r = fetch(session, link, timeout=25)
        html = r.text

        acptno = extract_acptno(link, html)
        docnos = extract_docno_candidates(html)

        print(f"   [FOUND] acptNo={acptno} docNo_candidates={len(docnos)}")

        best_bag = {}
        best_score = -1
        best_contents = ""

        if acptno and docnos:
            for idx, docno in enumerate(docnos, 1):
                viewer_url, contents_url, _ = build_urls(acptno, docno)

                # viewer 먼저 열기
                try:
                    fetch(session, viewer_url, timeout=15)
                except:
                    pass

                bag = parse_html_tables(contents_url, session)
                score = len(bag) + sum(1 for v in bag.values() if re.search(r"\d", str(v)))
                print(f"   [TRY {idx}] docNo={docno} bag_keys={len(bag)} score={score}")

                if score > best_score:
                    best_score = score
                    best_bag = bag
                    best_contents = contents_url

                if score >= 50:
                    break
        else:
            # fallback
            best_bag = parse_html_tables(link, session)
            best_score = len(best_bag)
            best_contents = link

        if best_score <= 0:
            print("   [WARN] 본문 표 추출 실패(키=0).")

        # 디버그: 너무 적게 나오면 키 일부 출력
        if len(best_bag) <= 8:
            ks = list(best_bag.keys())[:30]
            print(f"   [BAG keys sample] {ks}")

        mapped = map_to_target(best_bag)

        # ✅ 회사명/상장시장은 무조건 제목 기반으로 "덮어쓰기" (정확도 100%)
        if title_company:
            mapped["회사명"] = title_company
        if title_market:
            mapped["상장시장"] = title_market

        # ✅ (원) 숫자 합산해서 증자금액 자동 채우기 (PURPOSE_KEYS 없이)
        if not mapped.get("증자금액"):
            total_won = compute_total_won_from_bag(best_bag)
            if total_won > 0:
                mapped["증자금액"] = fmt_int(total_won)

        # 확정발행금액(억원)이 비면 total_won으로 억원 환산(선택)
        if not mapped.get("확정발행금액(억원)") and mapped.get("증자금액"):
            n = parse_int_like(mapped["증자금액"])
            if n:
                mapped["확정발행금액(억원)"] = str(round(n / 100_000_000, 2))

        status = decide_status(mapped)
        print(f"   [BEST] score={best_score} status={status}")
        print(f"   [URL] contents={best_contents}")

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        try:
            # RAW 저장
            raw_id = get_next_id(raw_ws)
            raw_ws.append_row([raw_id, now, pub, title, link, guid, status], value_input_option="USER_ENTERED")

            # ✅ ISSUE 저장: is_correction 컬럼 제거 (컬럼 밀림 해결)
            issue_row = [raw_id, now, pub, title, link, guid] + [mapped.get(k, "") for k in TARGET_KEYS.keys()]
            issue_ws.append_row(issue_row, value_input_option="USER_ENTERED")

            if guid not in seen_list:
                seen_list.append(guid)

            if status != "SUCCESS":
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
