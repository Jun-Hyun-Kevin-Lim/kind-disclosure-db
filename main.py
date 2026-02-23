# ====== KIND Disclosure Bot (Real Table Parsing: rowspan/colspan matrix + target-key extraction) ======
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
KEYWORDS = ["유상증자"]  # 테스트: [] 로 두면 전체

SHEET_NAME = "KIND_대경"
RAW_TAB = "RAW"
ISSUE_TAB = "ISSUE"

SEEN_FILE = "seen.json"
RETRY_FILE = "retry_queue.json"

TARGET_KEYS = {
  "회사명": ["회사명", "발행회사", "발행인", "상호", "명칭", "종속회사인", "종속회사"],
  "상장시장": ["상장시장", "시장구분", "시장"],

  "최초 이사회결의일": ["이사회결의일", "결의일", "결정일", "이사회 결의일"],

  "증자방식": ["증자방식", "발행방식", "배정방법", "배정방식"],
  "발행상품": ["신주의 종류", "주식의 종류", "증권종류"],

  "신규발행주식수": ["신규발행주식수", "발행주식수", "발행할 주식의 수", "신주수", "증자할 주식수"],
  "증자전 주식수": ["증자전 주식수", "증자전 발행주식총수", "발행주식총수", "기발행주식총수"],

  "확정발행가(원)": ["확정발행가", "신주발행가액", "발행가", "발행가액", "1주당 발행가액", "확정 발행가"],
  "기준주가": ["기준주가", "기준주가액"],

  "확정발행금액(억원)": ["확정발행금액", "모집총액", "발행총액", "발행금액", "모집금액", "조달금액"],
  "할인(할증률)": ["할인율", "할증률", "할인율(%)"],

  "증자비율": ["증자비율", "증자비율(%)"],

  "청약일": ["청약일", "청약기간", "청약시작일"],
  "납입일": ["납입일", "대금납입일"],

  "주관사": ["주관사", "대표주관회사", "공동주관회사", "인수회사", "인수단"],

  "자금용도": ["자금용도", "자금조달의 목적", "자금사용 목적", "자금조달 목적"],
  "투자자": ["투자자", "제3자배정대상자", "배정대상자", "발행대상자", "대상자", "인수인"],

  "증자금액": ["증자금액", "발행규모", "조달금액", "모집금액", "총 조달금액"],
}

BASE = "https://kind.krx.co.kr"
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
    "Accept": "application/xml,text/xml;q=0.9,text/html;q=0.8,*/*;q=0.7",
    "Connection": "keep-alive",
}
SLEEP_SECONDS = 1

# ✅ RSS 제목의 시장 코드(확정된 메타) → 사람이 읽는 값으로만 변환
#    알 수 없는 코드는 그대로 반환 (네가 말한 “정해놓지 말라” 반영)
MARKET_CODE_MAP = {
    "코": "코스닥",
    "유": "유가증권",
    "넥": "코넥스",
    "KQ": "코스닥",
    "KS": "유가증권",
}

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
    # "[코]유티아이 ..." → "유티아이"
    m = re.match(r"^\[([^\]]+)\]\s*([^\s]+)", (title or "").strip())
    return m.group(2) if m else ""

def extract_market_from_title(title: str):
    # "[코]..." "[유]..." "[넥]..." 등
    m = re.match(r"^\[([^\]]+)\]", (title or "").strip())
    if not m:
        return ""
    code = m.group(1).strip()
    return MARKET_CODE_MAP.get(code, code)  # 모르는 코드는 그대로

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
    soup = BeautifulSoup(html, "lxml")
    vals = []
    for opt in soup.find_all("option"):
        v = (opt.get("value") or "").strip()
        if not v:
            continue
        m = re.match(r"^(\d{6,14})\|", v)
        if m:
            vals.append(m.group(1))
    vals += re.findall(r"option\s+value=['\"](\d{6,14})\|", html)
    vals = [x for x in vals if x.isdigit()]
    uniq, seen = [], set()
    for x in vals:
        if x not in seen:
            uniq.append(x)
            seen.add(x)
    return uniq[:30]

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
# 핵심: rowspan/colspan 펼쳐서 표를 "매트릭스"로 만들기
# =====================
def table_to_matrix(table):
    """
    HTML table을 rowspan/colspan 반영해서 2D matrix로 펼친다.
    """
    matrix = []
    span_map = {}  # col_idx -> (text, remaining_rows)

    rows = table.find_all("tr")
    for tr in rows:
        row = []
        col = 0

        # 이전 rowspan 채우기
        while col in span_map:
            txt, remain = span_map[col]
            row.append(txt)
            remain -= 1
            if remain <= 0:
                del span_map[col]
            else:
                span_map[col] = (txt, remain)
            col += 1

        cells = tr.find_all(["th", "td"])
        for cell in cells:
            # 다음 빈 col 찾기
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
    """
    row[key_idx]가 key일 때, 오른쪽에서 value를 고른다.
    '-' / ''은 스킵하고, 가장 먼저 의미있는 값을 반환.
    """
    for j in range(key_idx + 1, len(row)):
        v = (row[j] or "").strip()
        if not v or v == "-" or v == "—":
            continue
        return v
    return ""

def extract_from_matrix(matrix, aliases):
    """
    matrix 전체를 훑어서 alias가 포함된 cell을 찾고, 오른쪽에서 value를 뽑는다.
    (단일 테이블/다단 테이블 모두 대응)
    """
    aliases_norm = [norm(a) for a in aliases]
    for r in matrix:
        for i, cell in enumerate(r):
            c = (cell or "").strip()
            if not c:
                continue
            cn = norm(c)

            # 번호 제거 ("5. 증자방식" -> "증자방식")
            cn2 = re.sub(r"^\d+\.\s*", "", cn)

            hit = None
            for a in aliases_norm:
                if a and (a in cn2 or a in cn):
                    hit = True
                    break
            if hit:
                v = pick_value_from_row(r, i)
                if v:
                    return v
    return ""

def parse_all_tables_to_matrix(html: str):
    soup = BeautifulSoup(html, "lxml")
    matrices = []
    for table in soup.find_all("table"):
        try:
            matrices.append(table_to_matrix(table))
        except:
            continue
    return matrices

def parse_html_contents_to_mapped(html: str):
    """
    1) 모든 table을 matrix로 펼친다
    2) TARGET_KEYS의 alias를 matrix에서 직접 찾아 value를 뽑는다 (정확도 ↑)
    3) 추가: (원) 금액 합산해서 증자금액 보강
    """
    matrices = parse_all_tables_to_matrix(html)
    mapped = {k: "" for k in TARGET_KEYS.keys()}

    # table 기반 추출
    for target, aliases in TARGET_KEYS.items():
        for mtx in matrices:
            v = extract_from_matrix(mtx, aliases)
            if v:
                mapped[target] = v
                break

    # (원) 금액 합산으로 증자금액 보강(하드코딩 목적키 없이)
    if not mapped.get("증자금액"):
        total = 0
        for mtx in matrices:
            for r in mtx:
                # row 안에 "(원)" + 숫자 패턴이 있으면 잡기
                joined = " ".join([x for x in r if x and x != "-"])
                hits = re.findall(r"\(원\)\s*([\d,]+)", joined)
                for h in hits:
                    n = parse_int_like(h)
                    if n and n >= 1_000_000:
                        total += n
        if total > 0:
            mapped["증자금액"] = fmt_int(total)

    return mapped

def fetch_and_parse_contents(session, contents_url):
    r = fetch(session, contents_url, timeout=25)
    return r.text, parse_html_contents_to_mapped(r.text)

def score_mapped(mapped: dict):
    # 값이 채워진 target 개수로 score
    return sum(1 for v in mapped.values() if str(v).strip())

# =====================
# Status
# =====================
def decide_status(mapped: dict):
    # 회사명만 있는 건 불완전
    filled = [k for k, v in mapped.items() if str(v).strip()]
    if "회사명" in filled and len(filled) >= 3:
        return "SUCCESS"
    # 돈/수량/날짜 중 하나라도 있으면 성공 취급
    strong_fields = ["증자금액", "확정발행가(원)", "신규발행주식수", "납입일", "청약일"]
    if any(str(mapped.get(f, "")).strip() for f in strong_fields):
        return "SUCCESS"
    return "INCOMPLETE"

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

    # dedupe
    uniq = {}
    for it in items:
        uniq[it["guid"]] = it
    items = list(uniq.values())

    print(f"[QUEUE] to_process={len(items)}")
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

        # ✅ 제목에서 얻을 수 있는 메타(확정된 것만 fallback)
        title_company = extract_company_from_title(title)
        title_market = extract_market_from_title(title)

        # 1) RSS link 페이지에서 acpt/doc 후보 얻기
        link_res = fetch(session, link, timeout=25)
        link_html = link_res.text

        acptno = extract_acptno(link, link_html)
        docnos = extract_docno_candidates(link_html)

        print(f"   [FOUND] acptNo={acptno} docNo_candidates={len(docnos)}")

        best = None  # (score, contents_url, html, mapped)

        if acptno and docnos:
            for idx, docno in enumerate(docnos, 1):
                viewer_url, contents_url, _ = build_urls(acptno, docno)

                # viewer 한번 열어 세션 흐름 맞추기
                try:
                    fetch(session, viewer_url, timeout=15)
                except:
                    pass

                html, mapped = fetch_and_parse_contents(session, contents_url)
                sc = score_mapped(mapped)
                print(f"   [TRY {idx}] docNo={docno} filled={sc}")

                if best is None or sc > best[0]:
                    best = (sc, contents_url, html, mapped)

                if sc >= 8:  # 충분히 많이 채워지면 조기 종료
                    break
        else:
            # fallback: link 자체로 시도
            html, mapped = fetch_and_parse_contents(session, link)
            best = (score_mapped(mapped), link, html, mapped)

        filled_score, best_contents, best_html, mapped = best
        print(f"   [BEST] filled={filled_score} url={best_contents}")

        # ✅ 회사명/시장: 본문에서 못 찾았을 때만 title fallback
        if not str(mapped.get("회사명", "")).strip() and title_company:
            mapped["회사명"] = title_company
        if not str(mapped.get("상장시장", "")).strip() and title_market:
            mapped["상장시장"] = title_market

        status = decide_status(mapped)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 디버그: 너무 안 채워지면, HTML에서 table 개수만 찍어보기
        if filled_score <= 2:
            soup = BeautifulSoup(best_html, "lxml")
            print(f"   [DEBUG] tables_found={len(soup.find_all('table'))}")

        try:
            raw_id = get_next_id(raw_ws)

            # RAW
            raw_ws.append_row([raw_id, now, pub, title, link, guid, status], value_input_option="USER_ENTERED")

            # ISSUE (중요: 너 시트 구조에 맞게 is_correction 같은 extra 컬럼 없음)
            issue_row = [raw_id, now, pub, title, link, guid] + [mapped.get(k, "") for k in TARGET_KEYS.keys()]
            issue_ws.append_row(issue_row, value_input_option="USER_ENTERED")

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
