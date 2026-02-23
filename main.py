# ====== KIND Disclosure Bot (Debug + Stable RSS + Save Even If Incomplete) ======
import os, json, time, re, io
from datetime import datetime
from urllib.parse import urlparse, parse_qs, urljoin

import feedparser
import pandas as pd
import requests
import gspread
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials

# =====================
# Config
# =====================
# ✅ currentPageSize=200 (15개만 받아서 키워드 매칭 0개 되는 문제 방지)
RSS_URL = "https://kind.krx.co.kr/disclosure/rsstodaydistribute.do?method=searchRssTodayDistribute&repIsuSrtCd=&mktTpCd=0&searchCorpName=&currentPageSize=100"

# ✅ 키워드 없으면 전체 저장(테스트할 때 유용)
KEYWORDS = ["유상증자"]

SHEET_NAME = "KIND_대경"
RAW_TAB = "RAW"
ISSUE_TAB = "ISSUE"

SEEN_FILE = "seen.json"
RETRY_FILE = "retry_queue.json"

# 완성도 체크 기준(원하면 조정)
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
    bozo = getattr(feed, "bozo", 0)
    print(f"[RSS] entries={len(feed.entries)} bozo={bozo}")
    if bozo:
        print(f"[RSS] bozo_exception={getattr(feed,'bozo_exception',None)}")
    # title sample
    for i, e in enumerate(feed.entries[:5]):
        print(f"[RSS sample {i+1}] {e.get('title','')}")
    return feed


# =====================
# KIND URL building
# =====================
def extract_qs(url: str):
    qs = parse_qs(urlparse(url).query)

    def pick(*keys):
        for k in keys:
            if k in qs and qs[k]:
                return qs[k][0]
        return None

    return {
        "acptNo": pick("acptNo", "acptno"),
        "docNo": pick("docNo", "docno"),
        "rcpNo": pick("rcpNo", "rcpno"),
        "orgId": pick("orgId", "orgid"),
        "langTpCd": pick("langTpCd"),
        "viewerhost": pick("viewerhost"),
        "tran": pick("tran"),
    }


def build_urls(original_url: str, session: requests.Session):
    q = extract_qs(original_url)

    # 없으면 원본 페이지에서 최대한 추출
    if not q["acptNo"] or not q["docNo"]:
        try:
            r = fetch(session, original_url, timeout=20)
            txt = r.text

            if not q["acptNo"]:
                m = re.search(r'acptNo"\s*value="(\d+)"', txt)
                if m:
                    q["acptNo"] = m.group(1)
                else:
                    m = re.search(r'_TRK_PN\s*=\s*"(\d+)"', txt)
                    if m:
                        q["acptNo"] = m.group(1)

            if not q["docNo"]:
                m = re.search(r"(docNo|docno)=(\d{1,14})", txt)
                if m:
                    q["docNo"] = m.group(2)
                else:
                    m = re.search(r"option\s+value=['\"](\d+)\|", txt)
                    if m:
                        q["docNo"] = m.group(1)
        except:
            pass

    if not q["acptNo"] or not q["docNo"]:
        return original_url, None, None

    extra = []
    for k in ["rcpNo", "orgId", "langTpCd", "viewerhost", "tran"]:
        if q.get(k):
            extra.append(f"{k}={q[k]}")
    extra_qs = ("&" + "&".join(extra)) if extra else ""

    viewer_url = f"{BASE}/common/disclsviewer.do?method=search&acptNo={q['acptNo']}&docNo={q['docNo']}{extra_qs}"
    contents_url = f"{BASE}/common/disclsviewer.do?method=searchContents&acptNo={q['acptNo']}&docNo={q['docNo']}{extra_qs}"
    excel_url = f"{BASE}/common/disclsviewer.do?method=downloadExcel&acptNo={q['acptNo']}&docNo={q['docNo']}{extra_qs}"
    return viewer_url, contents_url, excel_url


# =====================
# HTML parsing
# =====================
def flatten_tables_from_html(html: str):
    bag = {}
    tables = pd.read_html(io.StringIO(html))
    for df in tables:
        df = df.fillna("").astype(str)
        for r in range(len(df)):
            for c in range(len(df.columns) - 1):
                k = str(df.iloc[r, c]).strip()
                v = str(df.iloc[r, c + 1]).strip()
                if k and v and len(k) < 60:
                    bag[k] = v
    return bag


def _find_contents_url(html: str):
    m = re.search(r"(\/common\/disclsviewer\.do\?method=searchContents[^\"'\s]+)", html)
    if m:
        return urljoin(BASE, m.group(1))
    m = re.search(r"(https?:\/\/kind\.krx\.co\.kr\/common\/disclsviewer\.do\?method=searchContents[^\"'\s]+)", html)
    if m:
        return m.group(1)
    return None


def parse_html_tables(url: str, session: requests.Session, depth: int = 0):
    try:
        r = fetch(session, url, timeout=25)
        html = r.text

        try:
            return flatten_tables_from_html(html)
        except ValueError:
            pass

        soup = BeautifulSoup(html, "lxml")

        iframe = soup.find("iframe")
        if iframe and iframe.get("src") and depth < 3:
            return parse_html_tables(urljoin(BASE, iframe["src"]), session, depth + 1)

        frame = soup.find("frame")
        if frame and frame.get("src") and depth < 3:
            return parse_html_tables(urljoin(BASE, frame["src"]), session, depth + 1)

        cu = _find_contents_url(html)
        if cu and depth < 3:
            return parse_html_tables(cu, session, depth + 1)

        return {}
    except:
        return {}


# =====================
# Excel fallback (Referer)
# =====================
def parse_excel_fallback(excel_url: str, session: requests.Session, referer: str):
    bag = {}
    try:
        headers = {"Referer": referer}
        r = fetch(session, excel_url, timeout=25, headers=headers)

        ct = (r.headers.get("Content-Type") or "").lower()
        cd = (r.headers.get("Content-Disposition") or "").lower()
        print(f"   [Excel HTTP] status={r.status_code} ct={ct} cd={cd} bytes={len(r.content)}")

        if r.status_code != 200 or len(r.content) < 200:
            return {}

        head = r.content[:400].lstrip().lower()

        # HTML(안내/에러)면 preview 출력
        if ("text/html" in ct) or head.startswith(b"<!doctype html") or head.startswith(b"<html") or (b"<table" in head):
            preview = (r.text[:350]).replace("\n", " ")
            print(f"   [Excel HTML Preview] {preview}")
            try:
                return flatten_tables_from_html(r.text)
            except:
                return {}

        # 진짜 엑셀이면
        df = pd.read_excel(io.BytesIO(r.content))
        df = df.fillna("").astype(str)
        for rr in range(len(df)):
            for cc in range(len(df.columns) - 1):
                k = df.iloc[rr, cc].strip()
                v = df.iloc[rr, cc + 1].strip()
                if k and v and len(k) < 60:
                    bag[k] = v
    except Exception as e:
        print(f"   [Excel Parse Error] {e}")
    return bag


# =====================
# Mapping & completeness
# =====================
def _norm(s: str):
    return re.sub(r"\s+", "", str(s or "")).lower()


def map_to_target(bag):
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


def check_completeness(mapped_data):
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

    # ✅ RSS를 requests로 가져와서 파싱(상태/entries 디버그)
    feed = fetch_rss_feed(session)

    # --- Collect items ---
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

        # 키워드 필터(키워드 비어있으면 전체)
        if KEYWORDS:
            if not any(k in title for k in KEYWORDS):
                continue

        kw_match += 1
        if guid in seen_list:
            continue

        new_items.append({"title": title, "link": link, "guid": guid, "pub": pub})

    print(f"[FILTER] total_entries={total_entries} keyword_matched={kw_match} new_items={len(new_items)}")

    # Retry 합치기
    items_to_process = new_items + retry_queue

    # guid 기준 dedupe
    uniq = {}
    for it in items_to_process:
        uniq[it["guid"]] = it
    items_to_process = list(uniq.values())

    print(f"[QUEUE] to_process={len(items_to_process)} (new + retry - dedupe)")

    if not items_to_process:
        # ✅ 여기서 끝나면 "시트에 아무것도 안 써지는" 게 정상
        print("[INFO] 처리할 항목이 0개라서 시트에 기록되지 않았습니다.")
        print("      - (1) RSS entries=0 이거나")
        print("      - (2) 최근 200개 안에 키워드가 없거나")
        print("      - (3) 전부 seen에 들어가 있거나 / retry도 비어있음")
        print("      위 로그([RSS]/[FILTER]/[STATE])로 원인 확인 가능")
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

        viewer_url, contents_url, excel_url = build_urls(link, session)

        # viewer 먼저 열기(세션/쿠키)
        try:
            fetch(session, viewer_url, timeout=20)
        except:
            pass

        # HTML 파싱
        bag = {}
        if contents_url:
            bag = parse_html_tables(contents_url, session)
        if not bag:
            bag = parse_html_tables(viewer_url, session)

        mapped = map_to_target(bag)
        is_complete = check_completeness(mapped)

        # Excel fallback
        if not is_complete and excel_url:
            print("-> [Fallback] 엑셀 데이터 다운로드 시도...")
            fallback_bag = parse_excel_fallback(excel_url, session, referer=viewer_url)
            fallback_mapped = map_to_target(fallback_bag)

            for k, v in fallback_mapped.items():
                if (not mapped.get(k)) and v:
                    mapped[k] = v
            is_complete = check_completeness(mapped)

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        status = "SUCCESS" if is_complete else "INCOMPLETE"

        # ✅ 여기 핵심: 실패해도 RAW/ISSUE에 "한 줄은" 남긴다
        try:
            raw_id = get_next_id(raw_ws)

            raw_ws.append_row(
                [raw_id, now, pub, title, link, guid, status],
                value_input_option="USER_ENTERED",
            )

            issue_row = [raw_id, now, pub, title, link, guid, is_correction] + [
                mapped[k] for k in TARGET_KEYS.keys()
            ]
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
