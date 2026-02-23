# ====== KIND Disclosure Bot (Advanced System) ======
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
RSS_URL = "http://kind.krx.co.kr:80/disclosure/rsstodaydistribute.do?method=searchRssTodayDistribute&repIsuSrtCd=&mktTpCd=0&searchCorpName=&currentPageSize=15"
KEYWORDS = ["유상증자"]  # 필요하면 ["유상증자", "전환사채", ...] 같이 확장

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
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
}

SLEEP_SECONDS = 2


# =====================
# 1) 상태 관리 (Seen & Retry Queue)
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
# 2) Google Sheets 연결
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
    return sh.worksheet(RAW_TAB), sh.worksheet(ISSUE_TAB)


def get_next_id(ws):
    """1열을 보고 다음 ID를 계산(헤더가 있어도 안전)."""
    col = ws.col_values(1)
    if not col:
        return 0
    max_id = -1
    for v in col:
        v = str(v).strip()
        if v.isdigit():
            max_id = max(max_id, int(v))
    return max_id + 1


# =====================
# 3) HTTP Fetch
# =====================
def fetch(session: requests.Session, url: str, timeout=20):
    r = session.get(url, headers=DEFAULT_HEADERS, timeout=timeout, allow_redirects=True)
    # KIND는 euc-kr/혼합 인코딩 케이스가 있어 보정
    if not r.encoding or r.encoding.lower() == "iso-8859-1":
        r.encoding = r.apparent_encoding or "utf-8"
    return r


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


# =====================
# 4) KIND 본문/엑셀 URL 만들기 (핵심)
# =====================
def build_contents_and_excel_urls(original_url: str, session: requests.Session):
    """
    RSS link/Kind viewer link에서 acptNo/docNo를 최대한 추출해서
    searchContents, downloadExcel URL을 생성.
    acptNo/docNo를 못 구하면 original_url을 그대로 반환(iframe 추적으로 처리).
    """
    q = extract_qs(original_url)

    # 링크에 acptNo/docNo가 없으면 원본 페이지에서 추출 시도
    if not q["acptNo"] or not q["docNo"]:
        try:
            r = fetch(session, original_url, timeout=20)
            txt = r.text

            if not q["acptNo"]:
                m = re.search(r"(acptNo|acptno)=(\d{8,14})", txt)
                if m:
                    q["acptNo"] = m.group(2)
                else:
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
                    # option value="12345|..."
                    m = re.search(r"option\s+value=['\"](\d+)\|", txt)
                    if m:
                        q["docNo"] = m.group(1)
        except:
            pass

    # 그래도 acptNo/docNo를 못 구하면: 원본 URL로 HTML 파싱(iframe 추적이 처리)
    if not q["acptNo"] or not q["docNo"]:
        return original_url, None

    # 원래 URL 파라미터 일부 유지(케이스 따라 필요)
    extra = []
    for k in ["rcpNo", "orgId", "langTpCd", "viewerhost", "tran"]:
        if q.get(k):
            extra.append(f"{k}={q[k]}")
    extra_qs = ("&" + "&".join(extra)) if extra else ""

    contents_url = (
        f"{BASE}/common/disclsviewer.do?method=searchContents&acptNo={q['acptNo']}&docNo={q['docNo']}{extra_qs}"
    )
    excel_url = (
        f"{BASE}/common/disclsviewer.do?method=downloadExcel&acptNo={q['acptNo']}&docNo={q['docNo']}{extra_qs}"
    )
    return contents_url, excel_url


# =====================
# 5) HTML 파싱 (표 데이터 플래트닝 + iframe 추적)
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
                if k and v and len(k) < 50:
                    bag[k] = v
    return bag


def parse_html_tables(url: str, session: requests.Session, depth: int = 0):
    """
    1) URL 받아서 read_html로 표 찾기
    2) 표 없으면 iframe src 따라가기 (KIND가 이 구조가 많음)
    """
    try:
        r = fetch(session, url, timeout=25)
        html = r.text

        # 1) table 파싱 시도
        try:
            return flatten_tables_from_html(html)
        except ValueError:
            pass

        # 2) table 없으면 iframe 추적
        soup = BeautifulSoup(html, "lxml")
        iframe = soup.find("iframe")
        if iframe and iframe.get("src") and depth < 2:
            next_url = urljoin(BASE, iframe["src"])
            return parse_html_tables(next_url, session, depth + 1)

        return {}
    except:
        print("-> [HTML Parse Error] 표를 찾을 수 없거나 형식이 다릅니다.")
        return {}


# =====================
# 6) Excel Fallback 파싱 (진짜 엑셀 or HTML-table 엑셀)
# =====================
def parse_excel_fallback(excel_url: str, session: requests.Session):
    bag = {}
    try:
        print("-> [Fallback] 엑셀 데이터 다운로드 시도...")
        r = fetch(session, excel_url, timeout=25)
        ct = (r.headers.get("Content-Type") or "").lower()

        # 디버그(원인 확인용)
        print(f"   [Excel HTTP] status={r.status_code} ct={ct} bytes={len(r.content)}")

        if r.status_code != 200 or len(r.content) < 200:
            return {}

        head = r.content[:300].lstrip().lower()

        # (1) 응답이 HTML이면: read_html로 처리 (KIND 엑셀이 실제로 HTML인 경우 있음)
        if ("text/html" in ct) or head.startswith(b"<!doctype html") or head.startswith(b"<html") or (b"<table" in head):
            try:
                return flatten_tables_from_html(r.text)
            except:
                return {}

        # (2) 진짜 xls/xlsx면: read_excel로 처리 (xls는 xlrd 필요)
        df = pd.read_excel(io.BytesIO(r.content))
        df = df.fillna("").astype(str)
        for rr in range(len(df)):
            for cc in range(len(df.columns) - 1):
                k = df.iloc[rr, cc].strip()
                v = df.iloc[rr, cc + 1].strip()
                if k and v and len(k) < 50:
                    bag[k] = v
    except Exception as e:
        print(f"-> [Excel Parse Error] {e}")
    return bag


# =====================
# 7) 타겟 키 매핑 & 완성도 체크
# =====================
def _norm(s: str):
    return re.sub(r"\s+", "", str(s or "")).lower()


def map_to_target(bag):
    out = {}
    # bag key들을 normalize해서 빠르게 찾기
    norm_map = {_norm(k): k for k in bag.keys()}

    for target, aliases in TARGET_KEYS.items():
        val = ""
        for a in aliases:
            na = _norm(a)
            # bag 키 중 alias 포함(부분매칭)
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
    for field in REQUIRED_FIELDS:
        if not mapped_data.get(field):
            return False
    return True


# =====================
# 8) Main Pipeline
# =====================
def main():
    raw_ws, issue_ws = connect_gs()

    seen_list = load_json(SEEN_FILE, [])
    retry_queue = load_json(RETRY_FILE, [])

    session = requests.Session()

    feed = feedparser.parse(RSS_URL)
    items_to_process = []

    # (A) RSS 신규 수집
    for entry in feed.entries:
        title = entry.get("title", "") or ""
        link = entry.get("link", "") or ""
        guid = entry.get("id") or link
        pub = entry.get("published", "") or ""

        if not guid:
            continue
        if guid in seen_list:
            continue
        if not any(k in title for k in KEYWORDS):
            continue

        items_to_process.append({"title": title, "link": link, "guid": guid, "pub": pub})

    # (B) Retry 합치기
    items_to_process.extend(retry_queue)

    # ✅ 같은 guid 중복 제거(네 로그처럼 같은 항목이 2~3번 도는 문제 방지)
    uniq = {}
    for it in items_to_process:
        uniq[it["guid"]] = it
    items_to_process = list(uniq.values())

    new_retry_queue = []

    for item in items_to_process:
        title = item["title"]
        link = item["link"]
        guid = item["guid"]
        pub = item.get("pub", "")

        print(f"Processing: {title}")
        is_correction = 1 if "[정정]" in title else 0

        # 1) 본문/엑셀 URL 구성
        real_url, excel_url = build_contents_and_excel_urls(link, session)

        # 2) HTML 파싱 시도(iframe 포함)
        bag = parse_html_tables(real_url, session)
        mapped = map_to_target(bag)
        is_complete = check_completeness(mapped)

        # 3) 부족하면 엑셀 fallback (진짜 엑셀 or HTML-table)
        if not is_complete and excel_url:
            fallback_bag = parse_excel_fallback(excel_url, session)
            fallback_mapped = map_to_target(fallback_bag)

            # 빈 값만 보강
            for k, v in fallback_mapped.items():
                if (not mapped.get(k)) and v:
                    mapped[k] = v
            is_complete = check_completeness(mapped)

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # ✅ 완전하지 않아도 회사명이라도 있으면 저장(너 기존 정책 유지)
        if is_complete or mapped.get("회사명"):
            try:
                raw_id = get_next_id(raw_ws)

                # RAW 저장
                raw_ws.append_row(
                    [raw_id, now, pub, title, link, guid, "SUCCESS"],
                    value_input_option="USER_ENTERED",
                )

                # ISSUE 저장
                issue_row = [raw_id, now, pub, title, link, guid, is_correction] + [
                    mapped[k] for k in TARGET_KEYS.keys()
                ]
                issue_ws.append_row(issue_row, value_input_option="USER_ENTERED")

                if guid not in seen_list:
                    seen_list.append(guid)

                print("-> Success & Saved")
            except Exception as e:
                print(f"-> [Google Sheets Error] {e}")
                new_retry_queue.append(item)
        else:
            print("-> [Incomplete Data] 핵심 데이터 추출 실패. 재시도 큐로 이동.")
            new_retry_queue.append(item)

        time.sleep(SLEEP_SECONDS)

    save_json(SEEN_FILE, seen_list)
    save_json(RETRY_FILE, new_retry_queue)
    print("✅ 모든 작업 완료!")


if __name__ == "__main__":
    main()
