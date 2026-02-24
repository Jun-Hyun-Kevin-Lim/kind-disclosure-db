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
BOT_VERSION = "kind-bot-v3.1-refined"

RSS_URL = "http://kind.krx.co.kr:80/disclosure/rsstodaydistribute.do?method=searchRssTodayDistribute&repIsuSrtCd=&mktTpCd=0&searchCorpName=&currentPageSize=100"
BASE = "https://kind.krx.co.kr"

# ★ 중요: 데이터가 실제로 있는 '결정' 공시 키워드로 한정
KEYWORDS = ["유상증자결정", "전환사채권발행결정", "교환사채권발행결정"]

SHEET_NAME = os.getenv("SHEET_NAME", "KIND_대경")
RAW_TAB = "RAW"
TAB_YUSANG = "유상증자"
TAB_JEONHWAN = "전환사채"
TAB_GYOHWAN = "교환사채"

SEEN_FILE = os.getenv("SEEN_FILE", "seen.json")
RETRY_FILE = os.getenv("RETRY_FILE", "retry_queue.json")

SUCCESS_FILLED_MIN = 8  # '결정' 공시의 경우 최소 8개 이상은 채워져야 성공으로 간주
SLEEP_SECONDS = 0.8

PW_NAV_TIMEOUT_MS = 25000
PW_WAIT_MS = 3000

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9",
}

ISSUE_FIELDS = [
    "회사명","상장시장","최초 이사회결의일","증자방식","발행상품","신규발행주식수","확정발행가(원)","기준주가","확정발행금액(억원)","할인(할증률)",
    "증자전 주식수","증자비율","청약일","납입일","주관사","자금용도","투자자","증자금액",
]

ALIASES = {
    "최초 이사회결의일": ["최초 이사회결의일","이사회결의일","결의일","결정일","이사회 결의일"],
    "증자방식": ["증자방식","발행방식","배정방법","배정방식","사채발행방법","발행방법"],
    "발행상품": ["발행상품","신주의 종류","주식의 종류","증권종류","사채의 종류"],
    "신규발행주식수": ["신규발행주식수","발행주식수","발행할 주식의 수","신주수","증자할 주식수","전환에 따라 발행할 주식","교환에 따라 발행할 주식"],
    "확정발행가(원)": ["확정발행가","신주발행가액","발행가","발행가액","1주당 발행가액","전환가액","교환가액"],
    "기준주가": ["기준주가","기준주가액"],
    "확정발행금액(억원)": ["확정발행금액","모집총액","발행총액","발행금액","모집금액","조달금액","사채의 권면총액","권면총액"],
    "할인(할증률)": ["할인(할증률)","할인율","할증률"],
    "증자전 주식수": ["증자전 주식수","증자전 발행주식총수","발행주식총수","기발행주식총수"],
    "증자비율": ["증자비율","증자비율(%)","주식총수 대비 비율"],
    "청약일": ["청약일","청약기간","청약시작일","청약일자"],
    "납입일": ["납입일","대금납입일","납입기일","납입일자"],
    "주관사": ["주관사","대표주관회사","공동주관회사","인수회사","인수단"],
    "자금용도": ["자금용도","자금조달의 목적","자금사용 목적","자금조달 목적"],
    "투자자": ["투자자","제3자배정대상자","배정대상자","발행대상자","인수인","상대방"],
    "증자금액": ["증자금액","발행규모","조달금액","모집금액","총 조달금액"],
}

# =========================
# Core Utils
# =========================
def norm(s: str) -> str:
    return re.sub(r"\s+", "", str(s or "")).lower()

def is_valid_format(text: str, key_name: str) -> bool:
    val = (text or "").strip()
    if not val or val in ("-", "—", ".", "0", "해당사항 없음"): return False
    
    # 날짜 필드인데 연도가 없으면 무효
    if any(x in key_name for x in ["일", "기간"]):
        return bool(re.search(r"20[2-3]\d", val))
    
    # 금액/수량 필드인데 숫자가 없거나 날짜만 있으면 무효
    if any(x in key_name for x in ["가", "금액", "수", "규모"]):
        if re.search(r"20[2-3]\d[\-\.\/]\d", val): return False # 날짜 오탐지 방지
        return bool(re.search(r"\d", val))
        
    return True

def table_to_matrix(table_tag) -> list[list[str]]:
    rows = table_tag.find_all("tr")
    grid = {}
    max_r, max_c = 0, 0
    for r_idx, tr in enumerate(rows):
        c_idx = 0
        for td in tr.find_all(["th", "td"]):
            while grid.get((r_idx, c_idx)) is not None: c_idx += 1
            rowspan = int(td.get("rowspan", 1))
            colspan = int(td.get("colspan", 1))
            text = td.get_text(" ", strip=True)
            for i in range(rowspan):
                for j in range(colspan):
                    grid[(r_idx + i, c_idx + j)] = text
            c_idx += colspan
        max_r = max(max_r, r_idx)
    
    matrix = []
    for r in range(max_r + 1):
        c_max = max([c for (row, c) in grid.keys() if row == r] + [-1])
        matrix.append([grid.get((r, c), "") for c in range(c_max + 1)])
    return matrix

def extract_from_matrix(matrix: list[list[str]], key_name: str) -> str:
    aliases = [norm(a) for a in ALIASES.get(key_name, [])]
    for r_idx, row in enumerate(matrix):
        for c_idx, cell in enumerate(row):
            cell_txt = norm(cell)
            if any(a == cell_txt or (len(a) > 3 and a in cell_txt) for a in aliases):
                # 우측 탐색
                for j in range(c_idx + 1, len(row)):
                    if is_valid_format(row[j], key_name): return row[j].strip()
                # 아래 행 탐색
                if r_idx + 1 < len(matrix):
                    next_row = matrix[r_idx + 1]
                    for j in range(c_idx, min(c_idx + 2, len(next_row))):
                        if is_valid_format(next_row[j], key_name): return next_row[j].strip()
    return ""

# (나머지 fetch, connect_gs 등은 기존 코드와 유사하므로 생략하거나 핵심만 유지)
# [기존 Utils 함수들 포함되어 있다고 가정...]

def main():
    # 1. 시트 연결 및 헤더 확인
    try:
        raw_ws, ws_yusang, ws_jeonhwan, ws_gyohwan = connect_gs()
        ensure_headers(raw_ws, [ws_yusang, ws_jeonhwan, ws_gyohwan])
    except Exception as e:
        print(f"시트 연결 실패: {e}")
        return

    seen_list = load_json(SEEN_FILE, [])
    retry_queue = load_json(RETRY_FILE, [])
    session = requests.Session()
    feed = fetch_rss(session)

    # 2. 아이템 수집 및 필터링
    items_to_process = []
    for entry in feed.entries:
        title = entry.get("title", "")
        # 핵심 필터: '결정'이 들어간 공시만 타겟팅
        if not any(k in title for k in KEYWORDS):
            continue
        
        guid = entry.get("id") or entry.get("link")
        if guid in seen_list: continue
        items_to_process.append({"title": title, "link": entry.get("link"), "guid": guid, "pub": entry.get("published", "")})

    # 재시도 큐 합치기
    for r_item in retry_queue:
        if r_item["guid"] not in [x["guid"] for x in items_to_process]:
            items_to_process.append(r_item)

    print(f"[QUEUE] 처리 대상: {len(items_to_process)}건")

    new_retry = []
    for item in items_to_process:
        title, link, guid = item["title"], item["link"], item["guid"]
        print(f"\n[ITEM] {title}")

        try:
            # 3. KIND 뷰어 접속 및 HTML 추출
            # (기존 get_kind_contents_html_by_playwright 함수 사용)
            link_res = fetch(session, link)
            acptno = extract_acptno(link, link_res.text)
            if not acptno: raise Exception("acptNo 추출 실패")

            viewer_shell = build_viewer_url(acptno, None)
            vr_shell = fetch(session, viewer_shell)
            options = parse_docno_options(vr_shell.text)
            ranked = heuristic_docno_rank(options, clean_report_title(title))

            best_data = None
            for _, docno, doc_title in ranked[:3]: # 상위 3개 후보만 탐색
                viewer_doc = build_viewer_url(acptno, docno)
                html, label, khits, tcnt = get_kind_contents_html_by_playwright(viewer_doc)
                
                fields, tables_cnt, filled = parse_contents_html(html)
                if best_data is None or filled > best_data['filled']:
                    best_data = {'fields': fields, 'filled': filled, 'docno': docno, 'tcnt': tcnt}
                
                if filled >= SUCCESS_FILLED_MIN: break

            if not best_data or best_data['filled'] < 4:
                print(f"   -> [INCOMPLETE] 데이터 부족 (filled={best_data['filled'] if best_data else 0})")
                new_retry.append(item)
                continue

            # 4. 시트 기록
            fields = best_data['fields']
            status = "SUCCESS" if best_data['filled'] >= SUCCESS_FILLED_MIN else "INCOMPLETE"
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            rid = get_next_id(raw_ws)
            
            # (시트 기록 로직 - 기존과 동일)
            # ... (생략)
            
            if status == "SUCCESS":
                seen_list.append(guid)
            else:
                new_retry.append(item)
            
            print(f"   -> [{status}] filled={best_data['filled']} docno={best_data['docno']}")

        except Exception as e:
            print(f"   -> [오류] {e}")
            new_retry.append(item)
        
        time.sleep(SLEEP_SECONDS)

    save_json(SEEN_FILE, seen_list)
    save_json(RETRY_FILE, new_retry)
    print("\n✅ 모든 작업 완료!")
