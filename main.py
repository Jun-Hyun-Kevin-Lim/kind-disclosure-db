# ====== KIND Disclosure Bot (Advanced System) ======
import os, json, time, re
from datetime import datetime
import feedparser
import pandas as pd
import requests
import gspread
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials

RSS_URL = "http://kind.krx.co.kr:80/disclosure/rsstodaydistribute.do?method=searchRssTodayDistribute&repIsuSrtCd=&mktTpCd=0&searchCorpName=&currentPageSize=15"
KEYWORDS = ["유상증자", "전환사채", "교환사채"]

SHEET_NAME = "KIND_대경"
RAW_TAB = "RAW"
ISSUE_TAB = "ISSUE"

SEEN_FILE = "seen.json"
RETRY_FILE = "retry_queue.json"

REQUIRED_FIELDS = ["회사명", "확정발행가(원)", "증자금액"] # 완성도 체크 기준

TARGET_KEYS = {
    "회사명": ["회사명", "발행회사", "상호", "명칭"],
    "상장시장": ["상장시장", "시장구분", "시장"],
    "최초 이사회결의일": ["이사회결의일", "결의일"],
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
    "투자자": ["투자자", "배정대상자", "발행대상자"],
    "증자금액": ["증자금액", "발행규모"]
}

# --- 1. 상태 관리 (Seen & Retry Queue) ---
def load_json(filepath, default_val):
    if os.path.exists(filepath):
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    return default_val

def save_json(filepath, data):
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# --- 2. Google Sheets 연결 ---
def connect_gs():
    creds_dict = json.loads(os.environ["GOOGLE_CREDS"])
    # 아래 scopes 부분에 drive 권한을 추가합니다.
    creds = Credentials.from_service_account_info(
        creds_dict, 
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
    )
    client = gspread.authorize(creds)
    sh = client.open(SHEET_NAME)
    return sh.worksheet(RAW_TAB), sh.worksheet(ISSUE_TAB)

# --- 3. KIND 본문 URL 및 엑셀 다운로드 링크 추출 (핵심) ---
def get_real_content_info(url):
    headers = {"User-Agent": "Mozilla/5.0"}
    res = requests.get(url, headers=headers)
    soup = BeautifulSoup(res.text, 'html.parser')
    
    real_html_url = None
    excel_url = None
    
    # 1. iframe 내부 실제 공시문서 URL 찾기
    iframe = soup.find('iframe')
    if iframe and iframe.get('src'):
        src = iframe.get('src')
        real_html_url = "https://kind.krx.co.kr" + src if src.startswith('/') else src

    # 2. 첨부된 엑셀 다운로드 링크 찾기 (Excel Fallback용)
    excel_btn = soup.find('a', href=re.compile(r'downloadExcel'))
    if excel_btn:
        excel_url = "https://kind.krx.co.kr" + excel_btn.get('href')

    return real_html_url or url, excel_url

# --- 4. HTML 파싱 (표 데이터 플래트닝) ---
def parse_html_tables(url):
    bag = {}
    try:
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        # HTML 내의 모든 표 추출
        tables = pd.read_html(res.text)
        for df in tables:
            df = df.fillna("").astype(str)
            # 표의 모든 셀을 순회하며 키워드가 있는 셀의 오른쪽 셀 값을 저장
            for r in range(len(df)):
                for c in range(len(df.columns) - 1):
                    k = df.iloc[r, c].strip()
                    v = df.iloc[r, c+1].strip()
                    if k and v and len(k) < 30: # 너무 긴 문장은 키워드가 아님
                        bag[k] = v
    except Exception as e:
        print(f"[HTML Parse Error] {e}")
    return bag

# --- 5. Excel Fallback 파싱 ---
def parse_excel_fallback(excel_url):
    bag = {}
    try:
        print("-> [Fallback] 엑셀 데이터 다운로드 시도...")
        res = requests.get(excel_url, headers={"User-Agent": "Mozilla/5.0"})
        # 엑셀 바이너리를 판다스로 읽기
        df = pd.read_excel(res.content)
        df = df.fillna("").astype(str)
        for r in range(len(df)):
            for c in range(len(df.columns) - 1):
                k = df.iloc[r, c].strip()
                v = df.iloc[r, c+1].strip()
                if k and v and len(k) < 30:
                    bag[k] = v
    except Exception as e:
        print(f"[Excel Parse Error] {e}")
    return bag

# --- 6. 타겟 키 매핑 및 완성도 체크 ---
def map_to_target(bag):
    out = {}
    for target, aliases in TARGET_KEYS.items():
        val = ""
        for a in aliases:
            # 부분 일치 또는 정확한 일치 검색
            matched_key = next((k for k in bag.keys() if a.replace(" ", "") in k.replace(" ", "")), None)
            if matched_key:
                val = bag[matched_key]
                break
        out[target] = val
    return out

def check_completeness(mapped_data):
    # 필수 필드가 비어있지 않은지 확인
    for field in REQUIRED_FIELDS:
        if not mapped_data.get(field):
            return False
    return True

# --- 메인 파이프라인 ---
def main():
    raw_ws, issue_ws = connect_gs()
    seen_list = load_json(SEEN_FILE, [])
    retry_queue = load_json(RETRY_FILE, [])
    
    # 1. RSS 수집
    feed = feedparser.parse(RSS_URL)
    items_to_process = []

    # 2. 필터링 및 대기열 추가
    for entry in feed.entries:
        title = entry.get("title", "")
        link = entry.get("link", "")
        guid = entry.get("id") or link

        if guid in seen_list:
            continue
        if not any(k in title for k in KEYWORDS):
            continue
            
        items_to_process.append({"title": title, "link": link, "guid": guid, "pub": entry.get("published", "")})

    # Retry Queue에 있던 항목 합치기
    items_to_process.extend(retry_queue)
    new_retry_queue = []

    for item in items_to_process:
        title = item["title"]
        link = item["link"]
        guid = item["guid"]
        pub = item["pub"]
        
        print(f"Processing: {title}")

        # [정정공시 버전 관리]
        is_correction = 1 if "[정정]" in title else 0

        # 데이터 추출 파이프라인
        real_url, excel_url = get_real_content_info(link)
        
        # 기본 HTML 파싱
        bag = parse_html_tables(real_url)
        mapped = map_to_target(bag)

        # 완성도 체크 및 Excel Fallback
        is_complete = check_completeness(mapped)
        if not is_complete and excel_url:
            fallback_bag = parse_excel_fallback(excel_url)
            fallback_mapped = map_to_target(fallback_bag)
            
            # 기존에 비어있던 데이터만 엑셀 데이터로 채우기
            for k, v in fallback_mapped.items():
                if not mapped.get(k) and v:
                    mapped[k] = v
            is_complete = check_completeness(mapped)

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if is_complete:
            # 성공 처리: 구글 시트 저장
            try:
                raw_id = len(raw_ws.get_col_values(1)) # 1번 컬럼(ID) 기준 행 수 계산
                raw_ws.append_row([raw_id, now, pub, title, link, guid, "SUCCESS"])
                
                issue_row = [raw_id, now, pub, title, link, guid, is_correction] + \
                            [mapped[k] for k in TARGET_KEYS.keys()]
                issue_ws.append_row(issue_row)
                
                if guid not in seen_list:
                    seen_list.append(guid)
                print("-> Success & Saved")
            except Exception as e:
                print(f"-> [Google Sheets Error] {e}")
                new_retry_queue.append(item) # 시트 에러 시 재시도 큐로
        else:
            # 실패 처리: 완성도 미달 시 재시도 큐로 이동
            print("-> [Incomplete Data] Missing required fields. Added to Retry Queue.")
            if item not in new_retry_queue:
                new_retry_queue.append(item)
                
        time.sleep(2) # 서버 부하 방지 및 IP 차단 방지 (필수)

    # 상태 업데이트
    save_json(SEEN_FILE, seen_list)
    save_json(RETRY_FILE, new_retry_queue)

if __name__ == "__main__":
    main()
