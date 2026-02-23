# ====== KIND Disclosure Bot (Advanced System) ======
import os, json, time, re, io
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
    "증자금액": ["증자금액", "발행규모"]
}

# --- 1. 상태 관리 (Seen & Retry Queue) ---
def load_json(filepath, default_val):
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except: pass
    return default_val

def save_json(filepath, data):
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# --- 2. Google Sheets 연결 ---
def connect_gs():
    creds_dict = json.loads(os.environ["GOOGLE_CREDS"])
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
    real_html_url = url
    excel_url = None
    try:
        res = requests.get(url, headers=headers, timeout=10)
        # 1. 문서 접수번호(acptNo)를 정규식으로 직접 추출 (가장 확실한 방법)
        match = re.search(r'acptNo"\s*value="(\d+)"', res.text)
        if not match:
            match = re.search(r'_TRK_PN\s*=\s*"(\d+)"', res.text)
            
        if match:
            acpt_no = match.group(1)
            # KIND의 실제 데이터가 담긴 표준 서식 전용 URL로 강제 생성
            real_html_url = f"https://kind.krx.co.kr/common/disclsviewer.do?method=searchContents&acptNo={acpt_no}&docNo=&p_pageIndex=1"
            # 2. 엑셀 다운로드 링크도 접수번호 기반으로 생성
            excel_url = f"https://kind.krx.co.kr/common/disclsviewer.do?method=downloadExcel&acptNo={acpt_no}"
    except Exception as e:
        print(f"-> https://repairit.wondershare.com/file-repair/fix-windows-cannot-complete-the-extraction.html {e}")
        
    return real_html_url, excel_url

# --- 4. HTML 파싱 (표 데이터 플래트닝) ---
def parse_html_tables(url):
    bag = {}
    try:
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        tables = pd.read_html(io.StringIO(res.text))
        for df in tables:
            df = df.fillna("").astype(str)
            for r in range(len(df)):
                for c in range(len(df.columns) - 1):
                    k = df.iloc[r, c].strip()
                    v = df.iloc[r, c+1].strip()
                    if k and v and len(k) < 30:
                        bag[k] = v
    except Exception as e:
        print(f"-> [HTML Parse Error] 표를 찾을 수 없거나 형식이 다릅니다.")
    return bag

# --- 5. Excel Fallback 파싱 ---
def parse_excel_fallback(excel_url):
    bag = {}
    try:
        print("-> [Fallback] 엑셀 데이터 다운로드 시도...")
        res = requests.get(excel_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        df = pd.read_excel(io.BytesIO(res.content))
        df = df.fillna("").astype(str)
        for r in range(len(df)):
            for c in range(len(df.columns) - 1):
                k = df.iloc[r, c].strip()
                v = df.iloc[r, c+1].strip()
                if k and v and len(k) < 30:
                    bag[k] = v
    except Exception as e:
        print(f"-> [Excel Parse Error] 엑셀 다운로드 실패")
    return bag

# --- 6. 타겟 키 매핑 및 완성도 체크 ---
def map_to_target(bag):
    out = {}
    for target, aliases in TARGET_KEYS.items():
        val = ""
        for a in aliases:
            matched_key = next((k for k in bag.keys() if a.replace(" ", "") in k.replace(" ", "")), None)
            if matched_key:
                val = bag[matched_key]
                break
        out[target] = val
    return out

def check_completeness(mapped_data):
    for field in REQUIRED_FIELDS:
        if not mapped_data.get(field):
            return False
    return True

# --- 메인 파이프라인 ---
def main():
    raw_ws, issue_ws = connect_gs()
    seen_list = load_json(SEEN_FILE, [])
    retry_queue = load_json(RETRY_FILE, [])
    
    feed = feedparser.parse(RSS_URL)
    items_to_process = []

    for entry in feed.entries:
        title = entry.get("title", "")
        link = entry.get("link", "")
        guid = entry.get("id") or link

        if guid in seen_list:
            continue
        if not any(k in title for k in KEYWORDS):
            continue
            
        items_to_process.append({"title": title, "link": link, "guid": guid, "pub": entry.get("published", "")})

    items_to_process.extend(retry_queue)
    new_retry_queue = []

    for item in items_to_process:
        title = item["title"]
        link = item["link"]
        guid = item["guid"]
        pub = item["pub"]
        
        print(f"Processing: {title}")
        is_correction = 1 if "[정정]" in title else 0

        real_url, excel_url = get_real_content_info(link)
        
        bag = parse_html_tables(real_url)
        mapped = map_to_target(bag)

        is_complete = check_completeness(mapped)
        
        if not is_complete and excel_url:
            fallback_bag = parse_excel_fallback(excel_url)
            fallback_mapped = map_to_target(fallback_bag)
            
            for k, v in fallback_mapped.items():
                if not mapped.get(k) and v:
                    mapped[k] = v
            is_complete = check_completeness(mapped)

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 필수 항목이 없더라도 일단 회사명이라도 추출되었으면 성공으로 간주하여 저장 시도
        if is_complete or mapped.get("회사명"):
            try:
                # RAW_TAB 저장
                raw_id = len(raw_ws.get_col_values(1))
                raw_ws.append_row([raw_id, now, pub, title, link, guid, "SUCCESS"])
                
                # ISSUE_TAB 저장
                issue_row = [raw_id, now, pub, title, link, guid, is_correction] + \
                            [mapped[k] for k in TARGET_KEYS.keys()]
                issue_ws.append_row(issue_row)
                
                if guid not in seen_list:
                    seen_list.append(guid)
                print("-> Success & Saved")
            except Exception as e:
                print(f"-> [Google Sheets Error] {e}")
                new_retry_queue.append(item)
        else:
            print("-> [Incomplete Data] 핵심 데이터 추출 실패. 재시도 큐로 이동.")
            if item not in new_retry_queue:
                new_retry_queue.append(item)
                
        time.sleep(2)

    save_json(SEEN_FILE, seen_list)
    save_json(RETRY_FILE, new_retry_queue)
    print("✅ 모든 작업 완료!")

if __name__ == "__main__":
    main()
