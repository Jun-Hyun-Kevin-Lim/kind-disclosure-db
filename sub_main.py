# ====== KIND Disclosure Bot (Sub-Main: Parameter Fix) ======
import os, json, time, re, io
from datetime import datetime, timedelta
import pandas as pd
import requests
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials

# 설정
LIST_URL = "https://kind.krx.co.kr/disclosure/todaydisclosure.do?method=searchTodayDisclosureSub"
KEYWORDS = ["유상증자결정", "전환사채", "교환사채"]
SHEET_NAME = "KIND_대경"
RAW_TAB = "RAW"
ISSUE_TAB = "ISSUE"
SEEN_FILE = "seen_sub.json"

TARGET_KEYS = {
    "회사명": ["회사명", "발행회사", "상호"],
    "상장시장": ["시장구분", "상장시장"],
    "최초 이사회결의일": ["이사회결의일", "사채발행결정일", "결의일"],
    "증자방식": ["증자방식", "발행방식"],
    "발행상품": ["증권종류", "사채의 종류", "발행상품"],
    "신규발행주식수": ["신규발행주식수", "발행주식수"],
    "확정발행가(원)": ["확정발행가", "발행가액", "전환가액", "교환가액", "발행가"],
    "확정발행금액(억원)": ["사채의 권면총액", "발행총액", "확정발행금액"],
    "납입일": ["납입일", "대금납입일"],
    "자금용도": ["자금조달의 목적", "자금용도"],
    "투자자": ["배정대상자", "발행대상자", "투자자", "대상자"],
    "증자금액": ["증자금액", "발행규모", "권면총액"]
}

def connect_gs():
    creds_dict = json.loads(os.environ["GOOGLE_CREDS"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ])
    client = gspread.authorize(creds)
    sh = client.open(SHEET_NAME)
    return sh.worksheet(RAW_TAB), sh.worksheet(ISSUE_TAB)

def get_disclosure_list():
    session = requests.Session()
    # GitHub Actions(UTC) 환경에서도 한국 날짜를 정확히 가져옴
    kst_now = datetime.utcnow() + timedelta(hours=9)
    today = kst_now.strftime("%Y-%m-%d")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://kind.krx.co.kr/disclosure/todaydisclosure.do",
    }
    
    # [핵심 수정] 파라미터를 브라우저와 똑같이 맞춤
    payload = {
        "method": "searchTodayDisclosureSub",
        "currentPageSize": "100",
        "pageIndex": "1",
        "orderIndex": "0",
        "orderMode": "0",
        "forward": "todaydisclosure_sub",
        "mktTpCd": "0",       # 0: 전체시장 (코스피, 코스닥, 코넥스 포함)
        "searchCodeType": "",
        "searchCorpName": "",
        "selDate": today
    }
    
    items = []
    try:
        print(f"📡 {today} 날짜로 KIND 서버에 요청 중...")
        res = session.post(LIST_URL, headers=headers, data=payload)
        soup = BeautifulSoup(res.text, 'html.parser')
        
        # 테이블 파싱 시작
        table = soup.find('table', {'class': 'list'})
        if not table:
            print("⚠️ 공시 테이블을 찾을 수 없습니다. (HTML 구조 확인 필요)")
            return items
            
        rows = table.find_all('tr')[1:] 
        for row in rows:
            cols = row.find_all('td')
            if len(cols) < 4: continue
            
            title_tag = cols[3].find('a')
            if title_tag:
                title_text = title_tag.text.strip()
                # 키워드 검사 (대소문자 및 공백 제거 후 비교)
                if any(k in title_text.replace(" ", "") for k in KEYWORDS):
                    onclick = title_tag.get('onclick', '')
                    acpt_no = re.search(r"openDisclsViewer\('(\d+)'", onclick)
                    if acpt_no:
                        items.append({
                            "title": title_text,
                            "corp": cols[2].text.strip(),
                            "time": cols[0].text.strip(),
                            "acptNo": acpt_no.group(1),
                            "link": f"https://kind.krx.co.kr/common/disclsviewer.do?method=searchContents&acptNo={acpt_no.group(1)}"
                        })
    except Exception as e:
        print(f"❌ 리스트 수집 에러: {e}")
    return items

def parse_details(acpt_no):
    bag = {}
    mobile_url = f"https://m.kind.krx.co.kr/disclosure/details.do?method=searchDetails&acptNo={acpt_no}"
    headers = {"User-Agent": "Mozilla/5.0 (Linux; Android 10; Mobile)"}
    try:
        res = requests.get(mobile_url, headers=headers, timeout=10)
        tables = pd.read_html(io.StringIO(res.text))
        for df in tables:
            df = df.fillna("").astype(str)
            if df.shape[1] >= 2:
                for _, row in df.iterrows():
                    bag[row[0].strip()] = row[1].strip()
    except: pass
    return bag

def main():
    raw_ws, issue_ws = connect_gs()
    # 테스트를 위해 seen_sub.json 로딩 생략하거나 초기화 권장
    seen_list = [] 
    
    items = get_disclosure_list()
    print(f"🎯 키워드 일치 공시: {len(items)}건 발견")
    
    for item in items:
        print(f"🏗️ 데이터 추출 중: [{item['corp']}] {item['title']}")
        bag = parse_details(item['acptNo'])
        
        # 데이터가 비어있으면 HTML 구조가 바뀐 것일 수 있음
        if not bag:
            print(f"⚠️ 상세 데이터를 가져오지 못했습니다: {item['acptNo']}")
            continue

        mapped = {target: next((bag[k] for a in aliases for k in bag.keys() if a in k.replace(" ", "")), "") 
                  for target, aliases in TARGET_KEYS.items()}
        
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            raw_id = len(raw_ws.get_col_values(1)) + 1
            raw_ws.append_row([raw_id, now, item['time'], item['title'], item['link'], item['acptNo'], "SUB_SUCCESS"])
            
            issue_row = [raw_id, now, item['time'], item['title'], item['link'], item['acptNo'], (1 if "[정정]" in item['title'] else 0)] + \
                        [mapped.get(k, "") for k in TARGET_KEYS.keys()]
            issue_ws.append_row(issue_row)
            print(f"✅ 저장 성공: {item['corp']}")
        except Exception as e: 
            print(f"❌ 시트 저장 실패: {e}")
        
        time.sleep(2)

    print("🏁 테스트 종료")

if __name__ == "__main__":
    main()
