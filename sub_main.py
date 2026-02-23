# ====== KIND Disclosure Bot (Sub-Main: POST Method Test) ======
import os, json, time, re, io
from datetime import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials

# 설정 (기존과 동일)
LIST_URL = "https://kind.krx.co.kr/disclosure/todaydisclosure.do?method=searchTodayDisclosureSub"
KEYWORDS = ["유상증자", "전환사채", "교환사채"]
SHEET_NAME = "KIND_대경"
RAW_TAB = "RAW"
ISSUE_TAB = "ISSUE"
SEEN_FILE = "seen_sub.json" # 메인과 겹치지 않게 별도 관리

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

def load_json(filepath, default_val):
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as f: return json.load(f)
        except: pass
    return default_val

def save_json(filepath, data):
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

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
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://kind.krx.co.kr/disclosure/todaydisclosure.do",
    }
    today = datetime.now().strftime("%Y-%m-%d")
    payload = {
        "method": "searchTodayDisclosureSub",
        "currentPageSize": "100",
        "pageIndex": "1",
        "orderIndex": "0",
        "orderMode": "0",
        "forward": "todaydisclosure_sub",
        "selDate": today
    }
    
    items = []
    try:
        res = session.post(LIST_URL, headers=headers, data=payload)
        soup = BeautifulSoup(res.text, 'html.parser')
        rows = soup.find_all('tr')[1:] 
        for row in rows:
            cols = row.find_all('td')
            if len(cols) < 4: continue
            title_tag = cols[3].find('a')
            if title_tag and any(k in title_tag.text for k in KEYWORDS):
                onclick = title_tag.get('onclick', '')
                acpt_no = re.search(r"openDisclsViewer\('(\d+)'", onclick)
                if acpt_no:
                    items.append({
                        "title": title_tag.text.strip(),
                        "corp": cols[2].text.strip(),
                        "time": cols[0].text.strip(),
                        "acptNo": acpt_no.group(1),
                        "link": f"https://kind.krx.co.kr/common/disclsviewer.do?method=searchContents&acptNo={acpt_no.group(1)}"
                    })
    except Exception as e: print(f"-> [List Error] {e}")
    return items

def parse_details(acpt_no):
    bag = {}
    # 모바일 뷰어가 파싱 성공률이 가장 높음
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
    seen_list = load_json(SEEN_FILE, [])
    
    print(f"🔍 [SUB-MAIN] {datetime.now().strftime('%Y-%m-%d')} 공시 탐색 시작...")
    items = get_disclosure_list()
    print(f"🎯 키워드 일치 공시: {len(items)}건")
    
    for item in items:
        if item['acptNo'] in seen_list:
            print(f"⏩ 건너뜀 (이미 처리됨): {item['corp']}")
            continue
        
        print(f"🏗️ 추출 중: [{item['corp']}] {item['title']}")
        bag = parse_details(item['acptNo'])
        
        mapped = {target: next((bag[k] for a in aliases for k in bag.keys() if a in k.replace(" ", "")), "") 
                  for target, aliases in TARGET_KEYS.items()}
        
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if mapped.get("회사명") or item['corp']:
            try:
                raw_id = len(raw_ws.get_col_values(1)) + 1
                raw_ws.append_row([raw_id, now, item['time'], item['title'], item['link'], item['acptNo'], "SUB_SUCCESS"])
                
                issue_row = [raw_id, now, item['time'], item['title'], item['link'], item['acptNo'], (1 if "[정정]" in item['title'] else 0)] + \
                            [mapped.get(k, "") for k in TARGET_KEYS.keys()]
                issue_ws.append_row(issue_row)
                
                seen_list.append(item['acptNo'])
                print(f"✅ 저장 완료: {item['corp']}")
            except Exception as e: print(f"❌ 시트 저장 에러: {e}")
        
        time.sleep(2)

    save_json(SEEN_FILE, seen_list)
    print("🏁 [SUB-MAIN] 테스트 종료")

if __name__ == "__main__":
    main()
