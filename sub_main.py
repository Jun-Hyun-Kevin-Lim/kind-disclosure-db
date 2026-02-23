# ====== KIND Disclosure Bot (Sub-Main: Robust Table Parsing) ======
import os, json, time, io
from datetime import datetime, timedelta
import pandas as pd
import requests
import gspread
from google.oauth2.service_account import Credentials

# 설정
EXCEL_URL = "https://kind.krx.co.kr/disclosure/details.do?method=downloadDisclosureListExcel"
KEYWORDS = ["유상증자", "전환사채", "교환사채"]
SHEET_NAME = "KIND_대경"
RAW_TAB = "RAW"

def connect_gs():
    creds_dict = json.loads(os.environ["GOOGLE_CREDS"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ])
    client = gspread.authorize(creds)
    sh = client.open(SHEET_NAME)
    return sh.worksheet(RAW_TAB)

def get_today_disclosure_df():
    kst_now = datetime.utcnow() + timedelta(hours=9)
    today = kst_now.strftime("%Y-%m-%d")
    
    payload = {
        "forward": "details_com",
        "mktTpCd": "0",
        "fromDate": today,
        "toDate": today,
        "reportNm": "",
        "isMainIsu": "",
        "sortIndex": "2",
        "orderMode": "0",
        "currentPageSize": "100"
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://kind.krx.co.kr/disclosure/details.do",
    }
    
    try:
        print(f"📡 {today} 데이터 요청 중...")
        res = requests.post(EXCEL_URL, data=payload, headers=headers)
        
        # [수정] header=None으로 가져와서 구조에 상관없이 데이터를 읽습니다.
        dfs = pd.read_html(io.BytesIO(res.content), header=None)
        if not dfs: return None
        
        return dfs[0]
    except Exception as e:
        print(f"❌ 요청 에러: {e}")
        return None

def main():
    raw_ws = connect_gs()
    df = get_today_disclosure_df()
    
    if df is None or df.empty:
        print("📭 데이터가 비어있습니다.")
        return

    print(f"📊 총 {len(df)}개의 행을 검사합니다.")
    
    count = 0
    for i, row in df.iterrows():
        # 행 전체를 하나의 문자열로 합침 (NaN 제외)
        row_str = " ".join(row.fillna("").astype(str))
        
        # 행 안에 키워드가 하나라도 들어있는지 확인
        if any(k in row_str for k in KEYWORDS):
            count += 1
            # 대략적인 데이터 위치 추정 (KIND 엑셀 기준: 1번 회사명, 2번 제목, 3번 시간)
            # 구조가 바뀌어도 에러 안 나게 안전하게 처리
            corp = str(row[1]) if len(row) > 1 else "알수없음"
            title = str(row[2]) if len(row) > 2 else "제목없음"
            pub_time = str(row[0]) if len(row) > 0 else "00:00"
            
            print(f"✅ 일치 발견: [{corp}] {title}")
            
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            try:
                raw_ws.append_row([
                    len(raw_ws.get_col_values(1)) + 1, 
                    now, 
                    pub_time, 
                    title, 
                    "ROBUST_MODE", 
                    corp, 
                    "SUCCESS"
                ])
                time.sleep(1)
            except Exception as e:
                print(f"❌ 저장 에러: {e}")

    if count == 0:
        print("🔎 키워드와 일치하는 행이 없습니다. (상세 필터링 확인 필요)")
    else:
        print(f"🏁 총 {count}건 처리 완료")

if __name__ == "__main__":
    main()
