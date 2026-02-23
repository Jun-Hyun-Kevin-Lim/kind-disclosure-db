# ====== KIND Disclosure Bot (Sub-Main: Session Persist Version) ======
import os, json, time, io
from datetime import datetime, timedelta
import pandas as pd
import requests
import gspread
from google.oauth2.service_account import Credentials

# 탭 이름이 실제 구글 시트와 일치하는지 꼭 확인하세요!
SHEET_NAME = "KIND_대경" 
RAW_TAB = "RAW" 
KEYWORDS = ["유상증자", "전환사채", "교환사채"]

def connect_gs():
    creds_dict = json.loads(os.environ["GOOGLE_CREDS"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ])
    client = gspread.authorize(creds)
    sh = client.open(SHEET_NAME)
    return sh.worksheet(RAW_TAB)

def get_today_data():
    # 세션 시작 (쿠키를 유지함)
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    })

    kst_now = datetime.utcnow() + timedelta(hours=9)
    today = kst_now.strftime("%Y-%m-%d")

    try:
        # 1단계: 검색 메인 페이지 접속 (세션 쿠키 획득)
        session.get("https://kind.krx.co.kr/disclosure/details.do?method=searchDetailsMain")

        # 2단계: 엑셀 다운로드 요청 (세션을 유지한 채로)
        excel_url = "https://kind.krx.co.kr/disclosure/details.do?method=downloadDisclosureListExcel"
        payload = {
            "forward": "details_com",
            "mktTpCd": "0",
            "fromDate": today,
            "toDate": today,
            "sortIndex": "2",
            "orderMode": "0",
            "currentPageSize": "100"
        }
        
        print(f"📡 {today} 데이터 세션 유지 요청 중...")
        res = session.post(excel_url, data=payload)
        
        # 3단계: 표 읽기
        dfs = pd.read_html(io.BytesIO(res.content), header=None)
        if not dfs: return None
        return dfs[0]

    except Exception as e:
        print(f"❌ 데이터 수집 중 에러: {e}")
        return None

def main():
    try:
        raw_ws = connect_gs()
    except Exception as e:
        print(f"❌ 구글 시트 연결 실패: {e} (시트 이름 '{SHEET_NAME}'을 확인하세요)")
        return

    df = get_today_data()
    if df is None or len(df) <= 1:
        print("📭 오늘자 공시가 없거나 서버가 데이터를 주지 않았습니다.")
        return

    print(f"📊 총 {len(df)}행을 정밀 검사합니다.")
    count = 0
    
    for i, row in df.iterrows():
        row_str = " ".join(row.fillna("").astype(str))
        
        if any(k in row_str.replace(" ", "") for k in KEYWORDS):
            count += 1
            # KIND 엑셀 표준 열 순서: [0]시간, [1]회사명, [2]보고서명
            pub_time = str(row[0]) if len(row) > 0 else ""
            corp = str(row[1]) if len(row) > 1 else ""
            title = str(row[2]) if len(row) > 2 else ""

            print(f"✅ 일치: [{corp}] {title}")
            
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            try:
                # 구글 시트에 기록
                raw_ws.append_row([len(raw_ws.get_col_values(1))+1, now, pub_time, title, "SESSION_MODE", corp, "SUCCESS"])
                time.sleep(1)
            except Exception as e:
                print(f"❌ 시트 저장 실패: {e}")

    print(f"🏁 {count}건 처리 완료")

if __name__ == "__main__":
    main()
