# ====== KIND Disclosure Bot (Sub-Main: Today Only & Column Fix) ======
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
    # 한국 시간 기준 오늘 날짜 설정 (GitHub Actions의 UTC 무관하게 작동)
    kst_now = datetime.utcnow() + timedelta(hours=9)
    today = kst_now.strftime("%Y-%m-%d")
    
    # 엑셀 다운로드 파라미터 (당일만 조회)
    payload = {
        "forward": "details_com",
        "mktTpCd": "0",        # 전체 시장
        "fromDate": today,     # 시작일: 오늘
        "toDate": today,       # 종료일: 오늘
        "reportNm": "",
        "isMainIsu": "",
        "sortIndex": "2",      # 시간순 정렬
        "orderMode": "0",
        "currentPageSize": "100"
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://kind.krx.co.kr/disclosure/details.do",
    }
    
    try:
        print(f"📡 {today} 당일 엑셀 데이터 요청 중...")
        res = requests.post(EXCEL_URL, data=payload, headers=headers)
        
        # KIND의 엑셀(HTML 형식)을 읽어옵니다.
        dfs = pd.read_html(io.BytesIO(res.content))
        if not dfs: return None
        
        df = dfs[0]
        
        # [에러 해결 핵심] 컬럼명에 숫자가 섞여있어도 안전하게 문자열로 변환
        # 'int' object has no attribute 'replace' 에러 방지
        df.columns = [str(c).replace(" ", "") for c in df.columns]
        
        return df
    except Exception as e:
        print(f"❌ 데이터 처리 중 에러 발생: {e}")
        return None

def main():
    raw_ws = connect_gs()
    df = get_today_disclosure_df()
    
    if df is None or df.empty:
        print("📭 오늘 조건에 맞는 공시가 없습니다.")
        return

    # 공시제목(또는 보고서명) 컬럼 확인
    col_name = "공시제목" if "공시제목" in df.columns else (df.columns[3] if len(df.columns) > 3 else "")
    
    if not col_name:
        print("⚠️ 공시 정보를 찾을 수 없는 표 형식입니다.")
        return

    # 키워드 필터링
    filtered_df = df[df[col_name].str.contains("|".join(KEYWORDS), na=False)]
    print(f"🎯 검색된 오늘 키워드 공시: {len(filtered_df)}건")
    
    for _, row in filtered_df.iterrows():
        corp = row.get('회사명', '알수없음')
        title = row.get(col_name, '제목없음')
        pub_time = row.get('시간', '00:00')
        
        print(f"✅ 수집 성공: [{corp}] {title}")
        
        # 구글 시트에 기록
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            raw_ws.append_row([
                len(raw_ws.get_col_values(1)) + 1, 
                now, 
                pub_time, 
                title, 
                "EXCEL_MODE", 
                corp, 
                "TODAY_SUCCESS"
            ])
            time.sleep(1) # 시트 과부하 방지
        except Exception as e:
            print(f"❌ 시트 저장 에러: {e}")

    print("🏁 [SUB-MAIN] 오늘자 데이터 수집 완료")

if __name__ == "__main__":
    main()
