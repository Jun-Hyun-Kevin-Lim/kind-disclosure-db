# ====== KIND Disclosure Bot (Sub-Main: Excel Download Method) ======
import os, json, time, io
from datetime import datetime, timedelta
import pandas as pd
import requests
import gspread
from google.oauth2.service_account import Credentials

# 설정
# 상세검색 엑셀 다운로드 엔드포인트
EXCEL_URL = "https://kind.krx.co.kr/disclosure/details.do?method=downloadDisclosureListExcel"
KEYWORDS = ["유상증자", "전환사채", "교환사채"]
SHEET_NAME = "KIND_대경"
RAW_TAB = "RAW"
ISSUE_TAB = "ISSUE"

def connect_gs():
    creds_dict = json.loads(os.environ["GOOGLE_CREDS"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ])
    client = gspread.authorize(creds)
    sh = client.open(SHEET_NAME)
    return sh.worksheet(RAW_TAB), sh.worksheet(ISSUE_TAB)

def get_disclosure_df():
    # 한국 시간 기준 오늘/한달 전 날짜 설정
    kst_now = datetime.utcnow() + timedelta(hours=9)
    today = kst_now.strftime("%Y-%m-%d")
    from_date = (kst_now - timedelta(days=7)).strftime("%Y-%m-%d") # 최근 7일치
    
    # 엑셀 다운로드를 위한 상세 파라미터 (브라우저 엑셀 버튼 동작 복제)
    payload = {
        "forward": "details_com",
        "mktTpCd": "0",        # 전체시장
        "searchCodeType": "",
        "searchCorpName": "",
        "fromDate": from_date,
        "toDate": today,
        "reportNm": "",        # 보고서명 필터 (비워두면 전체)
        "isMainIsu": "1",      # 주요공시 여부
        "sortIndex": "2",      # 시간순 정렬
        "orderMode": "0",
        "currentPageSize": "100"
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://kind.krx.co.kr/disclosure/details.do",
    }
    
    try:
        print(f"📥 {from_date} ~ {today} 기간 엑셀 데이터 요청 중...")
        res = requests.post(EXCEL_URL, data=payload, headers=headers)
        
        # KIND의 엑셀은 사실 HTML table 형태이므로 read_html로 읽습니다.
        dfs = pd.read_html(io.BytesIO(res.content))
        if not dfs: return None
        
        df = dfs[0]
        # 컬럼명 정리 (KIND 엑셀 특유의 공백 제거)
        df.columns = [c.replace(" ", "") for c in df.columns]
        return df
    except Exception as e:
        print(f"❌ 엑셀 다운로드 실패: {e}")
        return None

def main():
    raw_ws, issue_ws = connect_gs()
    df = get_disclosure_df()
    
    if df is None or df.empty:
        print("📭 가져온 데이터가 없습니다.")
        return

    # 키워드 필터링 (공시제목 컬럼 기준)
    # 엑셀에서는 '보고서명'이나 '공시제목' 등으로 나옵니다.
    target_col = "공시제목" if "공시제목" in df.columns else df.columns[3]
    filtered_df = df[df[target_col].str.contains("|".join(KEYWORDS), na=False)]
    
    print(f"🎯 키워드 일치 공시: {len(filtered_df)}건 발견")
    
    for _, row in filtered_df.iterrows():
        corp = row['회사명'] if '회사명' in row else "알수없음"
        title = row[target_col]
        print(f"✅ 처리 중: [{corp}] {title}")
        
        # 엑셀 데이터에는 상세 내용 수치가 없으므로, 
        # 일단 리스트 정보를 시트에 기록하는 것으로 테스트합니다.
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            # 시트에 데이터 추가 (엑셀에서 제공하는 기본 정보만 우선 저장)
            raw_ws.append_row([len(raw_ws.get_col_values(1))+1, now, row.get('시간', ''), title, "EXCEL_LINK", "ID", "EXCEL_SUCCESS"])
            time.sleep(1)
        except Exception as e:
            print(f"❌ 시트 저장 에러: {e}")

    print("🏁 [EXCEL 방식] 테스트 종료")

if __name__ == "__main__":
    main()
