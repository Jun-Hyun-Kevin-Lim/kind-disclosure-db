import os
import json
import requests
from bs4 import BeautifulSoup
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import re
from datetime import datetime

# ==========================================
# 1. 기본 설정 및 구글 시트 연결
# ==========================================
SHEET_NAME = "KIND_대경"
KEYWORDS = ["유상증자", "전환사채", "신주인수권부사채", "교환사채"]

# GitHub Secrets에서 가져온 구글 인증 정보 로드
creds_json = os.environ.get('GOOGLE_CREDS')
if not creds_json:
    raise ValueError("GOOGLE_CREDS 환경 변수가 없습니다. GitHub Secrets를 확인하세요.")

creds_dict = json.loads(creds_json)
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
gc = gspread.authorize(credentials)

# 구글 시트 열기
try:
    doc = gc.open(SHEET_NAME)
    print(f"'{SHEET_NAME}' 시트에 성공적으로 연결되었습니다.")
except gspread.exceptions.SpreadsheetNotFound:
    raise Exception(f"'{SHEET_NAME}' 구글 시트를 찾을 수 없습니다. 서비스 계정 이메일이 시트에 공유되었는지 확인하세요.")

# ==========================================
# 2. 데이터 정제 함수 (오류 방지 및 '없음' 처리 핵심)
# ==========================================
def clean_dataframe(df):
    """
    데이터 값이 비어있거나 '-' 인 경우를 '없음'으로 명확하게 변경하여 데이터 정확도를 높입니다.
    (할인율, Call/Pull Option 등에서 데이터가 누락되는 문제 해결)
    """
    # 1. 모든 NaN(빈 값)을 '없음'으로 채움
    df = df.fillna('없음')
    
    # 2. 모든 데이터를 문자열로 변환 (정규식 처리를 위해)
    df = df.astype(str)
    
    # 3. '-' 기호나 공백만 있는 칸을 '없음'으로 교체
    df = df.replace(to_replace=r'^\s*-\s*$', value='없음', regex=True)
    df = df.replace(to_replace=r'^\s*$', value='없음', regex=True)
    
    return df

# ==========================================
# 3. KIND 검색 및 크롤링 메인 로직
# ==========================================
today = datetime.now().strftime("%Y-%m-%d")

for keyword in KEYWORDS:
    print(f"\n[{keyword}] 보고서 검색 및 데이터 수집 시작...")
    
    # 해당 키워드의 시트(탭) 가져오기 (없으면 생성)
    try:
        worksheet = doc.worksheet(keyword)
    except gspread.exceptions.WorksheetNotFound:
        worksheet = doc.add_worksheet(title=keyword, rows="1000", cols="20")
        
    # KIND 공시 상세검색 API (POST 요청)
    search_url = "https://kind.krx.co.kr/disclosure/details.do"
    payload = {
        'method': 'searchDetails',
        'reportNm': keyword,
        'fromDate': today, # 오늘 날짜 기준 (필요시 '2024-01-01' 등으로 수정)
        'toDate': today,
    }
    
    response = requests.post(search_url, data=payload)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    result_rows = soup.select('.info.type-00.t-line tbody tr')
    
    if not result_rows or "결과가 없습니다" in result_rows[0].text:
        print(f" -> 오늘 '{keyword}' 관련 공시가 없습니다.")
        # 공시가 없어도 기존 데이터를 덮어씌우지 않으려면 아래 pass 유지
        pass 
        continue

    all_sheet_data = []
    
    for row in result_rows:
        company_tag = row.select_one('.first')
        company_name = company_tag.text.strip() if company_tag else "알수없음"
        
        title_tag = row.select_one('td:nth-child(4) a')
        if not title_tag: continue
            
        report_title = title_tag.text.strip()
        onclick_text = title_tag.get('onclick', '')
        
        acptno_match = re.search(r"openDisclsViewer\('(\d+)'\)", onclick_text)
        if acptno_match:
            acptno = acptno_match.group(1)
            print(f" -> [{company_name}] 발견 (접수번호: {acptno})")
            
            # 팝업창 표 데이터 추출
            popup_url = f"https://kind.krx.co.kr/common/disclsviewer.do?method=search&acptno={acptno}"
            
            try:
                tables = pd.read_html(popup_url, encoding='utf-8')
                
                # 헤더 추가
                all_sheet_data.append([f"1. [{company_name}] {report_title} (acptNo: {acptno})"])
                
                for i, table in enumerate(tables):
                    # 데이터 정확도 보정 (clean_dataframe 적용)
                    clean_table = clean_dataframe(table)
                    
                    col_count = len(clean_table.columns) if len(clean_table.columns) > 0 else 1
                    
                    # tableIndex 행 추가
                    index_row = [f"tableIndex: {i}"] + [""] * (col_count - 1)
                    all_sheet_data.append(index_row)
                    
                    # 컬럼명(헤더) 추가
                    all_sheet_data.append(clean_table.columns.tolist())
                    # 실제 데이터 추가
                    all_sheet_data.extend(clean_table.values.tolist())
                    
                all_sheet_data.append([""]) # 공시와 공시 사이에 빈 줄 추가
                
            except Exception as e:
                print(f" -> {company_name} 표 추출 실패: {e}")

    # ==========================================
    # 4. 구글 시트 업데이트
    # ==========================================
    if all_sheet_data:
        worksheet.clear() # 기존 내용을 지우고 오늘의 데이터로 덮어쓰기
        worksheet.update('A1', all_sheet_data)
        print(f"✅ '{keyword}' 시트 업데이트 완료!")
        
print("\n🎉 모든 작업이 성공적으로 완료되었습니다!")
