import os
import json
import requests
from bs4 import BeautifulSoup
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import re
from datetime import datetime, timedelta, timezone

# 1. 구글 시트 연결
SHEET_NAME = "KIND_대경"
KEYWORDS = ["유상증자", "전환사채", "신주인수권부사채", "교환사채"]

creds_json = os.environ.get('GOOGLE_CREDS')
creds_dict = json.loads(creds_json)
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
gc = gspread.authorize(credentials)

doc = gc.open(SHEET_NAME)

# 2. 데이터 정제 (할인율, 빈칸 완벽 처리)
def clean_dataframe(df):
    df = df.fillna('없음')
    df = df.astype(str)
    df = df.replace(to_replace=r'^\s*-\s*$', value='없음', regex=True)
    df = df.replace(to_replace=r'^\s*$', value='없음', regex=True)
    df = df.replace(['nan', 'NaN', 'None', 'null'], '없음')
    return df

# 3. 날짜 설정 (외국 서버 시간을 한국 시간으로 변경)
kst = timezone(timedelta(hours=9))
today = datetime.now(kst).strftime("%Y-%m-%d")

# ★★★ 테스트용: 아래 줄의 맨 앞 '#'을 지우면 2월 20일(로지스몬 있던 날) 데이터로 테스트할 수 있습니다! ★★★
today = "2026-02-20" 

print(f"📅 검색 기준일: {today}")

# 4. 해당 날짜의 공시 3000개 싹 다 가져오기
url = "https://kind.krx.co.kr/disclosure/todaydisclosure.do"
payload = {
    'method': 'searchTodayDisclosureSub',
    'currentPageSize': '3000', 
    'pageIndex': '1',
    'orderMode': '0',
    'orderStat': 'D',
    'forward': 'todaydisclosure_sub',
    'todayFlag': 'Y',
    'selDate': today
}
headers = {"User-Agent": "Mozilla/5.0"}

res = requests.post(url, data=payload, headers=headers)
soup = BeautifulSoup(res.text, 'html.parser')
result_rows = soup.select('tbody tr')

if not result_rows or "결과가 없습니다" in result_rows[0].text:
    print("해당 날짜에는 올라온 공시가 아예 없습니다.")
    exit()

print(f"✅ 총 {len(result_rows)}개의 공시를 가져와서 필터링을 시작합니다.\n")

# 5. 키워드별로 찾아서 표 긁어오고 구글 시트에 꽂기
for keyword in KEYWORDS:
    worksheet = doc.worksheet(keyword)
    all_sheet_data = []
    found_count = 0
    
    for row in result_rows:
        title_tag = row.select_one('a[onclick*="openDisclsViewer"]')
        if not title_tag: continue
            
        report_title = title_tag.text.strip()
        
        if keyword in report_title:
            company_tag = row.select_one('.first')
            company_name = company_tag.text.strip() if company_tag else "알수없음"
            
            onclick_text = title_tag.get('onclick', '')
            acptno_match = re.search(r"openDisclsViewer\('(\d+)'\)", onclick_text)
            
            if acptno_match:
                acptno = acptno_match.group(1)
                popup_url = f"https://kind.krx.co.kr/common/disclsviewer.do?method=search&acptno={acptno}"
                
                try:
                    tables = pd.read_html(requests.get(popup_url, headers=headers).text)
                    all_sheet_data.append([f"1. [{company_name}] {report_title} (acptNo: {acptno})"])
                    
                    for i, table in enumerate(tables):
                        clean_table = clean_dataframe(table)
                        col_count = len(clean_table.columns) if len(clean_table.columns) > 0 else 1
                        
                        all_sheet_data.append([f"tableIndex: {i}"] + [""] * (col_count - 1))
                        all_sheet_data.append(clean_table.columns.tolist())
                        all_sheet_data.extend(clean_table.values.tolist())
                        
                    all_sheet_data.append([""]) # 빈 줄
                    found_count += 1
                    print(f"  -> 득템! [{company_name}] {report_title} 긁어옴!")
                    
                except Exception as e:
                    print(f"  -> 에러: {company_name} 표 추출 실패 ({e})")

    if all_sheet_data:
        worksheet.clear()
        worksheet.update('A1', all_sheet_data)
        print(f"🚀 '{keyword}' 시트에 {found_count}개 꽂아 넣기 완료!\n")
    else:
        print(f"  -> '{keyword}' 공시는 없어서 패스!\n")

print("🎉 모든 작업 완료!")
