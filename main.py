from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import pandas as pd
import time
import re

# 1. 찾고자 하는 보고서명 키워드
keywords = ["유상증자", "전환사채", "신주인수권부사채", "교환사채"]

# 파이썬이 조종할 크롬 브라우저 열기
print("브라우저를 실행합니다...")
driver = webdriver.Chrome()

# 모든 엑셀 데이터를 담을 바구니
all_excel_data = []

for keyword in keywords:
    print(f"\n[{keyword}] 보고서 검색 중...")
    
    # 2. KIND 공시 상세검색 페이지 접속
    driver.get("https://kind.krx.co.kr/disclosure/details.do")
    time.sleep(2) # 사이트 로딩 대기
    
    # 3. '보고서명' 입력칸을 찾아서 키워드 입력
    search_box = driver.find_element(By.ID, "reportNm")
    search_box.clear()
    search_box.send_keys(keyword)
    
    # 4. 검색 버튼 클릭
    # (KIND 사이트의 검색 버튼 위치 클릭)
    search_btn = driver.find_element(By.CSS_SELECTOR, ".btn-sprite.type-00.v-m")
    search_btn.click()
    time.sleep(3) # 검색 결과가 나올 때까지 대기
    
    # 5. 검색 결과 목록에서 데이터 빼오기
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    result_rows = soup.select('.info.type-00.t-line tbody tr')
    
    if not result_rows or "결과가 없습니다" in result_rows[0].text:
        print(f" -> 오늘 '{keyword}' 관련 공시가 없습니다.")
        continue
        
    for row in result_rows:
        # 회사명 찾기
        company_tag = row.select_one('.first')
        company_name = company_tag.text.strip() if company_tag else "알수없음"
        
        # 보고서명 및 숨겨진 팝업창 번호(acptno) 찾기
        title_tag = row.select_one('td:nth-child(4) a')
        if not title_tag:
            continue
            
        report_title = title_tag.text.strip()
        onclick_text = title_tag.get('onclick', '')
        
        # 정규식으로 '20260220001169' 같은 14자리 숫자만 쏙 빼오기
        acptno_match = re.search(r"openDisclsViewer\('(\d+)'\)", onclick_text)
        
        if acptno_match:
            acptno = acptno_match.group(1)
            print(f" -> [{company_name}] {report_title} 찾음! (접수번호: {acptno})")
            
            # 6. 찾아낸 번호로 팝업창 인터넷 주소(URL) 만들기
            popup_url = f"https://kind.krx.co.kr/common/disclsviewer.do?method=search&acptno={acptno}"
            
            try:
                # 팝업창 접속해서 표 싹 다 긁어오기
                tables = pd.read_html(popup_url, encoding='utf-8')
                
                # 엑셀 맨 위에 [회사명] 보고서명 적어주기
                header_df = pd.DataFrame([[f"1. [{company_name}] {report_title} (acptNo: {acptno})"]], columns=[0])
                all_excel_data.append(header_df)
                
                # 표에 tableIndex 달아주기
                for i, table in enumerate(tables):
                    
                    # 데이터 정확도 보정: 빈칸이나 '-' 기호는 '없음'으로 명확히 표기
                    table = table.fillna('없음')
                    table = table.replace('-', '없음')
                    table = table.replace(' - ', '없음')
                    
                    col_count = len(table.columns) if len(table.columns) > 0 else 1
                    index_row = pd.DataFrame([[f"tableIndex: {i}"] + [""] * (col_count - 1)], columns=table.columns)
                    
                    all_excel_data.append(index_row)
                    all_excel_data.append(table)
                    
            except Exception as e:
                print(f" -> {company_name} 표 데이터 추출 실패: {e}")

# 브라우저 닫기
driver.quit()

# 7. 하나의 엑셀 파일로 뭉쳐서 저장하기
if all_excel_data:
    final_df = pd.concat(all_excel_data, ignore_index=True)
    final_df.to_excel("today_reports_결과.xlsx", index=False, header=False)
    print("\n✅ 작업 완료! 'today_reports_결과.xlsx' 파일이 만들어졌습니다.")
else:
    print("\n❌ 긁어올 공시 데이터가 하나도 없습니다.")
