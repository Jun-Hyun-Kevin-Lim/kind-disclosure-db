import requests
from bs4 import BeautifulSoup
import re

def test_kind_post_request():
    url = "https://kind.krx.co.kr/disclosure/details.do"

    # 1. 다윗형님 진단 1: 완벽한 사용자 위장 (Headers)
    # 봇이 아닌 진짜 크롬 브라우저에서 요청한 것처럼 서버를 속입니다.
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://kind.krx.co.kr/disclosure/details.do", # 내가 이 페이지에서 클릭했다는 증명
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Content-Type": "application/x-www-form-urlencoded" # 폼(Form) 데이터를 보낼 때 필수
    }

    # 2. 다윗형님 진단 2: 개발자 도구에서 긁어온 Payload (Form Data) 복제
    # 직접 화면에서 날짜를 고르고 "유상증자"를 친 다음 검색 버튼을 누른 것과 완전히 동일한 효과를 냅니다.
    payload = {
        "method": "searchDetailsMain",
        "fromDate": "2026-02-20",   # 시작일 (넉넉하게 세팅)
        "toDate": "2026-02-26",     # 종료일
        "reportNm": "유상증자",       # 검색어
        "currentPageSize": "100",   # 한 번에 100개씩 가져오기
        "pageIndex": "1",
        "searchMode": "",
        "searchCodeType": "",
        "searchCorpName": "",
        "repIsuSrtCd": "",
        "forward": "details_main"
    }

    print(f"▶️ 서버로 POST 요청 전송 중... (검색어: {payload['reportNm']})")
    
    # 3. POST 요청 날리기
    response = requests.post(url, headers=headers, data=payload)
    
    if response.status_code != 200:
        print(f"❌ 접속 실패: HTTP {response.status_code}")
        return

    # KIND 사이트 특유의 한글 깨짐 방지
    response.encoding = 'euc-kr' 
    soup = BeautifulSoup(response.text, 'html.parser')

    # 4. 결과 출력
    results = []
    for tr in soup.find_all('tr'):
        tds = tr.find_all('td')
        if len(tds) >= 4:
            time_str = tds[0].get_text(strip=True)
            company = tds[1].get_text(strip=True)
            title_a = tds[2].find('a')
            
            if title_a and 'openDisclsViewer' in title_a.get('onclick', ''):
                title = title_a.get_text(strip=True)
                acptno_match = re.search(r"openDisclsViewer\('(\d+)'", title_a['onclick'])
                if acptno_match:
                    acptno = acptno_match.group(1)
                    results.append(f"[{time_str}] {company} - {title} (접수번호: {acptno})")

    if results:
        print(f"\n✅ 성공! 총 {len(results)}건의 데이터를 긁어왔습니다:\n")
        for res in results[:10]: # 결과가 너무 길면 안 되니 10개만 출력
            print(res)
        if len(results) > 10:
            print("... (이하 생략) ...")
    else:
        print("\n❌ 접속은 성공(200 OK)했지만 데이터를 찾지 못했습니다.")
        print("HTML 일부 확인:", response.text[:500])

if __name__ == "__main__":
    test_kind_post_request()
