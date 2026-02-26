import requests

# 1. 타겟 URL (KIND 상세검색 메인)
url = "https://kind.krx.co.kr/disclosure/details.do"

# 2. 서버에 보낼 검색 조건 (최근 일주일 데이터 요청)
payload = {
    "method": "searchDetailsMain",
    "fromDate": "2026-02-20", # 날짜는 넉넉하게
    "toDate": "2026-02-26",
    "currentPageSize": "100",
    "pageIndex": "1"
}

# 3. 최소한의 사용자 위장 (다윗형님이 말한 '헤더 쪽에 실제 사용자처럼 정보 넣기'의 기본)
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

print("▶️ [Step 1] 단순 requests로 KIND 서버 찌르기 테스트 중...\n")

# POST 요청 보내기
response = requests.post(url, data=payload, headers=headers)

# 결과 상태 코드 확인 (200이면 정상 접속)
print(f"✔️ 서버 응답 코드: {response.status_code}")

# KIND 서버의 고질적인 한글 깨짐 방지
response.encoding = 'euc-kr' 

# 받아온 HTML 텍스트 분석
html_text = response.text

print("\n--- 받아온 HTML 구조 일부 ---")
print(html_text[:500]) # 너무 기니까 앞부분만 출력
print("------------------------------\n")

# 동적 렌더링(SPA) 여부 판별
if "<table" in html_text or "<tbody>" in html_text:
    print("✅ [진단 결과] HTML 안에 표(Table) 데이터가 그대로 박혀 있습니다!")
    print("👉 결론: 리액트 같은 SPA가 아닙니다. 무거운 셀레니움 없이 빠르고 가벼운 requests만으로 다 뚫을 수 있습니다.")
else:
    print("❌ [진단 결과] 데이터가 안 보입니다.")
    print("👉 결론: 자바스크립트로 화면을 늦게 그리는 구조이거나 차단당했습니다. 다윗형님 말씀대로 '셀레니움'으로 넘어가야 합니다.")
