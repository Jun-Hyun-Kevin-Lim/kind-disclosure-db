import os, json, time, re
from datetime import datetime
from urllib.parse import urlparse, parse_qs, urlencode
from io import StringIO

import feedparser
import requests
import gspread
import pandas as pd
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ==========================================
# 1. 정밀 추출용 정규식 엔진 (Strict Regex)
# ==========================================
class Extractor:
    @staticmethod
    def date(text):
        text = str(text).strip()
        if len(text) > 50: return ""
        # 2026.02.24, 2026-02-24, 2026년 2월 24일 모두 대응
        m = re.search(r"(20[2-3]\d)\s*[\-\.\/년]\s*(\d{1,2})\s*[\-\.\/월]\s*(\d{1,2})", text)
        return f"{m.group(1)}-{m.group(2).zfill(2)}-{m.group(3).zfill(2)}" if m else ""

    @staticmethod
    def money_to_eok(text):
        text = str(text).replace(",", "").strip()
        if "해당사항" in text or len(text) > 100: return ""
        # 숫자만 추출 (소수점 포함)
        m = re.search(r"(\d+(?:\.\d+)?)", text)
        if m:
            val = float(m.group(1))
            if "백만원" in text: val /= 100.0
            elif "억원" in text or "억" in text: pass
            elif val >= 10000000: val /= 100000000.0 # 단위가 원일 때
            return str(round(val, 2))
        return ""

    @staticmethod
    def number(text):
        text = str(text).replace(",", "").strip()
        m = re.search(r"(\d+)", text)
        return m.group(1) if m else ""

    @staticmethod
    def ratio(text):
        m = re.search(r"(\d+(?:\.\d+)?)\s*%", text)
        return m.group(1) if m else ""

# ==========================================
# 2. 메인 파싱 엔진 (Pandas + Anchor Search)
# ==========================================
def parse_kind_report(html_content):
    fields = {k: "" for k in [
        "최초 이사회결의일", "증자방식", "발행상품", "신규발행주식수", 
        "확정발행가(원)", "확정발행금액(억원)", "납입일", "투자자", "자금용도"
    ]}
    
    if not html_content: return fields
    
    # 1. HTML 전처리 (표 안의 줄바꿈 해결)
    html_content = html_content.replace("<br>", " ").replace("<br/>", " ")
    try:
        dfs = pd.read_html(StringIO(html_content))
    except:
        return fields

    # 2. 항목별 타겟 매칭 (목차 번호 포함하여 정밀 검색)
    def find_val(keywords, func, is_money=False):
        regex = "|".join(keywords)
        for df in dfs:
            df = df.astype(str).replace('nan', '')
            for r in range(len(df)):
                for c in range(len(df.columns)):
                    cell = df.iloc[r, c].replace(" ", "")
                    if re.search(regex, cell):
                        # 1순위: 우측 칸 탐색
                        for next_c in range(c + 1, len(df.columns)):
                            cand = df.iloc[r, next_c]
                            res = func(cand, to_eok=True) if is_money else func(cand)
                            if res: return res
                        # 2순위: 아래 칸 탐색 (병합 대비)
                        if r + 1 < len(df):
                            cand = df.iloc[r+1, c]
                            res = func(cand, to_eok=True) if is_money else func(cand)
                            if res: return res
        return ""

    # 3. 핀셋 추출 시작
    fields["최초 이사회결의일"] = find_val(["이사회결의일", "결정일"], Extractor.date)
    fields["납입일"] = find_val(["납입일", "대금납입일"], Extractor.date)
    fields["신규발행주식수"] = find_val(["신주의종류와수", "신규발행주식수", "발행할주식의수"], Extractor.number)
    fields["확정발행가(원)"] = find_val(["발행가액", "전환가액", "교환가격"], Extractor.number)
    fields["확정발행금액(억원)"] = find_val(["확정발행금액", "모집총액", "사채의권면총액"], Extractor.money_to_eok, is_money=True)
    fields["증자방식"] = find_val(["증자방식", "발행방식", "사채발행방법"], lambda x: str(x)[:50])
    fields["투자자"] = find_val(["제3자배정대상자", "배정대상자", "대상자"], lambda x: str(x)[:100])
    
    return fields

# ==========================================
# 3. 데이터 추출기 (정밀 필터링)
# ==========================================
def extract_date(text):
    if len(text) > 80: return ""
    m = re.search(r"(20[1-3]\d)\s*[\-\.\/년]\s*(\d{1,2})\s*[\-\.\/월]\s*(\d{1,2})", text)
    if m: return f"{m.group(1)}-{m.group(2).zfill(2)}-{m.group(3).zfill(2)}"
    return ""

def extract_price(text):
    if len(text) > 150 or "해당사항" in text.replace(" ", ""): return ""
    clean_t = re.sub(r"\([^)]*\)", "", text) 
    matches = re.findall(r"(\d{1,3}(?:,\d{3})+|\d{4,})\s*원?", clean_t)
    for num_str in reversed(matches): 
        val = int(num_str.replace(",", ""))
        if val not in [100, 200, 500, 1000, 2500, 5000]: # 액면가 배제
            return str(val)
    return ""

def extract_number(text, to_eok=False):
    if len(text) > 200 or "해당사항" in text.replace(" ", ""): return ""
    clean_t = re.sub(r"\([^)]*\)", "", text)
    matches = re.findall(r"(\d{1,3}(?:,\d{3})*(?:\.\d+)?)", clean_t)
    for num_str in matches:
        val = float(num_str.replace(",", ""))
        if val == 0: continue
        if to_eok:
            if "백만원" in text: val = val / 100.0
            elif val >= 10000000: val = val / 100000000.0
        return str(int(val)) if val.is_integer() else str(round(val, 2))
    return ""

def extract_ratio(text):
    if len(text) > 100: return ""
    m = re.search(r"(\d+(?:\.\d+)?)\s*%", text)
    if m: return m.group(1)
    return ""

def extract_text(text, max_len=60):
    clean_t = re.sub(r"\s+", " ", str(text)).strip()
    if not clean_t or clean_t in ["-", ".", "해당사항 없음", "해당사항없음", "nan"]: return ""
    if len(clean_t) <= max_len: return clean_t
    return ""

# ==========================================
# 4. ★ Pandas 엑셀 변환 파싱 핵심 엔진 ★
# ==========================================
def search_in_dfs(dfs, keywords, ext_func, **kwargs):
    """모든 데이터프레임(표)을 순회하며 키워드를 찾고 우측/하단 값을 추출합니다."""
    kw_norm = [re.sub(r"[\d\.\-\s\(\)\[\]]", "", k).lower() for k in keywords]
    
    for df in dfs:
        # 데이터프레임의 행(row)과 열(col)을 순회
        for r_idx in range(len(df)):
            for c_idx in range(len(df.columns)):
                cell_val = str(df.iloc[r_idx, c_idx])
                if cell_val == 'nan' or not cell_val.strip(): continue
                
                cell_norm = re.sub(r"[\d\.\-\s\(\)\[\]]", "", cell_val).lower()
                
                if any(k in cell_norm for k in kw_norm):
                    # 1. 같은 행의 우측 칸 탐색
                    for next_c in range(c_idx + 1, len(df.columns)):
                        val_str = str(df.iloc[r_idx, next_c])
                        if val_str == 'nan': continue
                        res = ext_func(val_str, **kwargs) if kwargs else ext_func(val_str)
                        if res: return res
                    
                    # 2. 아래쪽 행 탐색 (병합으로 인해 밀린 경우)
                    if r_idx + 1 < len(df):
                        val_str = str(df.iloc[r_idx + 1, c_idx])
                        if val_str != 'nan':
                            res = ext_func(val_str, **kwargs) if kwargs else ext_func(val_str)
                            if res: return res
                            
                        # 대각선 우측 하단 탐색
                        if c_idx + 1 < len(df.columns):
                            val_str = str(df.iloc[r_idx + 1, c_idx + 1])
                            if val_str != 'nan':
                                res = ext_func(val_str, **kwargs) if kwargs else ext_func(val_str)
                                if res: return res
    return ""

def parse_with_pandas(html_str):
    fields = {k: "" for k in ISSUE_FIELDS}
    if not html_str: return fields, 0, 0

    # HTML 태그 정리 (br 태그를 띄어쓰기로 변경하여 글자 붙음 방지)
    html_str = html_str.replace("<br>", " ").replace("<br/>", " ")
    
    try:
        # 💡 여기가 핵심: HTML 표를 완벽하게 엑셀(Dataframe) 리스트로 변환
        dfs = pd.read_html(StringIO(html_str))
    except ValueError:
        return fields, 0, 0 # 표가 없는 경우

    t_cnt = len(dfs)

    # 각 필드별로 데이터프레임을 뒤져서 값을 찾아냄
    fields["최초 이사회결의일"] = search_in_dfs(dfs, ["최초이사회결의일", "이사회결의일", "결정일"], extract_date)
    fields["청약일"] = search_in_dfs(dfs, ["청약일", "청약기간", "청약기일"], extract_date)
    fields["납입일"] = search_in_dfs(dfs, ["납입일", "대금납입일", "납입기일"], extract_date)
    
    fields["신규발행주식수"] = search_in_dfs(dfs, ["신규발행주식수", "발행할주식의수", "신주의종류와수", "전환에따라발행할주식", "교환에따라발행할주식"], extract_number)
    fields["증자전 주식수"] = search_in_dfs(dfs, ["증자전발행주식총수", "기발행주식총수"], extract_number)
    
    fields["확정발행가(원)"] = search_in_dfs(dfs, ["1주당확정발행가액", "신주발행가액", "확정발행가", "전환가액", "교환가액", "교환가격"], extract_price)
    fields["기준주가"] = search_in_dfs(dfs, ["기준주가"], extract_price)
    fields["확정발행금액(억원)"] = search_in_dfs(dfs, ["확정발행금액", "모집총액", "사채의권면총액", "권면총액"], extract_number, to_eok=True)
    
    fields["할인(할증률)"] = search_in_dfs(dfs, ["할인율", "할증률", "할인할증률"], extract_ratio)
    fields["증자비율"] = search_in_dfs(dfs, ["증자비율"], extract_ratio)
    
    fields["증자방식"] = search_in_dfs(dfs, ["증자방식", "발행방식", "사채발행방법", "배정방법"], extract_text, max_len=40)
    fields["발행상품"] = search_in_dfs(dfs, ["발행상품", "사채의종류", "신주의종류", "증권종류"], extract_text, max_len=40)
    fields["주관사"] = search_in_dfs(dfs, ["주관사", "대표주관회사", "인수회사"], extract_text, max_len=40)
    fields["투자자"] = search_in_dfs(dfs, ["투자자", "제3자배정대상자", "배정대상자", "사채발행대상자"], extract_text, max_len=80)
    fields["자금용도"] = search_in_dfs(dfs, ["자금용도", "자금조달의목적", "자금사용목적"], extract_text, max_len=80)

    if fields["확정발행금액(억원)"]: fields["증자금액"] = fields["확정발행금액(억원)"]

    filled = sum(1 for v in fields.values() if str(v).strip())
    return fields, t_cnt, filled

# ==========================================
# 5. Playwright & Main 로직
# ==========================================
def extract_acptno(link, html_text):
    qs = parse_qs(urlparse(link).query)
    acpt = (qs.get("acptno") or qs.get("acptNo") or [None])[0]
    if acpt: return acpt
    m = re.search(r'acptNo"\s*value="(\d+)"', html_text)
    return m.group(1) if m else None

def get_best_html(viewer_url):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_default_navigation_timeout(PW_NAV_TIMEOUT_MS)
        try: page.goto(viewer_url, wait_until="networkidle")
        except: pass
        page.wait_for_timeout(PW_WAIT_MS)

        best_html, best_score = "", -1
        for fr in page.frames:
            try:
                html = fr.content()
                t_cnt = html.lower().count("<table")
                text_norm = re.sub(r"\s+", "", BeautifulSoup(html, "lxml").get_text(" ", strip=True)).lower()
                k_hits = sum(1 for key in ["발행가", "주식수", "이사회결의일", "납입일", "권면총액"] if key in text_norm)
                score = t_cnt * 2 + k_hits * 10
                if score > best_score:
                    best_score, best_html = score, html
            except: continue
        browser.close()
        return best_html

def main():
    raw_ws, ws_yusang, ws_jeonhwan, ws_gyohwan = connect_gs()
    ensure_headers(raw_ws, [ws_yusang, ws_jeonhwan, ws_gyohwan])

    seen_list = load_json(SEEN_FILE, [])
    retry_queue = load_json(RETRY_FILE, [])

    session = requests.Session()
    feed = fetch(session, RSS_URL, referer=f"{BASE}/")
    parsed_feed = feedparser.parse(feed.content)

    items = {it["guid"]: it for it in retry_queue}
    for entry in parsed_feed.entries:
        link = entry.get("link", "")
        guid = entry.get("id") or link
        title = entry.get("title", "")
        if guid and any(k in title for k in KEYWORDS) and guid not in seen_list:
            items[guid] = {"title": title, "link": link, "guid": guid, "pub": entry.get("published", "")}

    items = list(items.values())
    print(f"[QUEUE] 처리대상={len(items)} 완료={len(seen_list)}")
    if not items: return print("✅ 업데이트할 새 공시가 없습니다.")

    new_retry = []

    for item in items:
        title, link, guid, pub = item["title"], item["link"], item["guid"], item["pub"]
        print(f"\n[ITEM] {title}")

        m = re.match(r"^\[([^\]]+)\]\s*([^\s]+)", title.strip())
        market, company = (m.group(1), m.group(2)) if m else ("", "")

        link_res = fetch(session, link, referer=f"{BASE}/")
        acptno = extract_acptno(link, link_res.text)
        if not acptno:
            new_retry.append(item)
            continue

        viewer_url = f"{BASE}/common/disclsviewer.do?method=search&acptno={acptno}&viewerhost="
        vr_shell = fetch(session, viewer_url, referer=link)
        
        options = []
        for opt in BeautifulSoup(vr_shell.text, "lxml").find_all("option"):
            v = opt.get("value", "")
            if re.match(r"^(\d{10,14})\|", v): options.append(v.split("|")[0])

        best_cand = None
        for docno in options[:5]:
            doc_url = f"{viewer_url}&docno={docno}"
            html = get_best_html(doc_url)
            if "<title>창 닫기</title>" in html: continue
            
            # 여기서 Pandas 엔진을 호출합니다.
            fields, tables_cnt, filled = parse_with_pandas(html)
            cand = (filled, tables_cnt, docno, fields)
            if not best_cand or cand[0] > best_cand[0]: best_cand = cand
            if filled >= SUCCESS_FILLED_MIN: break

        if not best_cand:
            print("   [FAIL] 유효한 데이터를 찾지 못했습니다.")
            new_retry.append(item)
            continue

        filled, tables_cnt, docno, fields = best_cand
        status = "SUCCESS" if filled >= SUCCESS_FILLED_MIN else "INCOMPLETE"
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        version = f"{acptno}-{docno}"

        try:
            rid = get_next_id(raw_ws)
            raw_ws.append_row([rid, now, pub, title, link, guid, status, acptno, docno, filled, tables_cnt, version])

            target_ws = ws_jeonhwan if "전환사채" in title else ws_gyohwan if "교환사채" in title else ws_yusang
            iss_row = [rid, now, pub, company, market, title, link, guid] + [fields.get(k, "") for k in ISSUE_FIELDS] + [version, status, filled, tables_cnt, acptno, docno]
            target_ws.append_row(iss_row)

            if status == "SUCCESS":
                seen_list.append(guid)
                print(f"   -> [SUCCESS] 시트명:{target_ws.title} | 채움:{filled}/18")
            else:
                new_retry.append(item)
                print(f"   -> [INCOMPLETE] 시트명:{target_ws.title} | 채움:{filled}/18 (재시도)")

        except Exception as e:
            print(f"   -> [Error] 구글 시트 저장 실패: {e}")
            new_retry.append(item)

        time.sleep(SLEEP_SECONDS)

    save_json(SEEN_FILE, seen_list)
    save_json(RETRY_FILE, new_retry)
    print("\n✅ 전체 작업 완료!")

if __name__ == "__main__":
    main()
