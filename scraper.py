import os
import re
import json
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple, Set, Dict

import feedparser
import pandas as pd
from bs4 import BeautifulSoup
import gspread
from gspread.utils import rowcol_to_a1
from playwright.sync_api import sync_playwright


# ==========================================================
# Config (ENV)
# ==========================================================
BASE = "https://kind.krx.co.kr"
DEFAULT_RSS = (
    "http://kind.krx.co.kr:80/disclosure/rsstodaydistribute.do"
    "?method=searchRssTodayDistribute&mktTpCd=0&currentPageSize=100"
)

RSS_URL = os.getenv("RSS_URL", DEFAULT_RSS)
KEYWORDS = [x.strip() for x in os.getenv("KEYWORDS", "유상증자").split(",") if x.strip()]

HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
LIMIT = int(os.getenv("LIMIT", "30"))
RUN_ONE_ACPTNO = os.getenv("RUN_ONE_ACPTNO", "").strip()

GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "").strip()
GOOGLE_CREDENTIALS_JSON = (
    os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
    or os.environ.get("GOOGLE_CREDS", "").strip()
)

RIGHTS_OUT_SHEET = os.getenv("RIGHTS_OUT_SHEET", "유상증자")
SEEN_SHEET_NAME = os.getenv("SEEN_SHEET_NAME", "seen")

# Debug (원하면 유지: 실패 시 out/debug에 저장)
OUTDIR = Path(os.getenv("OUTDIR", "out"))
DEBUGDIR = OUTDIR / "debug"

RIGHTS_COLUMNS = [
    "회사명", "상장시장", "최초 이사회결의일", "증자방식", "발행상품", "신규발행주식수",
    "확정발행가(원)", "기준주가", "확정발행금액(억원)", "할인(할증률)",
    "증자전 주식수", "증자비율", "납입일", "신주의 배당기산일", "신주의 상장 예정일",
    "이사회결의일", "자금용도", "투자자", "링크", "접수번호"
]


@dataclass
class Target:
    acpt_no: str
    title: str
    link: str


# ==========================================================
# Utils
# ==========================================================
def _norm(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", "", s)
    s = s.replace(":", "")
    return s

def _to_int(s: str) -> Optional[int]:
    if s is None:
        return None
    t = str(s).replace(",", "")
    t = re.sub(r"[^\d\-]", "", t)
    if t in ("", "-"):
        return None
    try:
        return int(t)
    except:
        return None

def _to_float(s: str) -> Optional[float]:
    if s is None:
        return None
    t = str(s).replace(",", "")
    t = re.sub(r"[^\d\.\-]", "", t)
    if t in ("", "-", "."):
        return None
    try:
        return float(t)
    except:
        return None

def extract_acpt_no(text: str) -> Optional[str]:
    m = re.search(r"acptNo=(\d{14})", text or "")
    return m.group(1) if m else None

def company_from_title(title: str) -> str:
    m = re.search(r"\[([^\]]+)\]", title or "")
    return m.group(1).strip() if m else ""

def market_from_title(title: str) -> str:
    if not title:
        return ""
    if "[코]" in title:
        return "코스닥"
    if "[유]" in title:
        return "유가증권"
    if "[넥]" in title or "[코넥]" in title:
        return "코넥스"
    return ""

def viewer_url(acpt_no: str, docno: str = "") -> str:
    return f"{BASE}/common/disclsviewer.do?method=searchInitInfo&acptNo={acpt_no}&docno={docno}"

def match_keyword(title: str) -> bool:
    return bool(title) and any(k in title for k in KEYWORDS)


# ==========================================================
# RSS → targets
# ==========================================================
def parse_rss_targets() -> List[Target]:
    feed = feedparser.parse(RSS_URL)
    items = feed.entries or []
    targets: List[Target] = []

    for it in items:
        title = getattr(it, "title", "") or ""
        link = getattr(it, "link", "") or ""
        guid = getattr(it, "guid", "") or ""

        if not match_keyword(title):
            continue

        acpt_no = extract_acpt_no(link) or extract_acpt_no(guid)
        if not acpt_no:
            continue

        targets.append(Target(acpt_no=acpt_no, title=title, link=link))

    uniq = {}
    for t in targets:
        if t.acpt_no not in uniq:
            uniq[t.acpt_no] = t
    return list(uniq.values())


# ==========================================================
# Playwright: popup html → dfs
# ==========================================================
def is_block_page(html: str) -> bool:
    if not html:
        return True
    lower = html.lower()
    suspects = ["비정상", "접근", "제한", "차단", "오류", "error", "권한", "잠시 후", "관리자"]
    return any(s in lower for s in suspects) and ("<table" not in lower)

def frame_score(html: str) -> int:
    if not html:
        return -1
    lower = html.lower()
    tcnt = lower.count("<table")
    if tcnt == 0:
        return -1
    bonus_words = ["기준주가", "납입", "이사회", "할인", "할증", "발행", "청약", "증자방식", "자금조달"]
    bonus = sum(1 for w in bonus_words if w in lower)
    length_bonus = min(len(lower) // 2000, 50)
    return tcnt * 100 + bonus * 30 + length_bonus

def pick_best_frame_html(page) -> str:
    best_html, best_score = "", -1
    for fr in page.frames:
        try:
            html = fr.content()
            sc = frame_score(html)
            if sc > best_score:
                best_score = sc
                best_html = html
        except Exception:
            continue
    return best_html

def extract_tables_from_html_robust(html: str) -> List[pd.DataFrame]:
    """
    ✅ 라벨/값 추출 안정: header=None
    """
    html = (html or "").replace("\x00", "")

    try:
        dfs = pd.read_html(html, header=None)
        return [df.where(pd.notnull(df), "") for df in dfs]
    except Exception:
        pass

    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    tables = soup.find_all("table")
    results: List[pd.DataFrame] = []

    for tbl in tables:
        try:
            one = pd.read_html(str(tbl), header=None)
            if one:
                results.append(one[0].where(pd.notnull(one[0]), ""))
                continue
        except Exception:
            pass

        rows = []
        for tr in tbl.find_all("tr"):
            cells = tr.find_all(["th", "td"])
            row = [c.get_text(" ", strip=True) for c in cells]
            if row:
                rows.append(row)

        if rows:
            max_len = max(len(r) for r in rows)
            normed = [r + [""] * (max_len - len(r)) for r in rows]
            results.append(pd.DataFrame(normed))

    if not results:
        raise ValueError("No tables parsed (robust).")

    return results

def save_debug(acpt_no: str, page, html: str, reason: str):
    try:
        OUTDIR.mkdir(parents=True, exist_ok=True)
        DEBUGDIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        (DEBUGDIR / f"{acpt_no}_{ts}_{reason}.html").write_text(html or "", encoding="utf-8")
        try:
            page.screenshot(path=str(DEBUGDIR / f"{acpt_no}_{ts}_{reason}.png"), full_page=True)
        except Exception:
            pass
    except Exception:
        pass

def scrape_one(context, t: Target) -> Tuple[List[pd.DataFrame], str]:
    url = viewer_url(t.acpt_no)
    page = context.new_page()
    try:
        page.goto(url, wait_until="networkidle", timeout=60000)
        page.wait_for_timeout(1500)

        html = pick_best_frame_html(page) or ""
        if is_block_page(html) or html.lower().count("<table") == 0:
            save_debug(t.acpt_no, page, page.content(), "block_or_table0")
            raise RuntimeError("차단/오류/프레임 문제로 table을 못 찾음")

        dfs = extract_tables_from_html_robust(html)
        return dfs, url
    finally:
        try:
            page.close()
        except Exception:
            pass


# ==========================================================
# Google Sheets (structured only)
# ==========================================================
def gs_open():
    if not GOOGLE_SHEET_ID or not GOOGLE_CREDENTIALS_JSON:
        raise RuntimeError("GOOGLE_SHEET_ID / GOOGLE_CREDS 가 비어있습니다. Secrets 확인")

    creds = json.loads(GOOGLE_CREDENTIALS_JSON)
    gc = gspread.service_account_from_dict(creds)
    sh = gc.open_by_key(GOOGLE_SHEET_ID)

    # seen
    try:
        seen_ws = sh.worksheet(SEEN_SHEET_NAME)
    except gspread.WorksheetNotFound:
        seen_ws = sh.add_worksheet(title=SEEN_SHEET_NAME, rows=2000, cols=2)
        seen_ws.update("A1:B1", [["acptNo", "ts"]])

    # rights structured
    try:
        rights_ws = sh.worksheet(RIGHTS_OUT_SHEET)
    except gspread.WorksheetNotFound:
        rights_ws = sh.add_worksheet(title=RIGHTS_OUT_SHEET, rows=2000, cols=len(RIGHTS_COLUMNS) + 5)

    return sh, rights_ws, seen_ws

def load_seen(seen_ws) -> Set[str]:
    col = seen_ws.col_values(1)
    return set([x.strip() for x in col if x and x.strip().isdigit()])

def append_seen(seen_ws, acpt_no: str):
    seen_ws.append_row([acpt_no, datetime.now().strftime("%Y-%m-%d %H:%M:%S")], value_input_option="RAW")

def ensure_headers(ws, headers: List[str]):
    cur = ws.row_values(1)
    if cur != headers:
        end = rowcol_to_a1(1, len(headers))
        ws.update(f"A1:{end}", [headers])

def build_index(ws, headers: List[str], key_field: str) -> Dict[str, int]:
    ensure_headers(ws, headers)
    key_idx = headers.index(key_field) + 1
    col = ws.col_values(key_idx)
    idx = {}
    for r, v in enumerate(col, start=1):
        vv = str(v).strip()
        if vv.isdigit() and r > 1:
            idx[vv] = r
    return idx

def upsert(ws, headers: List[str], index: Dict[str, int], record: dict, key_field: str):
    ensure_headers(ws, headers)
    key = str(record.get(key_field, "")).strip()
    row_vals = [record.get(h, "") for h in headers]

    if key in index:
        r = index[key]
        end = rowcol_to_a1(r, len(headers))
        ws.update(f"A{r}:{end}", [row_vals])
    else:
        ws.append_row(row_vals, value_input_option="RAW")
        index[key] = len(ws.col_values(1))  # 다음부터 update 가능


# ==========================================================
# Rights issue parser (key-value only)
# ==========================================================
def scan_label_value(dfs: List[pd.DataFrame], label_candidates: List[str]) -> str:
    """
    라벨 매칭 후 값 후보: 오른쪽/두칸오른쪽/아래/아래오른쪽/같은행 값
    """
    cand = {_norm(x) for x in label_candidates}

    for df in dfs:
        arr = df.astype(str).values
        R, C = arr.shape

        for r in range(R):
            for c in range(C):
                cell = _norm(arr[r][c])
                if cell in cand:
                    checks = []
                    for rr, cc in [(r, c+1), (r, c+2), (r+1, c), (r+1, c+1)]:
                        if 0 <= rr < R and 0 <= cc < C:
                            v = str(arr[rr][cc]).strip()
                            if v and v.lower() != "nan":
                                checks.append(v)

                    row_vals = [str(x).strip() for x in arr[r].tolist()
                                if str(x).strip() and str(x).strip().lower() != "nan"]
                    row_vals = [x for x in row_vals if _norm(x) not in cand]

                    for v in checks + row_vals:
                        # "4." 같은 번호 제거
                        if re.fullmatch(r"\d+\.", v):
                            continue
                        return v
    return ""

def find_row_best_int(dfs: List[pd.DataFrame], must_contain: List[str]) -> Optional[int]:
    keys = [_norm(x) for x in must_contain]
    best = None

    for df in dfs:
        arr = df.astype(str).values
        for r in range(arr.shape[0]):
            row = [str(x).strip() for x in arr[r].tolist()]
            joined = _norm("".join(row))
            if all(k in joined for k in keys):
                nums = [_to_int(x) for x in row]
                nums = [x for x in nums if x is not None and x > 0]
                if nums:
                    m = max(nums)
                    if best is None or m > best:
                        best = m
    return best

def find_row_best_float(dfs: List[pd.DataFrame], must_contain: List[str]) -> Optional[float]:
    keys = [_norm(x) for x in must_contain]
    for df in dfs:
        arr = df.astype(str).values
        for r in range(arr.shape[0]):
            row = [str(x).strip() for x in arr[r].tolist()]
            joined = _norm("".join(row))
            if all(k in joined for k in keys):
                vals = [_to_float(x) for x in row]
                vals = [x for x in vals if x is not None]
                if vals:
                    return max(vals, key=lambda z: abs(z))
    return None

def build_fund_use_text(dfs: List[pd.DataFrame]) -> str:
    """
    자금용도 금액은 커서(원 단위) 100만 이상만 인정 → '시설자금:4' 같은 오인식 방지
    """
    keys = ["시설자금", "영업양수자금", "운영자금", "채무상환자금", "타법인증권취득자금", "타법인증권", "기타자금"]
    parts = []
    for k in keys:
        best = None
        for df in dfs:
            arr = df.astype(str).values
            for r in range(arr.shape[0]):
                row = [str(x).strip() for x in arr[r].tolist()]
                if _norm(k) in _norm("".join(row)):
                    nums = [_to_int(x) for x in row]
                    nums = [x for x in nums if x is not None and x >= 1_000_000]
                    if nums:
                        m = max(nums)
                        if best is None or m > best:
                            best = m
        if best is not None:
            parts.append(f"{k}:{best:,}")
    return "; ".join(parts)

def extract_investors(dfs: List[pd.DataFrame]) -> str:
    v = scan_label_value(dfs, ["제3자배정대상자", "제3자배정 대상자", "대표주관사/투자자", "투자자"])
    if v:
        return v
    return ""

def parse_rights_issue_record(dfs: List[pd.DataFrame], title: str, acpt_no: str, link: str) -> dict:
    rec = {k: "" for k in RIGHTS_COLUMNS}
    rec["접수번호"] = acpt_no
    rec["링크"] = link

    # 회사명/시장
    rec["회사명"] = scan_label_value(dfs, ["회 사 명", "회사명", "회사 명"]) or company_from_title(title)
    rec["상장시장"] = scan_label_value(dfs, ["상장시장", "시장구분", "시장 구분"]) or market_from_title(title)

    # 결의일
    rec["이사회결의일"] = scan_label_value(dfs, ["15. 이사회결의일(결정일)", "이사회결의일(결정일)", "이사회결의일", "결정일"])
    rec["최초 이사회결의일"] = scan_label_value(dfs, ["최초 이사회결의일", "최초이사회결의일"]) or rec["이사회결의일"]

    # 증자방식
    rec["증자방식"] = scan_label_value(dfs, ["5. 증자방식", "증자방식", "발행방법", "배정방식"])

    # 주식수
    issue_shares = find_row_best_int(dfs, ["신주의종류와수", "보통주식"]) or find_row_best_int(dfs, ["보통주식", "(주)"])
    prev_shares = find_row_best_int(dfs, ["증자전발행주식총수", "보통주식"]) or find_row_best_int(dfs, ["발행주식총수", "보통주식"])

    if issue_shares:
        rec["발행상품"] = "보통주식"
        rec["신규발행주식수"] = f"{issue_shares:,}"
    if prev_shares:
        rec["증자전 주식수"] = f"{prev_shares:,}"

    # 발행가/기준주가/할인율
    price = find_row_best_int(dfs, ["신주발행가액", "보통주식"]) or find_row_best_int(dfs, ["신주", "발행가액"])
    if price:
        rec["확정발행가(원)"] = f"{price:,}"
    else:
        rec["확정발행가(원)"] = scan_label_value(dfs, ["6. 신주 발행가액", "신주 발행가액"])

    base_price = find_row_best_int(dfs, ["기준주가", "보통주식"]) or find_row_best_int(dfs, ["기준주가"])
    if base_price:
        rec["기준주가"] = f"{base_price:,}"
    else:
        rec["기준주가"] = scan_label_value(dfs, ["7. 기준주가", "기준주가"])

    disc = find_row_best_float(dfs, ["기준주가에대한할인율또는할증율"]) or find_row_best_float(dfs, ["할인율또는할증율"])
    if disc is not None:
        rec["할인(할증률)"] = f"{disc}"
    else:
        rec["할인(할증률)"] = scan_label_value(dfs, ["7-2. 기준주가에 대한 할인율 또는 할증율 (%)", "기준주가에 대한 할인율 또는 할증율 (%)"])

    # 일정
    rec["납입일"] = scan_label_value(dfs, ["9. 납입일", "납입일"])
    rec["신주의 배당기산일"] = scan_label_value(dfs, ["10. 신주의 배당기산일", "신주의 배당기산일", "배당기산일"])
    rec["신주의 상장 예정일"] = scan_label_value(dfs, ["12. 신주의 상장 예정일", "신주의 상장 예정일", "상장예정일"])

    # 자금용도 / 투자자
    rec["자금용도"] = scan_label_value(dfs, ["4. 자금조달의 목적", "자금조달의 목적", "자금용도"]) or build_fund_use_text(dfs)
    rec["투자자"] = extract_investors(dfs)

    # 계산 보강
    sh = _to_int(rec["신규발행주식수"])
    pr = _to_int(rec["확정발행가(원)"])
    if sh and pr:
        rec["확정발행금액(억원)"] = f"{(sh * pr) / 100_000_000:,.2f}"

    pv = _to_int(rec["증자전 주식수"])
    if sh and pv and pv > 0:
        rec["증자비율"] = f"{sh / pv * 100:.2f}%"

    return rec


# ==========================================================
# Main
# ==========================================================
def run():
    _, rights_ws, seen_ws = gs_open()
    seen = load_seen(seen_ws)

    ensure_headers(rights_ws, RIGHTS_COLUMNS)
    rights_index = build_index(rights_ws, RIGHTS_COLUMNS, key_field="접수번호")

    # 대상 선정
    if RUN_ONE_ACPTNO:
        targets = [Target(acpt_no=RUN_ONE_ACPTNO, title=f"[MANUAL]{RUN_ONE_ACPTNO}", link="")]
    else:
        targets = parse_rss_targets()
        targets = [t for t in targets if t.acpt_no not in seen]
        targets = targets[:LIMIT] if LIMIT > 0 else targets

    if not targets:
        print("[INFO] 처리할 대상이 없습니다.")
        return

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=HEADLESS,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
        )
        context = browser.new_context(
            locale="ko-KR",
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
            viewport={"width": 1400, "height": 900},
        )

        ok = 0
        for t in targets:
            try:
                dfs, src = scrape_one(context, t)

                # 유상증자만 정규 컬럼 저장
                if "유상증자" in (t.title or ""):
                    rec = parse_rights_issue_record(dfs, t.title, t.acpt_no, src)
                    upsert(rights_ws, RIGHTS_COLUMNS, rights_index, rec, key_field="접수번호")

                append_seen(seen_ws, t.acpt_no)
                ok += 1
                print(f"[OK] {t.acpt_no} structured=Y")

            except Exception as e:
                print(f"[FAIL] {t.acpt_no} {t.title} :: {e}")

            time.sleep(0.4)

        context.close()
        browser.close()

    print(f"[DONE] ok={ok}")
