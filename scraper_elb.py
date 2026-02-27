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
KEYWORDS = [
    x.strip()
    for x in os.getenv("KEYWORDS", "전환사채,교환사채,신주인수권부사채").split(",")
    if x.strip()
]

HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
LIMIT = int(os.getenv("LIMIT", "30"))
RUN_ONE_ACPTNO = os.getenv("RUN_ONE_ACPTNO", "").strip()

GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "").strip()
GOOGLE_CREDENTIALS_JSON = (
    os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
    or os.environ.get("GOOGLE_CREDS", "").strip()
)

BOND_OUT_SHEET = os.getenv("BOND_OUT_SHEET", "주식연계채권")
SEEN_SHEET_NAME = os.getenv("SEEN_SHEET_NAME", "seen_elb")
USE_SEEN = os.getenv("USE_SEEN", "true").lower() == "true"

OUTDIR = Path(os.getenv("OUTDIR", "out"))
DEBUGDIR = OUTDIR / "debug"


# ==========================================================
# Output Columns
# ==========================================================
BOND_COLUMNS = [
    "구분",  # EB/CB/BW (title 기반)
    "회사명",
    "상장시장",
    "최초 이사회결의일",
    "권면총액(원)",
    "Coupon",
    "YTM",
    "만기",
    "전환청구 시작",
    "전환청구 종료",
    "Put Option",
    "Call Option",
    "Call 비율",
    "YTC",
    "모집방식",
    "발행상품",
    "행사(전환)가액(원)",
    "전환주식수",
    "주식총수대비 비율",
    "Refixing Floor",
    "납입일",
    "자금용도",
    "투자자",
    "링크",
    "접수번호",
]


# ==========================================================
# 라벨 후보 (PDF 패턴 반영)
# ==========================================================
LABEL_MAP = {
    "회사명": ["회사명", "회사 명", "회 사 명"],
    "상장시장": ["상장시장", "시장구분", "시장 구분"],
    "최초 이사회결의일": [
        "최초 이사회결의일", "최초이사회결의일",
        "이사회결의일(결정일)", "이사회결의일", "결정일"
    ],
    "권면총액(원)": [
        "사채의 권면(전자등록)총액 (원)", "사채의 권면(전자등록)총액(원)",
        "사채의 권면총액 (원)", "사채의 권면총액(원)",
        "권면(전자등록)총액 (원)",
        "권면총액(원)", "권면총액"
    ],
    "Coupon": ["표면이자율 (%)", "표면이자율(%)", "표면이자율"],
    "YTM": ["만기이자율 (%)", "만기이자율(%)", "만기이자율", "만기수익률"],
    "만기": ["사채만기일", "만기일", "만기"],
    "모집방식": ["사채발행방법", "사채 발행방법", "모집 또는 매출의 방법", "모집방법", "발행방법"],
    "발행상품": ["사채의 종류", "사채종류", "사채의종류", "1. 사채의 종류", "종류"],
    "행사(전환)가액(원)": [
        "전환가액 (원/주)", "전환가액(원/주)", "전환가액",
        "교환가액 (원/주)", "교환가액(원/주)", "교환가액",
        "행사가액 (원/주)", "행사가액(원/주)", "행사가액"
    ],
    "전환주식수": [
        "주식수", "전환에 따라 발행할 주식", "전환가능주식수",
        "교환대상주식수", "신주인수권 행사로 발행할 주식수"
    ],
    "주식총수대비 비율": [
        "주식총수 대비 비율(%)", "주식총수대비비율(%)",
        "주식총수 대비 비율", "발행주식총수 대비", "주식총수대비(%)"
    ],
    "Refixing Floor": ["최저 조정가액 (원)", "최저조정가액(원)", "최저 조정가액", "리픽싱 하한", "Refixing Floor"],
    "납입일": ["납입일", "납입예정일", "납입기일"],
}

START_LABELS_PERIOD_START = [
    "전환청구기간 시작일", "전환청구기간(시작일)", "전환청구기간(시작)", "전환청구기간 시작",
    "교환청구기간 시작일", "교환청구기간(시작일)", "교환청구기간(시작)", "교환청구기간 시작",
    "권리행사기간 시작일", "권리행사기간(시작일)", "권리행사기간(시작)", "권리행사기간 시작",
]
START_LABELS_PERIOD_END = [
    "전환청구기간 종료일", "전환청구기간(종료일)", "전환청구기간(종료)", "전환청구기간 종료",
    "교환청구기간 종료일", "교환청구기간(종료일)", "교환청구기간(종료)", "교환청구기간 종료",
    "권리행사기간 종료일", "권리행사기간(종료일)", "권리행사기간(종료)", "권리행사기간 종료",
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

# ✅ 구분: 라벨 후보 없이 제목 기반 고정
def bond_code_from_title(title: str) -> str:
    t = title or ""
    if "교환사채" in t:
        return "EB"
    if "전환사채" in t:
        return "CB"
    if "신주인수권부사채" in t:
        return "BW"
    return ""


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
    bonus_words = [
        "권면총액", "표면이자율", "만기이자율", "사채만기일",
        "전환청구기간", "교환청구기간", "권리행사기간",
        "조기상환청구권", "매도청구권", "call option", "put option",
        "특정인에 대한 대상자별", "조달자금의 구체적 사용 목적"
    ]
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
# Google Sheets
# ==========================================================
def gs_open():
    if not GOOGLE_SHEET_ID or not GOOGLE_CREDENTIALS_JSON:
        raise RuntimeError("GOOGLE_SHEET_ID / GOOGLE_CREDS 가 비어있습니다. Secrets 확인")

    creds = json.loads(GOOGLE_CREDENTIALS_JSON)
    gc = gspread.service_account_from_dict(creds)
    sh = gc.open_by_key(GOOGLE_SHEET_ID)

    try:
        seen_ws = sh.worksheet(SEEN_SHEET_NAME)
    except gspread.WorksheetNotFound:
        seen_ws = sh.add_worksheet(title=SEEN_SHEET_NAME, rows=2000, cols=2)
        seen_ws.update("A1:B1", [["acptNo", "ts"]])

    try:
        bond_ws = sh.worksheet(BOND_OUT_SHEET)
    except gspread.WorksheetNotFound:
        bond_ws = sh.add_worksheet(title=BOND_OUT_SHEET, rows=2000, cols=len(BOND_COLUMNS) + 5)

    return sh, bond_ws, seen_ws

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
        index[key] = len(ws.col_values(1))


# ==========================================================
# Extraction helpers
# ==========================================================
def scan_label_value(dfs: List[pd.DataFrame], label_candidates: List[str]) -> str:
    """
    라벨 매칭 후 값 후보: 오른쪽/두칸오른쪽/아래/아래오른쪽/같은행 값
    - exact + 부분포함(fuzzy)
    """
    cand = [_norm(x) for x in label_candidates if x]
    cand_set = set(cand)

    def _is_hit(cell_norm: str) -> bool:
        if cell_norm in cand_set:
            return True
        for cn in cand:
            if len(cn) >= 3 and cn in cell_norm:
                return True
        return False

    for df in dfs:
        arr = df.astype(str).values
        R, C = arr.shape
        for r in range(R):
            for c in range(C):
                cell_norm = _norm(arr[r][c])
                if not cell_norm:
                    continue
                if _is_hit(cell_norm):
                    checks = []
                    for rr, cc in [(r, c + 1), (r, c + 2), (r + 1, c), (r + 1, c + 1)]:
                        if 0 <= rr < R and 0 <= cc < C:
                            v = str(arr[rr][cc]).strip()
                            if v and v.lower() != "nan":
                                checks.append(v)

                    row_vals = [str(x).strip() for x in arr[r].tolist()
                                if str(x).strip() and str(x).strip().lower() != "nan"]

                    filtered = []
                    for x in row_vals:
                        xn = _norm(x)
                        if _is_hit(xn):
                            continue
                        filtered.append(x)

                    for v in checks + filtered:
                        if re.fullmatch(r"\d+\.", v):
                            continue
                        return v
    return ""

def dfs_to_text(dfs: List[pd.DataFrame]) -> str:
    lines = []
    for df in dfs:
        arr = df.astype(str).values
        for r in range(arr.shape[0]):
            row = [str(x).strip() for x in arr[r].tolist()]
            row = [x for x in row if x and x.lower() != "nan"]
            if row:
                lines.append("\t".join(row))
    return "\n".join(lines)

def _kw_to_regex(kw: str) -> str:
    esc = re.escape(kw)
    esc = esc.replace(r"\ ", r"\s*")
    return esc

def extract_text_block(
    dfs: List[pd.DataFrame],
    start_keywords: List[str],
    stop_keywords: List[str],
    max_chars: int = 900
) -> str:
    text = dfs_to_text(dfs)
    if not text:
        return ""

    start_res = [_kw_to_regex(k) for k in start_keywords if k]
    stop_res = [_kw_to_regex(k) for k in stop_keywords if k]

    start_pos = None
    start_len = 0
    for s in start_res:
        m = re.search(s, text, flags=re.IGNORECASE)
        if m:
            if start_pos is None or m.start() < start_pos:
                start_pos = m.start()
                start_len = max(0, m.end() - m.start())

    if start_pos is None:
        return ""

    end_pos = None
    search_from = start_pos + max(1, start_len)
    for s in stop_res:
        m = re.search(s, text[search_from:], flags=re.IGNORECASE)
        if m:
            cand = search_from + m.start()
            if end_pos is None or cand < end_pos:
                end_pos = cand

    snippet = text[start_pos:(end_pos if end_pos is not None else len(text))]
    snippet = re.sub(r"\s+", " ", snippet).strip()
    if len(snippet) > max_chars:
        snippet = snippet[:max_chars].rstrip() + " ..."
    return snippet

def extract_investors_from_table(dfs: List[pd.DataFrame], max_names: int = 8) -> str:
    """
    '발행 대상자명' 컬럼을 테이블에서 직접 추출
    """
    bad = {
        "발행대상자명", "발행 대상자명", "회사", "최대주주", "관계", "선정경위",
        "거래내역", "계획", "발행권면", "전자등록", "총액", "비고", "해당사항없음"
    }
    names = []
    seen = set()

    for df in dfs:
        arr = df.astype(str).values
        R, C = arr.shape

        header_rc = None
        for r in range(R):
            for c in range(C):
                v = str(arr[r][c]).strip()
                if "발행" in v and "대상자" in v and "명" in v:
                    header_rc = (r, c)
                    break
            if header_rc:
                break
        if not header_rc:
            continue

        hr, hc = header_rc
        for r in range(hr + 1, R):
            v = str(arr[r][hc]).strip()
            if not v or v.lower() == "nan":
                continue
            vn = _norm(v)
            if vn in {_norm(x) for x in bad}:
                continue
            if re.fullmatch(r"[\d,().%/\- ]+", v):
                continue
            if vn in seen:
                continue
            seen.add(vn)
            names.append(v)
            if len(names) >= max_names:
                break
        if len(names) >= max_names:
            break

    return "; ".join(names)

def extract_fund_use_block(dfs: List[pd.DataFrame]) -> str:
    """
    '조달자금의 구체적 사용 목적' 블록을 통째로 추출(다음 섹션에서 컷)
    """
    start = ["조달자금의 구체적 사용 목적"]
    stop = [
        "특정인에 대한 대상자별",
        "미상환 주권", "미상환주권",
        "신주인수권에 관한 사항",
        "전환(행사) 가능",
        "옵션에 관한 사항",
        "기타 투자판단에 참고할 사항",
    ]
    return extract_text_block(dfs, start, stop, max_chars=900)

def extract_call_ratio(call_text: str) -> str:
    t = call_text or ""
    m = re.search(r"(\d+(?:\.\d+)?)\s*/\s*100", t)  # 35/100
    if m:
        v = float(m.group(1))
        if 0 < v <= 100:
            return f"{v:g}%"
    vals = []
    for m in re.finditer(r"(\d+(?:\.\d+)?)\s*%", t):
        v = float(m.group(1))
        if 0 < v <= 100:
            vals.append(v)
    return f"{max(vals):g}%" if vals else ""

def extract_ytc_from_text(call_text: str, put_text: str) -> str:
    """
    Call/Put 본문에서 '연복리 xx%' / '수익률 xx%' 같은 표현이 있으면 잡기
    """
    for src in [call_text or "", put_text or ""]:
        m = re.search(r"연\s*복리\s*(\d+(?:\.\d+)?)\s*%", src)
        if m:
            return f"{m.group(1)}%"
        m = re.search(r"(매도청구수익률|조기상환수익률).{0,40}?(\d+(?:\.\d+)?)\s*%", src)
        if m:
            return f"{m.group(2)}%"
    return ""


# ==========================================================
# Parser
# ==========================================================
def parse_bond_record(dfs: List[pd.DataFrame], title: str, acpt_no: str, link: str) -> dict:
    rec = {k: "" for k in BOND_COLUMNS}
    rec["접수번호"] = acpt_no
    rec["링크"] = link

    # ✅ 구분: 제목 기반 (교환=EB / 전환=CB / 신주인수권부=BW)
    rec["구분"] = bond_code_from_title(title)

    # Scalar fields
    rec["회사명"] = scan_label_value(dfs, LABEL_MAP["회사명"]) or company_from_title(title)
    rec["상장시장"] = scan_label_value(dfs, LABEL_MAP["상장시장"]) or market_from_title(title)
    rec["최초 이사회결의일"] = scan_label_value(dfs, LABEL_MAP["최초 이사회결의일"])

    amt = _to_int(scan_label_value(dfs, LABEL_MAP["권면총액(원)"]))
    if amt is not None:
        rec["권면총액(원)"] = f"{amt:,}"

    coupon = _to_float(scan_label_value(dfs, LABEL_MAP["Coupon"]))
    if coupon is not None:
        rec["Coupon"] = f"{coupon}"

    ytm = _to_float(scan_label_value(dfs, LABEL_MAP["YTM"]))
    if ytm is not None:
        rec["YTM"] = f"{ytm}"

    rec["만기"] = scan_label_value(dfs, LABEL_MAP["만기"])

    rec["전환청구 시작"] = scan_label_value(dfs, START_LABELS_PERIOD_START)
    rec["전환청구 종료"] = scan_label_value(dfs, START_LABELS_PERIOD_END)

    rec["모집방식"] = scan_label_value(dfs, LABEL_MAP["모집방식"])

    rec["발행상품"] = scan_label_value(dfs, LABEL_MAP["발행상품"])
    if not rec["발행상품"]:
        if rec["구분"] == "CB":
            rec["발행상품"] = "전환사채"
        elif rec["구분"] == "EB":
            rec["발행상품"] = "교환사채"
        elif rec["구분"] == "BW":
            rec["발행상품"] = "신주인수권부사채"

    strike = _to_int(scan_label_value(dfs, LABEL_MAP["행사(전환)가액(원)"]))
    if strike is not None:
        rec["행사(전환)가액(원)"] = f"{strike:,}"

    shares = _to_int(scan_label_value(dfs, LABEL_MAP["전환주식수"]))
    if shares is not None:
        rec["전환주식수"] = f"{shares:,}"

    rec["주식총수대비 비율"] = scan_label_value(dfs, LABEL_MAP["주식총수대비 비율"])

    floor_raw = scan_label_value(dfs, LABEL_MAP["Refixing Floor"])
    floor_int = _to_int(floor_raw)
    rec["Refixing Floor"] = f"{floor_int:,}" if floor_int is not None else floor_raw

    rec["납입일"] = scan_label_value(dfs, LABEL_MAP["납입일"])

    # Put/Call 본문 블록 (PDF 관찰 패턴 반영)
    put_start = [
        "조기상환청구권(Put Option)에 관한 사항",
        "인수인의 조기상환청구권(Put Option)에 관한 사항",
        "조기상환청구권(Put option)에 관한 사항",
        "Put Option",
    ]
    call_start = [
        "매도청구권(Call Option)에 관한 사항",
        "매도청구권(Call option)에 관한 사항",
        "매도청구권(Call Option)",
        "Call Option",
        "매수청구권(Call Option)",
    ]
    stop_common = [
        "특정인에 대한 대상자별 사채발행내역",
        "조달자금의 구체적 사용 목적",
        "미상환 주권",
        "신주인수권에 관한 사항",
        "전환(행사) 가능",
        "기타 투자판단에 참고할 사항",
    ]

    put_text = extract_text_block(dfs, put_start, call_start + stop_common, max_chars=900)
    call_text = extract_text_block(dfs, call_start, stop_common, max_chars=900)

    # 어떤 공시는 "세부내용은 19. 기타..."만 찍고 본문은 다른 섹션에 있음 → 기타 투자판단에서 재시도
    if (len(put_text) < 120) or ("세부내용" in put_text and "기타" in put_text):
        put_text2 = extract_text_block(
            dfs,
            ["기타 투자판단에 참고할 사항", "23. 기타 투자판단에 참고할 사항", "19. 기타 투자판단에 참고할 사항"],
            call_start + stop_common,
            max_chars=900,
        )
        if put_text2 and "조기상환청구권" in put_text2:
            put_text = put_text2

    if (len(call_text) < 120) or ("세부내용" in call_text and "기타" in call_text):
        call_text2 = extract_text_block(
            dfs,
            ["기타 투자판단에 참고할 사항", "23. 기타 투자판단에 참고할 사항", "19. 기타 투자판단에 참고할 사항"],
            stop_common,
            max_chars=900,
        )
        if call_text2 and ("매도청구권" in call_text2 or "Call Option" in call_text2):
            call_text = call_text2

    rec["Put Option"] = put_text
    rec["Call Option"] = call_text
    rec["Call 비율"] = extract_call_ratio(call_text)
    rec["YTC"] = extract_ytc_from_text(call_text, put_text)

    # 자금용도: "조달자금의 구체적 사용 목적" 블록
    rec["자금용도"] = extract_fund_use_block(dfs)

    # 투자자: '발행 대상자명' 테이블 컬럼 추출
    rec["투자자"] = extract_investors_from_table(dfs)

    return rec


# ==========================================================
# Main
# ==========================================================
def run():
    _, bond_ws, seen_ws = gs_open()
    ensure_headers(bond_ws, BOND_COLUMNS)
    bond_index = build_index(bond_ws, BOND_COLUMNS, key_field="접수번호")

    seen = load_seen(seen_ws) if USE_SEEN else set()

    if RUN_ONE_ACPTNO:
        targets = [Target(acpt_no=RUN_ONE_ACPTNO, title=f"[MANUAL]{RUN_ONE_ACPTNO}", link="")]
    else:
        targets = parse_rss_targets()
        if USE_SEEN:
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
                rec = parse_bond_record(dfs, t.title, t.acpt_no, src)
                upsert(bond_ws, BOND_COLUMNS, bond_index, rec, key_field="접수번호")

                if USE_SEEN:
                    append_seen(seen_ws, t.acpt_no)

                ok += 1
                print(f"[OK] {t.acpt_no} type={rec.get('구분','')}")

            except Exception as e:
                print(f"[FAIL] {t.acpt_no} {t.title} :: {e}")

            time.sleep(0.4)

        context.close()
        browser.close()

    print(f"[DONE] ok={ok}")
