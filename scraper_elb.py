#주식연계형_코드V2

import os
import re
import json
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple, Set, Dict, Any

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
LIMIT = int(os.getenv("LIMIT", "0"))
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

def _strip_leading_numbering(s: str) -> str:
    # "3. 정정사항" / "1)" / "2)" 등 제거
    t = (s or "").strip()
    t = re.sub(r"^\s*\d+\s*[\.\)]\s*", "", t)
    return t

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

# ✅ 구분: 제목 기반 고정
def bond_code_from_title(title: str) -> str:
    t = title or ""
    if "교환사채" in t:
        return "EB"
    if "전환사채" in t:
        return "CB"
    if "신주인수권부사채" in t:
        return "BW"
    return ""

# ✅ 정정 감지: "정정"이 맨 앞에 붙은 케이스만
def is_correction_title(title: str) -> bool:
    return (title or "").lstrip().startswith("정정")


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
        "특정인에 대한 대상자별", "조달자금의 구체적 사용 목적",
        "정정사항", "정정 전", "정정 후"
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

def update_row(ws, headers: List[str], row: int, record: dict):
    ensure_headers(ws, headers)
    row_vals = [record.get(h, "") for h in headers]
    end = rowcol_to_a1(row, len(headers))
    ws.update(f"A{row}:{end}", [row_vals])

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
        # append 이후 row 번호 재계산 (단순)
        index[key] = len(ws.col_values(1))

def make_fingerprint(rec: Dict[str, Any]) -> str:
    # 정정공시에서 원문 접수번호를 못 찾을 때 fallback 매칭 키
    comp = _norm(rec.get("회사명", ""))
    kind = (rec.get("구분", "") or "").strip()
    dt = _norm(rec.get("최초 이사회결의일", ""))
    if not comp or not kind or not dt:
        return ""
    return f"{comp}|{kind}|{dt}"

def build_fingerprint_index(ws, headers: List[str]) -> Dict[str, int]:
    """
    1회 호출로 전체값을 가져와 fingerprint -> row 를 구축 (정정 fallback용)
    """
    ensure_headers(ws, headers)
    values = ws.get_all_values()
    if not values or len(values) < 2:
        return {}

    hdr = values[0]
    def _idx(name: str) -> int:
        return hdr.index(name) if name in hdr else -1

    i_comp = _idx("회사명")
    i_kind = _idx("구분")
    i_dt = _idx("최초 이사회결의일")
    i_key = _idx("접수번호")

    if min(i_comp, i_kind, i_dt, i_key) < 0:
        return {}

    fp_idx: Dict[str, int] = {}
    for r in range(2, len(values) + 1):
        row = values[r - 1]
        comp = row[i_comp] if i_comp < len(row) else ""
        kind = row[i_kind] if i_kind < len(row) else ""
        dt = row[i_dt] if i_dt < len(row) else ""
        key = row[i_key] if i_key < len(row) else ""
        if not key or not str(key).strip().isdigit():
            continue
        fp = f"{_norm(comp)}|{(kind or '').strip()}|{_norm(dt)}"
        if fp and fp not in fp_idx:
            fp_idx[fp] = r
    return fp_idx


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
# ✅ 정정사항(3. 정정사항 표) 처리
# ==========================================================
def _build_label_lookup() -> List[Tuple[str, str]]:
    """
    정정사항 표의 '항목' 텍스트를 BOND_COLUMNS 필드로 매핑하기 위한 룩업 테이블.
    """
    pairs: List[Tuple[str, str]] = []
    for field, labels in LABEL_MAP.items():
        for lb in labels:
            pairs.append((_norm(lb), field))

    # 기간 라벨도 매핑 (정정사항 표에 "전환청구기간 시작일" 등이 들어올 수 있음)
    for lb in START_LABELS_PERIOD_START:
        pairs.append((_norm(lb), "전환청구 시작"))
    for lb in START_LABELS_PERIOD_END:
        pairs.append((_norm(lb), "전환청구 종료"))

    # 자주 나오는 섹션명도(혹시 정정사항 표에 직접 들어오는 경우)
    pairs.append((_norm("조달자금의 구체적 사용 목적"), "자금용도"))
    pairs.append((_norm("발행 대상자명"), "투자자"))
    pairs.append((_norm("발행대상자명"), "투자자"))

    # Put/Call 섹션명
    pairs.append((_norm("조기상환청구권(Put Option)에 관한 사항"), "Put Option"))
    pairs.append((_norm("매도청구권(Call Option)에 관한 사항"), "Call Option"))

    # 중복 제거
    uniq = {}
    for k, v in pairs:
        if k and k not in uniq:
            uniq[k] = v
    # 길이 긴 후보가 매칭 우선이 되도록 정렬
    out = list(uniq.items())
    out.sort(key=lambda x: len(x[0]), reverse=True)
    return out

LABEL_LOOKUP = _build_label_lookup()

def map_item_to_field(item_text: str) -> str:
    t = _strip_leading_numbering(item_text or "")
    tn = _norm(t)
    if not tn:
        return ""

    # 가장 긴 후보부터 부분포함 매칭
    for cand_norm, field in LABEL_LOOKUP:
        if not cand_norm:
            continue
        if cand_norm in tn or tn in cand_norm:
            return field
    return ""

def extract_correction_overrides_and_meta(dfs: List[pd.DataFrame]) -> Tuple[Dict[str, str], Optional[str]]:
    """
    정정사항 표에서 '정정 후' 값을 우선으로 필드별 override를 만들고,
    가능하면 '정정 전 접수번호(원문 acptNo)'도 함께 추출한다.
    """
    overrides: Dict[str, str] = {}
    original_acpt_no: Optional[str] = None

    def _is_pre_header(x: str) -> bool:
        xn = _norm(x)
        return ("정정전" in xn) or (("정정" in xn) and ("전" in xn))

    def _is_post_header(x: str) -> bool:
        xn = _norm(x)
        return ("정정후" in xn) or (("정정" in xn) and ("후" in xn))

    def _is_item_header(x: str) -> bool:
        xn = _norm(x)
        return ("항목" in xn) or ("정정사항" in xn) or ("정정내용" in xn) or ("구분" == xn)

    for df in dfs:
        arr = df.astype(str).values
        R, C = arr.shape
        if R == 0 or C == 0:
            continue

        # 정정사항 관련 테이블 후보 빠르게 필터
        flat = " ".join(_norm(str(x)) for x in arr.flatten() if str(x).strip() and str(x).lower() != "nan")
        if ("정정사항" not in flat) and ("정정전" not in flat) and ("정정후" not in flat):
            continue

        header_r = None
        col_pre = None
        col_post = None
        col_item = None

        # 1) 헤더 행 탐색 (정정 전/정정 후가 같은 행에 있는 케이스)
        for r in range(R):
            row = [str(arr[r][c]) for c in range(C)]
            pre_cols = [c for c, v in enumerate(row) if _is_pre_header(v)]
            post_cols = [c for c, v in enumerate(row) if _is_post_header(v)]
            if post_cols:
                header_r = r
                col_post = post_cols[0]
                col_pre = pre_cols[0] if pre_cols else None

                item_cols = [c for c, v in enumerate(row) if _is_item_header(v)]
                if item_cols:
                    col_item = item_cols[0]
                else:
                    # 항목 헤더가 없으면, 정정전/정정후가 아닌 첫 컬럼을 항목으로
                    for c in range(C):
                        if c != col_post and (col_pre is None or c != col_pre):
                            col_item = c
                            break
                break

        if header_r is None or col_post is None or col_item is None:
            continue

        last_field = ""
        for r in range(header_r + 1, R):
            item = str(arr[r][col_item]).strip() if col_item < C else ""
            post = str(arr[r][col_post]).strip() if col_post < C else ""
            pre = str(arr[r][col_pre]).strip() if (col_pre is not None and col_pre < C) else ""

            if item.lower() == "nan":
                item = ""
            if post.lower() == "nan":
                post = ""
            if pre.lower() == "nan":
                pre = ""

            # 줄바꿈/병합 형태 처리: item이 비면 직전 항목 continuation으로 간주
            if not item and last_field and post:
                if overrides.get(last_field):
                    overrides[last_field] = (overrides[last_field] + " " + post).strip()
                else:
                    overrides[last_field] = post.strip()
                continue

            if not item or not post:
                continue

            # 원문 접수번호 후보: 항목에 "접수번호"가 들어가고 pre가 14자리면 원문으로 채택
            itemn = _norm(item)
            if ("접수번호" in itemn or "acptno" in itemn.lower()) and (not original_acpt_no):
                if re.fullmatch(r"\d{14}", _norm(pre)) if pre else False:
                    original_acpt_no = _norm(pre)
                # 혹시 post에 원문이 들어오는 반대 케이스도 보조로
                if (not original_acpt_no) and re.fullmatch(r"\d{14}", _norm(post)):
                    original_acpt_no = _norm(post)

            field = map_item_to_field(item)
            if not field:
                last_field = ""
                continue

            overrides[field] = post.strip()
            last_field = field

    # 2) 표에서 못 찾았으면 텍스트에서 "정정 전" 주변 14자리로 한 번 더 시도
    if not original_acpt_no:
        text = dfs_to_text(dfs)
        # 정정 전 ... acptNo=########## 형태
        m = re.search(r"정정\s*전.{0,200}?acptNo=(\d{14})", text, flags=re.IGNORECASE)
        if m:
            original_acpt_no = m.group(1)
        else:
            # 정정전 근처 14자리 숫자
            for mm in re.finditer(r"\d{14}", text):
                s = max(0, mm.start() - 60)
                e = min(len(text), mm.end() + 60)
                win = _norm(text[s:e])
                if "정정전" in win or ("정정" in win and "전" in win):
                    original_acpt_no = mm.group(0)
                    break

    return overrides, original_acpt_no


# ==========================================================
# Parser
# ==========================================================
def parse_bond_record(dfs: List[pd.DataFrame], title: str, acpt_no: str, link: str) -> Tuple[dict, dict]:
    """
    return: (record, meta)
      meta = {
        "is_correction": bool,
        "original_acpt_no": Optional[str],
        "overrides": Dict[str,str]
      }
    """
    rec = {k: "" for k in BOND_COLUMNS}
    rec["접수번호"] = acpt_no
    rec["링크"] = link

    # ✅ 구분: 제목 기반 (교환=EB / 전환=CB / 신주인수권부=BW)
    rec["구분"] = bond_code_from_title(title)

    corr = is_correction_title(title)
    overrides: Dict[str, str] = {}
    original_acpt_no: Optional[str] = None
    if corr:
        overrides, original_acpt_no = extract_correction_overrides_and_meta(dfs)

    def pick(field: str, candidates: List[str]) -> str:
        # ✅ 정정 공시라면 정정 후 값이 있으면 최우선
        v = overrides.get(field, "")
        if v:
            return v
        return scan_label_value(dfs, candidates)

    # Scalar fields
    rec["회사명"] = (overrides.get("회사명") or pick("회사명", LABEL_MAP["회사명"]) or company_from_title(title))
    rec["상장시장"] = (overrides.get("상장시장") or pick("상장시장", LABEL_MAP["상장시장"]) or market_from_title(title))
    rec["최초 이사회결의일"] = overrides.get("최초 이사회결의일") or pick("최초 이사회결의일", LABEL_MAP["최초 이사회결의일"])

    amt_raw = overrides.get("권면총액(원)") or pick("권면총액(원)", LABEL_MAP["권면총액(원)"])
    amt = _to_int(amt_raw)
    if amt is not None:
        rec["권면총액(원)"] = f"{amt:,}"

    coupon_raw = overrides.get("Coupon") or pick("Coupon", LABEL_MAP["Coupon"])
    coupon = _to_float(coupon_raw)
    if coupon is not None:
        rec["Coupon"] = f"{coupon}"

    ytm_raw = overrides.get("YTM") or pick("YTM", LABEL_MAP["YTM"])
    ytm = _to_float(ytm_raw)
    if ytm is not None:
        rec["YTM"] = f"{ytm}"

    rec["만기"] = overrides.get("만기") or pick("만기", LABEL_MAP["만기"])

    rec["전환청구 시작"] = overrides.get("전환청구 시작") or pick("전환청구 시작", START_LABELS_PERIOD_START)
    rec["전환청구 종료"] = overrides.get("전환청구 종료") or pick("전환청구 종료", START_LABELS_PERIOD_END)

    rec["모집방식"] = overrides.get("모집방식") or pick("모집방식", LABEL_MAP["모집방식"])

    rec["발행상품"] = overrides.get("발행상품") or pick("발행상품", LABEL_MAP["발행상품"])
    if not rec["발행상품"]:
        if rec["구분"] == "CB":
            rec["발행상품"] = "전환사채"
        elif rec["구분"] == "EB":
            rec["발행상품"] = "교환사채"
        elif rec["구분"] == "BW":
            rec["발행상품"] = "신주인수권부사채"

    strike_raw = overrides.get("행사(전환)가액(원)") or pick("행사(전환)가액(원)", LABEL_MAP["행사(전환)가액(원)"])
    strike = _to_int(strike_raw)
    if strike is not None:
        rec["행사(전환)가액(원)"] = f"{strike:,}"

    shares_raw = overrides.get("전환주식수") or pick("전환주식수", LABEL_MAP["전환주식수"])
    shares = _to_int(shares_raw)
    if shares is not None:
        rec["전환주식수"] = f"{shares:,}"

    rec["주식총수대비 비율"] = overrides.get("주식총수대비 비율") or pick("주식총수대비 비율", LABEL_MAP["주식총수대비 비율"])

    floor_raw = overrides.get("Refixing Floor") or pick("Refixing Floor", LABEL_MAP["Refixing Floor"])
    floor_int = _to_int(floor_raw)
    rec["Refixing Floor"] = f"{floor_int:,}" if floor_int is not None else floor_raw

    rec["납입일"] = overrides.get("납입일") or pick("납입일", LABEL_MAP["납입일"])

    # Put/Call 본문 블록
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

    # ✅ 정정사항 표에 Put/Call이 있으면 그것도 우선
    rec["Put Option"] = (overrides.get("Put Option") or put_text)
    rec["Call Option"] = (overrides.get("Call Option") or call_text)
    rec["Call 비율"] = extract_call_ratio(rec["Call Option"])
    rec["YTC"] = extract_ytc_from_text(rec["Call Option"], rec["Put Option"])

    # 자금용도 / 투자자
    rec["자금용도"] = overrides.get("자금용도") or extract_fund_use_block(dfs)
    rec["투자자"] = overrides.get("투자자") or extract_investors_from_table(dfs)

    meta = {
        "is_correction": corr,
        "original_acpt_no": original_acpt_no,
        "overrides": overrides,
    }
    return rec, meta


# ==========================================================
# Main
# ==========================================================
def run():
    _, bond_ws, seen_ws = gs_open()
    ensure_headers(bond_ws, BOND_COLUMNS)

    bond_index = build_index(bond_ws, BOND_COLUMNS, key_field="접수번호")
    fp_index = build_fingerprint_index(bond_ws, BOND_COLUMNS)  # ✅ 정정 fallback용

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
                rec, meta = parse_bond_record(dfs, t.title, t.acpt_no, src)

                # ======================================================
                # ✅ 정정 공시: "기존 값이 있으면 append 말고 update"
                #   1) 정정 전(원문) 접수번호가 시트에 있으면 그 행 덮어쓰기
                #   2) 없으면 fingerprint(회사명|구분|이사회결의일)로 행 찾아 덮어쓰기
                #   3) 둘 다 못 찾으면 일반 upsert(정정 접수번호로 신규 append)
                # ======================================================
                did_update_existing = False

                if meta.get("is_correction"):
                    orig = (meta.get("original_acpt_no") or "").strip()

                    # (1) 원문 접수번호로 매칭
                    if orig and orig in bond_index:
                        # 기존 키 유지(원문 접수번호로 행 선택), 링크는 정정 공시 링크로 최신화됨
                        rec["접수번호"] = orig
                        upsert(bond_ws, BOND_COLUMNS, bond_index, rec, key_field="접수번호")
                        did_update_existing = True

                    # (2) fingerprint fallback
                    if not did_update_existing:
                        fp = make_fingerprint(rec)
                        if fp and fp in fp_index:
                            row = fp_index[fp]
                            # 기존 행의 접수번호를 유지
                            key_col = BOND_COLUMNS.index("접수번호") + 1
                            existing_key = str(bond_ws.cell(row, key_col).value or "").strip()
                            if existing_key.isdigit():
                                rec["접수번호"] = existing_key
                            update_row(bond_ws, BOND_COLUMNS, row, rec)
                            did_update_existing = True
                            # 인덱스 최신화(혹시 모를 변경 대비)
                            bond_index[rec["접수번호"]] = row

                # (3) 일반 upsert
                if not did_update_existing:
                    upsert(bond_ws, BOND_COLUMNS, bond_index, rec, key_field="접수번호")

                if USE_SEEN:
                    # 정정 공시도 "정정 접수번호"로 seen 처리
                    append_seen(seen_ws, t.acpt_no)

                ok += 1
                tag = "CORR" if meta.get("is_correction") else "NEW"
                print(f"[OK] {t.acpt_no} {tag} type={rec.get('구분','')}")

            except Exception as e:
                print(f"[FAIL] {t.acpt_no} {t.title} :: {e}")

            time.sleep(0.4)

        context.close()
        browser.close()

    print(f"[DONE] ok={ok}")


if __name__ == "__main__":
    run()
