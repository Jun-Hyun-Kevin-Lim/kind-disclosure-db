import os
import re
import json
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple, Set

import feedparser
import pandas as pd
from bs4 import BeautifulSoup
import gspread
from playwright.sync_api import sync_playwright


# =========================
# Config (ENV)
# =========================
BASE = "https://kind.krx.co.kr"
DEFAULT_RSS = (
    "http://kind.krx.co.kr:80/disclosure/rsstodaydistribute.do"
    "?method=searchRssTodayDistribute&mktTpCd=0&currentPageSize=100"
)

RSS_URL = os.getenv("RSS_URL", DEFAULT_RSS)

# 보고서명 키워드(팀장님이 말한 방식 그대로: 제목 기준으로 대상 선정)
KEYWORDS = [x.strip() for x in os.getenv("KEYWORDS", "유상증자,전환사채,교환사채,신주인수권부사채").split(",") if x.strip()]

HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
LIMIT = int(os.getenv("LIMIT", "30"))  # 하루 처리 상한
RUN_ONE_ACPTNO = os.getenv("RUN_ONE_ACPTNO", "").strip()  # 테스트용: 특정 acptNo만 실행

# Google Sheets
GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "").strip()

# 너 레포 시크릿명이 GOOGLE_CREDS인 걸 반영 (fallback까지)
GOOGLE_CREDENTIALS_JSON = (
    os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
    or os.environ.get("GOOGLE_CREDS", "").strip()
)

DUMP_SHEET_NAME = os.getenv("DUMP_SHEET_NAME", "RAW_dump")  # RAW 덤프 탭
SEEN_SHEET_NAME = os.getenv("SEEN_SHEET_NAME", "seen")      # 중복방지 탭 (A열에 acptNo 저장)

# Debug output (optional)
OUTDIR = Path(os.getenv("OUTDIR", "out"))
DEBUGDIR = OUTDIR / "debug"


@dataclass
class Target:
    acpt_no: str
    title: str
    link: str


# =========================
# Helpers
# =========================
def extract_acpt_no(text: str) -> Optional[str]:
    m = re.search(r"acptNo=(\d{14})", text or "")
    return m.group(1) if m else None


def match_keyword(title: str) -> bool:
    return bool(title) and any(k in title for k in KEYWORDS)


def detect_category(title: str) -> str:
    for k in KEYWORDS:
        if k in (title or ""):
            return k
    return ""


def viewer_url(acpt_no: str, docno: str = "") -> str:
    # 팀장님 말한 "공시 클릭하면 뜨는 팝업/새창" URL
    return f"{BASE}/common/disclsviewer.do?method=searchInitInfo&acptNo={acpt_no}&docno={docno}"


def ensure_sheet_size(ws, extra_rows_needed: int, min_cols: int):
    """
    append 전에 grid limit 방지:
    - 컬럼 부족하면 늘리고
    - 현재 row_count + 추가 row 만큼 충분히 rows 늘림
    """
    # cols
    if ws.col_count < min_cols:
        ws.add_cols(min_cols - ws.col_count)

    # rows
    target_rows = ws.row_count + max(extra_rows_needed, 0) + 50  # buffer
    if ws.row_count < target_rows:
        ws.add_rows(target_rows - ws.row_count)


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

    # 중복 제거 (첫 등장 기준)
    uniq = {}
    for t in targets:
        if t.acpt_no not in uniq:
            uniq[t.acpt_no] = t
    return list(uniq.values())


def is_block_page(html: str) -> bool:
    """
    KIND가 간혹 차단/오류 페이지를 줄 때 감지
    (문구는 케이스마다 다를 수 있어 최소한으로 체크)
    """
    if not html:
        return True
    lower = html.lower()
    suspects = [
        "비정상", "접근이 제한", "차단", "권한", "error", "에러", "오류",
        "서비스를 이용", "잠시 후", "관리자에게"
    ]
    return any(s in lower for s in suspects) and ("<table" not in lower)


def frame_score(html: str) -> int:
    """
    공시 본문에 가까운 frame 고르는 점수:
    - table 개수 우선
    - 특정 키워드(기준주가/납입일/이사회/할인 등) 있으면 가산
    """
    if not html:
        return -1
    lower = html.lower()
    tcnt = lower.count("<table")
    if tcnt == 0:
        return -1

    bonus_words = ["기준주가", "납입", "이사회", "할인", "할증", "발행", "청약", "사채", "교환", "전환", "유상"]
    bonus = sum(1 for w in bonus_words if w in lower)

    # content length도 약간 반영
    length_bonus = min(len(lower) // 2000, 50)

    return tcnt * 100 + bonus * 30 + length_bonus


def pick_best_frame_html(page) -> str:
    best_html = ""
    best_score = -1
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
    read_html이 한 번에 실패하는 케이스를 강하게 방어:
    1) 전체 read_html
    2) 실패하면 table 하나씩 read_html
    3) 그래도 실패하는 table은 BeautifulSoup로 수동 파싱
    """
    html = (html or "").replace("\x00", "")

    # 1) 통째로 시도
    try:
        dfs = pd.read_html(html)
        return [df.where(pd.notnull(df), "") for df in dfs]
    except Exception:
        pass

    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    tables = soup.find_all("table")
    results: List[pd.DataFrame] = []

    for tbl in tables:
        # 2) table 단위 read_html
        try:
            one = pd.read_html(str(tbl))
            if one:
                df = one[0].where(pd.notnull(one[0]), "")
                results.append(df)
                continue
        except Exception:
            pass

        # 3) 최후 수동 파싱
        rows = []
        for tr in tbl.find_all("tr"):
            cells = tr.find_all(["th", "td"])
            row = [c.get_text(" ", strip=True) for c in cells]
            if row:
                rows.append(row)

        if rows:
            max_len = max(len(r) for r in rows)
            norm = [r + [""] * (max_len - len(r)) for r in rows]
            results.append(pd.DataFrame(norm))

    if not results:
        raise ValueError("No tables parsed (robust).")

    return results


def gs_open():
    if not GOOGLE_SHEET_ID or not GOOGLE_CREDENTIALS_JSON:
        raise RuntimeError("GOOGLE_SHEET_ID / GOOGLE_CREDS(또는 GOOGLE_CREDENTIALS_JSON)가 비어있습니다. Secrets 설정 필요")

    creds = json.loads(GOOGLE_CREDENTIALS_JSON)
    gc = gspread.service_account_from_dict(creds)
    sh = gc.open_by_key(GOOGLE_SHEET_ID)

    # dump 탭
    try:
        dump_ws = sh.worksheet(DUMP_SHEET_NAME)
    except gspread.WorksheetNotFound:
        dump_ws = sh.add_worksheet(title=DUMP_SHEET_NAME, rows=2000, cols=60)

    # seen 탭
    try:
        seen_ws = sh.worksheet(SEEN_SHEET_NAME)
    except gspread.WorksheetNotFound:
        seen_ws = sh.add_worksheet(title=SEEN_SHEET_NAME, rows=2000, cols=2)
        seen_ws.update("A1", [["acptNo"], ["(do not edit manually)"]])

    return sh, dump_ws, seen_ws


def load_seen_from_sheet(seen_ws) -> Set[str]:
    # A열 전체(acptNo) 읽기
    col = seen_ws.col_values(1)  # includes header
    vals = [x.strip() for x in col if x and x.strip().isdigit()]
    return set(vals)


def append_seen(seen_ws, acpt_no: str):
    # 마지막에 추가
    seen_ws.append_row([acpt_no, datetime.now().strftime("%Y-%m-%d %H:%M:%S")], value_input_option="RAW")


def df_to_rowlists(df: pd.DataFrame) -> Tuple[List[str], List[List[str]]]:
    cols = [str(c) for c in list(df.columns)]
    values = []
    for _, row in df.iterrows():
        values.append([str(x) if x != "" else "" for x in row.tolist()])
    return cols, values


def build_dump_rows(acpt_no: str, title: str, src_url: str, category: str, dfs: List[pd.DataFrame], run_ts: str) -> List[List[str]]:
    """
    RAW_dump 시트에 엑셀 덤프처럼 저장:
    [acptNo, tableIndex, rowType, ...payload]
    """
    rows: List[List[str]] = []

    # META
    rows.append([acpt_no, "", "META", category, title, src_url, run_ts])
    rows.append([acpt_no, "", "BLANK"])

    for i, df in enumerate(dfs):
        cols, data_rows = df_to_rowlists(df)

        rows.append([acpt_no, str(i), "TABLE_LABEL", f"tableIndex: {i}"])
        rows.append([acpt_no, str(i), "HEADER"] + cols)

        width = max(len(cols), max((len(r) for r in data_rows), default=0))
        for r in data_rows:
            r = r + [""] * (width - len(r))
            rows.append([acpt_no, str(i), "DATA"] + r)

        rows.append([acpt_no, "", "BLANK"])

    return rows


def append_rows_chunked(ws, rows: List[List[str]], min_cols: int = 220, chunk: int = 200):
    # grid limit 방지: append 전 시트 크기 보장
    max_len = max((len(r) for r in rows), default=0)
    ensure_sheet_size(ws, extra_rows_needed=len(rows), min_cols=max(min_cols, max_len + 5))

    for i in range(0, len(rows), chunk):
        ws.append_rows(rows[i:i + chunk], value_input_option="RAW")
        time.sleep(0.2)


def save_debug(acpt_no: str, page, html: str, reason: str):
    """
    실패 시 디버그 저장 (Actions artifact로 받을 수 있게 out/debug에 저장)
    """
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

        if is_block_page(html):
            save_debug(t.acpt_no, page, page.content(), "block_or_empty")
            raise RuntimeError("차단/오류/빈 페이지 가능 (table 거의 없음)")

        if html.lower().count("<table") == 0:
            save_debug(t.acpt_no, page, page.content(), "table0")
            raise RuntimeError("table 0개로 보임 (frame 선택 실패/차단 가능)")

        dfs = extract_tables_from_html_robust(html)
        return dfs, url

    finally:
        try:
            page.close()
        except Exception:
            pass


def run():
    # 구글 시트 연결
    _, dump_ws, seen_ws = gs_open()
    seen_set = load_seen_from_sheet(seen_ws)

    # 대상 선정
    if RUN_ONE_ACPTNO:
        targets = [Target(acpt_no=RUN_ONE_ACPTNO, title=f"MANUAL_{RUN_ONE_ACPTNO}", link="")]
    else:
        targets = parse_rss_targets()
        # 중복 제거 (seen 탭 기준)
        targets = [t for t in targets if t.acpt_no not in seen_set]
        targets = targets[:LIMIT] if LIMIT > 0 else targets

    if not targets:
        print("[INFO] 처리할 대상이 없습니다. (이미 처리했거나, 키워드 매칭 0건)")
        return

    run_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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
                category = detect_category(t.title)

                rows = build_dump_rows(
                    acpt_no=t.acpt_no,
                    title=t.title,
                    src_url=src,
                    category=category,
                    dfs=dfs,
                    run_ts=run_ts,
                )

                append_rows_chunked(dump_ws, rows)
                append_seen(seen_ws, t.acpt_no)  # 영구 중복방지
                ok += 1

                print(f"[OK] {t.acpt_no} tables={len(dfs)} -> GoogleSheet:{DUMP_SHEET_NAME}")

            except Exception as e:
                print(f"[FAIL] {t.acpt_no} {t.title} :: {e}")

            time.sleep(0.5)

        context.close()
        browser.close()

    print(f"[DONE] ok={ok} / new={ok} / total_seen={len(load_seen_from_sheet(seen_ws))}")
