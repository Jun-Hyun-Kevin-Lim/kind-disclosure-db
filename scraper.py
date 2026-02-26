import os
import re
import json
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

import feedparser
import pandas as pd
from playwright.sync_api import sync_playwright


# =========================
# Config
# =========================
BASE = "https://kind.krx.co.kr"

DEFAULT_RSS = (
    "http://kind.krx.co.kr:80/disclosure/rsstodaydistribute.do"
    "?method=searchRssTodayDistribute&mktTpCd=0&currentPageSize=100"
)

RSS_URL = os.getenv("RSS_URL", DEFAULT_RSS)
KEYWORDS = [x.strip() for x in os.getenv("KEYWORDS", "유상증자,전환사채,교환사채").split(",") if x.strip()]

HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
OUTDIR = Path(os.getenv("OUTDIR", "out"))
DEBUGDIR = OUTDIR / "debug"

SEEN_FILE = Path(os.getenv("SEEN_FILE", "seen.json"))

# 한 번에 너무 많이 돌면 무거우니 상한 (원하면 늘려)
LIMIT = int(os.getenv("LIMIT", "20"))

# 특정 acptNo만 테스트하고 싶을 때 (옵션)
RUN_ONE_ACPTNO = os.getenv("RUN_ONE_ACPTNO", "").strip()


@dataclass
class Target:
    acpt_no: str
    title: str
    link: str


# =========================
# Utils
# =========================
def sanitize_filename(name: str) -> str:
    name = re.sub(r"[\\/:*?\"<>|]", "_", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name[:180] if len(name) > 180 else name


def extract_acpt_no(text: str) -> Optional[str]:
    m = re.search(r"acptNo=(\d{14})", text or "")
    return m.group(1) if m else None


def match_keyword(title: str) -> bool:
    if not title:
        return False
    return any(k in title for k in KEYWORDS)


def viewer_url(acpt_no: str, docno: str = "") -> str:
    # 팀장님이 말한 팝업/새창 공시뷰어 URL
    return f"{BASE}/common/disclsviewer.do?method=searchInitInfo&acptNo={acpt_no}&docno={docno}"


def load_seen() -> set:
    if SEEN_FILE.exists():
        try:
            return set(json.loads(SEEN_FILE.read_text(encoding="utf-8")))
        except Exception:
            return set()
    return set()


def save_seen(seen: set) -> None:
    SEEN_FILE.write_text(json.dumps(sorted(list(seen)), ensure_ascii=False, indent=2), encoding="utf-8")


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

    # RSS는 최신순인 경우가 많지만, 혹시 모르니 중복 제거(첫 등장만)
    uniq = {}
    for t in targets:
        if t.acpt_no not in uniq:
            uniq[t.acpt_no] = t
    return list(uniq.values())


def pick_best_frame_html(page) -> str:
    """
    공시뷰어 본문이 iframe/frame에 있을 때가 많음.
    frames를 훑어서 <table>이 가장 많은 frame의 HTML을 선택.
    """
    best_html = ""
    best_count = -1
    for fr in page.frames:
        try:
            html = fr.content()
            cnt = html.lower().count("<table")
            if cnt > best_count:
                best_count = cnt
                best_html = html
        except Exception:
            continue
    return best_html


def extract_tables_from_html(html: str) -> List[pd.DataFrame]:
    dfs = pd.read_html(html)  # HTML 내 모든 table 추출
    cleaned = []
    for df in dfs:
        df = df.copy().where(pd.notnull(df), "")
        cleaned.append(df)
    return cleaned


def dump_tables_to_excel(dfs: List[pd.DataFrame], out_path: Path, title_line: str):
    """
    너가 올린 예시처럼:
    - tableIndex: 0
    - (표)
    - tableIndex: 1
    - (표)
    한 시트에 쭉 덤프
    """
    sheet = "dump"
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        row = 0
        pd.DataFrame([[title_line]]).to_excel(writer, sheet_name=sheet, startrow=row, index=False, header=False)
        row += 2

        for i, df in enumerate(dfs):
            pd.DataFrame([[f"tableIndex: {i}"]]).to_excel(
                writer, sheet_name=sheet, startrow=row, index=False, header=False
            )
            row += 1
            df.to_excel(writer, sheet_name=sheet, startrow=row, index=False)
            row += len(df) + 3


def scrape_one_popup_to_excel(context, t: Target, ts: str) -> Tuple[bool, str]:
    """
    팀장님 방식: 팝업/새창 공시뷰어를 열고, 표(table)를 전부 뽑아 엑셀로 덤프
    return (success, message)
    """
    url = viewer_url(t.acpt_no)

    page = context.new_page()
    try:
        page.goto(url, wait_until="networkidle", timeout=60000)
        page.wait_for_timeout(1500)  # 렌더 안정화

        html = pick_best_frame_html(page)

        # table 없으면 디버깅 파일 저장
        if not html or html.lower().count("<table") == 0:
            dbg_html = DEBUGDIR / f"debug_{t.acpt_no}.html"
            dbg_png = DEBUGDIR / f"debug_{t.acpt_no}.png"
            dbg_html.write_text(page.content(), encoding="utf-8")
            try:
                page.screenshot(path=str(dbg_png), full_page=True)
            except Exception:
                pass
            return False, f"[WARN] table 0개로 보임: {t.acpt_no} (debug 저장: {dbg_html}, {dbg_png})"

        # 표 전부 추출
        try:
            dfs = extract_tables_from_html(html)
        except Exception as e:
            # read_html이 실패하면 debug 저장
            dbg_html = DEBUGDIR / f"readhtml_fail_{t.acpt_no}.html"
            dbg_html.write_text(html, encoding="utf-8")
            return False, f"[ERROR] read_html 실패: {t.acpt_no} ({e}) debug={dbg_html}"

        safe_title = sanitize_filename(t.title) if t.title else f"acptNo={t.acpt_no}"
        out_path = OUTDIR / f"kind_popup_dump_{ts}_{t.acpt_no}.xlsx"
        title_line = f"{safe_title} (acptNo: {t.acpt_no})"

        dump_tables_to_excel(dfs, out_path, title_line)

        return True, f"[OK] {t.acpt_no} tables={len(dfs)} -> {out_path}"

    except Exception as e:
        return False, f"[ERROR] {t.acpt_no} 실패: {e}"
    finally:
        try:
            page.close()
        except Exception:
            pass


def run():
    OUTDIR.mkdir(parents=True, exist_ok=True)
    DEBUGDIR.mkdir(parents=True, exist_ok=True)

    seen = load_seen()

    # 1) 특정 acptNo만 테스트 모드
    if RUN_ONE_ACPTNO:
        targets = [Target(acpt_no=RUN_ONE_ACPTNO, title=f"MANUAL_{RUN_ONE_ACPTNO}", link="")]
    else:
        # 2) RSS에서 키워드 매칭된 공시만 잡기
        targets = parse_rss_targets()

        # 이미 처리한 건 제외
        targets = [t for t in targets if t.acpt_no not in seen]

        if LIMIT > 0:
            targets = targets[:LIMIT]

    if not targets:
        print("[INFO] 처리할 대상이 없습니다. (키워드 매칭/중복 제외 결과 0건)")
        return

    ts = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")

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

        ok_cnt = 0
        for t in targets:
            success, msg = scrape_one_popup_to_excel(context, t, ts)
            print(msg)
            if success:
                seen.add(t.acpt_no)
                ok_cnt += 1
            time.sleep(0.5)

        context.close()
        browser.close()

    save_seen(seen)
    print(f"[DONE] ok={ok_cnt} / seen_total={len(seen)} / outdir={OUTDIR}")


if __name__ == "__main__":
    run()
