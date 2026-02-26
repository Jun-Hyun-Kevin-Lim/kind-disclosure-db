import os
import re
import json
from datetime import datetime
from pathlib import Path

import feedparser
import pandas as pd
from playwright.sync_api import sync_playwright


# ======================
# 설정
# ======================
RSS_URL = os.getenv(
    "RSS_URL",
    "http://kind.krx.co.kr:80/disclosure/rsstodaydistribute.do?method=searchRssTodayDistribute&mktTpCd=0&currentPageSize=100",
)

# 팀장님 말처럼 "보고서명 키워드"로 잡기
KEYWORDS = [x.strip() for x in os.getenv("KEYWORDS", "유상증자,전환사채,교환사채").split(",") if x.strip()]

BASE = "https://kind.krx.co.kr"
OUTDIR = Path(os.getenv("OUTDIR", "out"))
SEEN_FILE = Path(os.getenv("SEEN_FILE", "seen.json"))

HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"


def load_seen() -> set[str]:
    if SEEN_FILE.exists():
        try:
            return set(json.loads(SEEN_FILE.read_text(encoding="utf-8")))
        except Exception:
            return set()
    return set()


def save_seen(seen: set[str]) -> None:
    SEEN_FILE.write_text(json.dumps(sorted(list(seen)), ensure_ascii=False, indent=2), encoding="utf-8")


def extract_acpt_no(text: str) -> str | None:
    # RSS link/guid 등에서 acptNo=숫자 추출
    m = re.search(r"acptNo=(\d{14})", text or "")
    return m.group(1) if m else None


def match_keyword(title: str) -> bool:
    if not title:
        return False
    return any(k in title for k in KEYWORDS)


def sanitize_filename(name: str) -> str:
    name = re.sub(r"[\\/:*?\"<>|]", "_", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name[:180] if len(name) > 180 else name


def viewer_url(acpt_no: str) -> str:
    # 공시 클릭하면 뜨는 "새창/팝업" 주소 형태
    return f"{BASE}/common/disclsviewer.do?method=searchInitInfo&acptNo={acpt_no}&docno="


def pick_best_frame_html(page) -> str:
    """
    공시뷰어는 본문이 iframe/frame 안에 있을 때가 많아서,
    page.frames 전체를 훑고 table이 가장 많은 frame의 HTML을 선택.
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


def extract_tables_from_html(html: str) -> list[pd.DataFrame]:
    dfs = pd.read_html(html)  # HTML 내 모든 table 추출
    cleaned = []
    for df in dfs:
        df = df.copy().where(pd.notnull(df), "")
        cleaned.append(df)
    return cleaned


def dump_tables_to_excel(dfs: list[pd.DataFrame], out_path: Path, title_line: str):
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


def main():
    OUTDIR.mkdir(parents=True, exist_ok=True)
    seen = load_seen()

    feed = feedparser.parse(RSS_URL)
    items = feed.entries or []

    targets = []
    for it in items:
        title = getattr(it, "title", "") or ""
        link = getattr(it, "link", "") or ""
        guid = getattr(it, "guid", "") or ""

        if not match_keyword(title):
            continue

        acpt_no = extract_acpt_no(link) or extract_acpt_no(guid)
        if not acpt_no:
            continue

        if acpt_no in seen:
            continue

        targets.append((acpt_no, title))

    if not targets:
        print("[INFO] 처리할 대상이 없습니다. (키워드 매칭/중복 제외 결과 0건)")
        return

    ts = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=HEADLESS,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ],
        )
        context = browser.new_context(
            locale="ko-KR",
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
            viewport={"width": 1400, "height": 900},
        )

        for acpt_no, title in targets:
            url = viewer_url(acpt_no)

            page = context.new_page()
            try:
                page.goto(url, wait_until="networkidle", timeout=60000)
                page.wait_for_timeout(1500)  # 렌더링 안정화

                html = pick_best_frame_html(page)
                if not html or html.lower().count("<table") == 0:
                    # 디버그용 HTML 저장
                    dbg = OUTDIR / f"debug_{acpt_no}.html"
                    dbg.write_text(page.content(), encoding="utf-8")
                    print(f"[WARN] table 0개로 보임: {acpt_no} (debug 저장: {dbg})")
                    page.close()
                    continue

                dfs = extract_tables_from_html(html)

                safe_title = sanitize_filename(title) if title else f"acptNo={acpt_no}"
                out_path = OUTDIR / f"kind_popup_dump_{ts}_{acpt_no}.xlsx"
                title_line = f"{safe_title} (acptNo: {acpt_no})"

                dump_tables_to_excel(dfs, out_path, title_line)

                seen.add(acpt_no)
                print(f"[OK] {acpt_no} tables={len(dfs)} -> {out_path}")

            except Exception as e:
                print(f"[ERROR] {acpt_no} 실패: {e}")
            finally:
                try:
                    page.close()
                except Exception:
                    pass

        context.close()
        browser.close()

    save_seen(seen)
    print(f"[DONE] 완료. OUTDIR={OUTDIR} / seen={len(seen)}")


if __name__ == "__main__":
    main()
