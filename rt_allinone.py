# rt_allinone.py
# -*- coding: utf-8 -*-
"""
국토부 실거래가 공개시스템(조건별 자료제공) 자동화
- 아파트/매매 탭
- 기간/시도 설정 → "EXCEL 다운"
- 다운로드 파일을 전처리(열 리네임/숫자화/주소 분리) 후 저장 + 간단 피벗
- 전국: 최근 3개월(당월 포함·당월은 오늘까지) → "전국 YYMM_YYMMDD.xlsx"
- 서울: 전년도 10/01 ~ 오늘(한 번에) → "서울시 YYMMDD.xlsx"
- CI 환경: (되면) Google Drive 업로드·보관일수 정리, Google Sheets 갱신 (실패해도 본 작업 계속)
"""

from __future__ import annotations

import os
import re
import sys
import json
import time
import shutil
import tempfile
from typing import Optional, Tuple
from pathlib import Path
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pandas as pd

# ---------- Selenium ----------
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.alert import Alert

# ---------- Google (optional) ----------
DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID", "").strip()
DRIVE_RETENTION_DAYS = int(os.getenv("DRIVE_RETENTION_DAYS", "3") or "3")
SHEET_ID = os.getenv("SHEET_ID", "").strip()

# ---------- 환경/경로 ----------
URL = "https://rt.molit.go.kr/pt/xls/xls.do?mobileAt="

SAVE_DIR = Path(os.getenv("OUT_DIR", "output")).resolve()
TMP_DL = (Path.cwd() / "_rt_downloads").resolve()
SAVE_DIR.mkdir(parents=True, exist_ok=True)
TMP_DL.mkdir(parents=True, exist_ok=True)

DOWNLOAD_TIMEOUT = int(os.getenv("DOWNLOAD_TIMEOUT", "120"))  # 1회 다운로드 대기
CLICK_RETRY_MAX = int(os.getenv("CLICK_RETRY_MAX", "10"))     # 클릭 재시도 횟수
CLICK_RETRY_WAIT = int(os.getenv("CLICK_RETRY_WAIT", "30"))   # 재시도 간 대기(초)

IS_CI = os.getenv("CI", "") == "1"


# ---------- 유틸 ----------
def debug(msg: str):
    sys.stdout.write(msg.rstrip() + "\n")
    sys.stdout.flush()


def today_kst() -> date:
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo("Asia/Seoul")
        return datetime.now(tz).date()
    except Exception:
        return (datetime.utcnow() + timedelta(hours=9)).date()


def month_first(d: date) -> date:
    return d.replace(day=1)


def shift_months(d: date, k: int) -> date:
    y, m = d.year, d.month
    m2 = (m - 1) + k
    y2 = y + m2 // 12
    m3 = (m2 % 12) + 1
    return date(y2, m3, 1)


def yymm(d: date) -> str:
    return d.strftime("%y%m")


def yymmdd(d: date) -> str:
    return d.strftime("%y%m%d")


# ---------- 드라이버 ----------
def build_driver(download_dir: Path) -> webdriver.Chrome:
    opts = Options()
    # 안정 옵션
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-notifications")
    opts.add_argument("--window-size=1400,900")
    opts.add_argument("--lang=ko-KR")
    opts.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    prefs = {
        "download.default_directory": str(download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    opts.add_experimental_option("prefs", prefs)

    # Actions에서 지정된 바이너리/드라이버가 있으면 사용
    chrome_bin = os.getenv("CHROME_BIN")
    if chrome_bin:
        opts.binary_location = chrome_bin

    chromedriver_bin = os.getenv("CHROMEDRIVER_BIN")
    if chromedriver_bin and Path(chromedriver_bin).exists():
        service = Service(chromedriver_bin)
    else:
        from webdriver_manager.chrome import ChromeDriverManager
        service = Service(ChromeDriverManager().install())

    # 세션 충돌 방지: 임시 프로필
    tmp_profile = Path(tempfile.mkdtemp(prefix="chrome_prof_"))
    opts.add_argument(f"--user-data-dir={tmp_profile.as_posix()}")

    driver = webdriver.Chrome(service=service, options=opts)

    # 직접 다운로드 허용 (CDP)
    try:
        driver.execute_cdp_cmd(
            "Page.setDownloadBehavior",
            {"behavior": "allow", "downloadPath": str(download_dir)},
        )
    except Exception as e:
        debug(f"  - warn: setDownloadBehavior failed: {e}")

    import atexit
    atexit.register(lambda: shutil.rmtree(tmp_profile, ignore_errors=True))
    return driver


# ---------- 페이지 조작 ----------
def _looks_like_date_input(el) -> bool:
    t = (el.get_attribute("type") or "").lower()
    ph = (el.get_attribute("placeholder") or "").lower()
    val = (el.get_attribute("value") or "").lower()
    name = (el.get_attribute("name") or "").lower()
    id_ = (el.get_attribute("id") or "").lower()
    txt = " ".join([ph, val, name, id_])
    return (
        t in ("date", "text", "") and (
            re.search(r"\d{4}-\d{2}-\d{2}", ph) or
            re.search(r"\d{4}-\d{2}-\d{2}", val) or
            "yyyy-mm-dd" in ph or "yyyy" in ph or
            "start" in txt or "end" in txt or "from" in txt or "to" in txt
        )
    )


def find_date_inputs(driver: webdriver.Chrome) -> Tuple:
    inputs = driver.find_elements(By.CSS_SELECTOR, "input")
    cands = [el for el in inputs if _looks_like_date_input(el)]
    if len(cands) >= 2:
        return cands[0], cands[1]

    # fallback들
    dates = [e for e in inputs if (e.get_attribute("type") or "").lower() == "date"]
    if len(dates) >= 2:
        return dates[0], dates[1]

    texts = [e for e in inputs if (e.get_attribute("type") or "").lower() in ("text", "")]
    if len(texts) >= 2:
        return texts[0], texts[1]

    raise RuntimeError("날짜 입력 박스를 찾지 못했습니다.")


def _ensure_value_with_js(driver, el, val: str) -> bool:
    try:
        driver.execute_script(
            """
            const el = arguments[0], v = arguments[1];
            el.value = v;
            el.dispatchEvent(new Event('input', {bubbles:true}));
            el.dispatchEvent(new Event('change', {bubbles:true}));
            el.blur();
            """,
            el, val,
        )
        time.sleep(0.2)
        return (el.get_attribute("value") or "").strip() == val
    except Exception:
        return False


def _type_and_verify(el, val: str) -> bool:
    try:
        el.click()
        el.send_keys(Keys.CONTROL, "a")
        el.send_keys(Keys.DELETE)
        el.send_keys(val)
        time.sleep(0.2)
        el.send_keys(Keys.TAB)
        time.sleep(0.2)
        return (el.get_attribute("value") or "").strip() == val
    except Exception:
        return False


def set_dates(driver: webdriver.Chrome, start: date, end: date):
    s_el, e_el = find_date_inputs(driver)
    s_val, e_val = start.isoformat(), end.isoformat()

    ok_s = _type_and_verify(s_el, s_val) or _ensure_value_with_js(driver, s_el, s_val)
    ok_e = _type_and_verify(e_el, e_val) or _ensure_value_with_js(driver, e_el, e_val)

    if not ok_s or not ok_e:
        sv = (s_el.get_attribute("value") or "").strip()
        ev = (e_el.get_attribute("value") or "").strip()
        debug(f"  - warn: date fill verify failed. want=({s_val},{e_val}) got=({sv},{ev})")
    debug(f"  - set_dates: {start} ~ {end}")


def select_sido(driver: webdriver.Chrome, wanted: str) -> bool:
    """시도 셀렉트박스에서 텍스트로 선택 (예: '서울특별시')"""
    opts = driver.find_elements(By.TAG_NAME, "select")
    for sel in opts:
        try:
            items = sel.find_elements(By.TAG_NAME, "option")
            labels = [i.text.strip() for i in items]
            if any("전체" in t or "서울" in t or "광역" in t for t in labels):
                for i in items:
                    if i.text.strip() == wanted:
                        i.click()
                        time.sleep(0.2)
                        return True
        except Exception:
            pass
    return False


def _try_accept_alert(driver: webdriver.Chrome, wait: float = 2.0) -> bool:
    t0 = time.time()
    while time.time() - t0 < wait:
        try:
            alert = Alert(driver)
            text = alert.text
            alert.accept()
            debug(f"    · alert accepted: {text}")
            return True
        except Exception:
            time.sleep(0.1)
    return False


def _click_by_locators(driver: webdriver.Chrome, label: str) -> bool:
    locators = [
        (By.XPATH, f"//button[normalize-space()='{label}']"),
        (By.XPATH, f"//a[normalize-space()='{label}']"),
        (By.XPATH, f"//*[normalize-space(text())='{label}']"),
        (By.CSS_SELECTOR, "button.btn, a.btn, button, a"),
    ]
    for by, sel in locators:
        try:
            els = driver.find_elements(by, sel)
            for el in els:
                if el.is_displayed() and el.is_enabled():
                    el.click()
                    return True
        except Exception:
            pass
    return False


def click_download(driver: webdriver.Chrome, kind: str = "excel") -> bool:
    label = "EXCEL 다운" if kind == "excel" else "CSV 다운"

    _try_accept_alert(driver, wait=1.0)
    if _click_by_locators(driver, label):
        _try_accept_alert(driver, wait=3.0)
        return True

    # JS 함수 직접 호출 (페이지별 다름 → 후보 시도)
    js_funcs = ["excelDown", "xlsDown", "excelDownload", "fnExcel", "fnExcelDown", "fncExcel"]
    for fn in js_funcs:
        try:
            driver.execute_script(f"if (typeof {fn}==='function') {fn}();")
            _try_accept_alert(driver, wait=3.0)
            return True
        except Exception:
            pass
    return False


def wait_download(folder: Path, snapshot: set[str], timeout: int = DOWNLOAD_TIMEOUT) -> Path:
    """새 파일 등장 후 크기 안정화 확인"""
    t0 = time.time()
    last_size = None
    stable_for = 0
    target: Optional[Path] = None
    while time.time() - t0 < timeout:
        current = set(p.name for p in folder.glob("*"))
        new_names = [n for n in current if n not in snapshot]
        if new_names:
            # .crdownload 제외
            for n in sorted(new_names):
                p = folder / n
                if p.suffix.lower() in (".xlsx", ".xls", ".html", ".zip"):
                    sz = p.stat().st_size
                    if last_size is None or sz != last_size:
                        last_size = sz
                        stable_for = 0
                    else:
                        stable_for += 1
                    if stable_for >= 3:     # 약 3회(≈3초) 같은 크기 → 완료로 간주
                        return p
        time.sleep(1.0)
    raise TimeoutError("다운로드 시작/완료 감지 실패")


# ---------- 데이터 읽기/전처리/저장 ----------
def read_table(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in (".xlsx", ".xls"):
        # 국토부 엑셀은 첫 시트 표
        return pd.read_excel(path, engine="openpyxl")
    # 가끔 HTML표가 내려오는 경우 대비
    tables = pd.read_html(path.read_text("utf-8", errors="ignore"))
    if not tables:
        raise ValueError("테이블을 읽지 못했습니다.")
    # 'NO' 헤더를 가진 첫 표 아래부터 데이터
    t = tables[0]
    if (t.columns == "NO").any():
        return t
    return tables[0]


def clean_df(df: pd.DataFrame, split_month: bool) -> pd.DataFrame:
    # 필요한 열 리네임(가끔 공백/한글 괄호 차이)
    rename = {}
    for c in list(df.columns):
        k = str(c).strip()
        if "거래금액" in k:
            rename[c] = "거래금액(만원)"
        if "전용면적" in k:
            rename[c] = "전용면적(㎡)"
        if k == "지번" or k == "지번주소":
            rename[c] = "지번"
    if rename:
        df = df.rename(columns=rename)

    # 맨 앞쪽 'NO' 제거
    for c in list(df.columns):
        if str(c).strip().upper() == "NO":
            df = df[df[c].notna()].drop(columns=[c])

    # 숫자화
    for c in ["거래금액(만원)", "전용면적(㎡)"]:
        if c in df.columns:
            df[c] = (
                df[c]
                .astype(str)
                .str.replace(r"[^\d\.]", "", regex=True)
                .replace({"": np.nan})
            )
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # 시군구 쪼개기
    if "시군구" in df.columns:
        parts = df["시군구"].astype(str).str.split(expand=True, n=2)
        for i, name in enumerate(["광역", "구", "법정동"]):
            if parts.shape[1] > i:
                df[name] = parts[i].fillna("")
            else:
                df[name] = ""

    # 계약년/월 분리(서울 피벗용)
    if split_month and "계약년월" in df.columns:
        s = df["계약년월"].astype(str).str.replace(r"\D", "", regex=True)
        df["계약년"] = s.str.slice(0, 4)
        df["계약월"] = s.str.slice(4, 6)

    return df


def make_pivot(df: pd.DataFrame, mode: str) -> pd.DataFrame:
    if mode == "national":
        if "광역" not in df.columns:
            return pd.DataFrame(columns=["광역", "건수"])
        pv = df.groupby("광역", dropna=False).size().reset_index(name="건수")
        pv = pv.sort_values("광역").reset_index(drop=True)
        return pv

    # seoul: 구 x 월(당월만 쓰기용)
    if "구" not in df.columns or "계약월" not in df.columns:
        return pd.DataFrame()
    pv = df.groupby(["구", "계약월"], dropna=False).size().unstack(fill_value=0)
    # '01'~'12' 순으로 정렬
    cols = [f"{i:02d}" for i in range(1, 13)]
    for c in cols:
        if c not in pv.columns:
            pv[c] = 0
    pv = pv[cols]
    pv = pv.reset_index()
    return pv


def save_excel(path: Path, df: pd.DataFrame, pivot: Optional[pd.DataFrame] = None, pivot_name: str = "피벗"):
    from openpyxl import Workbook
    with pd.ExcelWriter(path, engine="openpyxl") as xw:
        df.to_excel(xw, index=False, sheet_name="원본")
        if pivot is not None and not pivot.empty:
            pivot.to_excel(xw, index=False, sheet_name=pivot_name)


# ---------- Google 연동(있으면 실행) ----------
def load_sa_credentials(sa_path: Path):
    try:
        from google.oauth2 import service_account
        creds = service_account.Credentials.from_service_account_file(
            str(sa_path),
            scopes=[
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/spreadsheets",
            ],
        )
        debug("  - SA loaded.")
        return creds
    except Exception as e:
        debug(f"  ! service account load failed: {e}")
        return None


def drive_upload_and_cleanup(creds, file_path: Path):
    if not (IS_CI and creds and DRIVE_FOLDER_ID):
        return
    try:
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaFileUpload

        svc = build("drive", "v3", credentials=creds, cache_discovery=False)
        media = MediaFileUpload(str(file_path), resumable=False)
        meta = {"name": file_path.name, "parents": [DRIVE_FOLDER_ID]}
        svc.files().create(body=meta, media_body=media, fields="id,name").execute()
        debug(f"  - uploaded to Drive: {file_path.name}")

        # 보관일수 초과 정리
        if DRIVE_RETENTION_DAYS > 0:
            q = f"'{DRIVE_FOLDER_ID}' in parents and trashed=false"
            items = svc.files().list(q=q, fields="files(id,name,createdTime)").execute().get("files", [])
            cutoff = time.time() - DRIVE_RETENTION_DAYS * 86400
            from dateutil import parser as dtp
            for it in items:
                try:
                    ts = dtp.parse(it.get("createdTime", "")).timestamp()
                except Exception:
                    continue
                if ts < cutoff:
                    try:
                        svc.files().delete(fileId=it["id"]).execute()
                        debug(f"  - deleted old file: {it['name']}")
                    except Exception:
                        pass
    except Exception as e:
        debug(f"  ! drive error: {e}")


def sheets_write(creds, outname: str, pivot: pd.DataFrame, mode: str, today_str: str):
    if not (IS_CI and creds and SHEET_ID):
        return
    try:
        import gspread
        from google.auth.transport.requests import AuthorizedSession
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SHEET_ID)

        if mode == "national":
            # 시트명: "전국 YY년 MM월" (파일명 "전국 YYMM_YYMMDD"에서 YYMM 사용)
            y, m = "20" + outname[3:5], outname[5:7]
            title = f"전국 {y[-2:]}년 {m}월"
            try:
                ws = sh.worksheet(title)
            except Exception:
                ws = sh.add_worksheet(title=title, rows="200", cols="30")

            vals = ws.get_all_values()
            row_idx = None
            for i, row in enumerate(vals, start=1):
                if row and row[0].strip() == today_str:
                    row_idx = i
                    break
            if row_idx is None:
                row_idx = len(vals) + 1
                ws.update_cell(row_idx, 1, today_str)

            # 헤더(광역들) 보강
            counts = pivot.set_index("광역")["건수"].to_dict() if not pivot.empty else {}
            header = ws.row_values(1)[1:]
            for region in counts.keys():
                if region not in header:
                    ws.update_cell(1, len(header) + 2, region)
                    header.append(region)
            header = ws.row_values(1)[1:]
            for j, region in enumerate(header, start=2):
                ws.update_cell(row_idx, j, int(counts.get(region, 0)))

        else:  # seoul
            title = f"서울 {today_str[:2]}년 {today_str[2:4]}월"
            try:
                ws = sh.worksheet(title)
            except Exception:
                ws = sh.add_worksheet(title=title, rows="200", cols="30")

            month_now = today_str[2:4]
            if pivot.empty or month_now not in pivot.columns:
                return

            ser = pivot.set_index("구")[month_now]
            header = ws.row_values(1)[1:]
            for g in ser.index:
                if g not in header:
                    ws.update_cell(1, len(header) + 2, g)
                    header.append(g)

            vals = ws.get_all_values()
            row_idx = None
            for i, row in enumerate(vals, start=1):
                if row and row[0].strip() == today_str:
                    row_idx = i
                    break
            if row_idx is None:
                row_idx = len(vals) + 1
                ws.update_cell(row_idx, 1, today_str)

            header = ws.row_values(1)[1:]
            for j, g in enumerate(header, start=2):
                ws.update_cell(row_idx, j, int(ser.get(g, 0)))
    except Exception as e:
        debug(f"  ! sheets error: {e}")


# ---------- 메인 처리 ----------
def fetch_and_process(
    driver: webdriver.Chrome,
    sido: Optional[str],
    start: date,
    end: date,
    outname: str,
    pivot_mode: str,   # 'national' or 'seoul'
    creds,
) -> None:

    # 페이지 진입
    driver.get(URL)
    time.sleep(0.8)

    # 날짜/시도 설정
    set_dates(driver, start, end)
    if sido:
        ok = select_sido(driver, sido)
        debug(f"  - select_sido({sido}): {ok}")

    # 다운로드(재시도)
    got_file: Optional[Path] = None
    before = set(p.name for p in TMP_DL.glob("*"))
    for attempt in range(1, CLICK_RETRY_MAX + 1):
        ok = click_download(driver, kind="excel")
        debug(f"  - click_download(excel) / attempt {attempt}: {ok}")
        if not ok:
            time.sleep(1.0)

        try:
            got = wait_download(TMP_DL, before, timeout=DOWNLOAD_TIMEOUT)
            got_file = got
            debug(f"  - got file: {got}  size={got.stat().st_size:,}  ext={got.suffix}")
            break
        except TimeoutError:
            debug(f"  - warn: 다운로드 시작 감지 실패(시도 {attempt}/{CLICK_RETRY_MAX})")
            # 새 시도 전 폼 재설정
            driver.get(URL)
            time.sleep(0.8)
            set_dates(driver, start, end)
            if sido:
                select_sido(driver, sido)
            time.sleep(CLICK_RETRY_WAIT)

    if not got_file:
        raise RuntimeError("다운로드 시작 감지 실패(최대 시도 초과)")

    # 읽기/전처리/피벗/저장
    df_raw = read_table(got_file)
    split_month = (pivot_mode == "seoul")
    df = clean_df(df_raw, split_month=split_month)
    pv = make_pivot(df, pivot_mode)

    out = SAVE_DIR / outname
    save_excel(out, df, pv)
    debug(f"완료: {out}")

    # (옵션) 드라이브 업로드 + 보관 정리
    drive_upload_and_cleanup(creds, out)

    # (옵션) 구글시트 쓰기
    today_str = yymmdd(today_kst())
    sheets_write(creds, outname.replace(".xlsx", ""), pv, pivot_mode, today_str)


def main():
    # SA(있으면) 로딩 — 실패해도 계속
    sa_path = Path(os.getenv("SA_PATH", "sa.json"))
    creds = load_sa_credentials(sa_path) if sa_path.exists() else None

    driver = build_driver(TMP_DL)
    try:
        t = today_kst()

        # 전국: 최근 3개월(당월 포함; 당월은 오늘까지)
        months = [shift_months(month_first(t), k) for k in [0, -1, -2]]
        months.sort()
        for base in months:
            start = base
            end = t if (base.year, base.month) == (t.year, t.month) else (shift_months(base, +1) - timedelta(days=1))
            name = f"전국 {yymm(base)}_{yymmdd(t)}.xlsx"
            debug(f"[전국] {start} ~ {end} → {name}")
            fetch_and_process(driver, None, start, end, name, pivot_mode="national", creds=creds)

        # 서울: 전년도 10/01 ~ 오늘
        last_oct1 = date(t.year - 1, 10, 1)
        name_seoul = f"서울시 {yymmdd(t)}.xlsx"
        debug(f"[서울] {last_oct1} ~ {t} → {name_seoul}")
        fetch_and_process(driver, "서울특별시", last_oct1, t, name_seoul, pivot_mode="seoul", creds=creds)

    finally:
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    main()
