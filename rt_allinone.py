# -*- coding: utf-8 -*-
"""
rt_allinone.py
- rt.molit.go.kr 조건별 자료제공 페이지에서 월별/서울 데이터를 자동 다운로드
- 다운받은 원본을 전처리(행/열 정리, 숫자화, 주소 분리) 후 엑셀 저장 + 간단 피벗
- 전국: 최근 3개월(과거 2개월 + 당월은 오늘까지)
- 서울: 전년도 10월 1일 ~ 오늘(1회) → 구별 x 월 피벗
- (선택) Google Drive 업로드 + 오래된 파일 자동 삭제(보관일수)
- (선택) Google Sheets 기록
"""

from __future__ import annotations
import os, re, sys, json, time, tempfile
from datetime import date, timedelta, datetime, timezone
from pathlib import Path
from typing import Optional, Tuple, List, Dict

import pandas as pd
import numpy as np

# Selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import (
    TimeoutException, ElementClickInterceptedException, ElementNotInteractableException
)
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Google API (optional)
try:
    import gspread
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload
except Exception:
    gspread = None
    Credentials = None
    build = None
    MediaFileUpload = None

# -------------------------
# 환경설정
# -------------------------
URL = "https://rt.molit.go.kr/pt/xls/xls.do?mobileAt="
ROOT_DIR = Path.cwd()
SAVE_DIR = ROOT_DIR / "_out"
TMP_DL   = ROOT_DIR / "_rt_downloads"
SAVE_DIR.mkdir(parents=True, exist_ok=True)
TMP_DL.mkdir(parents=True, exist_ok=True)

WAIT_AFTER_SETDATES   = 3
CLICK_MAX_TRY         = 10
DOWNLOAD_TIMEOUT_EACH = 30
COOLDOWN_BETWEEN_JOBS = 2

ENV_SA_PATH  = os.environ.get("SA_PATH", str(ROOT_DIR / "sa.json"))
ENV_SHEET_ID = os.environ.get("SHEET_ID", "")
ENV_FOLDERID = os.environ.get("DRIVE_FOLDER_ID", "")
ENV_RET_DAYS = int(os.environ.get("DRIVE_RETENTION_DAYS", "3"))

# -------------------------
# 날짜 유틸
# -------------------------
def today_kst() -> date:
    return date.today()

def month_first(d: date) -> date:
    return date(d.year, d.month, 1)

def shift_months(d: date, k: int) -> date:
    y, m = d.year, d.month
    m2 = m + k
    y += (m2 - 1) // 12
    m2 = (m2 - 1) % 12 + 1
    end = (date(y, m2, 1) + timedelta(days=40)).replace(day=1) - timedelta(days=1)
    return date(y, m2, min(d.day, end.day))

def last_day_of_month(base: date) -> date:
    return (shift_months(month_first(base), +1) - timedelta(days=1))

def yymm(d: date) -> str:
    return f"{d.year%100:02d}{d.month:02d}"

def yymmdd(d: date) -> str:
    return f"{d.year%100:02d}{d.month:02d}{d.day:02d}"

# -------------------------
# 브라우저
# -------------------------
def build_driver(download_dir: Path) -> webdriver.Chrome:
    opts = Options()
    chrome_bin = os.environ.get("CHROME_BIN")
    if chrome_bin:
        opts.binary_location = chrome_bin

    ua = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
          "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1400,900")
    opts.add_argument("--remote-debugging-port=0")
    opts.add_argument("--lang=ko-KR")
    opts.add_argument(f"--user-agent={ua}")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    prefs = {
        "download.default_directory": str(download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    opts.add_experimental_option("prefs", prefs)

    # 매 실행마다 고유 프로필 (세션 충돌 방지)
    profile_dir = Path(tempfile.mkdtemp(prefix="_chrome_profile_"))
    opts.add_argument(f"--user-data-dir={profile_dir}")

    cd_bin = os.environ.get("CHROMEDRIVER_BIN")
    if cd_bin and Path(cd_bin).exists():
        service = Service(cd_bin)
    else:
        from webdriver_manager.chrome import ChromeDriverManager
        service = Service(ChromeDriverManager().install())

    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_window_size(1400, 900)
    try:
        driver.execute_cdp_cmd("Page.setDownloadBehavior", {
            "behavior": "allow",
            "downloadPath": str(download_dir)
        })
        driver.execute_cdp_cmd("Network.enable", {})
        driver.execute_cdp_cmd("Network.setExtraHTTPHeaders", {
            "headers": {"Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7"}
        })
        driver.execute_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
    except Exception:
        pass
    return driver

def dump_debug_page(driver: webdriver.Chrome, tag: str):
    try:
        html_path = ROOT_DIR / f"page_dump_{tag}.html"
        png_path  = ROOT_DIR / f"page_dump_{tag}.png"
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        driver.save_screenshot(str(png_path))
        print(f"  - debug dump: {html_path.name}, {png_path.name}")
    except Exception as e:
        print(f"  - debug dump failed: {e}")

# -------------------------
# 페이지 조작
# -------------------------
def find_date_inputs(driver: webdriver.Chrome):
    inputs = driver.find_elements(By.CSS_SELECTOR, "input")
    cands = []
    for el in inputs:
        try:
            t = (el.get_attribute("value") or "") + " " + (el.get_attribute("placeholder") or "")
            t = t.strip()
            if re.search(r"\d{4}-\d{2}-\d{2}", t) or "YYYY" in t or "yyyy" in t or "YYYY-MM-DD" in t:
                cands.append(el)
        except Exception:
            pass
    if len(cands) >= 2:
        return cands[0], cands[1]
    text_inputs = [e for e in inputs if (e.get_attribute("type") or "").lower() in ("text", "")]
    if len(text_inputs) >= 2:
        return text_inputs[0], text_inputs[1]
    raise RuntimeError("날짜 입력 박스를 찾을 수 없습니다.")

def clear_and_type(el, s: str):
    el.click()
    el.send_keys(Keys.CONTROL, "a")
    el.send_keys(Keys.DELETE)
    el.send_keys(s)

def set_dates(driver: webdriver.Chrome, start: date, end: date):
    s_el, e_el = find_date_inputs(driver)
    clear_and_type(s_el, start.isoformat())
    time.sleep(0.2)
    clear_and_type(e_el, end.isoformat())
    time.sleep(0.2)

def select_sido(driver: webdriver.Chrome, wanted: str) -> bool:
    selects = driver.find_elements(By.TAG_NAME, "select")
    wanted = wanted.strip()
    for sel in selects:
        try:
            opts = sel.find_elements(By.TAG_NAME, "option")
            txts = [o.text.strip() for o in opts]
            if "전체" in txts and "서울특별시" in txts:
                for o in opts:
                    if o.text.strip() == wanted:
                        o.click(); time.sleep(0.2)
                        return True
        except Exception:
            pass
    return False

def wait_download(download_dir: Path, before: set[Path], timeout: int) -> Path:
    t0 = time.time()
    while time.time() - t0 < timeout:
        now = set(download_dir.glob("*"))
        new_files = [p for p in now - before if p.is_file()]
        done = [p for p in new_files if not p.name.endswith(".crdownload")]
        if done:
            return max(done, key=lambda p: p.stat().st_mtime)
        time.sleep(0.5)
    raise TimeoutError("다운로드 대기 초과")

def robust_download(driver: webdriver.Chrome,
                    start: date, end: date,
                    sido: Optional[str],
                    kind="excel") -> Path:
    driver.get(URL)
    set_dates(driver, start, end)
    if sido:
        select_sido(driver, sido)
    time.sleep(WAIT_AFTER_SETDATES)

    label = "EXCEL 다운" if kind == "excel" else "CSV 다운"

    for attempt in range(1, CLICK_MAX_TRY + 1):
        print(f"  - attempt {attempt}/{CLICK_MAX_TRY}: title='{driver.title}' url='{driver.current_url}'")
        try:
            btn = WebDriverWait(driver, 8).until(
                EC.element_to_be_clickable((By.XPATH, f"//button[contains(normalize-space(), '{label}')]"))
            )
            print("    · button=found(clickable)")
        except TimeoutException:
            btns = driver.find_elements(By.XPATH, f"//button[contains(normalize-space(), '{label}')]")
            print(f"    · button=count={len(btns)} (not clickable yet)")
            dump_debug_page(driver, f"noclick_{attempt}")
            driver.refresh()
            time.sleep(1.0)
            set_dates(driver, start, end)
            if sido:
                select_sido(driver, sido)
            time.sleep(WAIT_AFTER_SETDATES)
            continue

        before = set(TMP_DL.glob("*"))
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            time.sleep(0.2)
            try:
                btn.click()
            except (ElementClickInterceptedException, ElementNotInteractableException):
                driver.execute_script("arguments[0].click();", btn)
            try:
                WebDriverWait(driver, 2).until(EC.alert_is_present())
                a = driver.switch_to.alert
                print(f"    · alert: '{a.text[:80]}'")
                a.accept()
            except TimeoutException:
                pass
        except Exception as e:
            print(f"    · click error: {e}")
            dump_debug_page(driver, f"clickerr_{attempt}")
            driver.refresh()
            time.sleep(1.0)
            set_dates(driver, start, end)
            if sido:
                select_sido(driver, sido)
            time.sleep(WAIT_AFTER_SETDATES)
            continue

        try:
            got = wait_download(TMP_DL, before, timeout=DOWNLOAD_TIMEOUT_EACH)
            print(f"  - got file: {got}  size={got.stat().st_size:,}  ext={got.suffix}")
            return got
        except TimeoutError:
            print(f"  - warn: 다운로드 시작 감지 실패(시도 {attempt}/{CLICK_MAX_TRY})")
            dump_debug_page(driver, f"timeout_{attempt}")
            driver.refresh()
            time.sleep(1.0)
            set_dates(driver, start, end)
            if sido:
                select_sido(driver, sido)
            time.sleep(WAIT_AFTER_SETDATES)

    raise RuntimeError("다운로드 실패(최대 시도 초과)")

# -------------------------
# 읽기/전처리/피벗
# -------------------------
def _read_html_table(path: Path) -> pd.DataFrame:
    tables = pd.read_html(str(path), flavor="bs4", thousands=",", displayed_only=False)
    for t in tables:
        row0 = [str(x).strip() for x in list(t.columns)]
        if ("시군구" in row0 and "단지명" in row0) or ("NO" in row0 and "시군구" in row0):
            return t
        ser0 = t.iloc[:,0].astype(str).str.strip()
        idx = ser0[ser0.eq("NO")].index.tolist()
        if idx:
            hdr = idx[0]
            tt = t.iloc[hdr+1:].copy()
            tt.columns = t.iloc[hdr].astype(str).str.strip()
            return tt
    return tables[0]

def read_table(path: Path) -> pd.DataFrame:
    ext = path.suffix.lower()
    if ext in (".xlsx", ".xls"):
        try:
            df0 = pd.read_excel(path, header=None, dtype=str, engine="openpyxl" if ext==".xlsx" else None)
        except Exception:
            return _read_html_table(path)
        hdr_idx = None
        max_scan = min(80, len(df0))
        for i in range(max_scan):
            row = df0.iloc[i].astype(str).str.strip().tolist()
            if row and (row[0] in ("NO","No","no")):
                hdr_idx = i; break
            if ("시군구" in row) and ("단지명" in row):
                hdr_idx = i; break
        if hdr_idx is None:
            return _read_html_table(path)
        cols = df0.iloc[hdr_idx].astype(str).str.strip()
        df = df0.iloc[hdr_idx+1:].copy()
        df.columns = cols
        return df.reset_index(drop=True)
    return _read_html_table(path)

def clean_df(df: pd.DataFrame, split_month: bool) -> pd.DataFrame:
    if "시군구 " in df.columns and "시군구" not in df.columns:
        df = df.rename(columns={"시군구 ":"시군구"})
    rename_map: Dict[str,str] = {}
    for c in df.columns:
        k = str(c).replace(" ", "")
        if k == "거래금액(만원)" and c != "거래금액(만원)": rename_map[c] = "거래금액(만원)"
        if k == "전용면적(㎡)" and c != "전용면적(㎡)": rename_map[c] = "전용면적(㎡)"
    if rename_map:
        df = df.rename(columns=rename_map)
    for c in list(df.columns):
        if str(c).strip().upper() == "NO":
            df = df[df[c].notna()].drop(columns=[c])
    for c in ["거래금액(만원)","전용면적(㎡)"]:
        if c in df.columns:
            df[c] = (df[c].astype(str)
                           .str.replace(",", "", regex=False)
                           .str.replace(" ", "", regex=False)
                           .str.replace("-", "", regex=False)
                           .replace({"": np.nan}))
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if "시군구" in df.columns:
        parts = df["시군구"].astype(str).str.split(expand=True, n=2)
        for i, name in enumerate(["광역","구","법정동"]):
            if parts.shape[1] > i:
                df[name] = parts[i].fillna("")
            else:
                df[name] = ""
    if split_month and "계약년월" in df.columns:
        s = df["계약년월"].astype(str).str.replace(r"\D","", regex=True)
        df["계약년"] = s.str.slice(0,4)
        df["계약월"] = s.str.slice(4,6)
    return df.reset_index(drop=True)

def pivot_national(df: pd.DataFrame) -> pd.DataFrame:
    if "광역" in df.columns:
        pv = df.pivot_table(index="광역", values="거래금액(만원)", aggfunc="count").rename(columns={"거래금액(만원)":"건수"})
        return pv.reset_index()
    return pd.DataFrame()

def pivot_seoul(df: pd.DataFrame) -> pd.DataFrame:
    if {"구","계약월"}.issubset(df.columns):
        pv = df.pivot_table(index="구", columns="계약월", values="거래금액(만원)", aggfunc="count", fill_value=0)
        pv = pv.sort_index(axis=1)
        return pv.reset_index()
    return pd.DataFrame()

def save_excel(path: Path, df: pd.DataFrame, pivot: Optional[pd.DataFrame], pivot_name="피벗"):
    with pd.ExcelWriter(path, engine="openpyxl") as xw:
        df.to_excel(xw, index=False, sheet_name="data")
        if pivot is not None and not pivot.empty:
            pivot.to_excel(xw, index=False, sheet_name=pivot_name)

# -------------------------
# Google 연동
# -------------------------
def load_service_account(sa_path: str):
    if not Credentials:
        print("  ! google libraries not available; skip google features")
        return None, None, None
    try:
        with open(sa_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        scopes = [
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/spreadsheets"
        ]
        creds = Credentials.from_service_account_info(data, scopes=scopes)
        drive = build("drive", "v3", credentials=creds)
        gc = gspread.authorize(creds)
        return creds, drive, gc
    except Exception as e:
        print(f"  ! service account load failed: {e}")
        return None, None, None

def upload_to_drive(drive, folder_id: str, file_path: Path) -> Optional[str]:
    if not drive or not folder_id:
        return None
    meta = {"name": file_path.name, "parents": [folder_id]}
    media = MediaFileUpload(str(file_path), resumable=True)
    file = drive.files().create(body=meta, media_body=media, fields="id").execute()
    fid = file.get("id")
    print(f"  - drive uploaded: id={fid}")
    return fid

def cleanup_drive(drive, folder_id: str, keep_days: int):
    if not drive or not folder_id or keep_days <= 0:
        return
    cutoff = datetime.now(timezone.utc) - timedelta(days=keep_days)
    q = f"'{folder_id}' in parents and trashed = false"
    page_token = None
    to_delete = []
    while True:
        resp = drive.files().list(q=q, fields="nextPageToken, files(id, name, modifiedTime)", pageToken=page_token).execute()
        for f in resp.get("files", []):
            mtime = datetime.fromisoformat(f["modifiedTime"].replace("Z","+00:00"))
            if mtime < cutoff:
                to_delete.append((f["id"], f["name"], mtime))
        page_token = resp.get("nextPageToken")
        if not page_token: break
    for fid, name, mt in to_delete:
        try:
            drive.files().delete(fileId=fid).execute()
            print(f"  - drive deleted: {name} ({fid})")
        except Exception as e:
            print(f"  - drive delete failed: {name} ({fid}) - {e}")

def _ensure_worksheet(gc, sheet_id: str, title: str):
    try:
        sh = gc.open_by_key(sheet_id)
        try:
            ws = sh.worksheet(title)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=title, rows=200, cols=50)
        return ws
    except Exception as e:
        print(f"  ! sheet open/create failed: {e}")
        return None

def _ensure_header(ws, header: List[str]) -> List[str]:
    cur = ws.row_values(1)
    if not cur:
        ws.update('A1', [header])
        return header
    if len(cur) < len(header):
        cur = cur + [""]*(len(header)-len(cur))
    for i, h in enumerate(header, start=1):
        if not cur[i-1]:
            cur[i-1] = h
    ws.update('A1', [cur])
    return cur

def _find_or_append_date_row(ws, date_str: str) -> int:
    colA = ws.col_values(1)
    for idx, val in enumerate(colA, start=1):
        if idx == 1: continue
        if str(val).strip() == date_str:
            return idx
    next_row = len(colA) + 1 if colA else 2
    ws.update_cell(next_row, 1, date_str)
    return next_row

def _ensure_columns(ws, headers: List[str]) -> List[str]:
    cur = ws.row_values(1)
    if not cur:
        ws.update('A1', [headers]); return headers
    missing = [h for h in headers if h not in cur]
    if missing:
        cur = cur + missing
        ws.update('A1', [cur])
    return cur

def sheet_title_national(y: int, m: int) -> List[str]:
    yy = y % 100
    return [f"전국 {yy:02d}년 {m}월", f"전국 {yy:02d}년 {m:02d}월"]

def sheet_title_seoul(y: int, m: int) -> List[str]:
    yy = y % 100
    return [f"서울 {yy:02d}년 {m}월", f"서울 {yy:02d}년 {m:02d}월"]

def write_national_to_sheet(gc, sheet_id: str, when: date, pv: pd.DataFrame):
    if gc is None or not sheet_id or pv.empty: return
    titles = sheet_title_national(when.year, when.month)
    ws = None
    for t in titles:
        ws = _ensure_worksheet(gc, sheet_id, t)
        if ws: break
    if ws is None: return
    regions = pv["광역"].astype(str).tolist()
    header = ["날짜"] + regions
    header = _ensure_header(ws, header)
    dstr = when.isoformat()
    r = _find_or_append_date_row(ws, dstr)
    header = _ensure_columns(ws, ["날짜"] + regions)
    counts = {row["광역"]: int(row["건수"]) for _, row in pv.iterrows()}
    row_vals = [dstr] + [counts.get(col, "") for col in header[1:]]
    ws.update(f"A{r}", [row_vals])

def write_seoul_to_sheet(gc, sheet_id: str, when: date, pv: pd.DataFrame):
    if gc is None or not sheet_id or pv.empty: return
    cols = [c for c in pv.columns if c not in ("구",)]
    for mon in cols:
        try:
            m = int(str(mon).lstrip("0") or "0")
        except Exception:
            continue
        titles = sheet_title_seoul(when.year, m)
        ws = None
        for t in titles:
            ws = _ensure_worksheet(gc, sheet_id, t)
            if ws: break
        if ws is None: continue
        gus = pv["구"].astype(str).tolist()
        header = _ensure_header(ws, ["날짜"] + gus)
        dstr = when.isoformat()
        r = _find_or_append_date_row(ws, dstr)
        header = _ensure_columns(ws, ["날짜"] + gus)
        val_by_gu = {row["구"]: int(row.get(mon, 0)) for _, row in pv.iterrows()}
        row_vals = [dstr] + [val_by_gu.get(col, "") for col in header[1:]]
        ws.update(f"A{r}", [row_vals])

# -------------------------
# 파이프라인
# -------------------------
def process_one(driver: webdriver.Chrome,
                start: date, end: date,
                outname: str,
                sido: Optional[str],
                pivot_mode: str,
                gc, drive):
    print(f"[작업] {start.isoformat()} ~ {end.isoformat()} → {outname}")
    got = robust_download(driver, start, end, sido, kind="excel")

    df_raw = read_table(got)
    split_month = (pivot_mode == "seoul")
    df = clean_df(df_raw, split_month=split_month)
    pv = pivot_national(df) if pivot_mode == "national" else pivot_seoul(df)

    out = SAVE_DIR / outname
    with pd.ExcelWriter(out, engine="openpyxl") as xw:
        df.to_excel(xw, index=False, sheet_name="data")
        if not pv.empty:
            pv.to_excel(xw, index=False, sheet_name="피벗")
    print(f"  - saved: {out}")

    if ENV_FOLDERID:
        try:
            upload_to_drive(drive, ENV_FOLDERID, out)
            cleanup_drive(drive, ENV_FOLDERID, ENV_RET_DAYS)
        except Exception as e:
            print(f"  - drive step failed: {e}")

    try:
        if pivot_mode == "national":
            write_national_to_sheet(gc, ENV_SHEET_ID, today_kst(), pv)
        else:
            write_seoul_to_sheet(gc, ENV_SHEET_ID, today_kst(), pv)
    except Exception as e:
        print(f"  - sheet write failed: {e}")

    time.sleep(COOLDOWN_BETWEEN_JOBS)

def main():
    print("== env ==")
    print(f"  SA_PATH={ENV_SA_PATH}")
    print(f"  SHEET_ID={'set' if ENV_SHEET_ID else 'not set'}")
    print(f"  DRIVE_FOLDER_ID={'set' if ENV_FOLDERID else 'not set'}  RET_DAYS={ENV_RET_DAYS}")
    print("==========")

    creds, drive, gc = load_service_account(ENV_SA_PATH)
    driver = build_driver(TMP_DL)
    try:
        t = today_kst()

        # 전국: 과거 2개월 + 당월(오늘까지)
        months = [shift_months(month_first(t), k) for k in [-2, -1, 0]]
        for base in months:
            start = base
            end = t if (base.year == t.year and base.month == t.month) else last_day_of_month(base)
            name = f"전국 {yymm(base)}_{yymmdd(t)}.xlsx"
            process_one(driver, start, end, name, None, "national", gc, drive)

        # 서울: 전년도 10월 1일 ~ 오늘
        start_seoul = date(t.year - 1, 10, 1)
        if start_seoul > t:
            start_seoul = date(t.year, 1, 1)
        name_seoul = f"서울시 {yymmdd(t)}.xlsx"
        process_one(driver, start_seoul, t, name_seoul, "서울특별시", "seoul", gc, drive)
    finally:
        try:
            driver.quit()
        except Exception:
            pass

if __name__ == "__main__":
    main()
