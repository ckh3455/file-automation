# -*- coding: utf-8 -*-
"""
rt_allinone.py
- rt.molit.go.kr에서 월별/서울 자료 다운로드 → 전처리 → (선택) 구글시트 기록
- CI(GitHub Actions) 환경에서 '먹통' 방지: 헤드리스, 즉시 로그flush, 타임아웃/재시도 강화
"""

from __future__ import annotations
import os, re, sys, time, json, shutil
from datetime import date, timedelta
from pathlib import Path
from typing import Optional, Tuple, List

import pandas as pd
import numpy as np

# ---- 즉시 flush ----
print = lambda *a, **k: (sys.__stdout__.write((" ".join(map(str,a)) + "\n")), sys.__stdout__.flush())

# -------------------------
# 설정
# -------------------------
URL = "https://rt.molit.go.kr/pt/xls/xls.do?mobileAt="
SAVE_DIR = Path(os.getenv("OUT_DIR", "./_out")).resolve()
TMP_DL   = Path("./_rt_downloads").resolve()
SAVE_DIR.mkdir(parents=True, exist_ok=True)
TMP_DL.mkdir(parents=True, exist_ok=True)

DOWNLOAD_TIMEOUT = int(os.getenv("DOWNLOAD_TIMEOUT", "90"))   # 파일 다운로드 대기 (초)
CLICK_RETRY_MAX  = int(os.getenv("CLICK_RETRY_MAX", "10"))    # 엑셀버튼 클릭 재시도 횟수
CLICK_GAP_SEC    = float(os.getenv("CLICK_GAP_SEC", "3"))     # 기간설정 후 대기
STEP_TIMEOUT     = int(os.getenv("STEP_TIMEOUT", "20"))       # 클릭 시도 한 번당 대기 상한

SHEET_ID = os.getenv("SHEET_ID", "").strip()
DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID", "").strip()
RETENTION_DAYS = int(os.getenv("DRIVE_RETENTION_DAYS", "3"))
SA_PATH = os.getenv("SA_PATH", "").strip()

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
    y += (m2-1)//12
    m2 = (m2-1)%12 + 1
    end = (date(y, m2, 1) + timedelta(days=40)).replace(day=1) - timedelta(days=1)
    return date(y, m2, min(d.day, end.day))

def yymm(d: date) -> str:
    return f"{d.year%100:02d}{d.month:02d}"

def yymmdd(d: date) -> str:
    return f"{d.year%100:02d}{d.month:02d}{d.day:02d}"

# -------------------------
# 브라우저 준비
# -------------------------
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException, ElementNotInteractableException, NoSuchElementException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

def build_driver(download_dir: Path) -> webdriver.Chrome:
    opts = Options()
    # CI 헤드리스/리눅스 안정 옵션
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1400,900")
    opts.add_experimental_option("prefs", {
        "download.default_directory": str(download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    })
    # Setup Chrome 액션이 제공한 경로
    chrome_bin = os.getenv("CHROME_BIN")
    if chrome_bin:
        opts.binary_location = chrome_bin
        print(f"CHROME_BIN={chrome_bin}")

    # 드라이버: 환경변수 지정되면 사용, 아니면 webdriver-manager로 매칭
    svc_path = os.getenv("CHROMEDRIVER_BIN") or ChromeDriverManager().install()
    print(f"CHROMEDRIVER={svc_path}")
    service = Service(svc_path)

    drv = webdriver.Chrome(service=service, options=opts)
    drv.set_page_load_timeout(60)
    return drv

# -------------------------
# 페이지 조작
# -------------------------
def find_date_inputs(driver: webdriver.Chrome) -> Tuple[object, object]:
    inputs = driver.find_elements(By.CSS_SELECTOR, "input")
    cands = []
    for el in inputs:
        try:
            t = ((el.get_attribute("value") or "") + " " + (el.get_attribute("placeholder") or "")).strip()
            if re.search(r"\d{4}-\d{2}-\d{2}", t) or "YYYY" in t or "yyyy" in t:
                cands.append(el)
        except Exception:
            pass
    if len(cands) >= 2:
        return cands[0], cands[1]
    # fallback
    text_inputs = [e for e in inputs if (e.get_attribute("type") or "").lower() in ("text", "")]
    if len(text_inputs) >= 2:
        return text_inputs[0], text_inputs[1]
    raise RuntimeError("날짜 입력 박스를 찾을 수 없습니다.")

def clear_and_type(el, s: str):
    el.click()
    el.send_keys(Keys.CONTROL, "a"); el.send_keys(Keys.DELETE); el.send_keys(s)

def set_dates(driver: webdriver.Chrome, start: date, end: date):
    s_el, e_el = find_date_inputs(driver)
    clear_and_type(s_el, start.isoformat()); time.sleep(0.2)
    clear_and_type(e_el, end.isoformat());   time.sleep(0.2)
    print(f"  - set_dates: {start} ~ {end}")

def click_excel_download(driver: webdriver.Chrome) -> bool:
    # 페이지 하단에 종종 위치하므로 스크롤
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);"); time.sleep(0.3)
    # 텍스트 기준으로 버튼 탐색 (라벨 변화 대비 contains)
    xpaths = [
        "//button[contains(.,'EXCEL')]",
        "//button[contains(normalize-space(),'EXCEL')]",
        "//button[contains(.,'다운') and contains(.,'EXCEL')]",
    ]
    btn = None
    for xp in xpaths:
        els = driver.find_elements(By.XPATH, xp)
        if els:
            btn = els[0]; break
    if not btn:
        return False
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn); time.sleep(0.2)
    try:
        btn.click()
    except (ElementClickInterceptedException, ElementNotInteractableException):
        driver.execute_script("arguments[0].click();", btn)
    # 알림 확인(있으면)
    try:
        WebDriverWait(driver, 3).until(EC.alert_is_present())
        driver.switch_to.alert.accept()
    except TimeoutException:
        pass
    return True

def wait_download(download_dir: Path, before: set[Path], timeout: int = DOWNLOAD_TIMEOUT) -> Path:
    t0 = time.time()
    while time.time() - t0 < timeout:
        now = set(download_dir.glob("*"))
        new_files = [p for p in now - before if p.is_file()]
        done = [p for p in new_files if not p.name.endswith(".crdownload")]
        if done:
            return max(done, key=lambda p: p.stat().st_mtime)
        time.sleep(0.5)
    raise TimeoutError("다운로드 대기 초과")

# -------------------------
# 전처리 (읽기 + 정리)
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
        max_scan = min(100, len(df0))
        for i in range(max_scan):
            row = df0.iloc[i].astype(str).str.strip().tolist()
            if row and (row[0].upper()=="NO"):
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
    rename_map = {}
    for c in df.columns:
        k = str(c).replace(" ", "")
        if k == "거래금액(만원)" and c != "거래금액(만원)": rename_map[c] = "거래금액(만원)"
        if k == "전용면적(㎡)" and c != "전용면적(㎡)": rename_map[c] = "전용면적(㎡)"
    if rename_map:
        df = df.rename(columns=rename_map)
    for c in list(df.columns):
        if str(c).strip().upper()=="NO":
            df = df[df[c].notna()].drop(columns=[c])
    for c in ["거래금액(만원)","전용면적(㎡)"]:
        if c in df.columns:
            df[c] = (df[c].astype(str)
                           .str.replace(",","",regex=False)
                           .str.replace(" ","",regex=False)
                           .str.replace("-","",regex=False)
                           .replace({"": np.nan}))
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if "시군구" in df.columns:
        parts = df["시군구"].astype(str).str.split(expand=True, n=2)
        for i, name in enumerate(["광역","구","법정동"]):
            df[name] = parts[i].fillna("") if parts.shape[1]>i else ""
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
        return pv.sort_index(axis=1).reset_index()
    return pd.DataFrame()

def save_excel(path: Path, df: pd.DataFrame, pivot: Optional[pd.DataFrame], pivot_name="피벗"):
    from openpyxl import Workbook  # ensure engine import works in CI
    with pd.ExcelWriter(path, engine="openpyxl") as xw:
        df.to_excel(xw, index=False, sheet_name="data")
        if pivot is not None and not pivot.empty:
            pivot.to_excel(xw, index=False, sheet_name=pivot_name)
    print(f"완료: {path}")

# -------------------------
# Google Sheets 기록(간단)
# -------------------------
def write_to_sheets(national_outputs: List[tuple[date, pd.DataFrame]], seoul_pivot: Optional[pd.DataFrame]):
    if not SHEET_ID or not SA_PATH or not Path(SA_PATH).exists():
        print("  - skip sheets: SHEET_ID/SA_PATH not set")
        return
    import gspread
    from google.oauth2.service_account import Credentials
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(SA_PATH, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)

    # 전국: 각 월 pivot -> “전국 YY년 M월” 시트에 지역별 건수 쓰기 (간단예시)
    for base, pv in national_outputs:
        if pv is None or pv.empty: continue
        title = f"전국 {base.year%100:02d}년 {base.month}월"
        try:
            ws = sh.worksheet(title)
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(title=title, rows="200", cols="10")
        values = [["광역","건수"]] + pv[["광역","건수"]].values.tolist()
        ws.clear()
        ws.update("A1", values)
        print(f"  - sheets updated: {title}")

    # 서울: 피벗을 “서울 집계” 시트에 통째로 갱신(간단예시)
    if seoul_pivot is not None and not seoul_pivot.empty:
        title = "서울 집계"
        try:
            ws = sh.worksheet(title)
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(title=title, rows="200", cols="20")
        values = [seoul_pivot.columns.tolist()] + seoul_pivot.values.tolist()
        ws.clear()
        ws.update("A1", values)
        print(f"  - sheets updated: {title}")

# -------------------------
# 한 번의 다운로드 + 전처리 + 저장
# -------------------------
def fetch_and_process(driver: webdriver.Chrome,
                      sido: Optional[str],
                      start: date, end: date,
                      outname: str,
                      pivot_mode: str) -> Optional[pd.DataFrame]:
    driver.get(URL)
    set_dates(driver, start, end)

    time.sleep(CLICK_GAP_SEC)

    # 다운로드 버튼 재시도 루프 (페이지 리프레시 포함)
    ok = False
    for attempt in range(1, CLICK_RETRY_MAX+1):
        before = set(TMP_DL.glob("*"))
        if click_excel_download(driver):
            print(f"  - click_download attempt {attempt}")
            try:
                got = wait_download(TMP_DL, before, timeout=STEP_TIMEOUT)
                print(f"  - got file: {got}  size={got.stat().st_size:,}  ext={got.suffix}")
                ok = True
                break
            except TimeoutError:
                print(f"  - warn: 다운로드 시작 감지 실패(시도 {attempt}/{CLICK_RETRY_MAX})")
        else:
            print(f"  - warn: 버튼 탐색 실패(시도 {attempt}/{CLICK_RETRY_MAX})")
        driver.refresh()
        time.sleep(2)

    if not ok:
        print("  ! 실패: 다운로드 시작 감지 실패")
        return None

    # 읽고 전처리
    df_raw = read_table(got)
    print(f"  - parsed: rows={len(df_raw)}  cols={df_raw.shape[1]}")
    split_month = (pivot_mode == "seoul")
    df = clean_df(df_raw, split_month=split_month)

    # 피벗
    pv = pivot_national(df) if pivot_mode=="national" else pivot_seoul(df)

    out = SAVE_DIR / outname
    save_excel(out, df, pv)
    return pv

# -------------------------
# 메인
# -------------------------
def main():
    # SA 존재 확인(로그만)
    if SA_PATH and Path(SA_PATH).exists():
        try:
            with open(SA_PATH, "r", encoding="utf-8") as f:
                _j = json.load(f)
            print("service account loaded:", _j.get("client_email"))
        except Exception as e:
            print("  ! service account load failed:", e)

    driver = build_driver(TMP_DL)
    national_pivots: List[tuple[date, pd.DataFrame]] = []
    seoul_pivot: Optional[pd.DataFrame] = None

    try:
        t = today_kst()

        # 전국: 최근 3개월(당월 포함). 당월은 오늘까지만
        months = [shift_months(month_first(t), k) for k in [0, -1, -2]]
        months.sort()
        for base in months:
            start = base
            end = min(shift_months(base, +1) - timedelta(days=1), t)
            name = f"전국 {yymm(base)}_{yymmdd(t)}.xlsx"
            print(f"[전국] {start} ~ {end} → {name}")
            pv = fetch_and_process(driver, None, start, end, name, pivot_mode="national")
            if pv is not None:
                national_pivots.append((base, pv))
            time.sleep(1)

        # 서울: 전년도 10월1일 ~ 오늘 (한번에)
        start_seoul = date(t.year-1, 10, 1)
        if start_seoul > t:
            start_seoul = date(t.year, 1, 1)
        name_seoul = f"서울시 {yymmdd(t)}.xlsx"
        print(f"[서울] {start_seoul} ~ {t} → {name_seoul}")
        seoul_pivot = fetch_and_process(driver, "서울특별시", start_seoul, t, name_seoul, pivot_mode="seoul")

    finally:
        try:
            driver.quit()
        except Exception:
            pass

    # 구글시트 기록(간단 버전)
    try:
        write_to_sheets(national_pivots, seoul_pivot)
    except Exception as e:
        print("  - sheets write skipped/error:", e)

if __name__ == "__main__":
    main()
