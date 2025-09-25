# -*- coding: utf-8 -*-
"""
rt_allinone.py — 깃헙 액션(헤드리스) 최적화 버전
- 날짜 설정 후 바로 다운로드(검색 버튼 없음)
- 알림(처리중…) 자동 확인
- 헤드리스에서 다운로드 허용(CDP)
- CI 환경에서는 재시도/대기 짧게
- 전국: 최근 3개월(당월은 오늘까지만), 서울: 전년도 10/1 ~ 오늘 1회 다운로드
"""

from __future__ import annotations
import os, re, time, sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple, List

import pandas as pd
import numpy as np

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.alert import Alert
from selenium.common.exceptions import (
    TimeoutException, ElementClickInterceptedException, ElementNotInteractableException, NoSuchElementException
)
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# -------------------------
# 환경/설정
# -------------------------
URL = "https://rt.molit.go.kr/pt/xls/xls.do?mobileAt="

# CI(깃헙액션) 감지
IS_CI = (os.getenv("CI") == "1") or (os.getenv("GITHUB_ACTIONS") == "true")

# 타이밍(헤드리스는 더 짧게)
RETRY_MAX   = 6 if IS_CI else 10      # 다운로드 버튼 재시도 횟수
RETRY_WAIT  = 12 if IS_CI else 30      # 재시도 간격(초)
DOWNLOAD_TIMEOUT = 120 if IS_CI else 180
COOLDOWN    = 2 if IS_CI else 4        # 월별 사이 살짝 쉬기
AFTER_DATE_SET_WAIT = 2 if IS_CI else 3  # 날짜 입력 후 안정화 대기

# 저장 폴더
WORKDIR = Path(os.getenv("GITHUB_WORKSPACE", Path.cwd()))
SAVE_DIR = WORKDIR / "out"
TMP_DL   = WORKDIR / "_rt_downloads"
SAVE_DIR.mkdir(parents=True, exist_ok=True)
TMP_DL.mkdir(parents=True, exist_ok=True)

# 로그 헬퍼
def log(msg: str):
    print(msg, flush=True)

# -------------------------
# 날짜 유틸
# -------------------------
def today_kst() -> date:
    # 워크플로에서 TZ=Asia/Seoul 이 설정되어 있으면 로컬 today()로 충분
    return date.today()

def month_first(d: date) -> date:
    return date(d.year, d.month, 1)

def month_last(d: date) -> date:
    # d가 속한 달의 말일
    first_next = (date(d.year, d.month, 1) + timedelta(days=40)).replace(day=1)
    return first_next - timedelta(days=1)

def yymm(d: date) -> str:
    return f"{d.year%100:02d}{d.month:02d}"

def yymmdd(d: date) -> str:
    return f"{d.year%100:02d}{d.month:02d}{d.day:02d}"

# -------------------------
# 브라우저 (헤드리스 최적화)
# -------------------------
def build_driver(download_dir: Path) -> webdriver.Chrome:
    opts = Options()
    # 액션에서 깔아둔 경로(없으면 기본)
    chrome_bin = os.getenv("CHROME_BIN") or "/usr/bin/chromium-browser"
    driver_bin = os.getenv("CHROMEDRIVER_BIN") or "/usr/bin/chromedriver"
    if Path(chrome_bin).exists():
        opts.binary_location = chrome_bin

    # 헤드리스 필수 옵션
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--lang=ko-KR")
    opts.add_argument("--window-size=1400,900")
    # 자동화 감춤
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("--disable-blink-features=AutomationControlled")

    prefs = {
        "download.default_directory": str(download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    opts.add_experimental_option("prefs", prefs)

    service = Service(driver_bin) if Path(driver_bin).exists() else Service()
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_window_size(1400, 900)

    # 헤드리스 다운로드 허용
    try:
        driver.execute_cdp_cmd(
            "Page.setDownloadBehavior",
            {"behavior": "allow", "downloadPath": str(download_dir)}
        )
    except Exception:
        pass

    return driver

# -------------------------
# 페이지 조작
# -------------------------
def find_date_inputs(driver: webdriver.Chrome):
    # 시작/종료 날짜 INPUT 두 개 탐색
    inputs = driver.find_elements(By.CSS_SELECTOR, "input")
    cands = []
    for el in inputs:
        v = (el.get_attribute("value") or "") + " " + (el.get_attribute("placeholder") or "")
        if re.search(r"\d{4}-\d{2}-\d{2}", v) or ("YYYY" in v) or ("yyyy" in v):
            cands.append(el)
    if len(cands) >= 2:
        return cands[0], cands[1]
    raise RuntimeError("날짜 입력 박스를 찾지 못했습니다.")

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
    time.sleep(AFTER_DATE_SET_WAIT)
    log(f"  - set_dates: {start} ~ {end}")

def select_sido(driver: webdriver.Chrome, wanted: str) -> bool:
    selects = driver.find_elements(By.TAG_NAME, "select")
    for sel in selects:
        try:
            opts = sel.find_elements(By.TAG_NAME, "option")
            txts = [o.text.strip() for o in opts]
            if "전체" in txts and "서울특별시" in txts:
                for o in opts:
                    if o.text.strip() == wanted:
                        o.click()
                        time.sleep(0.3)
                        return True
        except Exception:
            pass
    return False

def accept_alert_if_any(driver, wait_sec=2):
    try:
        WebDriverWait(driver, wait_sec).until(EC.alert_is_present())
        Alert(driver).accept()
        time.sleep(0.3)
        return True
    except TimeoutException:
        return False

def click_download(driver, kind="excel", max_try=RETRY_MAX) -> bool:
    label = "EXCEL 다운" if kind == "excel" else "CSV 다운"
    for i in range(1, max_try+1):
        # 잔여 알림 정리
        accept_alert_if_any(driver, wait_sec=1)

        # 버튼 찾기
        btns = driver.find_elements(By.XPATH, f"//button[normalize-space()='{label}']")
        if not btns:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(0.4)
            btns = driver.find_elements(By.XPATH, f"//button[normalize-space()='{label}']")
        if not btns:
            time.sleep(1.0); continue

        btn = btns[0]
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
        time.sleep(0.2)
        try:
            btn.click()
        except (ElementClickInterceptedException, ElementNotInteractableException):
            driver.execute_script("arguments[0].click();", btn)

        # “처리중입니다…” 알림 확인/닫기
        accept_alert_if_any(driver, wait_sec=5)

        log(f"  - click_download({kind}) / attempt {i}")
        return True

    return False

def wait_download(download_dir: Path, before: set[Path], timeout: int = DOWNLOAD_TIMEOUT) -> Path:
    t0 = time.time()
    while time.time() - t0 < timeout:
        now = set(download_dir.glob("*"))
        new_files = [p for p in now - before if p.is_file()]
        done = [p for p in new_files if not p.name.endswith(".crdownload")]
        # xlsx/xls/htm 모두 허용(사이트가 엑셀 내보내기 html을 주기도 함)
        for p in sorted(done, key=lambda x: x.stat().st_mtime, reverse=True):
            if p.suffix.lower() in (".xlsx", ".xls", ".htm", ".html"):
                log(f"  - got file: {p}  size={p.stat().st_size:,}  ext={p.suffix.lower()}")
                return p
        time.sleep(0.5)
    raise TimeoutError("다운로드 대기 초과")

# -------------------------
# 전처리
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
        # 헤더 찾기
        hdr_idx = None
        for i in range(min(80, len(df0))):
            row = df0.iloc[i].astype(str).str.strip().tolist()
            if row and (row[0].upper() == "NO" or ("시군구" in row and "단지명" in row)):
                hdr_idx = i; break
        if hdr_idx is None:
            return _read_html_table(path)
        cols = df0.iloc[hdr_idx].astype(str).str.strip()
        df = df0.iloc[hdr_idx+1:].copy()
        df.columns = cols
        return df.reset_index(drop=True)
    else:
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
            df[name] = parts[i].fillna("") if parts.shape[1] > i else ""

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
        pv = pv.sort_index(axis=1).reset_index()
        return pv
    return pd.DataFrame()

def save_excel(path: Path, df: pd.DataFrame, pivot: Optional[pd.DataFrame], pivot_name="피벗"):
    with pd.ExcelWriter(path, engine="openpyxl") as xw:
        df.to_excel(xw, index=False, sheet_name="data")
        if pivot is not None and not pivot.empty:
            pivot.to_excel(xw, index=False, sheet_name=pivot_name)

# -------------------------
# 다운로드+전처리 한 묶음
# -------------------------
def fetch_and_process(driver: webdriver.Chrome,
                      sido: Optional[str],
                      start: date, end: date,
                      outname: str,
                      pivot_mode: str) -> None:
    driver.get(URL)
    set_dates(driver, start, end)

    if sido:
        if not select_sido(driver, sido):
            log("  ! 시도 드롭다운 못찾음(무시하고 진행)")

    kind = "excel"
    before = set(TMP_DL.glob("*"))
    ok = False
    for attempt in range(1, RETRY_MAX+1):
        if click_download(driver, kind):
            ok = True
            break
        time.sleep(RETRY_WAIT)
    if not ok:
        raise RuntimeError("다운로드 버튼 클릭 실패")

    got = wait_download(TMP_DL, before, timeout=DOWNLOAD_TIMEOUT)

    df_raw = read_table(got)
    split_month = (pivot_mode == "seoul")
    df = clean_df(df_raw, split_month=split_month)

    pv = pivot_national(df) if pivot_mode == "national" else pivot_seoul(df)

    out = SAVE_DIR / outname
    save_excel(out, df, pv)
    log(f"완료: {out}")

# -------------------------
# 메인
# -------------------------
def main():
    t = today_kst()
    SAVE_DIR.mkdir(exist_ok=True); TMP_DL.mkdir(exist_ok=True)

    driver = build_driver(TMP_DL)
    try:
        # 전국: 최근 3개월 (오래된 달부터). 당월은 오늘까지만.
        bases = [month_first(t) + timedelta(days=0)]
        bases += [month_first(t) - timedelta(days=1)]  # 지난달 末
        bases = [month_first(b) for b in [bases[0], bases[1], month_first(bases[1]-timedelta(days=1))]]
        months = sorted(set(bases))  # 중복 제거 + 정렬

        for base in months:
            m_start = base
            m_end = month_last(base)
            if base.year == t.year and base.month == t.month:
                m_end = t  # 당월은 오늘까지만
            name = f"전국 {yymm(base)}_{yymmdd(t)}.xlsx"
            log(f"[전국] {m_start} ~ {m_end} → {name}")
            try:
                fetch_and_process(driver, None, m_start, m_end, name, pivot_mode="national")
            except Exception as e:
                log(f"  ! 전국 {yymm(base)} 실패: {e}")
            time.sleep(COOLDOWN)

        # 서울: 전년도 10/1 ~ 오늘(1회)
        year0 = t.year - 1
        start_seoul = date(year0, 10, 1)
        if start_seoul > t:
            start_seoul = date(t.year, 1, 1)
        name_seoul = f"서울시 {yymmdd(t)}.xlsx"
        log(f"[서울] {start_seoul} ~ {t} → {name_seoul}")
        try:
            fetch_and_process(driver, "서울특별시", start_seoul, t, name_seoul, pivot_mode="seoul")
        except Exception as e:
            log(f"  ! 서울 실패: {e}")

    finally:
        try:
            driver.quit()
        except Exception:
            pass

if __name__ == "__main__":
    main()
