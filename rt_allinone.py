# -*- coding: utf-8 -*-
"""
rt_allinone.py
- rt.molit.go.kr 조건별 자료제공 페이지에서 월별/지역별 데이터를 자동 다운로드
- 시도(전국/서울)별로 기간 지정 → EXCEL 다운 버튼 클릭
- 클릭 후 '다운로드 시작'을 30초 안에 감지하지 못하면 페이지 새로고침하고 재시도
- 시도 횟수 10회, 시작 감지 타임아웃 30초 (요청사항)
- 파일 시작이 보이면 완료(.xlsx)까지는 최대 180초 대기
- 완성 파일은 전처리(행/열 정리, 숫자화, 주소 분리) 후 엑셀 저장 + 간단 피벗
- 전국: 최근 3개월(당월은 오늘까지) → "전국 YYMM_YYMMDD.xlsx"
- 서울: 전년도 10/01 ~ 오늘 1회 다운로드 → "서울시 YYMMDD.xlsx" (구×월 건수 피벗)
"""

from __future__ import annotations
import re, time, os
from datetime import date, timedelta
from pathlib import Path
from typing import Optional, Tuple, Set

import pandas as pd
import numpy as np

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.alert import Alert
from selenium.common.exceptions import (
    TimeoutException, ElementClickInterceptedException, ElementNotInteractableException,
    UnexpectedAlertPresentException
)

# -------------------------
# 설정
# -------------------------
URL = "https://rt.molit.go.kr/pt/xls/xls.do?mobileAt="
SAVE_DIR = Path(r"D:\OneDrive\excel data").resolve()  # 결과 저장 폴더
TMP_DL   = (Path.cwd() / "_rt_downloads").resolve()   # 임시 다운로드 폴더
SAVE_DIR.mkdir(parents=True, exist_ok=True)
TMP_DL.mkdir(parents=True, exist_ok=True)

# 재시도/대기 정책
MAX_ATTEMPTS     = 10          # 각 작업(한 달 / 서울 1년)에 대한 최대 시도 횟수
START_TIMEOUT    = 30          # "다운로드 시작" 감지 타임아웃(초) ← 요청사항
FINISH_TIMEOUT   = 180         # 다운로드 완료(.xlsx)까지 대기 타임아웃(초)
AFTER_SET_WAIT   = 3           # 기간/시도 설정 후 클릭 전 대기(초)

# -------------------------
# 날짜 유틸
# -------------------------
def today_kst() -> date:
    # 로컬 기준 사용
    return date.today()

def month_first(d: date) -> date:
    return date(d.year, d.month, 1)

def month_last(d: date) -> date:
    return (month_first(d) + timedelta(days=40)).replace(day=1) - timedelta(days=1)

def shift_months(d: date, k: int) -> date:
    """d 기준으로 k개월 이동한 같은 '일'(월말 초과 시 월말로 보정)"""
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
def build_driver(download_dir: Path) -> webdriver.Chrome:
    opts = Options()
    prefs = {
        "download.default_directory": str(download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "safebrowsing.disable_download_protection": True,
    }
    opts.add_experimental_option("prefs", prefs)
    # 필요시 헤드리스 사용하려면 주석 해제
    # opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1400,900")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    return driver

# -------------------------
# 페이지 조작: 날짜, 시도(전국/서울), 다운로드
# -------------------------
def find_date_inputs(driver: webdriver.Chrome) -> Tuple[webdriver.remote.webelement.WebElement,
                                                        webdriver.remote.webelement.WebElement]:
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
    el.send_keys(Keys.CONTROL, "a")
    el.send_keys(Keys.DELETE)
    el.send_keys(s)

def set_dates(driver: webdriver.Chrome, start: date, end: date):
    s_el, e_el = find_date_inputs(driver)
    clear_and_type(s_el, start.isoformat())
    time.sleep(0.2)
    clear_and_type(e_el, end.isoformat())
    time.sleep(0.2)

def select_sido(driver: webdriver.Chrome, wanted: Optional[str]) -> None:
    """wanted가 None이면 건드리지 않음. '전체' 또는 '서울특별시' 등."""
    if not wanted:
        return
    selects = driver.find_elements(By.TAG_NAME, "select")
    wanted = wanted.strip()
    for sel in selects:
        try:
            opts = sel.find_elements(By.TAG_NAME, "option")
            txts = [o.text.strip() for o in opts]
            if ("전체" in txts and "서울특별시" in txts) or ("서울특별시" in txts and "부산광역시" in txts):
                for o in opts:
                    if o.text.strip() == wanted:
                        o.click()
                        time.sleep(0.3)
                        return
        except Exception:
            pass
    # 못 찾았어도 치명적이 아님(기본값이 전국일 수 있음)

def _dismiss_alert_if_any(driver: webdriver.Chrome, wait_sec: float = 1.0):
    try:
        WebDriverWait(driver, wait_sec).until(EC.alert_is_present())
        Alert(driver).accept()
        time.sleep(0.2)
    except TimeoutException:
        pass
    except UnexpectedAlertPresentException:
        try:
            Alert(driver).accept()
        except Exception:
            pass

def click_download(driver: webdriver.Chrome, kind: str = "excel") -> bool:
    """EXCEL 다운 버튼을 찾아 누른다. 성공적으로 클릭 동작을 보냈으면 True."""
    label = "EXCEL 다운" if kind == "excel" else "CSV 다운"

    # 잔여 알림 정리
    _dismiss_alert_if_any(driver, 0.5)

    # 버튼 찾기
    btns = driver.find_elements(By.XPATH, f"//button[normalize-space()='{label}']")
    if not btns:
        # 하단 쪽에 있을 수 있으니 스크롤 다운 후 한번 더
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(0.5)
        btns = driver.find_elements(By.XPATH, f"//button[normalize-space()='{label}']")
        if not btns:
            return False

    btn = btns[0]
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
    time.sleep(0.2)

    try:
        btn.click()
    except (ElementClickInterceptedException, ElementNotInteractableException):
        driver.execute_script("arguments[0].click();", btn)

    # “처리중입니다…” 알림이 올라오면 OK
    _dismiss_alert_if_any(driver, 3.0)
    return True

# -------------------------
# 다운로드 감시
# -------------------------
def list_files(d: Path) -> Set[Path]:
    return set(p for p in d.glob("*") if p.is_file())

def wait_download_start(download_dir: Path, before: Set[Path], timeout: int = START_TIMEOUT) -> Optional[Path]:
    """새 파일( .crdownload 또는 .xlsx 등 )이 생성되기 시작했는지 감시"""
    t0 = time.time()
    while time.time() - t0 < timeout:
        now = list_files(download_dir)
        new_files = [p for p in now - before if p.is_file()]
        if new_files:
            # 가장 최근 것을 반환
            return max(new_files, key=lambda p: p.stat().st_mtime)
        time.sleep(0.5)
    return None

def wait_download_finish(download_dir: Path, before: Set[Path], timeout: int = FINISH_TIMEOUT) -> Path:
    """새로 생성된 파일이 완전히(.xlsx) 내려올 때까지 대기"""
    t0 = time.time()
    while time.time() - t0 < timeout:
        now = list_files(download_dir)
        new_files = [p for p in now - before if p.is_file()]
        done = [p for p in new_files if not p.name.endswith(".crdownload")]
        if done:
            return max(done, key=lambda p: p.stat().st_mtime)
        time.sleep(0.5)
    raise TimeoutError("다운로드 대기 초과")

# -------------------------
# 전처리: 읽기 + 정리
# -------------------------
def _read_html_table(path: Path) -> pd.DataFrame:
    tables = pd.read_html(str(path), flavor="bs4", thousands=",", displayed_only=False)
    for t in tables:
        row0 = [str(x).strip() for x in list(t.columns)]
        if ("시군구" in row0 and "단지명" in row0) or ("NO" in row0 and "시군구" in row0):
            return t
        ser0 = t.iloc[:, 0].astype(str).str.strip()
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

        # 헤더 행 탐지(최대 100행 훑음)
        hdr_idx = None
        max_scan = min(100, len(df0))
        for i in range(max_scan):
            row = df0.iloc[i].astype(str).str.strip().tolist()
            if row and (row[0].upper() == "NO"):
                hdr_idx = i; break
            if ("시군구" in row) and ("단지명" in row):
                hdr_idx = i; break
        if hdr_idx is None:
            return _read_html_table(path)

        cols = df0.iloc[hdr_idx].astype(str).str.strip()
        df = df0.iloc[hdr_idx+1:].copy()
        df.columns = cols
        return df.reset_index(drop=True)

    # html 등
    return _read_html_table(path)

def clean_df(df: pd.DataFrame, split_month: bool) -> pd.DataFrame:
    # 컬럼 표준화
    if "시군구 " in df.columns and "시군구" not in df.columns:
        df = df.rename(columns={"시군구 ": "시군구"})
    # 'NO' 열 제거
    for c in list(df.columns):
        if str(c).strip().upper() == "NO":
            df = df[df[c].notna()]
            df = df.drop(columns=[c])

    # 숫자화
    for c in ["거래금액(만원)", "전용면적(㎡)"]:
        if c in df.columns:
            df[c] = (df[c].astype(str)
                           .str.replace(",", "", regex=False)
                           .str.replace(" ", "", regex=False)
                           .str.replace("-", "", regex=False)
                           .replace({"": np.nan}))
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # 시군구 → 광역, 구, 법정동
    if "시군구" in df.columns:
        parts = df["시군구"].astype(str).str.split(expand=True, n=2)
        for i, name in enumerate(["광역", "구", "법정동"]):
            if parts.shape[1] > i:
                df[name] = parts[i].fillna("")
            else:
                df[name] = ""

    # 서울만 계약년/월 분리
    if split_month and "계약년월" in df.columns:
        s = df["계약년월"].astype(str).str.replace(r"\D", "", regex=True)
        df["계약년"] = s.str.slice(0, 4)
        df["계약월"] = s.str.slice(4, 6)

    return df.reset_index(drop=True)

# -------------------------
# 피벗 & 저장
# -------------------------
def pivot_national(df: pd.DataFrame) -> pd.DataFrame:
    if "광역" in df.columns:
        pv = df.pivot_table(index="광역", values="거래금액(만원)", aggfunc="count").rename(columns={"거래금액(만원)":"건수"})
        return pv.reset_index()
    return pd.DataFrame()

def pivot_seoul(df: pd.DataFrame) -> pd.DataFrame:
    if {"구", "계약월"}.issubset(df.columns):
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
# 한 번의 다운로드 + 전처리 + 저장 (재시도 래퍼)
# -------------------------
def fetch_and_process(driver: webdriver.Chrome,
                      sido: Optional[str],
                      start: date, end: date,
                      outname: str,
                      pivot_mode: str) -> None:
    """
    pivot_mode: 'national' or 'seoul'
    - 시도 횟수: MAX_ATTEMPTS
    - 각 시도: 기간/시도 설정 → 3초 대기 → 다운로드 클릭 → 30초 안에 '시작' 감지되면 완료까지 기다림
               시작 감지 실패 시 driver.refresh() 후 다음 시도
    """
    for attempt in range(1, MAX_ATTEMPTS+1):
        try:
            driver.get(URL)
            set_dates(driver, start, end)
            select_sido(driver, "전체" if sido is None else sido)
            time.sleep(AFTER_SET_WAIT)

            before = set(list_files(TMP_DL))
            clicked = click_download(driver, "excel")
            print(f"  - click_download(excel) / attempt {attempt} -> {clicked}")
            if not clicked:
                # 버튼 못찾음 → 새로고침 후 다음 시도
                time.sleep(1.0)
                continue

            # 시작 감지(30초)
            started = wait_download_start(TMP_DL, before, timeout=START_TIMEOUT)
            if not started:
                print(f"  ! {outname.split()[0]} 실패: 다운로드 시작 감지 실패({START_TIMEOUT}초 초과)")
                driver.refresh()
                time.sleep(1.0)
                continue

            # 완료 대기(180초)
            got = wait_download_finish(TMP_DL, before, timeout=FINISH_TIMEOUT)
            size = got.stat().st_size
            print(f"  - got file: {got}  size={size:,}  ext={got.suffix.lower()}")

            # 읽고 전처리
            df_raw = read_table(got)
            split_month = (pivot_mode == "seoul")
            df = clean_df(df_raw, split_month=split_month)

            # 피벗
            pv = pivot_national(df) if pivot_mode == "national" else pivot_seoul(df)

            out = SAVE_DIR / outname
            save_excel(out, df, pv)
            print(f"완료: {out}")
            return  # 성공 종료

        except Exception as e:
            print(f"  ! 시도 {attempt} 오류: {e}")
            try:
                # 혹시 떠있는 알림 정리
                _dismiss_alert_if_any(driver, 0.5)
            except Exception:
                pass
            time.sleep(1.0)

    # MAX_ATTEMPTS 모두 소진
    raise RuntimeError(f"{outname} 실패: {MAX_ATTEMPTS}회 시도 후에도 다운로드 시작/완료를 확인하지 못했습니다.")

# -------------------------
# 메인: 전국 3개월 + 서울 1년
# -------------------------
def main():
    driver = build_driver(TMP_DL)
    try:
        t = today_kst()

        # 전국: 최근 3개월 (과거 두 달은 말일까지, 당월은 오늘까지)
        months = [shift_months(month_first(t), k) for k in [0, -1, -2]]
        months.sort()  # 오래된 달부터
        for base in months:
            start = base
            end = min(month_last(base), t)  # 당월이면 오늘까지
            name = f"전국 {yymm(base)}_{yymmdd(t)}.xlsx"
            print(f"[전국] {start.isoformat()} ~ {end.isoformat()} → {name}")
            fetch_and_process(driver, None, start, end, name, pivot_mode="national")
            time.sleep(1.0)

        # 서울: 전년도 10월 1일 ~ 오늘 (1회)
        start_seoul = date(t.year - 1, 10, 1)
        if start_seoul > t:
            start_seoul = date(t.year, 1, 1)
        name_seoul = f"서울시 {yymmdd(t)}.xlsx"
        print(f"[서울] {start_seoul.isoformat()} ~ {t.isoformat()} → {name_seoul}")
        fetch_and_process(driver, "서울특별시", start_seoul, t, name_seoul, pivot_mode="seoul")

    finally:
        try:
            driver.quit()
        except Exception:
            pass

if __name__ == "__main__":
    main()
