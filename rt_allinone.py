# -*- coding: utf-8 -*-
"""
rt_allinone.py
- rt.molit.go.kr(조건별 자료제공)에서 자동 다운로드 → 전처리 → 구글시트 기록까지 한 번에.
- 전국: 최근 3개월(당월 포함, 각 월 1~말일. 현재월은 오늘까지만) → "전국 YYMM_YYMMDD.xlsx"
- 서울: 전년도 10월 1일 ~ 오늘(1회) → "서울시 YYMMDD.xlsx"
- 피벗:
  * 전국: 광역별 거래건수 표 (행: 광역, 열: 건수)
  * 서울: 구 x 계약월 거래건수 표 (행: 구, 열: "01"~"12")
- 시트기록:
  * 전국: "전국 YY년 M월" 탭의 '월/일' 행에 광역별 건수 upsert
  * 서울: 각 월별("01"~"12")을 "서울 YY년 M월" 탭의 '월/일' 행에 구별 건수 upsert
환경변수:
  SHEET_ID: 구글시트 ID
  SA_PATH : 서비스계정 JSON 경로
"""

from __future__ import annotations
import os, re, time, json
from datetime import date, timedelta
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import numpy as np

# -------------------------
# 사이트/경로/타임아웃 설정
# -------------------------
URL = "https://rt.molit.go.kr/pt/xls/xls.do?mobileAt="
SAVE_DIR = Path(r"D:\OneDrive\excel data")     # 결과 파일 저장 폴더
TMP_DL   = Path.cwd() / "_rt_downloads"        # 임시 다운로드 폴더
SAVE_DIR.mkdir(parents=True, exist_ok=True)
TMP_DL.mkdir(parents=True, exist_ok=True)

# 다운로드/재시도 정책
DOWNLOAD_TIMEOUT = 30   # 각 시도에서 파일 등장까지 최대 대기(초)
MAX_TRIES        = 10   # 실패 시 페이지 새로고침하고 최대 시도 횟수
PAUSE_AFTER_SET  = 3.0  # 기간 설정 후 버튼 누르기 전 대기(초)

# -------------------------
# 날짜 유틸
# -------------------------
def today_kst() -> date:
    return date.today()  # GitHub Actions에선 TZ=Asia/Seoul로 실행 권장

def month_first(d: date) -> date:
    return date(d.year, d.month, 1)

def month_last(d: date) -> date:
    nd = (date(d.year, d.month, 1) + timedelta(days=40)).replace(day=1) - timedelta(days=1)
    return nd

def shift_months(d: date, k: int) -> date:
    y, m, dd = d.year, d.month, d.day
    m2 = m + k
    y += (m2-1)//12
    m2 = (m2-1)%12 + 1
    end = month_last(date(y, m2, 1))
    return date(y, m2, min(dd, end.day))

def yymm(d: date) -> str:
    return f"{d.year%100:02d}{d.month:02d}"

def yymmdd(d: date) -> str:
    return f"{d.year%100:02d}{d.month:02d}{d.day:02d}"

# -------------------------
# 브라우저 준비
# -------------------------
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import (
    TimeoutException, ElementClickInterceptedException, ElementNotInteractableException,
    NoSuchElementException, UnexpectedAlertPresentException
)
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.alert import Alert

def build_driver(download_dir: Path) -> webdriver.Chrome:
    opts = Options()
    # headless/new 모드 + 안정 옵션
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1400,900")
    # 다운로드 자동
    prefs = {
        "download.default_directory": str(download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    opts.add_experimental_option("prefs", prefs)

    # GitHub Actions(리눅스) : 환경변수 경로 우선
    chrome_bin = os.environ.get("CHROME_BIN")
    if chrome_bin:
        opts.binary_location = chrome_bin
    drv_bin = os.environ.get("CHROMEDRIVER_BIN")

    # 로컬: webdriver-manager, CI: 시스템 드라이버
    try:
        if drv_bin and Path(drv_bin).exists():
            service = Service(executable_path=drv_bin)
            driver = webdriver.Chrome(service=service, options=opts)
        else:
            from webdriver_manager.chrome import ChromeDriverManager
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=opts)
    except TypeError:
        # 구버전 시그니처 대응
        if drv_bin and Path(drv_bin).exists():
            driver = webdriver.Chrome(drv_bin, options=opts)  # type: ignore
        else:
            from webdriver_manager.chrome import ChromeDriverManager
            driver = webdriver.Chrome(ChromeDriverManager().install(), options=opts)  # type: ignore

    return driver

# -------------------------
# 페이지 조작
# -------------------------
def _all_inputs(driver):
    return driver.find_elements(By.CSS_SELECTOR, "input, input[type=text]")

def clear_and_type(el, s: str):
    el.click()
    el.send_keys(Keys.CONTROL, "a")
    el.send_keys(Keys.DELETE)
    el.send_keys(s)

def find_date_inputs(driver) -> Tuple:
    inputs = _all_inputs(driver)
    cands = []
    for el in inputs:
        try:
            val = (el.get_attribute("value") or "") + " " + (el.get_attribute("placeholder") or "")
            if re.search(r"\d{4}-\d{2}-\d{2}", val) or "YYYY" in val.upper():
                cands.append(el)
        except Exception:
            pass
    if len(cands) >= 2:
        return cands[0], cands[1]
    # fallback
    texts = [e for e in inputs if (e.get_attribute("type") or "").lower() in ("text", "")]
    if len(texts) >= 2:
        return texts[0], texts[1]
    raise RuntimeError("날짜 입력 박스를 찾을 수 없습니다.")

def set_dates(driver, start: date, end: date):
    s_el, e_el = find_date_inputs(driver)
    clear_and_type(s_el, start.isoformat())
    time.sleep(0.2)
    clear_and_type(e_el, end.isoformat())
    time.sleep(0.2)
    # 사용자가 요청: 설정 후 3초 정도만 대기
    time.sleep(PAUSE_AFTER_SET)

def select_sido(driver, wanted: Optional[str]) -> bool:
    if not wanted:
        return True
    wanted = wanted.strip()
    selects = driver.find_elements(By.TAG_NAME, "select")
    for sel in selects:
        try:
            opts = sel.find_elements(By.TAG_NAME, "option")
            txts = [o.text.strip() for o in opts]
            # 시도 셀렉터일 확률이 있는지
            if "전체" in txts or "서울특별시" in txts:
                for o in opts:
                    if o.text.strip() == wanted:
                        o.click()
                        time.sleep(0.3)
                        return True
        except Exception:
            pass
    return False

def accept_any_alert(driver, wait_sec=2):
    try:
        WebDriverWait(driver, wait_sec).until(EC.alert_is_present())
        Alert(driver).accept()
        time.sleep(0.3)
        return True
    except TimeoutException:
        return False
    except UnexpectedAlertPresentException:
        try:
            Alert(driver).accept()
            time.sleep(0.3)
            return True
        except Exception:
            return False

def click_download(driver, kind="excel") -> bool:
    """다운로드 버튼 클릭. 레이블은 'EXCEL 다운' 또는 'CSV 다운'."""
    label = "EXCEL 다운" if kind == "excel" else "CSV 다운"

    # 혹시 떠 있는 알림 닫기
    accept_any_alert(driver, wait_sec=1)

    # 버튼 찾기(여러 버전 대응)
    xpaths = [
        f"//button[normalize-space()='{label}']",
        f"//button[contains(normalize-space(), 'EXCEL')]",
        f"//a[normalize-space()='{label}']",
        f"//a[contains(normalize-space(),'EXCEL')]",
    ]
    btn = None
    for xp in xpaths:
        els = driver.find_elements(By.XPATH, xp)
        if els:
            btn = els[0]; break

    if not btn:
        # 아래로 스크롤 후 재시도
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(0.5)
        for xp in xpaths:
            els = driver.find_elements(By.XPATH, xp)
            if els:
                btn = els[0]; break

    if not btn:
        return False

    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
        time.sleep(0.2)
        btn.click()
    except (ElementClickInterceptedException, ElementNotInteractableException):
        driver.execute_script("arguments[0].click();", btn)
    except Exception:
        return False

    # '처리중입니다...' 알림이 뜨면 확인
    accept_any_alert(driver, wait_sec=3)
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
# 전처리: 읽기 + 정리
# -------------------------
def _read_html_table(path: Path) -> pd.DataFrame:
    tables = pd.read_html(str(path), flavor="bs4", thousands=",", displayed_only=False)
    for t in tables:
        # 헤더가 정상 컬럼으로 들어간 경우
        cols = [str(x).strip() for x in list(t.columns)]
        if ("시군구" in cols and "단지명" in cols) or ("NO" in cols and "시군구" in cols):
            return t
        # 헤더가 첫 행에 있는 경우
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
        # 헤더 행 탐지
        hdr_idx = None
        max_scan = min(120, len(df0))
        for i in range(max_scan):
            row = df0.iloc[i].astype(str).str.strip().tolist()
            if not row: 
                continue
            if row[0].upper() in ("NO","N0","No"):
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
    # 컬럼 공백/대소 균질화
    if "시군구 " in df.columns and "시군구" not in df.columns:
        df = df.rename(columns={"시군구 ":"시군구"})
    # 표준 이름 매핑
    ren = {}
    for c in df.columns:
        k = str(c).replace(" ", "")
        if k == "거래금액(만원)" and c != "거래금액(만원)": ren[c] = "거래금액(만원)"
        if k == "전용면적(㎡)" and c != "전용면적(㎡)": ren[c] = "전용면적(㎡)"
    if ren:
        df = df.rename(columns=ren)

    # NO 열 제거
    drop_no = [c for c in df.columns if str(c).strip().upper() == "NO"]
    if drop_no:
        df = df[df[drop_no[0]].notna()]  # 공란 행 제거
        df = df.drop(columns=drop_no)

    # 숫자화
    for c in ["거래금액(만원)", "전용면적(㎡)"]:
        if c in df.columns:
            df[c] = (df[c].astype(str)
                           .str.replace(",", "", regex=False)
                           .str.replace(" ", "", regex=False)
                           .str.replace("-", "", regex=False)
                           .replace({"": np.nan}))
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # 시군구 → 광역/구/법정동
    if "시군구" in df.columns:
        parts = df["시군구"].astype(str).str.split(expand=True, n=2)
        for i, name in enumerate(["광역","구","법정동"]):
            if parts.shape[1] > i:
                df[name] = parts[i].fillna("")
            else:
                df[name] = ""

    # 계약년/월 분리 (전국은 False, 서울은 True)
    if split_month and "계약년월" in df.columns:
        s = df["계약년월"].astype(str).str.replace(r"\D","", regex=True)
        df["계약년"] = s.str.slice(0,4)
        df["계약월"] = s.str.slice(4,6)

    return df.reset_index(drop=True)

# -------------------------
# 피벗
# -------------------------
def pivot_national(df: pd.DataFrame) -> pd.DataFrame:
    if "광역" in df.columns and "거래금액(만원)" in df.columns:
        pv = df.pivot_table(index="광역", values="거래금액(만원)", aggfunc="count").rename(columns={"거래금액(만원)":"건수"})
        return pv.reset_index()
    return pd.DataFrame()

def pivot_seoul(df: pd.DataFrame) -> pd.DataFrame:
    if {"구","계약월","거래금액(만원)"}.issubset(df.columns):
        pv = df.pivot_table(index="구", columns="계약월", values="거래금액(만원)", aggfunc="count", fill_value=0)
        pv = pv.reindex(sorted(pv.columns), axis=1)
        pv = pv.reset_index()
        return pv
    return pd.DataFrame()

# -------------------------
# 엑셀 저장
# -------------------------
def save_excel(path: Path, df: pd.DataFrame, pivot: Optional[pd.DataFrame], pivot_name="피벗"):
    with pd.ExcelWriter(path, engine="openpyxl") as xw:
        df.to_excel(xw, index=False, sheet_name="data")
        if pivot is not None and not pivot.empty:
            pivot.to_excel(xw, index=False, sheet_name=pivot_name)

# -------------------------
# 구글시트 기록 유틸
# -------------------------
import gspread
from google.oauth2.service_account import Credentials

def sheets_client_from_env():
    sa_path = os.environ.get("SA_PATH")
    if not sa_path or not Path(sa_path).exists():
        raise RuntimeError("SA_PATH 환경변수 또는 파일이 없습니다.")
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(sa_path, scopes=scopes)
    return gspread.authorize(creds)

def upsert_row(ws, key_col_name, key_value, row_dict, header):
    # 헤더 보장
    first_row = ws.row_count >= 1 and ws.col_count >= 1
    need_header = True
    if first_row:
        try:
            cur = ws.row_values(1)
            if cur and len(cur) == len(header) and all((str(cur[i])==str(header[i]) for i in range(len(header)))):
                need_header = False
        except Exception:
            pass
    if need_header:
        ws.clear()
        ws.update([header])

    data = ws.get_all_records(default_blank="")
    target_idx = None
    for i, rec in enumerate(data, start=2):
        if str(rec.get(key_col_name, "")) == str(key_value):
            target_idx = i; break

    row = [row_dict.get(col, "") for col in header]
    if target_idx:
        ws.update(f"A{target_idx}", [row])
    else:
        ws.append_row(row, value_input_option="USER_ENTERED")

def find_or_create(sheet, title, cols=10):
    try:
        return sheet.worksheet(title)
    except Exception:
        return sheet.add_worksheet(title=title, rows=2, cols=max(cols, 10))

def tab_title(prefix: str, y: int, m: int) -> str:
    return f"{str(y)[2:]}년 {m}월" if prefix.strip()=="" else f"{prefix} {str(y)[2:]}년 {m}월"

def write_national_to_sheet(pv_national: pd.DataFrame, when: date):
    if pv_national is None or pv_national.empty: 
        return
    gc = sheets_client_from_env()
    sheet_id = os.environ["SHEET_ID"]
    ss = gc.open_by_key(sheet_id)

    title = tab_title("전국", when.year, when.month)
    ws = find_or_create(ss, title, cols=1+len(pv_national["광역"].unique()))
    regions = sorted(pv_national["광역"].astype(str).unique())
    header = ["월/일"] + regions

    mmdd = f"{when.month}/{when.day}"
    row = {"월/일": mmdd}
    for _, r in pv_national.iterrows():
        row[str(r["광역"])] = int(r["건수"]) if pd.notna(r["건수"]) else ""
    upsert_row(ws, "월/일", mmdd, row, header)

def write_seoul_to_sheet(pv_seoul: pd.DataFrame, when: date):
    if pv_seoul is None or pv_seoul.empty:
        return
    gc = sheets_client_from_env()
    sheet_id = os.environ["SHEET_ID"]
    ss = gc.open_by_key(sheet_id)

    mmdd = f"{when.month}/{when.day}"
    # 월 컬럼(문자 "01"~"12")
    month_cols = [c for c in pv_seoul.columns if c.isdigit() and len(c)==2]
    for mcol in month_cols:
        m_int = int(mcol)
        title = tab_title("서울", when.year, m_int)
        ws = find_or_create(ss, title, cols=1+len(pv_seoul))
        gus = pv_seoul["구"].astype(str).tolist()
        header = ["월/일"] + gus

        row = {"월/일": mmdd}
        for _, r in pv_seoul.iterrows():
            row[str(r["구"])] = int(r[mcol]) if pd.notna(r[mcol]) else ""
        upsert_row(ws, "월/일", mmdd, row, header)

# -------------------------
# 한 번의 다운로드 + 전처리 + 저장 + 시트기록
# -------------------------
def fetch_and_process(driver: webdriver.Chrome,
                      sido: Optional[str],
                      start: date, end: date,
                      outname: str,
                      pivot_mode: str) -> Optional[Tuple[pd.DataFrame, pd.DataFrame]]:
    """
    pivot_mode: 'national' | 'seoul'
    return: (df, pivot) or None if download failed
    """
    # 현재월이라면 종료일을 오늘로 clamp
    t = today_kst()
    if start.year == t.year and start.month == t.month:
        end = min(end, t)

    # 페이지/기간/시도 설정 + 다운로드 재시도 루프
    driver.get(URL)
    select_sido(driver, sido)  # 시도 먼저 바꿔도 무방
    set_dates(driver, start, end)

    got_path = None
    for attempt in range(1, MAX_TRIES+1):
        before = set(TMP_DL.glob("*"))
        ok_btn = click_download(driver, "excel")
        print(f"  - click_download(excel) / attempt {attempt} -> {ok_btn}")
        if not ok_btn:
            driver.refresh()
            time.sleep(1.5)
            select_sido(driver, sido)
            set_dates(driver, start, end)
            continue
        try:
            got_path = wait_download(TMP_DL, before, timeout=DOWNLOAD_TIMEOUT)
            break
        except TimeoutError:
            print("  ! 다운로드 시작 감지 실패(타임아웃). 페이지 새로고침 후 재시도.")
            driver.refresh()
            time.sleep(1.5)
            select_sido(driver, sido)
            set_dates(driver, start, end)

    if not got_path:
        print(f"  ! 실패: 다운로드에 반복해서 실패했습니다. ({start}~{end})")
        return None

    size = got_path.stat().st_size
    print(f"  - got file: {got_path}  size={size:,}  ext={got_path.suffix.lower()}")

    # 읽기 + 전처리
    df_raw = read_table(got_path)
    split_month = (pivot_mode == "seoul")
    df = clean_df(df_raw, split_month=split_month)

    # 피벗
    if pivot_mode == "national":
        pv = pivot_national(df)
    else:
        pv = pivot_seoul(df)

    out = SAVE_DIR / outname
    save_excel(out, df, pv)
    print(f"완료: {out}")

    # 시트 기록
    if pivot_mode == "national":
        write_national_to_sheet(pv, t)
    else:
        write_seoul_to_sheet(pv, t)

    return df, pv

# -------------------------
# 메인
# -------------------------
def main():
    driver = build_driver(TMP_DL)
    try:
        t = today_kst()
        # 전국: 과거 2개월, 이전달, 현재달(총 3개) — 오래된 달부터
        months = [shift_months(month_first(t), k) for k in [-2, -1, 0]]
        for base in months:
            start = base
            end   = month_last(base)
            # 현재월은 fetch_and_process 내부에서 오늘로 clamp
            name = f"전국 {yymm(base)}_{yymmdd(t)}.xlsx"
            print(f"[전국] {start.isoformat()} ~ {end.isoformat()} → {name}")
            fetch_and_process(driver, None, start, end, name, pivot_mode="national")

        # 서울: 전년도 10월 1일 ~ 오늘 (1회)
        y0 = t.year - 1
        start_seoul = date(y0, 10, 1)
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
