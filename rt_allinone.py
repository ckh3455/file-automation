# -*- coding: utf-8 -*-
"""
rt_allinone.py
- rt.molit.go.kr 아파트(매매) 조건별 자료 페이지에서 자동 다운로드
- 월별(전국 최근 3개월) + 서울(전년도 10/01 ~ 오늘) 전처리 & 피벗
- 결과 엑셀 저장 후 Google Drive 업로드 + 보관기간 지난 파일 정리
- Google Sheets 에 월별 시트에 날짜컬럼으로 집계값 기록(전국=광역별, 서울=구별)

Secrets/Env (GitHub Actions):
  SA_PATH: 서비스계정 JSON 파일 경로
  DRIVE_FOLDER_ID: 업로드할 드라이브 폴더 ID
  DRIVE_RETENTION_DAYS: 보관 일수(기본 3)
  SHEET_ID: 기록할 구글 시트 ID
"""

from __future__ import annotations
import os, re, sys, time, json, math, io
from datetime import date, timedelta, datetime, timezone
from pathlib import Path
from typing import Optional, Tuple, List, Dict

import pandas as pd
import numpy as np

# ---------------- Selenium ----------------
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.alert import Alert
from selenium.common.exceptions import (
    TimeoutException, ElementClickInterceptedException, ElementNotInteractableException, NoAlertPresentException
)
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# -------------- Google APIs ---------------
import gspread
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# ---------------- Config ------------------
URL = "https://rt.molit.go.kr/pt/xls/xls.do?mobileAt="

CI = os.getenv("CI") == "1"
SA_PATH = Path(os.getenv("SA_PATH", "sa.json"))
DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID", "").strip()
DRIVE_RETENTION_DAYS = int(os.getenv("DRIVE_RETENTION_DAYS", "3"))
SHEET_ID = os.getenv("SHEET_ID", "").strip()

BASE_SAVE_DIR = Path.cwd() / "outputs"          # 로컬 결과물 저장 폴더(액션 러너에선 워크스페이스 내부)
TMP_DL = Path.cwd() / "_rt_downloads"           # 브라우저 다운로드 폴더

for p in (BASE_SAVE_DIR, TMP_DL):
    p.mkdir(parents=True, exist_ok=True)

DOWNLOAD_TIMEOUT = 120      # 단일 다운로드 대기
BTN_RETRY_MAX = 10          # 다운로드 버튼 재시도 횟수
BTN_RETRY_SLEEP = 30        # 재시도 전 대기(초)

# --------------- Date utils ---------------
KST = timezone(timedelta(hours=9))

def today_kst() -> date:
    return datetime.now(KST).date()

def month_first(d: date) -> date:
    return date(d.year, d.month, 1)

def month_last(d: date) -> date:
    return (month_first(d) + timedelta(days=40)).replace(day=1) - timedelta(days=1)

def shift_months(d: date, k: int) -> date:
    y, m = d.year, d.month
    m2 = m + k
    y += (m2 - 1) // 12
    m2 = (m2 - 1) % 12 + 1
    end = (date(y, m2, 1) + timedelta(days=40)).replace(day=1) - timedelta(days=1)
    return date(y, m2, min(d.day, end.day))

def yymm(d: date) -> str:
    return f"{d.year%100:02d}{d.month:02d}"

def yymmdd(d: date) -> str:
    return f"{d.year%100:02d}{d.month:02d}{d.day:02d}"

# ------------- Logging helper -------------
def log(msg: str):
    now = datetime.now(KST).strftime("%H:%M:%S")
    print(f"[{now}] {msg}", flush=True)

# ------------- Chrome driver --------------
def build_driver(download_dir: Path) -> webdriver.Chrome:
    opts = Options()
    prefs = {
        "download.default_directory": str(download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    opts.add_experimental_option("prefs", prefs)

    # CI 환경(headless)
    if CI:
        opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--window-size=1400,900")

    # 로컬/CI 바이너리 경로 설정
    chrome_bin = os.getenv("CHROME_BIN")
    if chrome_bin:
        opts.binary_location = chrome_bin

    chromedriver_bin = os.getenv("CHROMEDRIVER_BIN")
    if chromedriver_bin and Path(chromedriver_bin).exists():
        service = Service(executable_path=chromedriver_bin)
    else:
        service = Service(ChromeDriverManager().install())

    driver = webdriver.Chrome(service=service, options=opts)
    if not CI:
        driver.set_window_size(1400, 900)
    return driver

# ------------- Page actions ----------------
def find_date_inputs(driver: webdriver.Chrome) -> Tuple:
    inputs = driver.find_elements(By.CSS_SELECTOR, "input")
    cands = []
    for el in inputs:
        try:
            t = ((el.get_attribute("value") or "") + " " + (el.get_attribute("placeholder") or "")).strip()
            if re.search(r"\d{4}-\d{2}-\d{2}", t) or "YYYY" in t or "yyyy" in t or "YYYY-MM-DD" in t:
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
                        o.click()
                        time.sleep(0.2)
                        return True
        except Exception:
            pass
    return False

def close_alert_if_any(driver):
    try:
        WebDriverWait(driver, 0.6).until(EC.alert_is_present())
        Alert(driver).accept()
        time.sleep(0.3)
    except TimeoutException:
        pass
    except NoAlertPresentException:
        pass

def click_download(driver: webdriver.Chrome, kind="excel") -> bool:
    """페이지에 보이는 첫번째 'EXCEL 다운' 버튼 클릭"""
    label = "EXCEL 다운" if kind == "excel" else "CSV 다운"
    close_alert_if_any(driver)

    # 버튼 탐색
    btns = driver.find_elements(By.XPATH, f"//button[normalize-space()='{label}']")
    if not btns:
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

    # “처리중입니다…” alert 처리
    try:
        WebDriverWait(driver, 3).until(EC.alert_is_present())
        Alert(driver).accept()
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

# -------------- Read & Clean ---------------
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
    return _read_html_table(path)

def clean_df(df: pd.DataFrame, split_month: bool) -> pd.DataFrame:
    if "시군구 " in df.columns and "시군구" not in df.columns:
        df = df.rename(columns={"시군구 ":"시군구"})
    # 통일 rename
    rename_map = {}
    for c in df.columns:
        k = str(c).replace(" ", "")
        if k == "거래금액(만원)" and c != "거래금액(만원)": rename_map[c] = "거래금액(만원)"
        if k == "전용면적(㎡)" and c != "전용면적(㎡)": rename_map[c] = "전용면적(㎡)"
    if rename_map:
        df = df.rename(columns=rename_map)

    # NO 제거
    for c in list(df.columns):
        if str(c).strip().upper() == "NO":
            df = df[df[c].notna()]
            df = df.drop(columns=[c])

    # 숫자화
    for c in ["거래금액(만원)","전용면적(㎡)"]:
        if c in df.columns:
            s = (df[c].astype(str)
                    .str.replace(",", "", regex=False)
                    .str.replace(" ", "", regex=False)
                    .str.replace("-", "", regex=False)
                    .replace({"": np.nan}))
            df[c] = pd.to_numeric(s, errors="coerce")

    # 시군구 → 광역/구/법정동
    if "시군구" in df.columns:
        parts = df["시군구"].astype(str).str.split(expand=True, n=2)
        for i, name in enumerate(["광역","구","법정동"]):
            if parts.shape[1] > i:
                df[name] = parts[i].fillna("")
            else:
                df[name] = ""

    # 계약년/월 분리(서울만 사용)
    if split_month and "계약년월" in df.columns:
        s = df["계약년월"].astype(str).str.replace(r"\D","", regex=True)
        df["계약년"] = s.str.slice(0,4)
        df["계약월"] = s.str.slice(4,6)

    return df.reset_index(drop=True)

# --------------- Pivots --------------------
def pivot_national(df: pd.DataFrame) -> pd.DataFrame:
    if "광역" in df.columns:
        pv = df.pivot_table(index="광역", values="거래금액(만원)", aggfunc="count").rename(columns={"거래금액(만원)":"건수"})
        return pv.reset_index()
    return pd.DataFrame()

def pivot_seoul(df: pd.DataFrame) -> pd.DataFrame:
    if {"구","계약월"}.issubset(df.columns):
        pv = df.pivot_table(index="구", columns="계약월", values="거래금액(만원)", aggfunc="count", fill_value=0)
        pv = pv.sort_index(axis=1)
        pv = pv.reset_index()
        return pv
    return pd.DataFrame()

# --------------- Save excel ----------------
def save_excel(path: Path, df: pd.DataFrame, pivot: Optional[pd.DataFrame], pivot_name="피벗"):
    with pd.ExcelWriter(path, engine="openpyxl") as xw:
        df.to_excel(xw, index=False, sheet_name="data")
        if pivot is not None and not pivot.empty:
            pivot.to_excel(xw, index=False, sheet_name=pivot_name)

# ---------- Google: auth/clients -----------
def build_credentials():
    if not SA_PATH.exists():
        raise FileNotFoundError(f"서비스계정 파일이 없습니다: {SA_PATH}")
    scopes = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets"
    ]
    creds = service_account.Credentials.from_service_account_file(str(SA_PATH), scopes=scopes)
    return creds

def gdrive_client():
    return build("drive", "v3", credentials=build_credentials(), cache_discovery=False)

def gspread_client():
    return gspread.authorize(build_credentials())

# --------- Google Drive helpers ------------
def drive_upload(file_path: Path, folder_id: str) -> str:
    drive = gdrive_client()
    media = MediaFileUpload(str(file_path), mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", resumable=False)
    file_metadata = {"name": file_path.name, "parents": [folder_id]}
    f = drive.files().create(body=file_metadata, media_body=media, fields="id").execute()
    return f["id"]

def drive_cleanup(folder_id: str, keep_days: int):
    if keep_days <= 0: 
        return
    cutoff = datetime.now(timezone.utc) - timedelta(days=keep_days)
    drive = gdrive_client()
    q = f"'{folder_id}' in parents and trashed=false"
    page_token = None
    to_delete = []
    while True:
        resp = drive.files().list(q=q, spaces="drive", fields="nextPageToken, files(id,name,createdTime)", pageToken=page_token).execute()
        for f in resp.get("files", []):
            ct = datetime.fromisoformat(f["createdTime"].replace("Z","+00:00"))
            if ct < cutoff:
                to_delete.append(f["id"])
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    for fid in to_delete:
        try:
            drive.files().delete(fileId=fid).execute()
        except Exception as e:
            log(f"드라이브 삭제 실패 {fid}: {e}")

# --------- Google Sheets helpers -----------
def ensure_sheet(gc: gspread.Client, sheet_id: str, title: str) -> gspread.Worksheet:
    ss = gc.open_by_key(sheet_id)
    try:
        ws = ss.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title=title, rows=200, cols=200)
        ws.update("A1:B1", [["지역","날짜별"]])
    return ws

def upsert_col_by_date(ws: gspread.Worksheet, key_col_name: str, keys: List[str], date_label: str, values_map: Dict[str, int]):
    """
    시트 구조:
      A열 = 지역(키), B~ = 날짜별 컬럼 (헤더: yymmdd)
    동작:
      - 1행에서 date_label과 동일한 헤더가 있으면 해당 컬럼 갱신
      - 없으면 가장 오른쪽 다음 컬럼에 새로 추가
      - 키가 없으면 하단에 새 행 생성
    """
    # 읽기
    data = ws.get_all_values()
    if not data:
        data = [["지역"]]  # 최소 헤더
        ws.update("A1", data)

    # 헤더 파싱
    header = data[0]
    col_idx = None
    for j, h in enumerate(header):
        if h.strip() == date_label:
            col_idx = j
            break
    if col_idx is None:
        col_idx = len(header)
        header.append(date_label)

    # 지역 인덱스 맵
    key_row = {}
    for i in range(1, len(data)):
        k = (data[i][0] if len(data[i])>0 else "").strip()
        if k:
            key_row[k] = i

    # 필요한 행 확보
    for k in keys:
        if k not in key_row:
            data.append([k])
            key_row[k] = len(data)-1

    # 각 행 길이 보정
    max_cols = max(col_idx+1, max(len(r) for r in data))
    for r in data:
        if len(r) < max_cols:
            r += [""]*(max_cols-len(r))

    # 값 채우기
    for k in keys:
        i = key_row[k]
        v = values_map.get(k, 0)
        data[i][col_idx] = str(int(v))

    # 헤더 반영
    data[0] = header

    # 전체 덮어쓰기
    ws.clear()
    ws.update("A1", data)

def write_national_to_sheets(ss_id: str, t: date, pv: pd.DataFrame):
    # 시트명: "전국 YY년 M월"  (예: 전국 25년 7월)
    yy = t.year % 100
    mm = t.month
    title = f"전국 {yy}년 {mm}월"
    gc = gspread_client()
    ws = ensure_sheet(gc, ss_id, title)

    # keys = 광역 리스트, map = {광역: 건수}
    keys = pv["광역"].astype(str).tolist()
    vals = {r["광역"]: int(r["건수"]) for _, r in pv.iterrows()}
    # 날짜 라벨: yymmdd(오늘)
    date_label = yymmdd(today_kst())
    upsert_col_by_date(ws, "지역", keys, date_label, vals)

def write_seoul_to_sheets(ss_id: str, pv: pd.DataFrame, year: int):
    """
    pv: index=구, columns=계약월('01'~'12'), 값=건수
    각 월에 대해 시트명 "서울 YY년 M월"에 오늘 날짜 컬럼으로 건수 기록
    """
    if pv.empty:
        return
    gc = gspread_client()
    today_label = yymmdd(today_kst())
    yy = year % 100
    # 구 목록
    gu_list = pv["구"].astype(str).tolist()
    # 각 월마다 업데이트
    month_cols = [c for c in pv.columns if re.fullmatch(r"\d{2}", str(c))]
    for mstr in month_cols:
        m = int(mstr)
        title = f"서울 {yy}년 {m}월"
        ws = ensure_sheet(gc, ss_id, title)
        vals = {pv.loc[i, "구"]: int(pv.loc[i, mstr]) for i in pv.index}
        upsert_col_by_date(ws, "지역", gu_list, today_label, vals)

# --------- One round: fetch & process ------
def fetch_and_process(driver: webdriver.Chrome,
                      sido: Optional[str],
                      start: date, end: date,
                      outname: str,
                      pivot_mode: str) -> Path:
    """
    pivot_mode: 'national' or 'seoul'
    return: 저장된 결과 엑셀 경로
    """
    log(f"➡ 페이지 오픈: {URL}")
    driver.get(URL)
    time.sleep(1.0)
    # 날짜
    set_dates(driver, start, end)
    log(f"  - set_dates: {start} ~ {end}")

    # 시도 선택
    if sido:
        ok = select_sido(driver, sido)
        log(f"  - select_sido({sido}): {ok}")

    # 다운로드 반복 시도
    kind = "excel"
    success = False
    got_file: Optional[Path] = None

    for attempt in range(1, BTN_RETRY_MAX+1):
        before = set(TMP_DL.glob("*"))
        ok = click_download(driver, kind)
        log(f"  - click_download({kind}) / attempt {attempt} -> {ok}")
        if not ok:
            time.sleep(2.0)
            driver.refresh()
            time.sleep(2.0)
            continue
        try:
            got = wait_download(TMP_DL, before, timeout=DOWNLOAD_TIMEOUT)
            got_file = got
            success = True
            break
        except TimeoutError:
            log("  ! 다운로드 시작 감지 실패 -> 페이지 새로고침")
            driver.refresh()
            time.sleep(BTN_RETRY_SLEEP)

    if not success or not got_file:
        raise RuntimeError("다운로드 실패(재시도 한도 초과)")

    size = got_file.stat().st_size
    log(f"  - got file: {got_file}  size={size:,}  ext={got_file.suffix}")

    # 읽기 + 정리
    df_raw = read_table(got_file)
    log(f"  - parsed: rows={len(df_raw):,}  cols={len(df_raw.columns)}")
    split_month = (pivot_mode == "seoul")
    df = clean_df(df_raw, split_month=split_month)

    # 피벗
    if pivot_mode == "national":
        pv = pivot_national(df)
    else:
        pv = pivot_seoul(df)

    out = BASE_SAVE_DIR / outname
    save_excel(out, df, pv)
    log(f"✅ 저장: {out}")
    return out

# -------------------- MAIN -----------------
def main():
    t = today_kst()
    driver = build_driver(TMP_DL)
    outputs: List[Tuple[str, Path, Optional[pd.DataFrame]]] = []

    try:
        # 전국: 최근 3개월 (과거달은 말일까지, 당월은 오늘까지)
        months = [shift_months(month_first(t), k) for k in [0, -1, -2]]
        months.sort()
        for base in months:
            start = base
            end = month_last(base) if base.month != t.month or base.year != t.year else t
            name = f"전국 {yymm(base)}_{yymmdd(t)}.xlsx"
            log(f"[전국] {start} ~ {end} → {name}")
            p = fetch_and_process(driver, None, start, end, name, pivot_mode="national")
        # 서울: 전년도 10월 1일 ~ 오늘 (한 번에)
        start_seoul = date(t.year-1, 10, 1)
        if start_seoul > t:
            start_seoul = date(t.year, 1, 1)
        name_seoul = f"서울시 {yymmdd(t)}.xlsx"
        log(f"[서울] {start_seoul} ~ {t} → {name_seoul}")
        p2 = fetch_and_process(driver, "서울특별시", start_seoul, t, name_seoul, pivot_mode="seoul")
    finally:
        try:
            driver.quit()
        except Exception:
            pass

    # ---- 드라이브 업로드 & 보관정리 ----
    if DRIVE_FOLDER_ID:
        for f in sorted(BASE_SAVE_DIR.glob("*.xlsx")):
            try:
                fid = drive_upload(f, DRIVE_FOLDER_ID)
                log(f"📤 Drive 업로드 완료: {f.name} (id={fid})")
            except Exception as e:
                log(f"Drive 업로드 실패: {f} / {e}")

        try:
            drive_cleanup(DRIVE_FOLDER_ID, DRIVE_RETENTION_DAYS)
            log(f"🧹 보관정리 완료(>{DRIVE_RETENTION_DAYS}일)")
        except Exception as e:
            log(f"Drive 정리 실패: {e}")

    # ---- 구글 시트 기록 ----
    if SHEET_ID:
        try:
            # 전국: 가장 최근 파일(=당월)부터 월별 시트에 날짜 컬럼 업데이트
            for f in sorted(BASE_SAVE_DIR.glob("전국 *.xlsx")):
                m = re.search(r"전국\s(\d{4})_", f.name)
                if not m:
                    continue
                yyMM = m.group(1)
                yy = 2000 + int(yyMM[:2])
                mm = int(yyMM[2:])
                t_month = date(yy, mm, 1)
                # 피벗 읽기
                df_pv = pd.read_excel(f, sheet_name="피벗")
                write_national_to_sheets(SHEET_ID, t_month, df_pv)
                log(f"📝 Sheets 갱신(전국): {f.name}")

            # 서울: 한 파일에 월별 열이 있으므로 해당 연도 월 시트들 업데이트
            for f in sorted(BASE_SAVE_DIR.glob("서울시 *.xlsx")):
                df_pv = pd.read_excel(f, sheet_name="피벗")
                # 서울 시트 연도는 today 기준
                write_seoul_to_sheets(SHEET_ID, df_pv, year=t.year)
                log(f"📝 Sheets 갱신(서울): {f.name}")
        except Exception as e:
            log(f"Sheets 기록 실패: {e}")

if __name__ == "__main__":
    main()
