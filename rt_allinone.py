# -*- coding: utf-8 -*-
"""
rt_allinone.py

- rt.molit.go.kr 조건별 자료제공 페이지에서 월별(전국)/기간(서울) 데이터 자동 다운로드
- 원본 전처리(행/열 정리, 숫자화, 주소 분리) 후 엑셀 저장 + 간단 피벗
- 전처리 결과를 구글드라이브에 업로드(보관일수 경과분 삭제)
- 피벗 값을 구글시트 각 월 시트에 '날짜' 행으로 업서트(전국=광역별, 서울=구별)

필요 환경변수/시크릿
- 로컬:
  - SA_PATH : 서비스계정 JSON 파일 경로 (예: C:\path\sa.json)
  - SHEET_ID, DRIVE_FOLDER_ID, (선택)DRIVE_RETENTION_DAYS
- GitHub Actions:
  - 워크플로에서 GDRIVE_SA_JSON을 sa.json으로 저장
  - SHEET_ID, DRIVE_FOLDER_ID, (선택)DRIVE_RETENTION_DAYS
  - CHROME_BIN, CHROMEDRIVER_BIN (워크플로에서 apt로 설치 후 주입)
"""

from __future__ import annotations

import os
import re
import sys
import time
import json
import math
import shutil
import random
import tempfile
from pathlib import Path
from datetime import date, datetime, timedelta
from typing import Optional, Tuple, Dict, List

import numpy as np
import pandas as pd

# ---------------- Selenium ----------------
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import (
    TimeoutException, ElementClickInterceptedException,
    ElementNotInteractableException, NoSuchElementException
)
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ---------------- Google APIs ----------------
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# ---------------- 설정 ----------------
URL = "https://rt.molit.go.kr/pt/xls/xls.do?mobileAt="

# 저장 경로 (로컬 기본값 / CI는 워크스페이스 밑으로)
WORKDIR = Path.cwd()
SAVE_DIR = Path(os.environ.get("OUTPUT_DIR", str(WORKDIR / "outputs")))
TMP_DL   = Path(os.environ.get("TMP_DL_DIR", str(WORKDIR / "_rt_downloads")))
SAVE_DIR.mkdir(parents=True, exist_ok=True)
TMP_DL.mkdir(parents=True, exist_ok=True)

# 타임아웃/시도 설정
DOWNLOAD_TIMEOUT_EACH = int(os.environ.get("DOWNLOAD_TIMEOUT_EACH", "30"))   # 한 번 클릭 후 대기(초)
CLICK_MAX_TRY         = int(os.environ.get("CLICK_MAX_TRY", "10"))           # 재시도 횟수
WAIT_AFTER_SETDATES   = int(os.environ.get("WAIT_AFTER_SETDATES", "3"))      # 기간 설정 직후 대기(초)

# 구글드라이브 보관일수
DRIVE_RETENTION_DAYS  = int(os.environ.get("DRIVE_RETENTION_DAYS", "3"))

# 구글 시트 ID (없으면 시트 기록 스킵)
SHEET_ID = os.environ.get("SHEET_ID")

# SA JSON 경로: CI는 sa.json으로 저장, 로컬은 SA_PATH 사용
SA_PATH = os.environ.get("SA_PATH", str(WORKDIR / "sa.json"))

# CI 모드 여부
IS_CI = os.environ.get("CI", "") == "1"

# 내부 전역(임시 크롬 프로필 경로)
_PROFILE_DIR: Optional[Path] = None


# ---------------- 유틸(날짜/문자열) ----------------
def today_kst() -> date:
    # 러너/로컬 모두 시스템 로컬 날짜로 충분
    return date.today()

def month_first(d: date) -> date:
    return date(d.year, d.month, 1)

def month_end(d: date) -> date:
    # 대상월 말일 계산
    return (date(d.year, d.month, 1) + timedelta(days=40)).replace(day=1) - timedelta(days=1)

def shift_months(d: date, k: int) -> date:
    """d 기준 k개월 이동(같은 일, 말일 보정)"""
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

def label_month_sheet(prefix: str, y: int, m: int) -> str:
    return f"{prefix} {y%100:02d}년 {m:02d}월"


# ---------------- 크롬 드라이버 ----------------
def build_driver(download_dir: Path) -> webdriver.Chrome:
    """GitHub Actions/로컬 모두에서 안정적으로 동작하도록 구성"""
    global _PROFILE_DIR

    opts = Options()

    # 런너에 설치한 크로미움 바이너리 사용(있으면)
    chrome_bin = os.environ.get("CHROME_BIN")
    if chrome_bin:
        opts.binary_location = chrome_bin

    # 다운로드 폴더
    prefs = {
        "download.default_directory": str(download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    opts.add_experimental_option("prefs", prefs)

    # 안정 옵션
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1400,900")
    opts.add_argument("--remote-debugging-port=0")  # 포트 충돌 회피

    # 매 실행마다 고유 프로필
    _PROFILE_DIR = Path(tempfile.mkdtemp(prefix="_chrome_profile_"))
    opts.add_argument(f"--user-data-dir={_PROFILE_DIR}")

    # 러너에 설치된 chromedriver 우선
    cd_bin = os.environ.get("CHROMEDRIVER_BIN")
    if cd_bin and Path(cd_bin).exists():
        service = Service(cd_bin)
    else:
        # 로컬에선 webdriver-manager 사용
        from webdriver_manager.chrome import ChromeDriverManager
        service = Service(ChromeDriverManager().install())

    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_window_size(1400, 900)

    # (구버전 대비) 헤드리스 다운로드 허용 시도
    try:
        driver.command_executor._commands["send_command"] = (
            "POST", "/session/$sessionId/chromium/send_command"
        )
        driver.execute("send_command", {
            "cmd": "Page.setDownloadBehavior",
            "params": {"behavior": "allow", "downloadPath": str(download_dir)},
        })
    except Exception:
        pass

    return driver


# ---------------- 페이지 조작 ----------------
def find_date_inputs(driver: webdriver.Chrome) -> Tuple[webdriver.remote.webelement.WebElement,
                                                        webdriver.remote.webelement.WebElement]:
    inputs = driver.find_elements(By.CSS_SELECTOR, "input")
    cands = []
    for el in inputs:
        try:
            val = (el.get_attribute("value") or "")
            ph  = (el.get_attribute("placeholder") or "")
            txt = f"{val} {ph}".strip()
            if re.search(r"\d{4}-\d{2}-\d{2}", txt) or "YYYY" in txt or "yyyy" in txt or "YYYY-MM-DD" in txt:
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
    """시도 선택(전국은 None). wanted 예: '서울특별시'"""
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
                        time.sleep(0.3)
                        return True
        except Exception:
            pass
    return False

def _find_download_button(driver: webdriver.Chrome, label: str):
    # 정확히 일치 우선
    btns = driver.find_elements(By.XPATH, f"//button[normalize-space()='{label}']")
    if btns:
        return btns[0]
    # contains 보조
    btns = driver.find_elements(By.XPATH, f"//button[contains(normalize-space(), '{label}')]")
    return btns[0] if btns else None

def click_download(driver: webdriver.Chrome, kind="excel") -> bool:
    """다운로드 버튼 클릭만 책임. 시작 실패 시 False."""
    label = "EXCEL 다운" if kind == "excel" else "CSV 다운"

    # 알림 떠있으면 정리
    try:
        WebDriverWait(driver, 0.8).until(EC.alert_is_present())
        driver.switch_to.alert.accept()
        time.sleep(0.2)
    except TimeoutException:
        pass

    btn = _find_download_button(driver, label)
    if not btn:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(0.5)
        btn = _find_download_button(driver, label)
        if not btn:
            return False

    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
    time.sleep(0.15)
    try:
        btn.click()
    except (ElementClickInterceptedException, ElementNotInteractableException):
        try:
            driver.execute_script("arguments[0].click();", btn)
        except Exception:
            return False

    # “처리중입니다…” alert 확인 후 닫기(있을 때만)
    try:
        WebDriverWait(driver, 2).until(EC.alert_is_present())
        driver.switch_to.alert.accept()
    except TimeoutException:
        pass

    return True

def wait_download(download_dir: Path, before: set[Path], timeout: int) -> Path:
    """새 파일이 완전히 받아질 때까지 대기"""
    t0 = time.time()
    while time.time() - t0 < timeout:
        now = set(download_dir.glob("*"))
        new_files = [p for p in now - before if p.is_file()]
        done = [p for p in new_files if not p.name.endswith(".crdownload")]
        if done:
            # 가장 최신
            return max(done, key=lambda p: p.stat().st_mtime)
        time.sleep(0.5)
    raise TimeoutError("다운로드 시작 감지 실패(타임아웃)")

def robust_download(driver: webdriver.Chrome,
                    start: date, end: date,
                    sido: Optional[str],
                    kind="excel") -> Path:
    """기간/시도 설정 → 클릭 재시도/새로고침 포함 → 파일 경로 반환"""
    # 페이지 진입 및 설정
    driver.get(URL)
    set_dates(driver, start, end)
    if sido:
        select_sido(driver, sido)

    time.sleep(WAIT_AFTER_SETDATES)

    for attempt in range(1, CLICK_MAX_TRY + 1):
        before = set(TMP_DL.glob("*"))
        ok_click = click_download(driver, kind=kind)

        if not ok_click:
            # 버튼 못찾거나 클릭 실패 → 한번 더 스크롤/리트라이
            time.sleep(1.5)
        else:
            # 클릭됨 → 파일 대기
            try:
                got = wait_download(TMP_DL, before, timeout=DOWNLOAD_TIMEOUT_EACH)
                print(f"  - got file: {got}  size={got.stat().st_size:,}  ext={got.suffix}")
                return got
            except TimeoutError:
                print(f"  - warn: 다운로드 시작 감지 실패(시도 {attempt}/{CLICK_MAX_TRY})")

        # 리프레시 후 재설정
        driver.refresh()
        time.sleep(1.0)
        set_dates(driver, start, end)
        if sido:
            select_sido(driver, sido)
        time.sleep(WAIT_AFTER_SETDATES)

    raise RuntimeError("다운로드 실패(최대 시도 초과)")


# ---------------- 전처리 ----------------
def _read_html_table(path: Path) -> pd.DataFrame:
    tables = pd.read_html(str(path), flavor="bs4", thousands=",", displayed_only=False)
    for t in tables:
        row0 = [str(x).strip() for x in list(t.columns)]
        if ("시군구" in row0 and "단지명" in row0) or ("NO" in row0 and "시군구" in row0):
            return t
        # 헤더가 첫 행에 있는 경우
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
            df0 = pd.read_excel(path, header=None, dtype=str,
                                engine="openpyxl" if ext == ".xlsx" else None)
        except Exception:
            return _read_html_table(path)

        # 헤더 탐지
        hdr_idx = None
        max_scan = min(80, len(df0))
        for i in range(max_scan):
            row = df0.iloc[i].astype(str).str.strip().tolist()
            if row and (row[0] in ("NO", "No", "no")):
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

def clean_df(df: pd.DataFrame, split_year_month: bool) -> pd.DataFrame:
    # 컬럼 보정
    if "시군구 " in df.columns and "시군구" not in df.columns:
        df = df.rename(columns={"시군구 ": "시군구"})

    rename_map = {}
    for c in df.columns:
        k = str(c).replace(" ", "")
        if k == "거래금액(만원)" and c != "거래금액(만원)": rename_map[c] = "거래금액(만원)"
        if k == "전용면적(㎡)" and c != "전용면적(㎡)": rename_map[c] = "전용면적(㎡)"
        if k == "계약년월" and c != "계약년월": rename_map[c] = "계약년월"
    if rename_map:
        df = df.rename(columns=rename_map)

    # NO 제거
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

    # 시군구 분해
    if "시군구" in df.columns:
        parts = df["시군구"].astype(str).str.split(expand=True, n=2)
        for i, name in enumerate(["광역", "구", "법정동"]):
            if parts.shape[1] > i:
                df[name] = parts[i].fillna("")
            else:
                df[name] = ""

    # 계약년/월 분리(서울용)
    if split_year_month and "계약년월" in df.columns:
        s = df["계약년월"].astype(str).str.replace(r"\D", "", regex=True)
        df["계약년"] = s.str.slice(0, 4)
        df["계약월"] = s.str.slice(4, 6)

    return df.reset_index(drop=True)


# ---------------- 피벗 ----------------
def pivot_national(df: pd.DataFrame) -> pd.DataFrame:
    if "광역" in df.columns:
        pv = df.pivot_table(index="광역", values="거래금액(만원)",
                            aggfunc="count").rename(columns={"거래금액(만원)": "건수"})
        return pv.reset_index()
    return pd.DataFrame()

def pivot_seoul_monthly(df: pd.DataFrame) -> Dict[Tuple[int, int], pd.DataFrame]:
    """
    서울: (계약년,계약월)별로 구 x 건수 피벗 생성
    return: {(year, month): DataFrame[구, 건수]}
    """
    out: Dict[Tuple[int, int], pd.DataFrame] = {}
    if not {"구", "계약년", "계약월"}.issubset(df.columns):
        return out
    g = df.groupby(["계약년", "계약월", "구"], dropna=False)["거래금액(만원)"].count()
    g = g.rename("건수").reset_index()
    # 연-월 루프
    for (yy, mm), sub in g.groupby(["계약년", "계약월"]):
        try:
            y = int(str(yy))
            m = int(str(mm))
        except Exception:
            continue
        pv = sub.pivot_table(index="구", values="건수", aggfunc="sum", fill_value=0).reset_index()
        out[(y, m)] = pv
    return out


# ---------------- 저장 ----------------
def save_excel(path: Path, df_data: pd.DataFrame,
               pivot_df: Optional[pd.DataFrame] = None,
               pivot_dict: Optional[Dict[Tuple[int,int], pd.DataFrame]] = None):
    """
    pivot_df: 전국용(단일)
    pivot_dict: 서울용(월별 다중)
    """
    from openpyxl import Workbook
    with pd.ExcelWriter(path, engine="openpyxl") as xw:
        df_data.to_excel(xw, index=False, sheet_name="data")
        if pivot_df is not None and not pivot_df.empty:
            pivot_df.to_excel(xw, index=False, sheet_name="피벗")
        if pivot_dict:
            for (y, m), sub in sorted(pivot_dict.items()):
                ws_name = f"피벗_{y%100:02d}{m:02d}"
                sub.to_excel(xw, index=False, sheet_name=ws_name)


# ---------------- 구글 인증/업로드/시트 ----------------
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

def load_credentials(sa_path: str) -> Credentials:
    if not Path(sa_path).exists():
        raise FileNotFoundError(f"서비스계정 JSON이 없습니다: {sa_path}")
    return Credentials.from_service_account_file(sa_path, scopes=SCOPES)

def drive_service(creds: Credentials):
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def sheets_client(creds: Credentials) -> Optional[gspread.Client]:
    try:
        gc = gspread.authorize(creds)
        return gc
    except Exception:
        return None

def drive_upload_file(svc, folder_id: str, path: Path) -> str:
    from googleapiclient.errors import HttpError
    file_metadata = {
        "name": path.name,
        "parents": [folder_id],
    }
    media = MediaFileUpload(str(path), resumable=True)
    try:
        f = svc.files().create(body=file_metadata, media_body=media, fields="id").execute()
        return f["id"]
    except HttpError as e:
        raise RuntimeError(f"Drive 업로드 실패: {e}")

def drive_cleanup_old(svc, folder_id: str, keep_days: int):
    """폴더 내 생성일 기준 keep_days 이전 파일 삭제"""
    if keep_days <= 0:
        return
    cutoff = datetime.utcnow() - timedelta(days=keep_days)
    q = f"'{folder_id}' in parents and trashed=false"
    page_token = None
    while True:
        resp = svc.files().list(q=q, spaces="drive",
                                fields="nextPageToken, files(id, name, createdTime)",
                                pageToken=page_token).execute()
        for f in resp.get("files", []):
            ct = f.get("createdTime")
            try:
                dt = datetime.fromisoformat(ct.replace("Z", "+00:00"))
            except Exception:
                continue
            if dt < cutoff:
                try:
                    svc.files().delete(fileId=f["id"]).execute()
                    print(f"  - deleted(old): {f['name']}")
                except Exception:
                    pass
        page_token = resp.get("nextPageToken")
        if not page_token:
            break

def ensure_worksheet(gc: gspread.Client, sheet_id: str, title: str, headers: List[str]) -> gspread.Worksheet:
    sh = gc.open_by_key(sheet_id)
    try:
        ws = sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows="1000", cols=str(max(10, len(headers)+3)))
        ws.update([headers])
        return ws

    # 헤더 보정
    try:
        cur = ws.row_values(1)
    except Exception:
        cur = []
    if cur != headers:
        ws.resize(rows=1)  # 전체 내용 지우고
        ws.update([headers])
    return ws

def upsert_row_by_date(ws: gspread.Worksheet, date_str: str, headers: List[str], values_map: Dict[str, int]):
    # A열 '날짜'에서 date_str 찾기
    colA = ws.col_values(1)
    target_row = None
    for i, v in enumerate(colA, start=1):
        if i == 1:
            continue
        if v.strip() == date_str:
            target_row = i
            break
    # 한 줄 구성
    row_vals = [date_str]
    for h in headers[1:]:
        row_vals.append(int(values_map.get(h, 0)))
    if target_row:
        ws.update(f"A{target_row}:{chr(64+len(headers))}{target_row}", [row_vals])
    else:
        ws.append_row(row_vals, value_input_option="RAW")


# ---------------- 파이프라인 (다운+전처리+저장+업로드+시트기록) ----------------
def pipeline_national(driver, base_month: date, today: date,
                      creds: Optional[Credentials],
                      drive_folder_id: Optional[str]):
    # 월 범위: 과거월은 말일까지, 당월은 오늘까지
    start = month_first(base_month)
    end = today if (base_month.year == today.year and base_month.month == today.month) else month_end(base_month)
    outname = f"전국 {yymm(base_month)}_{yymmdd(today)}.xlsx"
    print(f"[전국] {start.isoformat()} ~ {end.isoformat()} → {outname}")

    got = robust_download(driver, start, end, sido=None, kind="excel")
    df_raw = read_table(got)
    df = clean_df(df_raw, split_year_month=False)
    pv = pivot_national(df)

    out = SAVE_DIR / outname
    save_excel(out, df, pivot_df=pv)
    print(f"  - saved: {out}")

    # Drive 업로드
    if creds and drive_folder_id:
        svc = drive_service(creds)
        fid = drive_upload_file(svc, drive_folder_id, out)
        print(f"  - uploaded to Drive: fileId={fid}")
        drive_cleanup_old(svc, drive_folder_id, DRIVE_RETENTION_DAYS)

    # Sheets 기록
    if SHEET_ID and creds and not pv.empty:
        gc = sheets_client(creds)
        if gc:
            # 헤더: 날짜 + 광역(정렬)
            regions = sorted(pv["광역"].astype(str).tolist())
            headers = ["날짜"] + regions
            ws_title = label_month_sheet("전국", base_month.year, base_month.month)
            ws = ensure_worksheet(gc, SHEET_ID, ws_title, headers)
            # 값 dict
            m = dict(zip(pv["광역"].astype(str), pv["건수"].astype(int)))
            upsert_row_by_date(ws, today.isoformat(), headers, m)
            print(f"  - sheets: {ws_title} updated")


def pipeline_seoul(driver, today: date,
                   creds: Optional[Credentials],
                   drive_folder_id: Optional[str]):
    # 전년도 10월 1일 ~ 오늘
    start = date(today.year - 1, 10, 1)
    if start > today:
        start = date(today.year, 1, 1)
    outname = f"서울시 {yymmdd(today)}.xlsx"
    print(f"[서울] {start.isoformat()} ~ {today.isoformat()} → {outname}")

    got = robust_download(driver, start, today, sido="서울특별시", kind="excel")
    df_raw = read_table(got)
    df = clean_df(df_raw, split_year_month=True)
    pv_dict = pivot_seoul_monthly(df)

    out = SAVE_DIR / outname
    save_excel(out, df, pivot_dict=pv_dict)
    print(f"  - saved: {out}")

    # Drive 업로드
    if creds and drive_folder_id:
        svc = drive_service(creds)
        fid = drive_upload_file(svc, drive_folder_id, out)
        print(f"  - uploaded to Drive: fileId={fid}")
        drive_cleanup_old(svc, drive_folder_id, DRIVE_RETENTION_DAYS)

    # Sheets 기록 (월별 시트)
    if SHEET_ID and creds and pv_dict:
        gc = sheets_client(creds)
        if gc:
            for (y, m), sub in sorted(pv_dict.items()):
                # 헤더: 날짜 + 구(정렬)
                gus = sorted(sub["구"].astype(str).tolist())
                headers = ["날짜"] + gus
                ws_title = label_month_sheet("서울", y, m)
                ws = ensure_worksheet(gc, SHEET_ID, ws_title, headers)
                mvals = dict(zip(sub["구"].astype(str), sub["건수"].astype(int)))
                upsert_row_by_date(ws, today.isoformat(), headers, mvals)
                print(f"  - sheets: {ws_title} updated")


# ---------------- 메인 ----------------
def main():
    # 크롬/드라이버 잔여 프로세스가 있으면 CI에서 충돌 가능 → (로컬은 생략)
    if IS_CI:
        try:
            os.system("pkill -f chromium || true")
        except Exception:
            pass

    # 인증 준비 (있으면 업로드/시트 실행)
    creds = None
    if Path(SA_PATH).exists():
        try:
            creds = load_credentials(SA_PATH)
            print("  - service account loaded")
        except Exception as e:
            print(f"  ! service account load failed: {e}")

    drive_folder_id = os.environ.get("DRIVE_FOLDER_ID")

    # 드라이버
    driver = build_driver(TMP_DL)

    t = today_kst()
    try:
        # 전국: 최근 3개월(당월 포함). 오래된 달부터 실행
        months = [shift_months(month_first(t), k) for k in [-2, -1, 0]]
        for base in months:
            try:
                pipeline_national(driver, base, t, creds, drive_folder_id)
            except Exception as e:
                print(f"  ! 전국 {yymm(base)} 실패: {e}")

        # 서울: 1회
        try:
            pipeline_seoul(driver, t, creds, drive_folder_id)
        except Exception as e:
            print(f"  ! 서울 실패: {e}")

    finally:
        try:
            driver.quit()
        except Exception:
            pass
        # 임시 프로필 정리
        global _PROFILE_DIR
        try:
            if _PROFILE_DIR and Path(_PROFILE_DIR).exists():
                shutil.rmtree(_PROFILE_DIR, ignore_errors=True)
        except Exception:
            pass


if __name__ == "__main__":
    # 출력 버퍼링 방지(액션 로그 실시간)
    try:
        import functools
        print = functools.partial(print, flush=True)  # type: ignore
    except Exception:
        pass
    main()
