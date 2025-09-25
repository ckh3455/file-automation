# -*- coding: utf-8 -*-
"""
rt_allinone.py
- rt.molit.go.kr 조건별 자료제공 페이지에서 (전국 최근3개월 + 서울 1년치) 자동 다운로드
- 원본 전처리(행/열 정리, 숫자화, 주소 분리) 후 엑셀 저장 + 간단 피벗
- Google Drive 업로드(+보관기간 초과 파일 정리), Google Sheets 기록

필수 환경변수(워크플로에서 주입):
  SA_PATH            : 서비스계정 JSON 파일 경로 (예: ./sa.json)
  DRIVE_FOLDER_ID    : 업로드할 구글드라이브 폴더 ID
  SHEET_ID           : 기록할 구글시트 ID
  CHROME_BIN         : /usr/bin/chromium-browser
  CHROMEDRIVER_BIN   : /usr/bin/chromedriver
"""

from __future__ import annotations
import os, re, sys, json, time, traceback
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple, List, Dict

import numpy as np
import pandas as pd

# Selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.alert import Alert
from selenium.common.exceptions import (
    TimeoutException, ElementClickInterceptedException, ElementNotInteractableException, NoSuchElementException
)
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# Google APIs
import gspread
from google.oauth2 import service_account
from googleapiclient.discovery import build

# --------------------------------
# 설정
# --------------------------------
URL = "https://rt.molit.go.kr/pt/xls/xls.do?mobileAt="

# Actions 런너의 임시 다운로드/결과 폴더
BASE_DIR = Path.cwd()
TMP_DL   = BASE_DIR / "_rt_downloads"
OUT_DIR  = BASE_DIR / "_rt_out"
TMP_DL.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

# 다운로드/대기
DOWNLOAD_TIMEOUT = 120

# --------------------------------
# 날짜 유틸
# --------------------------------
def today_kst() -> date:
    # 런너 TZ=Asia/Seoul 이므로 로컬 today()로 충분
    return date.today()

def month_first(d: date) -> date:
    return date(d.year, d.month, 1)

def month_last(d: date) -> date:
    return (month_first(d) + timedelta(days=40)).replace(day=1) - timedelta(days=1)

def shift_months(d: date, k: int) -> date:
    y, m = d.year, d.month
    m2 = m + k
    y += (m2 - 1) // 12
    m2 = (m2 - 1) % 12 + 1
    end = month_last(date(y, m2, 1))
    return date(y, m2, min(d.day, end.day))

def yymm(d: date) -> str:
    return f"{d.year%100:02d}{d.month:02d}"

def yymmdd(d: date) -> str:
    return f"{d.year%100:02d}{d.month:02d}{d.day:02d}"

# --------------------------------
# 크롬 드라이버
# --------------------------------
def build_driver(download_dir: Path) -> webdriver.Chrome:
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    # 사이트가 헤드리스 탐지 시 완화용
    opts.add_argument("--window-size=1400,900")
    ua = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "\
         "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    opts.add_argument(f"--user-agent={ua}")

    # 다운로드 경로
    prefs = {
        "download.default_directory": str(download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    opts.add_experimental_option("prefs", prefs)

    chrome_bin = os.environ.get("CHROME_BIN")
    if chrome_bin:
        opts.binary_location = chrome_bin

    drv_bin = os.environ.get("CHROMEDRIVER_BIN")
    service = Service(drv_bin) if drv_bin else Service()

    print(f"[dbg] starting chrome bin={chrome_bin} drv={drv_bin}", flush=True)
    driver = webdriver.Chrome(service=service, options=opts)
    print("[dbg] chrome started", flush=True)
    return driver

# --------------------------------
# 페이지 조작
# --------------------------------
def _clear_and_type(el, s: str):
    el.click()
    el.clear()
    el.send_keys(s)

def find_date_inputs(driver: webdriver.Chrome):
    inputs = driver.find_elements(By.CSS_SELECTOR, "input")
    cands = []
    for el in inputs:
        try:
            t = (el.get_attribute("value") or "") + " " + (el.get_attribute("placeholder") or "")
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

def set_dates(driver: webdriver.Chrome, start: date, end: date):
    s_el, e_el = find_date_inputs(driver)
    _clear_and_type(s_el, start.isoformat())
    time.sleep(0.2)
    _clear_and_type(e_el, end.isoformat())
    time.sleep(0.2)
    print(f"  - set_dates: {start} ~ {end}", flush=True)

def select_sido(driver: webdriver.Chrome, wanted: str) -> bool:
    wanted = wanted.strip()
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

def click_download(driver: webdriver.Chrome, kind="excel", after_wait=1.5) -> bool:
    """기간 설정 후 바로 다운로드 버튼 누르기. 알림(처리중…)은 자동 확인."""
    label = "EXCEL 다운" if kind == "excel" else "CSV 다운"
    time.sleep(after_wait)

    # 떠있는 alert 먼저 정리
    try:
        WebDriverWait(driver, 1).until(EC.alert_is_present())
        Alert(driver).accept()
        time.sleep(0.3)
    except TimeoutException:
        pass

    # 버튼 찾기/스크롤
    btns = driver.find_elements(By.XPATH, f"//button[normalize-space()='{label}']")
    if not btns:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(0.4)
        btns = driver.find_elements(By.XPATH, f"//button[normalize-space()='{label}']")
        if not btns:
            return False

    btn = btns[0]
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
    time.sleep(0.2)

    # 클릭 시도
    try:
        btn.click()
    except (ElementClickInterceptedException, ElementNotInteractableException):
        driver.execute_script("arguments[0].click();", btn)

    # “처리중입니다…” alert 확인 후 닫기
    try:
        WebDriverWait(driver, 5).until(EC.alert_is_present())
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

# --------------------------------
# 전처리
# --------------------------------
def _read_html_table(path: Path) -> pd.DataFrame:
    # 일부 환경에서 htm/엑셀이 섞여도 read_html로 표 인식
    tables = pd.read_html(str(path), flavor="bs4", thousands=",", displayed_only=False)
    for t in tables:
        row0 = [str(x).strip() for x in list(t.columns)]
        if ("시군구" in row0 and "단지명" in row0) or ("NO" in row0 and "시군구" in row0):
            return t
        # 헤더가 본문 첫행에 있는 케이스
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
    print(f"  - got file: {path}  size={path.stat().st_size:,}  ext={ext}", flush=True)
    if ext in (".xlsx", ".xls"):
        try:
            df0 = pd.read_excel(path, header=None, dtype=str, engine="openpyxl" if ext==".xlsx" else None)
        except Exception:
            return _read_html_table(path)

        # 헤더 행 탐색
        hdr_idx = None
        max_scan = min(100, len(df0))
        for i in range(max_scan):
            row = df0.iloc[i].astype(str).str.strip().tolist()
            if not row: 
                continue
            if row[0].strip().upper() == "NO":
                hdr_idx = i; break
            if ("시군구" in row) and ("단지명" in row):
                hdr_idx = i; break
        if hdr_idx is None:
            return _read_html_table(path)

        cols = df0.iloc[hdr_idx].astype(str).str.strip()
        df = df0.iloc[hdr_idx+1:].copy()
        df.columns = cols
        df = df.reset_index(drop=True)
        print(f"  - parsed: rows={len(df)}  cols={len(df.columns)}", flush=True)
        return df

    # html 등
    df = _read_html_table(path)
    print(f"  - parsed(HTML): rows={len(df)}  cols={len(df.columns)}", flush=True)
    return df

def clean_df(df: pd.DataFrame, split_month: bool) -> pd.DataFrame:
    # 컬럼 표준화
    if "시군구 " in df.columns and "시군구" not in df.columns:
        df = df.rename(columns={"시군구 ":"시군구"})
    rename_map = {}
    for c in list(df.columns):
        k = str(c).replace(" ", "")
        if k == "거래금액(만원)" and c != "거래금액(만원)": rename_map[c] = "거래금액(만원)"
        if k == "전용면적(㎡)" and c != "전용면적(㎡)": rename_map[c] = "전용면적(㎡)"
    if rename_map:
        df = df.rename(columns=rename_map)

    # NO 열 제거
    for c in list(df.columns):
        if str(c).strip().upper() == "NO":
            df = df[df[c].notna()]
            df = df.drop(columns=[c])

    # 숫자화
    for c in ["거래금액(만원)", "전용면적(㎡)"]:
        if c in df.columns:
            s = (df[c].astype(str)
                    .str.replace(",", "", regex=False)
                    .str.replace(" ", "", regex=False)
                    .str.replace("-", "", regex=False))
            s = s.replace({"": np.nan})
            df[c] = pd.to_numeric(s, errors="coerce")

    # 주소 분리
    if "시군구" in df.columns:
        parts = df["시군구"].astype(str).str.split(expand=True, n=2)
        for i, name in enumerate(["광역", "구", "법정동"]):
            if parts.shape[1] > i:
                df[name] = parts[i].fillna("")
            else:
                df[name] = ""

    # 전국: 계약년월 split 불필요 / 서울: 분리
    if split_month and "계약년월" in df.columns:
        s = df["계약년월"].astype(str).str.replace(r"\D","", regex=True)
        df["계약년"] = s.str.slice(0,4)
        df["계약월"] = s.str.slice(4,6)

    return df.reset_index(drop=True)

# --------------------------------
# 피벗 & 저장
# --------------------------------
def pivot_national(df: pd.DataFrame) -> pd.DataFrame:
    if "광역" in df.columns and "거래금액(만원)" in df.columns:
        pv = df.pivot_table(index="광역", values="거래금액(만원)", aggfunc="count")
        pv = pv.rename(columns={"거래금액(만원)":"건수"}).reset_index()
        return pv.sort_values("광역")
    return pd.DataFrame()

def pivot_seoul(df: pd.DataFrame) -> pd.DataFrame:
    # 구 x 계약월 → 건수
    if {"구", "계약월", "거래금액(만원)"}.issubset(df.columns):
        pv = df.pivot_table(index="구", columns="계약월",
                            values="거래금액(만원)", aggfunc="count", fill_value=0)
        pv = pv.sort_index(axis=1)  # 월 오름차순
        pv = pv.reset_index()
        return pv
    return pd.DataFrame()

def save_excel(path: Path, df: pd.DataFrame, pivot: Optional[pd.DataFrame], pivot_name="피벗"):
    with pd.ExcelWriter(path, engine="openpyxl") as xw:
        df.to_excel(xw, index=False, sheet_name="data")
        if pivot is not None and not pivot.empty:
            pivot.to_excel(xw, index=False, sheet_name=pivot_name)
    print(f"완료: {path}", flush=True)

# --------------------------------
# 구글 인증/업로드/시트 기록
# --------------------------------
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets"
]

def load_creds() -> service_account.Credentials:
    sa_path = os.environ.get("SA_PATH", "sa.json")
    with open(sa_path, "r", encoding="utf-8") as f:
        info = json.load(f)
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return creds

def drive_service(creds):
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def sheets_client(creds):
    return gspread.authorize(creds)

def upload_to_drive(file_path: Path, folder_id: str, svc):
    file_metadata = {
        "name": file_path.name,
        "parents": [folder_id]
    }
    media_body = None
    from googleapiclient.http import MediaFileUpload
    media_body = MediaFileUpload(str(file_path), resumable=True)
    file = svc.files().create(body=file_metadata, media_body=media_body, fields="id, name").execute()
    print(f"  - Drive uploaded: {file.get('name')} (id={file.get('id')})", flush=True)
    return file.get("id")

def apply_drive_retention(folder_id: str, days: int, svc):
    if days <= 0:
        return
    cutoff = datetime.utcnow() - timedelta(days=days)
    q = f"'{folder_id}' in parents and trashed = false"
    pageToken = None
    to_delete = []
    while True:
        resp = svc.files().list(q=q, spaces="drive",
                                fields="nextPageToken, files(id, name, createdTime)",
                                pageToken=pageToken).execute()
        for f in resp.get("files", []):
            ct = datetime.fromisoformat(f["createdTime"].replace("Z","+00:00"))
            if ct < cutoff:
                to_delete.append(f)
        pageToken = resp.get("nextPageToken")
        if not pageToken: break
    for f in to_delete:
        svc.files().delete(fileId=f["id"]).execute()
        print(f"  - Drive removed (retention): {f['name']} ({f['id']})", flush=True)

def ensure_sheet(ws_list, title: str, gc, sh):
    for ws in ws_list:
        if ws.title == title:
            return ws
    print(f"  - create sheet: {title}", flush=True)
    return sh.add_worksheet(title=title, rows=200, cols=200)

def write_national_to_sheet(pv: pd.DataFrame, gc, sh, run_date: date):
    # 시트명 예: "전국 25년 07월"
    y = run_date.year % 100
    m = run_date.month
    sheet_title = f"전국 {y:02d}년 {m:02d}월"
    ws = ensure_sheet(sh.worksheets(), sheet_title, gc, sh)

    # 레이아웃: 1행 = ["지역", "YYMMDD", ...], 1열 = 광역명
    today_key = yymmdd(run_date)

    values = ws.get_all_values()
    if not values:
        header = ["지역", today_key]
        body = [[r, 0] for r in pv["광역"].tolist()]
        ws.update("A1", [header] + body)
        # 채우기
        for i, row_name in enumerate(pv["광역"].tolist(), start=2):
            ws.update_cell(i, 2, int(pv.loc[pv["광역"]==row_name, "건수"].values[0]))
        print(f"  - Sheets init write: {sheet_title}", flush=True)
        return

    # 기존 헤더/행 맵
    header = values[0] if values else []
    col_map = {name: idx+1 for idx, name in enumerate(header)}
    row_map = {}
    for r_idx, row in enumerate(values[1:], start=2):
        if row and row[0]:
            row_map[row[0]] = r_idx

    # 없으면 날짜 열 추가
    if today_key not in col_map:
        ws.update_cell(1, len(header)+1, today_key)
        col_map[today_key] = len(header)+1

    # 지역 행 없으면 추가
    append_rows = []
    for region in pv["광역"].tolist():
        if region not in row_map:
            append_rows.append([region])
    if append_rows:
        ws.append_rows(append_rows, value_input_option="RAW")
        # 새로고침
        values = ws.get_all_values()
        header = values[0]
        col_map = {name: idx+1 for idx, name in enumerate(header)}
        row_map = {}
        for r_idx, row in enumerate(values[1:], start=2):
            if row and row[0]:
                row_map[row[0]] = r_idx

    # 값 채우기
    updates = []
    for region, cnt in pv[["광역","건수"]].itertuples(index=False):
        r = row_map[region]
        c = col_map[today_key]
        updates.append((r, c, int(cnt)))
    # batch update
    for r,c,val in updates:
        ws.update_cell(r, c, val)
    print(f"  - Sheets updated: {sheet_title} @ {today_key}", flush=True)

def write_seoul_to_sheet(pv: pd.DataFrame, gc, sh, run_date: date):
    # pv: 구 x (월1..월12) 건수
    # 각 월별 시트: "서울 25년 MM월"
    y = run_date.year % 100
    header_months = [c for c in pv.columns if c != "구"]
    header_months = sorted([int(m) for m in header_months if str(m).isdigit()])

    for m in header_months:
        sheet_title = f"서울 {y:02d}년 {m:02d}월"
        ws = ensure_sheet(sh.worksheets(), sheet_title, gc, sh)
        today_key = yymmdd(run_date)

        values = ws.get_all_values()
        if not values:
            header = ["구", today_key]
            # 해당 월의 건수만 뽑는다
            col = f"{m:02d}"
            body = [[gu, int(pv.loc[pv["구"]==gu, col].values[0])] for gu in pv["구"].tolist()]
            ws.update("A1", [header] + body)
            print(f"  - Sheets init write: {sheet_title}", flush=True)
            continue

        header = values[0]
        col_map = {name: idx+1 for idx, name in enumerate(header)}
        row_map = {}
        for r_idx, row in enumerate(values[1:], start=2):
            if row and row[0]:
                row_map[row[0]] = r_idx

        if today_key not in col_map:
            ws.update_cell(1, len(header)+1, today_key)
            col_map[today_key] = len(header)+1

        # 구 행 없는 경우 추가
        append_rows = []
        for gu in pv["구"].tolist():
            if gu not in row_map:
                append_rows.append([gu])
        if append_rows:
            ws.append_rows(append_rows, value_input_option="RAW")
            values = ws.get_all_values()
            header = values[0]
            col_map = {name: idx+1 for idx, name in enumerate(header)}
            row_map = {}
            for r_idx, row in enumerate(values[1:], start=2):
                if row and row[0]:
                    row_map[row[0]] = r_idx

        col = f"{m:02d}"
        updates = []
        for gu in pv["구"].tolist():
            cnt = int(pv.loc[pv["구"]==gu, col].values[0])
            r = row_map[gu]
            c = col_map[today_key]
            updates.append((r, c, cnt))
        for r,c,val in updates:
            ws.update_cell(r, c, val)
        print(f"  - Sheets updated: {sheet_title} @ {today_key}", flush=True)

# --------------------------------
# 한 번의 작업 (다운로드 → 전처리 → 저장 → 업로드/시트)
# --------------------------------
def fetch_and_process(driver: webdriver.Chrome,
                      sido: Optional[str],
                      start: date, end: date,
                      outname: str,
                      mode: str,
                      gc=None, sh=None, drv=None):
    """mode: 'national' or 'seoul'"""
    driver.get(URL)
    set_dates(driver, start, end)

    if sido:
        ok_sido = select_sido(driver, sido)
        if not ok_sido:
            print("  ! 시도 드롭다운 탐지 실패(무시하고 진행)", flush=True)

    # 다운로드 시도(짧게 기다렸다가 바로 클릭)
    before = set(TMP_DL.glob("*"))
    ok = click_download(driver, "excel", after_wait=1.5)
    print(f"  - click_download(excel) -> {ok}", flush=True)
    if not ok:
        raise RuntimeError("다운로드 버튼 클릭 실패")

    got = wait_download(TMP_DL, before, timeout=DOWNLOAD_TIMEOUT)

    # 읽기/전처리
    df_raw = read_table(got)
    split_month = (mode == "seoul")
    df = clean_df(df_raw, split_month=split_month)

    # 피벗
    if mode == "national":
        pv = pivot_national(df)
        pvt_name = "피벗(광역-건수)"
    else:
        pv = pivot_seoul(df)
        pvt_name = "피벗(구x월)"

    # 저장
    out_path = OUT_DIR / outname
    save_excel(out_path, df, pv, pivot_name=pvt_name)

    # 드라이브 업로드
    if drv:
        upload_to_drive(out_path, os.environ.get("DRIVE_FOLDER_ID",""), drv)

    # 구글시트 기록
    if gc and sh:
        run_d = today_kst()
        if mode == "national":
            write_national_to_sheet(pv, gc, sh, run_d)
        else:
            write_seoul_to_sheet(pv, gc, sh, run_d)

# --------------------------------
# 메인
# --------------------------------
def main():
    print("[dbg] script started", flush=True)

    # 구글 인증 준비
    creds = load_creds()
    gc = sheets_client(creds)
    sh = gc.open_by_key(os.environ.get("SHEET_ID",""))
    drv = drive_service(creds)

    # 드라이버
    driver = build_driver(TMP_DL)

    try:
        t = today_kst()

        # 전국: 최근 3개월 (이번달은 오늘까지만)
        months = [shift_months(month_first(t), k) for k in [-2, -1, 0]]
        for base in months:
            start = base
            if base.year == t.year and base.month == t.month:
                end = t
            else:
                end = month_last(base)
            outname = f"전국 {yymm(base)}_{yymmdd(t)}.xlsx"
            print(f"[전국] {start} ~ {end} → {outname}", flush=True)
            fetch_and_process(driver, None, start, end, outname, "national", gc, sh, drv)
            time.sleep(1.0)

        # 서울: 전년도 10월 1일 ~ 오늘(한 번에)
        start_seoul = date(t.year - 1, 10, 1)
        if start_seoul > t:
            start_seoul = date(t.year, 1, 1)
        outname_seoul = f"서울시 {yymmdd(t)}.xlsx"
        print(f"[서울] {start_seoul} ~ {t} → {outname_seoul}", flush=True)
        fetch_and_process(driver, "서울특별시", start_seoul, t, outname_seoul, "seoul", gc, sh, drv)

        # 드라이브 보관기간 정리(옵션)
        keep_days = int(os.environ.get("DRIVE_RETENTION_DAYS", "3"))
        if keep_days > 0:
            apply_drive_retention(os.environ.get("DRIVE_FOLDER_ID",""), keep_days, drv)

    finally:
        try:
            driver.quit()
        except Exception:
            pass

if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        sys.exit(1)
