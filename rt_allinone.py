# -*- coding: utf-8 -*-
"""
강화 로그 버전
- 단계별 상세 로그 + 하트비트 + 네트워크 타임아웃 + 예외 위치 명확 출력
"""

from __future__ import annotations
import os, re, sys, json, time, traceback, threading, socket
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple

import numpy as np
import pandas as pd

# 네트워크 전역 타임아웃 (gspread/Drive 등 블로킹 방지)
socket.setdefaulttimeout(30)

# Selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.alert import Alert
from selenium.common.exceptions import (
    TimeoutException, ElementClickInterceptedException,
    ElementNotInteractableException
)
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# Google APIs
import gspread
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# ---------------------------
# 로깅 유틸
# ---------------------------
def now():
    return datetime.now().strftime("%H:%M:%S")

def log(msg: str):
    print(f"[{now()}] {msg}", flush=True)

def start_heartbeat(every_sec=30):
    def beat():
        while True:
            time.sleep(every_sec)
            print(f"[hb] alive {datetime.now().isoformat()}", flush=True)
    t = threading.Thread(target=beat, daemon=True)
    t.start()

# ---------------------------
# 설정
# ---------------------------
URL = "https://rt.molit.go.kr/pt/xls/xls.do?mobileAt="

BASE_DIR = Path.cwd()
TMP_DL   = BASE_DIR / "_rt_downloads"
OUT_DIR  = BASE_DIR / "_rt_out"
TMP_DL.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

DOWNLOAD_TIMEOUT = 120

# ---------------------------
# 날짜 유틸
# ---------------------------
from datetime import date, timedelta

def today_kst() -> date:
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

# ---------------------------
# 크롬 드라이버
# ---------------------------
def build_driver(download_dir: Path) -> webdriver.Chrome:
    log("[drv] build_driver 시작")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1400,900")
    ua = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    opts.add_argument(f"--user-agent={ua}")

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

    log(f"[drv] starting chrome bin={chrome_bin} driver={drv_bin}")
    d = webdriver.Chrome(service=service, options=opts)
    log("[drv] chrome started")

    # 가벼운 smoke
    try:
        d.get("about:blank")
        log("[drv] about:blank ok")
    except Exception as e:
        log(f"[drv] about:blank 실패: {e!r}")
        raise
    return d

# ---------------------------
# 페이지 조작
# ---------------------------
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
    log(f"[ui] set_dates: {start} ~ {end}")

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
                        log(f"[ui] select_sido: {wanted}")
                        return True
        except Exception:
            pass
    log(f"[ui] select_sido 실패: {wanted}")
    return False

def click_download(driver: webdriver.Chrome, kind="excel", after_wait=1.5) -> bool:
    label = "EXCEL 다운" if kind == "excel" else "CSV 다운"
    time.sleep(after_wait)

    try:
        WebDriverWait(driver, 1).until(EC.alert_is_present())
        Alert(driver).accept()
        time.sleep(0.3)
        log("[ui] pre-alert accepted")
    except TimeoutException:
        pass

    btns = driver.find_elements(By.XPATH, f"//button[normalize-space()='{label}']")
    if not btns:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(0.4)
        btns = driver.find_elements(By.XPATH, f"//button[normalize-space()='{label}']")
        if not btns:
            log("[ui] 다운로드 버튼 미발견")
            return False

    btn = btns[0]
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
    time.sleep(0.2)

    try:
        btn.click()
        log("[ui] download click(plain)")
    except (ElementClickInterceptedException, ElementNotInteractableException):
        driver.execute_script("arguments[0].click();", btn)
        log("[ui] download click(JS)")

    try:
        WebDriverWait(driver, 5).until(EC.alert_is_present())
        Alert(driver).accept()
        log("[ui] processing alert accepted")
    except TimeoutException:
        pass

    return True

def wait_download(download_dir: Path, before: set[Path], timeout: int = DOWNLOAD_TIMEOUT) -> Path:
    log("[dl] wait_download 시작")
    t0 = time.time()
    while time.time() - t0 < timeout:
        now = set(download_dir.glob("*"))
        new_files = [p for p in now - before if p.is_file()]
        crs = [p for p in new_files if p.name.endswith(".crdownload")]
        fins = [p for p in new_files if not p.name.endswith(".crdownload")]
        if crs:
            log(f"[dl] in-progress: {', '.join(x.name for x in crs)}")
        if fins:
            got = max(fins, key=lambda p: p.stat().st_mtime)
            log(f"[dl] done: {got.name}")
            return got
        time.sleep(0.5)
    raise TimeoutError("다운로드 대기 초과")

# ---------------------------
# 전처리
# ---------------------------
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
    log(f"  - got file: {path}  size={path.stat().st_size:,}  ext={ext}")
    if ext in (".xlsx", ".xls"):
        try:
            df0 = pd.read_excel(path, header=None, dtype=str, engine="openpyxl" if ext==".xlsx" else None)
        except Exception as e:
            log(f"  - read_excel 실패, read_html 대체: {e!r}")
            return _read_html_table(path)

        hdr_idx = None
        max_scan = min(120, len(df0))
        for i in range(max_scan):
            row = df0.iloc[i].astype(str).str.strip().tolist()
            if not row:
                continue
            if row[0].strip().upper() == "NO":
                hdr_idx = i; break
            if ("시군구" in row) and ("단지명" in row):
                hdr_idx = i; break
        if hdr_idx is None:
            log("  - 헤더 탐지 실패, read_html 대체")
            return _read_html_table(path)

        cols = df0.iloc[hdr_idx].astype(str).str.strip()
        df = df0.iloc[hdr_idx+1:].copy()
        df.columns = cols
        df = df.reset_index(drop=True)
        log(f"  - parsed: rows={len(df)}  cols={len(df.columns)}")
        return df

    df = _read_html_table(path)
    log(f"  - parsed(HTML): rows={len(df)}  cols={len(df.columns)}")
    return df

def clean_df(df: pd.DataFrame, split_month: bool) -> pd.DataFrame:
    if "시군구 " in df.columns and "시군구" not in df.columns:
        df = df.rename(columns={"시군구 ":"시군구"})
    rename_map = {}
    for c in list(df.columns):
        k = str(c).replace(" ", "")
        if k == "거래금액(만원)" and c != "거래금액(만원)": rename_map[c] = "거래금액(만원)"
        if k == "전용면적(㎡)" and c != "전용면적(㎡)": rename_map[c] = "전용면적(㎡)"
    if rename_map:
        df = df.rename(columns=rename_map)

    for c in list(df.columns):
        if str(c).strip().upper() == "NO":
            df = df[df[c].notna()]
            df = df.drop(columns=[c])

    for c in ["거래금액(만원)", "전용면적(㎡)"]:
        if c in df.columns:
            s = (df[c].astype(str)
                    .str.replace(",", "", regex=False)
                    .str.replace(" ", "", regex=False)
                    .str.replace("-", "", regex=False))
            s = s.replace({"": np.nan})
            df[c] = pd.to_numeric(s, errors="coerce")

    if "시군구" in df.columns:
        parts = df["시군구"].astype(str).str.split(expand=True, n=2)
        for i, name in enumerate(["광역", "구", "법정동"]):
            df[name] = parts[i].fillna("") if parts.shape[1] > i else ""

    if split_month and "계약년월" in df.columns:
        s = df["계약년월"].astype(str).str.replace(r"\D","", regex=True)
        df["계약년"] = s.str.slice(0,4)
        df["계약월"] = s.str.slice(4,6)

    return df.reset_index(drop=True)

# ---------------------------
# 피벗 & 저장
# ---------------------------
def pivot_national(df: pd.DataFrame) -> pd.DataFrame:
    if "광역" in df.columns and "거래금액(만원)" in df.columns:
        pv = df.pivot_table(index="광역", values="거래금액(만원)", aggfunc="count")
        pv = pv.rename(columns={"거래금액(만원)":"건수"}).reset_index()
        return pv.sort_values("광역")
    return pd.DataFrame()

def pivot_seoul(df: pd.DataFrame) -> pd.DataFrame:
    if {"구", "계약월", "거래금액(만원)"}.issubset(df.columns):
        pv = df.pivot_table(index="구", columns="계약월",
                            values="거래금액(만원)", aggfunc="count", fill_value=0)
        pv = pv.sort_index(axis=1).reset_index()
        return pv
    return pd.DataFrame()

def save_excel(path: Path, df: pd.DataFrame, pivot: Optional[pd.DataFrame], pivot_name="피벗"):
    with pd.ExcelWriter(path, engine="openpyxl") as xw:
        df.to_excel(xw, index=False, sheet_name="data")
        if pivot is not None and not pivot.empty:
            pivot.to_excel(xw, index=False, sheet_name=pivot_name)
    log(f"완료: {path}")

# ---------------------------
# 구글
# ---------------------------
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets"
]

def load_creds():
    sa_path = os.environ.get("SA_PATH", "sa.json")
    log(f"[gcp] load_creds from {sa_path}")
    with open(sa_path, "r", encoding="utf-8") as f:
        info = json.load(f)
    return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)

def drive_service(creds):
    log("[gcp] build drive service")
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def sheets_client(creds):
    log("[gcp] authorize gspread")
    return gspread.authorize(creds)

def upload_to_drive(file_path: Path, folder_id: str, svc):
    log(f"[gcp] upload_to_drive → {file_path.name}")
    meta = {"name": file_path.name, "parents": [folder_id]}
    media = MediaFileUpload(str(file_path), resumable=True)
    f = svc.files().create(body=meta, media_body=media, fields="id,name").execute()
    log(f"[gcp] uploaded: {f.get('name')} ({f.get('id')})")
    return f.get("id")

def apply_drive_retention(folder_id: str, days: int, svc):
    if days <= 0: return
    log(f"[gcp] retention: {days} days in folder {folder_id}")
    cutoff = datetime.utcnow() - timedelta(days=days)
    q = f"'{folder_id}' in parents and trashed = false"
    page = None; removed = 0
    while True:
        resp = svc.files().list(q=q, spaces="drive",
                                fields="nextPageToken, files(id,name,createdTime)",
                                pageToken=page).execute()
        for f in resp.get("files", []):
            ct = datetime.fromisoformat(f["createdTime"].replace("Z","+00:00"))
            if ct < cutoff:
                svc.files().delete(fileId=f["id"]).execute()
                removed += 1
                log(f"[gcp] removed: {f['name']} ({f['id']})")
        page = resp.get("nextPageToken")
        if not page: break
    log(f"[gcp] retention done. removed={removed}")

def ensure_sheet(ws_list, title: str, gc, sh):
    for ws in ws_list:
        if ws.title == title:
            return ws
    log(f"[gs] create sheet: {title}")
    return sh.add_worksheet(title=title, rows=200, cols=200)

def write_national_to_sheet(pv: pd.DataFrame, gc, sh, run_date: date):
    y = run_date.year % 100
    m = run_date.month
    title = f"전국 {y:02d}년 {m:02d}월"
    ws = ensure_sheet(sh.worksheets(), title, gc, sh)
    today_key = yymmdd(run_date)

    values = ws.get_all_values()
    if not values:
        header = ["지역", today_key]
        body = [[r, int(pv.loc[pv["광역"]==r, "건수"].values[0])] for r in pv["광역"].tolist()]
        ws.update("A1", [header] + body)
        log(f"[gs] init write: {title}")
        return

    header = values[0]
    col_map = {name: idx+1 for idx, name in enumerate(header)}
    row_map = {row[0]: (i+2) for i, row in enumerate(values[1:]) if row and row[0]}

    if today_key not in col_map:
        ws.update_cell(1, len(header)+1, today_key)
        col_map[today_key] = len(header)+1

    # 행 추가 필요 시
    app = []
    for r in pv["광역"].tolist():
        if r not in row_map:
            app.append([r])
    if app:
        ws.append_rows(app, value_input_option="RAW")
        values = ws.get_all_values()
        header = values[0]
        col_map = {name: idx+1 for idx, name in enumerate(header)}
        row_map = {row[0]: (i+2) for i, row in enumerate(values[1:]) if row and row[0]}

    # 채우기
    for region, cnt in pv[["광역","건수"]].itertuples(index=False):
        ws.update_cell(row_map[region], col_map[today_key], int(cnt))
    log(f"[gs] updated: {title} @ {today_key}")

def write_seoul_to_sheet(pv: pd.DataFrame, gc, sh, run_date: date):
    y = run_date.year % 100
    months = [int(c) for c in pv.columns if c != "구" and str(c).isdigit()]
    months.sort()
    for m in months:
        title = f"서울 {y:02d}년 {m:02d}월"
        ws = ensure_sheet(sh.worksheets(), title, gc, sh)
        today_key = yymmdd(run_date)

        values = ws.get_all_values()
        if not values:
            header = ["구", today_key]
            col = f"{m:02d}"
            body = [[gu, int(pv.loc[pv["구"]==gu, col].values[0])] for gu in pv["구"].tolist()]
            ws.update("A1", [header] + body)
            log(f"[gs] init write: {title}")
            continue

        header = values[0]
        col_map = {name: idx+1 for idx, name in enumerate(header)}
        row_map = {row[0]: (i+2) for i, row in enumerate(values[1:]) if row and row[0]}

        if today_key not in col_map:
            ws.update_cell(1, len(header)+1, today_key)
            col_map[today_key] = len(header)+1

        app = []
        for gu in pv["구"].tolist():
            if gu not in row_map:
                app.append([gu])
        if app:
            ws.append_rows(app, value_input_option="RAW")
            values = ws.get_all_values()
            header = values[0]
            col_map = {name: idx+1 for idx, name in enumerate(header)}
            row_map = {row[0]: (i+2) for i, row in enumerate(values[1:]) if row and row[0]}

        col = f"{m:02d}"
        for gu in pv["구"].tolist():
            cnt = int(pv.loc[pv["구"]==gu, col].values[0])
            ws.update_cell(row_map[gu], col_map[today_key], cnt)
        log(f"[gs] updated: {title} @ {today_key}")

# ---------------------------
# 한 번의 작업
# ---------------------------
def fetch_and_process(driver: webdriver.Chrome,
                      sido: Optional[str],
                      start: date, end: date,
                      outname: str,
                      mode: str,
                      gc=None, sh=None, drv=None):
    log(f"[job] fetch_and_process start mode={mode} out={outname}")
    driver.get(URL)
    log("[job] page opened")
    set_dates(driver, start, end)
    if sido:
        select_sido(driver, sido)

    before = set(TMP_DL.glob("*"))
    ok = click_download(driver, "excel", after_wait=1.5)
    log(f"[job] click_download -> {ok}")
    if not ok:
        raise RuntimeError("다운로드 버튼 클릭 실패")

    got = wait_download(TMP_DL, before, timeout=DOWNLOAD_TIMEOUT)
    df_raw = read_table(got)
    split_month = (mode == "seoul")
    df = clean_df(df_raw, split_month=split_month)

    if mode == "national":
        pv = pivot_national(df); pvt_name="피벗(광역-건수)"
    else:
        pv = pivot_seoul(df);    pvt_name="피벗(구x월)"

    out_path = OUT_DIR / outname
    save_excel(out_path, df, pv, pivot_name=pvt_name)

    if drv:
        upload_to_drive(out_path, os.environ.get("DRIVE_FOLDER_ID",""), drv)

    if gc and sh:
        run_d = today_kst()
        if mode == "national":
            write_national_to_sheet(pv, gc, sh, run_d)
        else:
            write_seoul_to_sheet(pv, gc, sh, run_d)

    log(f"[job] fetch_and_process end {outname}")

# ---------------------------
# 메인
# ---------------------------
def main():
    start_heartbeat(30)
    log("script start")
    log(f"Python {sys.version.split()[0]} | pandas {pd.__version__}")

    # 1) 구글 인증/클라이언트
    log("[main] loading creds …")
    creds = load_creds()
    log("[main] creds ok")

    log("[main] build services …")
    drv = drive_service(creds)
    log("[main] drive ok")
    gc  = sheets_client(creds)
    log("[main] gspread ok")

    sh_id = os.environ.get("SHEET_ID","")
    log(f"[main] open sheet {sh_id[:6]}…")
    sh = gc.open_by_key(sh_id)
    log("[main] sheet open ok")

    # 2) 크롬
    driver = build_driver(TMP_DL)

    try:
        t = today_kst()
        months = [shift_months(month_first(t), k) for k in [-2, -1, 0]]
        for base in months:
            start = base
            end = t if (base.year, base.month)==(t.year, t.month) else month_last(base)
            outname = f"전국 {yymm(base)}_{yymmdd(t)}.xlsx"
            log(f"[전국] {start} ~ {end} → {outname}")
            fetch_and_process(driver, None, start, end, outname, "national", gc, sh, drv)
            time.sleep(1.0)

        start_seoul = date(t.year - 1, 10, 1)
        if start_seoul > t:
            start_seoul = date(t.year, 1, 1)
        outname_seoul = f"서울시 {yymmdd(t)}.xlsx"
        log(f"[서울] {start_seoul} ~ {t} → {outname_seoul}")
        fetch_and_process(driver, "서울특별시", start_seoul, t, outname_seoul, "seoul", gc, sh, drv)

        keep = int(os.environ.get("DRIVE_RETENTION_DAYS", "3"))
        if keep > 0:
            apply_drive_retention(os.environ.get("DRIVE_FOLDER_ID",""), keep, drv)

        log("script done")
    finally:
        try:
            driver.quit()
            log("[drv] quit")
        except Exception:
            pass

if __name__ == "__main__":
    try:
        main()
    except Exception:
        log("FATAL ERROR:")
        traceback.print_exc()
        sys.exit(1)
