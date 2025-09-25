# -*- coding: utf-8 -*-
"""
rt_allinone.py
- rt.molit.go.kr 조건별 자료제공 페이지에서 월별/지역별 데이터를 자동 다운로드
- 다운받은 원본을 전처리(행/열 정리, 숫자화, 주소 분리) 후 엑셀 저장 + 간단 피벗 탭 작성
- 전국: 최근 3개월(당월 포함) -> 각각 "전국 YYMM_YYMMDD.xlsx"
- 서울: 전년도 10월 1일 ~ 실행일 -> "서울시 YYMMDD.xlsx"
- (CI 모드) 다운로드 시작 감지 실패 시 즉시 재시도, '시도당 시작 감지 20초', 최대 15회
- 전처리 완료 후 Google Sheets에 누적 기록:
   * 전국: 시트명 "전국 YY년 M월", 행=광역, 열=날짜(YYMMDD)로 누적
   * 서울: 시트명 "서울 YY년 M월", 행=구, 열=날짜(YYMMDD)로 누적
"""

from __future__ import annotations
import os, re, time, json, traceback
from datetime import date, timedelta
from pathlib import Path
from typing import Optional, Tuple, List, Dict

import pandas as pd
import numpy as np

# -------------------------
# 환경/경로
# -------------------------
URL = "https://rt.molit.go.kr/pt/xls/xls.do?mobileAt="
ROOT = Path.cwd()
SAVE_DIR = ROOT / "output"               # 결과 파일 저장 폴더
TMP_DL   = ROOT / "_rt_downloads"        # 임시 다운로드 폴더
PROFILE  = ROOT / "_rt_profile"          # 크롬 프로필(세션 격리)
for p in (SAVE_DIR, TMP_DL, PROFILE):
    p.mkdir(parents=True, exist_ok=True)

DOWNLOAD_TIMEOUT_FINISH = 300  # 다운로드 완료 대기(최대) - 시작 감지 이후
CLICK_MAX_TRY = int(os.environ.get("CLICK_MAX_TRY", "15"))  # 최대 15회
START_DETECT_SEC = int(os.environ.get("START_DETECT_SEC", "20"))  # 시작 감지 20초
COOLDOWN_BETWEEN_FILES = 2

# Sheets/Drive
SHEET_ID = os.environ.get("SHEET_ID", "").strip()
SA_PATH  = os.environ.get("SA_PATH", "").strip()
DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID", "").strip()  # 없으면 Drive 업로드 skip
ARTIFACTS_MODE = os.environ.get("ARTIFACTS_MODE", "").strip()     # 이 값이 있으면 Drive 업로드 skip
TODAY = date.today()

# -------------------------
# 로깅 도우미
# -------------------------
def log(msg: str):
    print(msg, flush=True)

def yymm(d: date) -> str:
    return f"{d.year%100:02d}{d.month:02d}"

def yymmdd(d: date) -> str:
    return f"{d.year%100:02d}{d.month:02d}{d.day:02d}"

def month_first(d: date) -> date:
    return date(d.year, d.month, 1)

def shift_months(d: date, k: int) -> date:
    y, m = d.year, d.month
    m2 = m + k
    y += (m2-1)//12
    m2 = (m2-1)%12 + 1
    end = (date(y, m2, 1) + timedelta(days=40)).replace(day=1) - timedelta(days=1)
    return date(y, m2, min(d.day, end.day))

# -------------------------
# 브라우저 준비
# -------------------------
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.alert import Alert
from selenium.common.exceptions import (
    TimeoutException, ElementClickInterceptedException, ElementNotInteractableException
)

def build_driver(download_dir: Path) -> webdriver.Chrome:
    opts = Options()
    # GitHub Actions 스냅 크롬 경로 지원
    chrome_bin = os.environ.get("CHROME_BIN")
    if chrome_bin:
        opts.binary_location = chrome_bin

    prefs = {
        "download.default_directory": str(download_dir.resolve()),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    opts.add_experimental_option("prefs", prefs)

    # CI/headless 안정 옵션
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument(f"--user-data-dir={PROFILE.resolve()}")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_window_size(1400, 900)
    return driver

# -------------------------
# 페이지 조작: 날짜, 시도/시군구, 다운로드
# -------------------------
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
    log(f"  - set_dates: {start.isoformat()} ~ {end.isoformat()}")
    s_el, e_el = find_date_inputs(driver)
    clear_and_type(s_el, start.isoformat()); time.sleep(0.2)
    clear_and_type(e_el, end.isoformat());   time.sleep(0.2)

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
                        o.click(); time.sleep(0.3)
                        log(f"  - select_sido({wanted}): True")
                        return True
        except Exception:
            pass
    log(f"  - select_sido({wanted}): False")
    return False

def _close_alert_if_any(driver: webdriver.Chrome):
    try:
        WebDriverWait(driver, 0.8).until(EC.alert_is_present())
        Alert(driver).accept()
        time.sleep(0.2)
    except TimeoutException:
        pass

def _find_download_button(driver: webdriver.Chrome, kind="excel"):
    label = "EXCEL 다운" if kind == "excel" else "CSV 다운"
    # 버튼 텍스트가 미묘히 달라질 수 있어 contains도 병행
    btns = driver.find_elements(By.XPATH, f"//button[normalize-space()='{label}' or contains(., 'EXCEL') and contains(., '다운')]")
    if not btns:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(0.3)
        btns = driver.find_elements(By.XPATH, f"//button[normalize-space()='{label}' or contains(., 'EXCEL') and contains(., '다운')]")
    return btns[0] if btns else None

def _snapshot_files(download_dir: Path) -> set[Path]:
    return set(download_dir.glob("*"))

def _new_files_since(download_dir: Path, before: set[Path]) -> List[Path]:
    now = set(download_dir.glob("*"))
    new_files = [p for p in now - before if p.is_file()]
    return sorted(new_files, key=lambda p: p.stat().st_mtime)

def _wait_download_finish(download_dir: Path, before: set[Path], timeout: int = DOWNLOAD_TIMEOUT_FINISH) -> Path:
    """시작이 감지된 뒤, 완료(*.crdownload 제거)까지 기다림"""
    t0 = time.time()
    while time.time() - t0 < timeout:
        new_files = _new_files_since(download_dir, before)
        done = [p for p in new_files if not p.name.endswith(".crdownload")]
        if done:
            return max(done, key=lambda p: p.stat().st_mtime)
        time.sleep(0.5)
    raise TimeoutError("다운로드 완료 대기 초과")

def click_and_detect_start(driver: webdriver.Chrome, download_dir: Path, kind="excel", start_detect_sec=START_DETECT_SEC) -> Optional[Path]:
    """버튼 클릭 후 '다운로드 시작' (.crdownload 또는 신규 파일 생성) 20초 내 감지. 실패 시 None."""
    _close_alert_if_any(driver)
    btn = _find_download_button(driver, kind)
    if not btn:
        return None

    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
    time.sleep(0.1)
    try:
        btn.click()
    except (ElementClickInterceptedException, ElementNotInteractableException):
        driver.execute_script("arguments[0].click();", btn)

    # 처리중 alert 뜨면 닫기
    _close_alert_if_any(driver)

    before = _snapshot_files(download_dir)
    t0 = time.time()
    while time.time() - t0 < start_detect_sec:
        new_files = _new_files_since(download_dir, before)
        if new_files:
            # 시작 감지 성공
            return before
        time.sleep(0.5)
    return None

def download_with_retry(driver: webdriver.Chrome, download_dir: Path, max_try=CLICK_MAX_TRY) -> Path:
    """시작감지 실패 시 즉시 다음 시도로 넘어감. 시작 감지되면 완료까지 대기."""
    for i in range(1, max_try+1):
        before = click_and_detect_start(driver, download_dir, kind="excel", start_detect_sec=START_DETECT_SEC)
        log(f"  - click_download(excel) / attempt {i}: {bool(before)}")
        if before:
            # 시작 감지됨 -> 완료까지 대기
            try:
                got = _wait_download_finish(download_dir, before, timeout=DOWNLOAD_TIMEOUT_FINISH)
                log(f"  - got file: {got}  size={got.stat().st_size:,}  ext={got.suffix}")
                return got
            except TimeoutError as e:
                log(f"  ! 완료 대기 초과: {e}")
                # 페이지가 꼬였을 수 있으니 다음 시도 전 새로고침
                driver.refresh(); time.sleep(1.5)
        else:
            # 시작 감지 실패 -> 즉시 다음 시도(추가 대기 없음)
            if i % 5 == 0:
                # 5회마다 한번 새로고침으로 상태 초기화
                driver.refresh(); time.sleep(1.0)
    raise TimeoutError(f"다운로드 시작 감지 실패({max_try}회 초과)")

# -------------------------
# 전처리: 읽기 + 정리
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
            if not row: continue
            if row[0].upper() in ("NO","No","no"):
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

    # NO 열 제거
    for c in list(df.columns):
        if str(c).strip().upper() == "NO":
            df = df[df[c].notna()]
            df = df.drop(columns=[c])

    # 숫자화
    for c in ["거래금액(만원)","전용면적(㎡)"]:
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

# 피벗
def pivot_national(df: pd.DataFrame) -> pd.DataFrame:
    if "광역" in df.columns:
        pv = df.pivot_table(index="광역", values="거래금액(만원)", aggfunc="count").rename(columns={"거래금액(만원)":"건수"})
        return pv.reset_index()
    return pd.DataFrame()

def pivot_seoul(df: pd.DataFrame) -> pd.DataFrame:
    if {"구","계약월"}.issubset(df.columns):
        pv = df.pivot_table(index="구", columns="계약월", values="거래금액(만원)", aggfunc="count", fill_value=0)
        pv = pv.sort_index(axis=1)  # 월순
        return pv.reset_index()
    return pd.DataFrame()

def save_excel(path: Path, df: pd.DataFrame, pivot: Optional[pd.DataFrame], pivot_name="피벗"):
    with pd.ExcelWriter(path, engine="openpyxl") as xw:
        df.to_excel(xw, index=False, sheet_name="data")
        if pivot is not None and not pivot.empty:
            pivot.to_excel(xw, index=False, sheet_name=pivot_name)

# -------------------------
# Google Sheets 기록
# -------------------------
def load_service_account() -> Optional[dict]:
    if not SA_PATH or not Path(SA_PATH).exists():
        log("  ! service account not found; skip Drive/Sheets.")
        return None
    try:
        with open(SA_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        log("  - SA loaded.")
        return data
    except Exception as e:
        log(f"  ! service account load failed: {e}")
        return None

_gspread = None
def get_gspread_client():
    global _gspread
    if _gspread is not None:
        return _gspread
    sa = load_service_account()
    if not sa:
        return None
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/drive.file",
        ]
        creds = Credentials.from_service_account_info(sa, scopes=scopes)
        _gspread = gspread.authorize(creds)
        return _gspread
    except Exception as e:
        log(f"  ! gspread init failed: {e}")
        return None

def ensure_worksheet(spread, title: str):
    try:
        return spread.worksheet(title)
    except Exception:
        return spread.add_worksheet(title=title, rows=2000, cols=200)

def upsert_table_by_keys(ws, key_name: str, keys: List[str], col_label: str, values: Dict[str,int]):
    """첫 행: 헤더( key_name | ...날짜들 ), 첫 열: keys
       col_label(예: YYMMDD) 컬럼이 없으면 끝에 추가, 있으면 덮어쓰기
    """
    matrix = ws.get_all_values()
    if not matrix:
        # 새 시트 생성
        header = [key_name, col_label]
        rows = [[k, values.get(k, "")] for k in keys]
        ws.update("A1", [header] + rows)
        return

    header = matrix[0]
    # 키 열 인덱스와 대상 컬럼 인덱스
    if not header or header[0] != key_name:
        header = [key_name] + header[1:]
        ws.update("A1", [header])

    try:
        col_idx = header.index(col_label)
    except ValueError:
        col_idx = len(header)
        header.append(col_label)
        ws.update("A1", [header])

    # 기존 키 인덱스 맵
    existing_keys = {row[0]: i for i, row in enumerate(matrix[1:], start=2) if row}
    # 필요 시 하단에 키 추가
    new_rows = []
    for k in keys:
        if k not in existing_keys:
            new_rows.append([k])
    if new_rows:
        ws.append_rows(new_rows, value_input_option="USER_ENTERED")
        matrix = ws.get_all_values()
        existing_keys = {row[0]: i for i, row in enumerate(matrix[1:], start=2) if row}

    # 업데이트 페이로드
    updates = []
    for k in keys:
        r = existing_keys[k]
        c = col_idx + 1
        val = values.get(k, "")
        updates.append({
            "range": f"{gspread.utils.rowcol_to_a1(r, c)}",
            "values": [[val]]
        })
    if updates:
        ws.batch_update([{"range": u["range"], "values": u["values"]} for u in updates], value_input_option="USER_ENTERED")

def write_national_to_sheets(spread, base_month: date, pv: pd.DataFrame):
    if pv.empty: 
        log("  - sheets: national pivot empty -> skip")
        return
    # 시트명: "전국 YY년 M월"
    title = f"전국 {base_month.year%100:02d}년 {base_month.month}월"
    ws = ensure_worksheet(spread, title)
    # 키/값
    keys = sorted(pv["광역"].astype(str).tolist())
    values = dict(zip(pv["광역"].astype(str), pv["건수"].astype(int)))
    col_label = yymmdd(TODAY)
    upsert_table_by_keys(ws, "광역", keys, col_label, values)
    log(f"  - sheets: wrote national -> [{title}] {col_label}")

def month_year_map(start: date, end: date) -> Dict[str, int]:
    d = month_first(start)
    m = {}
    while d <= end:
        m[f"{d.month:02d}"] = d.year
        d = shift_months(d, +1)
    return m

def write_seoul_to_sheets(spread, start: date, end: date, pv: pd.DataFrame):
    if pv.empty:
        log("  - sheets: seoul pivot empty -> skip")
        return
    mymap = month_year_map(start, end)
    keys = sorted(pv["구"].astype(str).tolist())
    col_label = yymmdd(TODAY)
    # 각 월 컬럼(두자리)마다 해당 연도로 시트명 결정
    for col in pv.columns:
        if col == "구": 
            continue
        mm = str(col).zfill(2)
        yr = mymap.get(mm)
        if not yr:
            continue
        title = f"서울 {yr%100:02d}년 {int(mm)}월"
        ws = ensure_worksheet(spread, title)
        vals = dict(zip(pv["구"].astype(str), pv[col].astype(int)))
        upsert_table_by_keys(ws, "구", keys, col_label, vals)
        log(f"  - sheets: wrote seoul -> [{title}] {col_label}")

# -------------------------
# Drive 업로드 (옵션)
# -------------------------
def upload_to_drive(sa_info: dict, filepath: Path, folder_id: str) -> Optional[str]:
    if not folder_id:
        return None
    try:
        from googleapiclient.discovery import build
        from google.oauth2.service_account import Credentials
        from googleapiclient.http import MediaFileUpload
        scopes = ["https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
        drive = build("drive", "v3", credentials=creds)
        media = MediaFileUpload(str(filepath), mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        file_metadata = {"name": filepath.name, "parents": [folder_id]}
        res = drive.files().create(body=file_metadata, media_body=media, fields="id,name").execute()
        return res.get("id")
    except Exception as e:
        log(f"  ! drive error: {e}")
        return None

# -------------------------
# 한 번의 다운로드 + 전처리 + 저장 + 시트/드라이브
# -------------------------
def fetch_and_process(driver: webdriver.Chrome,
                      sido: Optional[str],
                      start: date, end: date,
                      outname: str,
                      pivot_mode: str,
                      spread=None,
                      sa_info: Optional[dict]=None):
    driver.get(URL)
    set_dates(driver, start, end)
    if sido:
        select_sido(driver, sido)

    # 다운로드(시작감지 20초, 최대 15회, 실패 시 즉시 재시도)
    got = download_with_retry(driver, TMP_DL, max_try=CLICK_MAX_TRY)

    # 읽기/전처리
    df_raw = read_table(got)
    df = clean_df(df_raw, split_month=(pivot_mode=="seoul"))

    # 피벗
    if pivot_mode == "national":
        pv = pivot_national(df)
    else:
        pv = pivot_seoul(df)

    # 엑셀 저장
    out = SAVE_DIR / outname
    save_excel(out, df, pv)
    log(f"완료: {out}")

    # (옵션) Drive 업로드
    if ARTIFACTS_MODE:
        log("  - skip Drive upload (Artifacts mode).")
    elif sa_info and DRIVE_FOLDER_ID:
        upload_to_drive(sa_info, out, DRIVE_FOLDER_ID)

    # (옵션) Sheets 기록
    if spread:
        if pivot_mode == "national":
            write_national_to_sheets(spread, base_month=start, pv=pv)
        else:
            write_seoul_to_sheets(spread, start=start, end=end, pv=pv)

# -------------------------
# 메인
# -------------------------
def main():
    # gspread client (시트 기록용)
    gs = get_gspread_client()
    spread = None
    if gs and SHEET_ID:
        try:
            spread = gs.open_by_key(SHEET_ID)
        except Exception as e:
            log(f"  ! sheets open failed: {e}")
            spread = None

    sa_info = load_service_account()

    # 브라우저
    driver = build_driver(TMP_DL)
    try:
        # 전국: 최근 3개월(당월 포함)
        t = TODAY
        months = [shift_months(month_first(t), k) for k in [0, -1, -2]]
        months.sort()
        for base in months:
            start = base
            # 당월은 오늘까지만
            end = min(shift_months(base, +1) - timedelta(days=1), t)
            name = f"전국 {yymm(base)}_{yymmdd(t)}.xlsx"
            log(f"[전국] {start.isoformat()} ~ {end.isoformat()} → {name}")
            fetch_and_process(driver, None, start, end, name, pivot_mode="national", spread=spread, sa_info=sa_info)
            time.sleep(COOLDOWN_BETWEEN_FILES)

        # 서울: 전년도 10월 1일 ~ 오늘
        year0 = t.year - 1
        start_seoul = date(year0, 10, 1)
        if start_seoul > t:
            start_seoul = date(t.year, 1, 1)
        name_seoul = f"서울시 {yymmdd(t)}.xlsx"
        log(f"[서울] {start_seoul.isoformat()} ~ {t.isoformat()} → {name_seoul}")
        fetch_and_process(driver, "서울특별시", start_seoul, t, name_seoul, pivot_mode="seoul", spread=spread, sa_info=sa_info)

    finally:
        try:
            driver.quit()
        except Exception:
            pass

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log("===== FATAL ERROR =====")
        log(str(e))
        traceback.print_exc()
