# -*- coding: utf-8 -*-
"""
rt_allinone.py
- rt.molit.go.kr 조건별 자료제공 페이지에서 월별/지역별 데이터를 자동 다운로드
- 다운로드 시작 감지 실패 시 즉시 다음 시도(시작감지 20초, 최대 15회)
- 전처리 후 엑셀 저장 + Google Sheets에 누적 기록
  * 전국: "전국 YY년 M월" 시트에 [광역 x 날짜(YYMMDD)] 누적
  * 서울: "서울 YY년 M월" 시트에 [구 x 날짜(YYMMDD)] 누적
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
SAVE_DIR = ROOT / "output"
TMP_DL   = ROOT / "_rt_downloads"
PROFILE  = ROOT / "_rt_profile"
for p in (SAVE_DIR, TMP_DL, PROFILE):
    p.mkdir(parents=True, exist_ok=True)

DOWNLOAD_TIMEOUT_FINISH = 300
CLICK_MAX_TRY = int(os.environ.get("CLICK_MAX_TRY", "15"))
START_DETECT_SEC = int(os.environ.get("START_DETECT_SEC", "20"))
COOLDOWN_BETWEEN_FILES = 2

SHEET_ID = os.environ.get("SHEET_ID", "").strip()
SA_PATH  = os.environ.get("SA_PATH", "").strip()
DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID", "").strip()
ARTIFACTS_MODE = os.environ.get("ARTIFACTS_MODE", "").strip()
TODAY = date.today()

def log(msg: str): print(msg, flush=True)
def yymm(d: date) -> str: return f"{d.year%100:02d}{d.month:02d}"
def yymmdd(d: date) -> str: return f"{d.year%100:02d}{d.month:02d}{d.day:02d}"
def month_first(d: date) -> date: return date(d.year, d.month, 1)
def shift_months(d: date, k: int) -> date:
    y, m = d.year, d.month
    m2 = m + k
    y += (m2-1)//12
    m2 = (m2-1)%12 + 1
    end = (date(y, m2, 1) + timedelta(days=40)).replace(day=1) - timedelta(days=1)
    return date(y, m2, min(d.day, end.day))

# -------------------------
# Selenium
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
    chrome_bin = os.environ.get("CHROME_BIN")
    if chrome_bin: opts.binary_location = chrome_bin
    prefs = {
        "download.default_directory": str(download_dir.resolve()),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    opts.add_experimental_option("prefs", prefs)
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
# 페이지 조작
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

# ---------- 새로 강화: 다운로드 버튼 탐색 ----------
DL_XPATHS = [
    # 텍스트계
    "//button[contains(.,'EXCEL') and (contains(.,'다운') or contains(.,'로드'))]",
    "//button[contains(.,'엑셀') and (contains(.,'다운') or contains(.,'로드'))]",
    "//a[contains(.,'EXCEL') and (contains(.,'다운') or contains(.,'로드'))]",
    "//a[contains(.,'엑셀') and (contains(.,'다운') or contains(.,'로드'))]",
    "//input[(translate(@value,'excelEXCEL','excelexcel')='excel' or contains(translate(@value,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'excel'))]",
    "//input[(contains(.,'EXCEL') or contains(@value,'EXCEL') or contains(@title,'EXCEL'))]",
    # 속성계(id/title/aria-label/onclick)
    "//*[@id][contains(translate(@id,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'excel')]",
    "//*[@title][contains(translate(@title,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'excel')]",
    "//*[@aria-label][contains(translate(@aria-label,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'excel')]",
    "//*[@onclick][contains(translate(@onclick,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'excel')]",
]

def _query_download_candidates(driver: webdriver.Chrome):
    elems: List = []
    for xp in DL_XPATHS:
        try:
            found = driver.find_elements(By.XPATH, xp)
            for el in found:
                if el not in elems:
                    elems.append(el)
        except Exception:
            pass
    return elems

def _try_click(driver: webdriver.Chrome, el) -> bool:
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        time.sleep(0.1)
        el.click()
        return True
    except (ElementClickInterceptedException, ElementNotInteractableException):
        try:
            driver.execute_script("arguments[0].click();", el)
            return True
        except Exception:
            return False
    except Exception:
        return False

def _switch_to_default(driver: webdriver.Chrome):
    try:
        driver.switch_to.default_content()
    except Exception:
        pass

def _switch_to_iframe_with_button(driver: webdriver.Chrome) -> bool:
    """버튼이 iframe 안에 있을 수 있으므로, 모든 top-level iframe을 순회해 버튼 존재 프레임으로 진입."""
    _switch_to_default(driver)
    # 1) 먼저 본문에서 찾아본다
    if _query_download_candidates(driver):
        return True
    # 2) iframe 순회
    iframes = driver.find_elements(By.TAG_NAME, "iframe")
    for i, fr in enumerate(iframes):
        try:
            driver.switch_to.frame(fr)
            if _query_download_candidates(driver):
                return True
            driver.switch_to.default_content()
        except Exception:
            _switch_to_default(driver)
    # 못 찾으면 기본으로 복귀
    _switch_to_default(driver)
    return False

def _find_download_button(driver: webdriver.Chrome):
    """강화된 버튼 탐색: 본문→iframe 순회→본문 재확인"""
    # 본문에서
    _switch_to_default(driver)
    elems = _query_download_candidates(driver)
    if elems:
        return elems[0]
    # iframe 진입
    if _switch_to_iframe_with_button(driver):
        elems = _query_download_candidates(driver)
        if elems:
            return elems[0]
    # 마지막으로 본문 재확인(동적 렌더 대비)
    _switch_to_default(driver)
    elems = _query_download_candidates(driver)
    return elems[0] if elems else None
# ---------- 끝: 버튼 탐색 강화 ----------

def _snapshot_files(download_dir: Path) -> set[Path]:
    return set(download_dir.glob("*"))

def _new_files_since(download_dir: Path, before: set[Path]) -> List[Path]:
    now = set(download_dir.glob("*"))
    new_files = [p for p in now - before if p.is_file()]
    return sorted(new_files, key=lambda p: p.stat().st_mtime)

def _wait_download_finish(download_dir: Path, before: set[Path], timeout: int = DOWNLOAD_TIMEOUT_FINISH) -> Path:
    t0 = time.time()
    while time.time() - t0 < timeout:
        new_files = _new_files_since(download_dir, before)
        done = [p for p in new_files if not p.name.endswith(".crdownload")]
        if done:
            return max(done, key=lambda p: p.stat().st_mtime)
        time.sleep(0.5)
    raise TimeoutError("다운로드 완료 대기 초과")

def click_and_detect_start(driver: webdriver.Chrome, download_dir: Path, start_detect_sec=START_DETECT_SEC) -> Optional[set]:
    """버튼 클릭 후 20초 내 '다운로드 시작'(신규 파일 생성) 감지. 실패 시 None."""
    _close_alert_if_any(driver)
    # 버튼이 iframe 안에 있을 수 있으니 전환 포함 탐색
    btn = _find_download_button(driver)
    if not btn:
        return None
    if not _try_click(driver, btn):
        return None

    _close_alert_if_any(driver)
    before = _snapshot_files(download_dir)
    t0 = time.time()
    while time.time() - t0 < start_detect_sec:
        new_files = _new_files_since(download_dir, before)
        if new_files:
            return before
        time.sleep(0.5)
    return None

def download_with_retry(driver: webdriver.Chrome, download_dir: Path, max_try=CLICK_MAX_TRY) -> Path:
    for i in range(1, max_try+1):
        before = click_and_detect_start(driver, download_dir, start_detect_sec=START_DETECT_SEC)
        log(f"  - click_download(excel) / attempt {i}: {bool(before)}")
        if before:
            try:
                got = _wait_download_finish(download_dir, before, timeout=DOWNLOAD_TIMEOUT_FINISH)
                log(f"  - got file: {got}  size={got.stat().st_size:,}  ext={got.suffix}")
                return got
            except TimeoutError as e:
                log(f"  ! 완료 대기 초과: {e}")
                driver.refresh(); time.sleep(1.5)
        else:
            if i % 5 == 0:
                driver.refresh(); time.sleep(1.0)
    raise TimeoutError(f"다운로드 시작 감지 실패({max_try}회 초과)")

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
        hdr_idx = None
        max_scan = min(80, len(df0))
        for i in range(max_scan):
            row = df0.iloc[i].astype(str).str.strip().tolist()
            if not row: continue
            if row[0].upper() in ("NO","NO.", "No","no"):
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
        if str(c).strip().upper() == "NO":
            df = df[df[c].notna()]
            df = df.drop(columns=[c])

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
            if parts.shape[1] > i: df[name] = parts[i].fillna("")
            else: df[name] = ""

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
# Google Sheets
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
    if not sa: return None
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
    try: return spread.worksheet(title)
    except Exception: return spread.add_worksheet(title=title, rows=2000, cols=200)

def upsert_table_by_keys(ws, key_name: str, keys: List[str], col_label: str, values: Dict[str,int]):
    import gspread
    matrix = ws.get_all_values()
    if not matrix:
        header = [key_name, col_label]
        rows = [[k, values.get(k, "")] for k in keys]
        ws.update("A1", [header] + rows)
        return
    header = matrix[0]
    if not header or header[0] != key_name:
        header = [key_name] + header[1:]
        ws.update("A1", [header])
    try:
        col_idx = header.index(col_label)
    except ValueError:
        col_idx = len(header)
        header.append(col_label)
        ws.update("A1", [header])
    existing_keys = {row[0]: i for i, row in enumerate(matrix[1:], start=2) if row}
    new_rows = []
    for k in keys:
        if k not in existing_keys: new_rows.append([k])
    if new_rows:
        ws.append_rows(new_rows, value_input_option="USER_ENTERED")
        matrix = ws.get_all_values()
        existing_keys = {row[0]: i for i, row in enumerate(matrix[1:], start=2) if row}
    updates = []
    for k in keys:
        r = existing_keys[k]; c = col_idx + 1; val = values.get(k, "")
        updates.append({"range": f"{gspread.utils.rowcol_to_a1(r, c)}", "values": [[val]]})
    if updates:
        ws.batch_update([{"range": u["range"], "values": u["values"]} for u in updates], value_input_option="USER_ENTERED")

def write_national_to_sheets(spread, base_month: date, pv: pd.DataFrame):
    if pv.empty: 
        log("  - sheets: national pivot empty -> skip"); return
    title = f"전국 {base_month.year%100:02d}년 {base_month.month}월"
    ws = ensure_worksheet(spread, title)
    keys = sorted(pv["광역"].astype(str).tolist())
    values = dict(zip(pv["광역"].astype(str), pv["건수"].astype(int)))
    col_label = yymmdd(TODAY)
    upsert_table_by_keys(ws, "광역", keys, col_label, values)
    log(f"  - sheets: wrote national -> [{title}] {col_label}")

def month_year_map(start: date, end: date) -> Dict[str, int]:
    d = month_first(start); m = {}
    while d <= end:
        m[f"{d.month:02d}"] = d.year
        d = shift_months(d, +1)
    return m

def write_seoul_to_sheets(spread, start: date, end: date, pv: pd.DataFrame):
    if pv.empty:
        log("  - sheets: seoul pivot empty -> skip"); return
    mymap = month_year_map(start, end)
    keys = sorted(pv["구"].astype(str).tolist())
    col_label = yymmdd(TODAY)
    for col in pv.columns:
        if col == "구": continue
        mm = str(col).zfill(2); yr = mymap.get(mm)
        if not yr: continue
        title = f"서울 {yr%100:02d}년 {int(mm)}월"
        ws = ensure_worksheet(spread, title)
        vals = dict(zip(pv["구"].astype(str), pv[col].astype(int)))
        upsert_table_by_keys(ws, "구", keys, col_label, vals)
        log(f"  - sheets: wrote seoul -> [{title}] {col_label}")

# -------------------------
# Drive (옵션)
# -------------------------
def upload_to_drive(sa_info: dict, filepath: Path, folder_id: str) -> Optional[str]:
    if not folder_id: return None
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
# 파이프라인
# -------------------------
def fetch_and_process(driver: webdriver.Chrome,
                      sido: Optional[str],
                      start: date, end: date,
                      outname: str,
                      pivot_mode: str,
                      spread=None,
                      sa_info: Optional[dict]=None):
    driver.get(URL)
    # 페이지 기초 안정화
    try:
        WebDriverWait(driver, 5).until(lambda d: d.find_elements(By.TAG_NAME, "body"))
    except TimeoutException:
        pass

    set_dates(driver, start, end)
    if sido: select_sido(driver, sido)

    got = download_with_retry(driver, TMP_DL, max_try=CLICK_MAX_TRY)

    df_raw = read_table(got)
    df = clean_df(df_raw, split_month=(pivot_mode=="seoul"))
    pv = pivot_national(df) if pivot_mode=="national" else pivot_seoul(df)

    out = SAVE_DIR / outname
    save_excel(out, df, pv)
    log(f"완료: {out}")

    if ARTIFACTS_MODE:
        log("  - skip Drive upload (Artifacts mode).")
    elif sa_info and DRIVE_FOLDER_ID:
        upload_to_drive(sa_info, out, DRIVE_FOLDER_ID)

    if spread:
        if pivot_mode == "national":
            write_national_to_sheets(spread, base_month=start, pv=pv)
        else:
            write_seoul_to_sheets(spread, start=start, end=end, pv=pv)

def main():
    gs = get_gspread_client()
    spread = None
    if gs and SHEET_ID:
        try:
            spread = gs.open_by_key(SHEET_ID)
        except Exception as e:
            log(f"  ! sheets open failed: {e}")
            spread = None
    sa_info = load_service_account()

    driver = build_driver(TMP_DL)
    try:
        t = TODAY
        months = [shift_months(month_first(t), k) for k in [0, -1, -2]]
        months.sort()
        for base in months:
            start = base
            end = min(shift_months(base, +1) - timedelta(days=1), t)
            name = f"전국 {yymm(base)}_{yymmdd(t)}.xlsx"
            log(f"[전국] {start.isoformat()} ~ {end.isoformat()} → {name}")
            fetch_and_process(driver, None, start, end, name, pivot_mode="national", spread=spread, sa_info=sa_info)
            time.sleep(COOLDOWN_BETWEEN_FILES)

        year0 = t.year - 1
        start_seoul = date(year0, 10, 1)
        if start_seoul > t: start_seoul = date(t.year, 1, 1)
        name_seoul = f"서울시 {yymmdd(t)}.xlsx"
        log(f"[서울] {start_seoul.isoformat()} ~ {t.isoformat()} → {name_seoul}")
        fetch_and_process(driver, "서울특별시", start_seoul, t, name_seoul, pivot_mode="seoul", spread=spread, sa_info=sa_info)

    finally:
        try: driver.quit()
        except Exception: pass

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log("===== FATAL ERROR =====")
        log(str(e))
        traceback.print_exc()
