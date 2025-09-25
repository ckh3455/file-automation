# -*- coding: utf-8 -*-
"""
rt_allinone.py — '성공했을 때' 동작으로 롤백
- 버튼 등장 대기: 12초
- 다운로드 시작 감지: 30초
- 클릭 시도: 10회 (3회마다 refresh)
- 핵심 수정: '클릭 전에' 다운로드 폴더 스냅샷 → 시작 감지 정확히 복구
- SA JSON 문제면 시트/드라이브는 자동 스킵, 다운로드/전처리/아티팩트는 계속
"""

from __future__ import annotations
import os, re, time, json, traceback
from datetime import date, timedelta
from pathlib import Path
from typing import Optional, List, Dict

import pandas as pd
import numpy as np

# ---------------- 기본 설정 ----------------
URL = "https://rt.molit.go.kr/pt/xls/xls.do?mobileAt="

ROOT = Path.cwd()
SAVE_DIR = ROOT / "output"
TMP_DL   = ROOT / "_rt_downloads"
PROFILE  = ROOT / "_rt_profile"
for p in (SAVE_DIR, TMP_DL, PROFILE):
    p.mkdir(parents=True, exist_ok=True)

# 성공 당시 파라미터
CLICK_MAX_TRY           = int(os.environ.get("CLICK_MAX_TRY", "10"))
BUTTON_APPEAR_WAIT      = float(os.environ.get("BUTTON_APPEAR_WAIT", "12"))
START_DETECT_SEC        = int(os.environ.get("START_DETECT_SEC", "30"))
DOWNLOAD_TIMEOUT_FINISH = 300
COOLDOWN_BETWEEN_FILES  = 2

# Google
SHEET_ID = os.environ.get("SHEET_ID", "").strip()
SA_PATH  = os.environ.get("SA_PATH", "").strip()
DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID", "").strip()
ARTIFACTS_MODE  = os.environ.get("ARTIFACTS_MODE", "").strip()

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

# ---------------- Selenium ----------------
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import (
    TimeoutException, ElementClickInterceptedException, ElementNotInteractableException
)

def build_driver(download_dir: Path) -> webdriver.Chrome:
    opts = Options()
    if os.environ.get("CHROME_BIN"):
        opts.binary_location = os.environ["CHROME_BIN"]
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
    # 안정성 위해 사용자 데이터 디렉토리 사용
    opts.add_argument(f"--user-data-dir={PROFILE.resolve()}")

    # runner에 설치된 chromedriver 우선 사용
    chromedriver_bin = os.environ.get("CHROMEDRIVER_BIN")
    if chromedriver_bin and Path(chromedriver_bin).exists():
        service = Service(chromedriver_bin)
    else:
        # 없으면 PATH에서 탐색
        service = Service()

    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_window_size(1400, 900)
    return driver

# ---------------- 날짜 입력 ----------------
START_HINTS = ["start","from","sdate","beg","st","fr","시작","startdate"]
END_HINTS   = ["end","to","edate","fin","endd","en","종료","enddate"]

def _score_input(el) -> int:
    sc = 0
    try:
        s = " ".join([(el.get_attribute(k) or "").lower()
                      for k in ("id","name","class","placeholder","title","aria-label")])
    except Exception:
        s = ""
    for h in START_HINTS:
        if h in s: sc += 2
    for h in END_HINTS:
        if h in s: sc += 2
    try:
        if (el.get_attribute("type") or "").lower() == "date":
            sc += 3
        if el.is_displayed(): sc += 1
    except Exception:
        pass
    return sc

def _clear_and_type(el, s: str):
    el.click()
    el.send_keys(Keys.CONTROL, "a")
    el.send_keys(Keys.DELETE)
    el.send_keys(s)

def _try_pair(driver, a, b, start_s, end_s) -> bool:
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", a)
        _clear_and_type(a, start_s); time.sleep(0.12)
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", b)
        _clear_and_type(b, end_s);   time.sleep(0.12)
        va = (a.get_attribute("value") or "").strip()
        vb = (b.get_attribute("value") or "").strip()
        ok = (va == start_s) and (vb == end_s)
        log(f"    · probe pair → value check: {va} / {vb} → {ok}")
        return ok
    except Exception:
        return False

def set_dates(driver: webdriver.Chrome, start: date, end: date):
    start_s, end_s = start.isoformat(), end.isoformat()
    log(f"  - set_dates: {start_s} ~ {end_s}")
    inputs = [e for e in driver.find_elements(By.CSS_SELECTOR, "input")
              if (e.get_attribute("type") or "").lower() in ("date","text","search","")]
    if not inputs:
        raise RuntimeError("날짜 입력 input을 못찾았습니다.")
    inputs = sorted(inputs, key=_score_input, reverse=True)
    log(f"    · input candidates: {len(inputs)} (scored)")
    tried = 0
    for i in range(min(8, len(inputs))):
        for j in range(i+1, min(i+1+8, len(inputs))):
            tried += 1
            if _try_pair(driver, inputs[i], inputs[j], start_s, end_s):
                time.sleep(0.4)
                log(f"    · selected pair index: {i},{j}")
                return
    raise RuntimeError(f"적합한 날짜 입력쌍 실패(tried={tried})")

def select_sido(driver: webdriver.Chrome, wanted: str) -> bool:
    for sel in driver.find_elements(By.TAG_NAME, "select"):
        try:
            for o in sel.find_elements(By.TAG_NAME, "option"):
                if o.text.strip() == wanted:
                    o.click(); time.sleep(0.2)
                    log(f"  - select_sido({wanted}): True"); return True
        except Exception:
            pass
    log(f"  - select_sido({wanted}): False")
    return False

# ---------------- 엑셀 버튼 ----------------
def _scroll_probe(driver):
    try:
        driver.execute_script("window.scrollTo(0,0);"); time.sleep(0.08)
        driver.execute_script("window.scrollBy(0,600);"); time.sleep(0.08)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);"); time.sleep(0.08)
        driver.execute_script("window.scrollBy(0,-400);"); time.sleep(0.08)
    except Exception:
        pass

def _visible_one(elems):
    for e in elems:
        try:
            if e.is_displayed(): return e
        except Exception:
            pass
    return None

def _search_button_in_context(ctx):
    # 성공 당시 단순했던 탐색 우선
    try:
        c = ctx.find_elements(By.CSS_SELECTOR, 'a[href*=".xlsx"], a[href*=".xls"], a[download], #excel, #btnExcel, .btn-excel, .excel')
        btn = _visible_one(c)
        if btn: return btn
    except Exception: pass
    # 텍스트 fallback
    xpaths = [
        ".//a[contains(text(),'엑셀') or contains(@title,'엑셀') or contains(text(),'EXCEL')]",
        ".//button[contains(text(),'엑셀') or contains(@title,'엑셀') or contains(text(),'EXCEL')]",
    ]
    for xp in xpaths:
        try:
            cand = ctx.find_elements(By.XPATH, xp)
            btn = _visible_one(cand)
            if btn: return btn
        except Exception:
            pass
    return None

def find_download_button(driver: webdriver.Chrome, wait_sec: float = BUTTON_APPEAR_WAIT):
    t0 = time.time()
    while time.time() - t0 < wait_sec:
        # 메인 문서에서
        btn = _search_button_in_context(driver)
        if btn: return btn
        # iframe 내도 확인(최대 3개만)
        ifr = driver.find_elements(By.TAG_NAME, "iframe")[:3]
        for fr in ifr:
            try:
                driver.switch_to.frame(fr)
                btn = _search_button_in_context(driver)
                if btn:
                    # 버튼은 frame 안의 element → 그냥 반환 (클릭시 그대로)
                    return btn
            except Exception:
                pass
            finally:
                try: driver.switch_to.default_content()
                except Exception: pass
        _scroll_probe(driver)
        time.sleep(0.25)
    return None

def _try_click(driver, el) -> bool:
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        time.sleep(0.05)
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

# ---------------- 다운로드 감시 ----------------
def _snapshot_files(d: Path) -> set[Path]: return set(d.glob("*"))
def _new_files_since(d: Path, before: set[Path]) -> List[Path]:
    now = set(d.glob("*"))
    return sorted([p for p in now - before if p.is_file()], key=lambda p: p.stat().st_mtime)

def _wait_download_finish(download_dir: Path, before: set[Path], timeout: int) -> Path:
    t0 = time.time()
    while time.time() - t0 < timeout:
        new_files = _new_files_since(download_dir, before)
        done = [p for p in new_files if not p.name.endswith(".crdownload")]
        if done:
            return max(done, key=lambda p: p.stat().st_mtime)
        time.sleep(0.4)
    raise TimeoutError("다운로드 완료 대기 초과")

def click_and_detect_start(driver: webdriver.Chrome, download_dir: Path, start_detect_sec: int) -> Optional[set]:
    btn = find_download_button(driver, wait_sec=BUTTON_APPEAR_WAIT)
    if not btn:
        return None
    # 🔴 핵심: 클릭 '전에' 스냅샷 (성공 버전 로직)
    before = _snapshot_files(download_dir)
    if not _try_click(driver, btn):
        return None
    t0 = time.time()
    while time.time() - t0 < start_detect_sec:
        if _new_files_since(download_dir, before):
            return before
        time.sleep(0.4)
    return None

def download_with_retry(driver: webdriver.Chrome, download_dir: Path, max_try: int) -> Path:
    for i in range(1, max_try+1):
        started = click_and_detect_start(driver, download_dir, START_DETECT_SEC)
        log(f"  - click_download(excel) / attempt {i}: {bool(started)}")
        if started:
            try:
                got = _wait_download_finish(download_dir, started, DOWNLOAD_TIMEOUT_FINISH)
                log(f"  - got file: {got}  size={got.stat().st_size:,}  ext={got.suffix}")
                return got
            except TimeoutError as e:
                log(f"  ! 완료 대기 초과: {e}")
        else:
            log(f"  - warn: 다운로드 시작 감지 실패(시도 {i}/{max_try})")
            if i % 3 == 0:
                driver.refresh(); time.sleep(1.0)
    raise TimeoutError(f"다운로드 시작 감지 실패({max_try}회 초과)")

# ---------------- 파싱/전처리/피벗 ----------------
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
    if ext in (".xlsx",".xls"):
        try:
            df0 = pd.read_excel(path, header=None, dtype=str, engine="openpyxl" if ext==".xlsx" else None)
        except Exception:
            return _read_html_table(path)
        hdr = None
        for i in range(min(80, len(df0))):
            row = df0.iloc[i].astype(str).str.strip().tolist()
            if row and (row[0].upper() in ("NO","NO.","No","no") or (("시군구" in row) and ("단지명" in row))):
                hdr = i; break
        if hdr is None:
            return _read_html_table(path)
        cols = df0.iloc[hdr].astype(str).str.strip()
        df = df0.iloc[hdr+1:].copy()
        df.columns = cols
        return df.reset_index(drop=True)
    return _read_html_table(path)

def clean_df(df: pd.DataFrame, split_month: bool) -> pd.DataFrame:
    if "시군구 " in df.columns and "시군구" not in df.columns:
        df = df.rename(columns={"시군구 ":"시군구"})
    ren = {}
    for c in df.columns:
        k = str(c).replace(" ","")
        if k == "거래금액(만원)" and c != "거래금액(만원)": ren[c] = "거래금액(만원)"
        if k == "전용면적(㎡)" and c != "전용면적(㎡)": ren[c] = "전용면적(㎡)"
    if ren: df = df.rename(columns=ren)
    for c in list(df.columns):
        if str(c).strip().upper() == "NO":
            df = df[df[c].notna()].drop(columns=[c])
    for c in ["거래금액(만원)","전용면적(㎡)"]:
        if c in df.columns:
            df[c] = (df[c].astype(str).str.replace(",","",regex=False)
                               .str.replace(" ","",regex=False)
                               .str.replace("-","",regex=False)
                               .replace({"": np.nan}))
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if "시군구" in df.columns:
        parts = df["시군구"].astype(str).str.split(expand=True, n=2)
        for i,name in enumerate(["광역","구","법정동"]):
            df[name] = parts[i] if parts.shape[1] > i else ""
    if split_month and "계약년월" in df.columns:
        s = df["계약년월"].astype(str).str.replace(r"\D","", regex=True)
        df["계약년"] = s.str.slice(0,4)
        df["계약월"] = s.str.slice(4,6)
    return df.reset_index(drop=True)

def pivot_national(df: pd.DataFrame) -> pd.DataFrame:
    if "광역" in df.columns and "거래금액(만원)" in df.columns:
        pv = df.pivot_table(index="광역", values="거래금액(만원)", aggfunc="count").rename(columns={"거래금액(만원)":"건수"})
        return pv.reset_index()
    return pd.DataFrame()

def pivot_seoul(df: pd.DataFrame) -> pd.DataFrame:
    if {"구","계약월","거래금액(만원)"}.issubset(df.columns):
        pv = df.pivot_table(index="구", columns="계약월", values="거래금액(만원)", aggfunc="count", fill_value=0)
        pv = pv.sort_index(axis=1)
        return pv.reset_index()
    return pd.DataFrame()

def save_excel(path: Path, df: pd.DataFrame, pv: Optional[pd.DataFrame], pivot_name="피벗"):
    with pd.ExcelWriter(path, engine="openpyxl") as xw:
        df.to_excel(xw, index=False, sheet_name="data")
        if pv is not None and not pv.empty:
            pv.to_excel(xw, index=False, sheet_name=pivot_name)

# ---------------- Sheets / Drive ----------------
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

_gs = None
def get_gspread_client():
    global _gs
    if _gs is not None: return _gs
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
        _gs = gspread.authorize(creds)
        return _gs
    except Exception as e:
        log(f"  ! gspread init failed: {e}")
        return None

def ensure_ws(spread, title: str):
    try: return spread.worksheet(title)
    except Exception: return spread.add_worksheet(title=title, rows=2000, cols=200)

def upsert_table(ws, key_name: str, keys: List[str], col_label: str, mapping: Dict[str,int]):
    import gspread
    matrix = ws.get_all_values()
    if not matrix:
        header = [key_name, col_label]
        rows = [[k, mapping.get(k,"")] for k in keys]
        ws.update("A1", [header] + rows)
        return
    header = matrix[0]
    if not header or header[0] != key_name:
        header = [key_name] + header[1:]
        ws.update("A1", [header])
    try:
        cidx = header.index(col_label)
    except ValueError:
        cidx = len(header)
        header.append(col_label)
        ws.update("A1", [header])
    existing = {row[0]: i for i,row in enumerate(matrix[1:], start=2) if row}
    new_rows = [[k] for k in keys if k not in existing]
    if new_rows:
        ws.append_rows(new_rows, value_input_option="USER_ENTERED")
        matrix = ws.get_all_values()
        existing = {row[0]: i for i,row in enumerate(matrix[1:], start=2) if row}
    updates = []
    for k in keys:
        r = existing[k]; c = cidx+1; val = mapping.get(k, "")
        updates.append({"range": gspread.utils.rowcol_to_a1(r,c), "values":[[val]]})
    if updates:
        ws.batch_update([{"range": u["range"], "values": u["values"]} for u in updates], value_input_option="USER_ENTERED")

def write_national(spread, base_month: date, pv: pd.DataFrame):
    if pv.empty: 
        log("  - sheets: national empty → skip"); return
    title = f"전국 {base_month.year%100:02d}년 {base_month.month}월"
    ws = ensure_ws(spread, title)
    keys = sorted(pv["광역"].astype(str))
    vals = dict(zip(pv["광역"].astype(str), pv["건수"].astype(int)))
    col_label = yymmdd(TODAY)
    upsert_table(ws, "광역", keys, col_label, vals)
    log(f"  - sheets: wrote national → [{title}] {col_label}")

def month_year_map(start: date, end: date) -> Dict[str,int]:
    d = month_first(start)
    m = {}
    while d <= end:
        m[f"{d.month:02d}"] = d.year
        d = shift_months(d, +1)
    return m

def write_seoul(spread, start: date, end: date, pv: pd.DataFrame):
    if pv.empty:
        log("  - sheets: seoul empty → skip"); return
    mymap = month_year_map(start, end)
    keys = sorted(pv["구"].astype(str))
    col_label = yymmdd(TODAY)
    for col in pv.columns:
        if col == "구": continue
        mm = str(col).zfill(2); yr = mymap.get(mm)
        if not yr: continue
        title = f"서울 {yr%100:02d}년 {int(mm)}월"
        ws = ensure_ws(spread, title)
        vals = dict(zip(pv["구"].astype(str), pv[col].astype(int)))
        upsert_table(ws, "구", keys, col_label, vals)
        log(f"  - sheets: wrote seoul → [{title}] {col_label}")

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
        meta = {"name": filepath.name, "parents":[folder_id]}
        res = drive.files().create(body=meta, media_body=media, fields="id,name").execute()
        return res.get("id")
    except Exception as e:
        log(f"  ! drive error: {e}")
        return None

# ---------------- 메인 루틴 ----------------
def fetch_and_process(driver: webdriver.Chrome,
                      sido: Optional[str],
                      start: date, end: date,
                      outname: str,
                      pivot_mode: str,
                      spread=None,
                      sa_info: Optional[dict]=None):
    driver.get(URL)
    time.sleep(0.8)  # 초기 안정화

    set_dates(driver, start, end)
    if sido:
        select_sido(driver, sido)

    got = download_with_retry(driver, TMP_DL, max_try=CLICK_MAX_TRY)

    df_raw = read_table(got)
    df = clean_df(df_raw, split_month=(pivot_mode=="seoul"))
    pv = (pivot_national(df) if pivot_mode=="national" else pivot_seoul(df))

    out = SAVE_DIR / outname
    save_excel(out, df, pv)
    log(f"완료: {out}")

    if ARTIFACTS_MODE:
        log("  - skip Drive upload (Artifacts mode).")
    elif sa_info and DRIVE_FOLDER_ID:
        upload_to_drive(sa_info, out, DRIVE_FOLDER_ID)

    if spread:
        if pivot_mode == "national":
            write_national(spread, base_month=start, pv=pv)
        else:
            write_seoul(spread, start=start, end=end, pv=pv)

def main():
    # 시트/드라이브는 SA 정상일 때만
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
        # 최근 3개월
        months = [shift_months(month_first(t), k) for k in [0, -1, -2]]
        months.sort()
        for base in months:
            start = base
            end   = min(shift_months(base, +1) - timedelta(days=1), t)
            name  = f"전국 {yymm(base)}_{yymmdd(t)}.xlsx"
            log(f"[전국] {start.isoformat()} ~ {end.isoformat()} → {name}")
            fetch_and_process(driver, None, start, end, name, pivot_mode="national",
                              spread=spread, sa_info=sa_info)
            time.sleep(COOLDOWN_BETWEEN_FILES)

        # 서울: 전년도 10/1 ~ 오늘
        year0 = t.year - 1
        start_seoul = date(year0, 10, 1)
        if start_seoul > t:
            start_seoul = date(t.year, 1, 1)
        name_seoul = f"서울시 {yymmdd(t)}.xlsx"
        log(f"[서울] {start_seoul.isoformat()} ~ {t.isoformat()} → {name_seoul}")
        fetch_and_process(driver, "서울특별시", start_seoul, t, name_seoul, pivot_mode="seoul",
                          spread=spread, sa_info=sa_info)
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
