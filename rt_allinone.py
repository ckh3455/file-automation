# -*- coding: utf-8 -*-
"""
국토부 실거래가 Excel 자동화 (크롬/셀레니움, GitHub Actions 헤드리스 대응)

요구사항 반영:
- 전국: 현재 달 포함 최근 1년(12개월) 월별 다운로드 → 전처리 저장
- 서울: 전년도 10월 1일 ~ 오늘 한 번에 다운로드 → 전처리 저장
- 성공했던 클릭 방식 유지: 버튼/링크 다각도 시도 + JS 함수 폴백
- 클릭 성공 후 다운로드 감지 대기 30초, 실패 시 즉시 다음 시도, 최대 15회
  (매 5회마다 폼을 재세팅하여 복구 시도)
- 전처리:
  · 전국: 시군구 → (광역, 구, 법정동, 리) 분할 후 시군구 삭제
  · 서울: 시군구 → (시, 구, 법정동) 분할 후 시군구 삭제
  · 계약년월 → 계약년, 계약월 분리 후 원본 컬럼 삭제
  · NO 컬럼 제거, 숫자열 정규화
- Drive/Sheets는 아티팩트 모드일 땐 스킵(로그만)
"""

from __future__ import annotations

import os, re, sys, time, json, shutil, tempfile
from pathlib import Path
from datetime import date, datetime, timedelta
from typing import Optional, Tuple, List

import numpy as np
import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.alert import Alert
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ---------- 환경/경로 ----------
URL = "https://rt.molit.go.kr/pt/xls/xls.do?mobileAt="

SAVE_DIR = Path(os.getenv("OUT_DIR", "output")).resolve()
TMP_DL   = (Path.cwd() / "_rt_downloads").resolve()
SAVE_DIR.mkdir(parents=True, exist_ok=True)
TMP_DL.mkdir(parents=True, exist_ok=True)

DOWNLOAD_TIMEOUT = int(os.getenv("DOWNLOAD_TIMEOUT", "30"))   # 클릭 성공 뒤 다운로드 감지 대기
CLICK_RETRY_MAX  = int(os.getenv("CLICK_RETRY_MAX", "15"))    # 최대 시도
CLICK_RETRY_WAIT = float(os.getenv("CLICK_RETRY_WAIT", "1"))  # 실패 간격

IS_CI = os.getenv("CI", "") == "1"
ARTIFACTS_ONLY = os.getenv("ARTIFACTS_ONLY", "") == "1"

def today_kst() -> date:
    # Actions는 UTC. 간단히 +9h 보정
    return (datetime.utcnow() + timedelta(hours=9)).date()

def month_first(d: date) -> date:
    return date(d.year, d.month, 1)

def shift_months(d: date, k: int) -> date:
    # d의 1일 기준 k개월 이동
    y = d.year + (d.month - 1 + k) // 12
    m = (d.month - 1 + k) % 12 + 1
    return date(y, m, 1)

def yymm(d: date) -> str:
    return d.strftime("%y%m")

def yymmdd(d: date) -> str:
    return d.strftime("%y%m%d")

def debug(msg: str):
    sys.stdout.write(msg.rstrip() + "\n"); sys.stdout.flush()

# ---------- 크롬 ----------
def build_driver(download_dir: Path) -> webdriver.Chrome:
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-notifications")
    opts.add_argument("--window-size=1400,900")
    opts.add_argument("--lang=ko-KR")
    opts.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    prefs = {
        "download.default_directory": str(download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    opts.add_experimental_option("prefs", prefs)

    if os.getenv("CHROME_BIN"):
        opts.binary_location = os.getenv("CHROME_BIN")

    tmp_profile = Path(tempfile.mkdtemp(prefix="chrome_prof_"))
    opts.add_argument(f"--user-data-dir={tmp_profile.as_posix()}")

    chromedriver_bin = os.getenv("CHROMEDRIVER_BIN")
    if chromedriver_bin and Path(chromedriver_bin).exists():
        service = Service(chromedriver_bin)
    else:
        from webdriver_manager.chrome import ChromeDriverManager
        service = Service(ChromeDriverManager().install())

    driver = webdriver.Chrome(service=service, options=opts)

    # 허용되지 않는 다운로드 차단 해제
    try:
        driver.execute_cdp_cmd("Page.setDownloadBehavior", {
            "behavior": "allow",
            "downloadPath": str(download_dir),
            "eventsEnabled": True
        })
    except Exception as e:
        debug(f"  - warn: setDownloadBehavior failed: {e}")

    import atexit
    atexit.register(lambda: shutil.rmtree(tmp_profile, ignore_errors=True))
    return driver

# ---------- UI 탐색 ----------
def _looks_like_date_input(el) -> bool:
    typ = (el.get_attribute("type") or "").lower()
    ph  = (el.get_attribute("placeholder") or "").lower()
    val = (el.get_attribute("value") or "").lower()
    name= (el.get_attribute("name") or "").lower()
    id_ = (el.get_attribute("id") or "").lower()
    txt = " ".join([ph, val, name, id_])
    return (
        typ in ("date", "text", "") and (
            re.search(r"\d{4}-\d{2}-\d{2}", ph) or
            re.search(r"\d{4}-\d{2}-\d{2}", val) or
            "yyyy" in ph or "yyyy-mm-dd" in ph or
            any(k in txt for k in ["start","end","from","to"])
        )
    )

def find_date_inputs(driver: webdriver.Chrome) -> Tuple:
    inputs = driver.find_elements(By.CSS_SELECTOR, "input")
    cands = [el for el in inputs if _looks_like_date_input(el)]
    if len(cands) >= 2:
        return cands[0], cands[1]
    dates = [e for e in inputs if (e.get_attribute("type") or "").lower() == "date"]
    if len(dates) >= 2:
        return dates[0], dates[1]
    text_inputs = [e for e in inputs if (e.get_attribute("type") or "").lower() in ("text","")]
    if len(text_inputs) >= 2:
        return text_inputs[0], text_inputs[1]
    raise RuntimeError("날짜 입력 박스를 찾지 못했습니다.")

def _type_and_verify(el, val: str) -> bool:
    try:
        el.click()
        el.send_keys(Keys.CONTROL, "a")
        el.send_keys(Keys.DELETE)
        el.send_keys(val)
        time.sleep(0.2)
        el.send_keys(Keys.TAB)
        time.sleep(0.2)
        return (el.get_attribute("value") or "").strip() == val
    except Exception:
        return False

def _ensure_value_with_js(driver, el, val: str) -> bool:
    try:
        driver.execute_script("""
            const el = arguments[0], v = arguments[1];
            el.value = v;
            el.dispatchEvent(new Event('input', {bubbles:true}));
            el.dispatchEvent(new Event('change', {bubbles:true}));
            el.blur();
        """, el, val)
        time.sleep(0.2)
        return (el.get_attribute("value") or "").strip() == val
    except Exception:
        return False

def set_dates(driver: webdriver.Chrome, start: date, end: date):
    s_el, e_el = find_date_inputs(driver)
    s_val, e_val = start.isoformat(), end.isoformat()
    ok_s = _type_and_verify(s_el, s_val) or _ensure_value_with_js(driver, s_el, s_val)
    ok_e = _type_and_verify(e_el, e_val) or _ensure_value_with_js(driver, e_el, e_val)
    if not ok_s or not ok_e:
        sv = (s_el.get_attribute("value") or "").strip()
        ev = (e_el.get_attribute("value") or "").strip()
        debug(f"  - warn: date fill verify failed. want=({s_val},{e_val}) got=({sv},{ev})")
    assert (s_el.get_attribute("value") or "").strip() == s_val
    assert (e_el.get_attribute("value") or "").strip() == e_val

def select_sido(driver: webdriver.Chrome, wanted: str) -> bool:
    sels = driver.find_elements(By.TAG_NAME, "select")
    for sel in sels:
        try:
            opts = sel.find_elements(By.TAG_NAME, "option")
            for op in opts:
                if wanted in (op.text or ""):
                    op.click(); time.sleep(0.2); return True
        except Exception:
            pass
    return False

def _try_accept_alert(driver: webdriver.Chrome, wait=1.5):
    t0 = time.time()
    while time.time() - t0 < wait:
        try:
            Alert(driver).accept()
            return True
        except Exception:
            time.sleep(0.2)
    return False

def _click_by_locators(driver: webdriver.Chrome, label: str) -> bool:
    locators = [
        (By.XPATH, f"//button[normalize-space()='{label}']"),
        (By.XPATH, f"//a[normalize-space()='{label}']"),
        (By.XPATH, f"//input[@type='button' and @value='{label}']"),
        (By.XPATH, f"//*[contains(@onclick,'excel') and (self::a or self::button or self::input)]"),
        (By.XPATH, "//*[@id='excelDown' or @id='btnExcel' or contains(@id,'excel')]"),
    ]
    for by, q in locators:
        try:
            els = driver.find_elements(by, q)
            for el in els:
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                time.sleep(0.1)
                el.click()
                _try_accept_alert(driver, 3.0)
                return True
        except Exception:
            continue
    return False

def click_download(driver: webdriver.Chrome, kind="excel") -> bool:
    label = "EXCEL 다운" if kind == "excel" else "CSV 다운"
    _try_accept_alert(driver, 1.0)
    if _click_by_locators(driver, label):
        _try_accept_alert(driver, 3.0)
        return True
    # JS 함수 폴백
    for fn in ["excelDown","xlsDown","excelDownload","fnExcel","fnExcelDown","fncExcel"]:
        try:
            driver.execute_script(f"if (typeof {fn}==='function') {fn}();")
            _try_accept_alert(driver, 3.0)
            return True
        except Exception:
            continue
    return False

# ---------- 다운로드 감지 ----------
def wait_download(dldir: Path, before: set, timeout: int) -> Path:
    """새 파일이 생기고 .crdownload가 사라질 때까지 감지"""
    endt = time.time() + timeout
    while time.time() < endt:
        allf = set(p for p in dldir.glob("*") if p.is_file())
        newf = [p for p in allf - before if not p.name.endswith(".crdownload")]
        if newf:
            # 가장 최신 파일 반환
            newest = max(newf, key=lambda p: p.stat().st_mtime)
            return newest
        time.sleep(1.0)
    raise TimeoutError("download not detected within timeout")

# ---------- 파일 읽기/전처리 ----------
def _read_excel_first_table(path: Path) -> pd.DataFrame:
    # 표 윗부분(설명행) 제거: 'NO' 혹은 '계약년월' 등장 행을 헤더로 삼음
    df = pd.read_excel(path, engine="openpyxl", dtype=str)
    df = df.fillna("")
    hdr_row = None
    for i, row in df.iterrows():
        row_up = [str(x).strip().upper() for x in row.tolist()]
        if "NO" in row_up or "계약년월" in row_up:
            hdr_row = i; break
    if hdr_row is None:
        # 그냥 첫 행을 헤더로
        df.columns = df.iloc[0].astype(str).str.strip()
        df = df.iloc[1:].copy()
    else:
        df.columns = df.iloc[hdr_row].astype(str).str.strip()
        df = df.iloc[hdr_row+1:].copy()
    # 공백 헤더 제거
    df = df.loc[:, [c for c in df.columns if str(c).strip() != ""]]
    return df.reset_index(drop=True)

def _drop_no_col(df: pd.DataFrame) -> pd.DataFrame:
    for c in list(df.columns):
        if str(c).strip().upper() == "NO":
            df = df[df[c].astype(str).str.strip() != ""]
            df = df.drop(columns=[c])
            break
    return df

def _split_sigungu(df: pd.DataFrame, mode: str) -> pd.DataFrame:
    # mode: 'national' → 광역/구/법정동/리,  'seoul' → 시/구/법정동
    if "시군구" not in df.columns:
        return df
    parts = df["시군구"].astype(str).str.split(expand=True, n=3)
    if mode == "national":
        cols = ["광역","구","법정동","리"]
        for i, name in enumerate(cols):
            df[name] = parts[i] if parts.shape[1] > i else ""
    else:
        cols = ["시","구","법정동"]
        for i, name in enumerate(cols):
            df[name] = parts[i] if parts.shape[1] > i else ""
    return df.drop(columns=["시군구"])

def _split_yymm(df: pd.DataFrame) -> pd.DataFrame:
    if "계약년월" not in df.columns:
        return df
    s = df["계약년월"].astype(str).str.replace(r"\D", "", regex=True)
    df["계약년"] = s.str.slice(0, 4)
    df["계약월"] = s.str.slice(4, 6)
    return df.drop(columns=["계약년월"])

def _normalize_numbers(df: pd.DataFrame) -> pd.DataFrame:
    for c in ["거래금액(만원)","전용면적(㎡)"]:
        if c in df.columns:
            df[c] = (
                df[c].astype(str)
                     .str.replace(r"[^0-9.\-]", "", regex=True)
                     .replace({"": np.nan})
            )
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def preprocess_df(df: pd.DataFrame, mode: str) -> pd.DataFrame:
    # 남길 컬럼 인덱스(헤더) 라인 이후로만 처리하므로 일단 전체 읽은 뒤 정제
    # ※ “남길 컬럼명이 적힌 부분이 인덱스” → 위의 _read_excel_first_table에서 이미 맞춤
    df = _drop_no_col(df)
    df = _split_sigungu(df, mode)
    df = _split_yymm(df)
    df = _normalize_numbers(df)
    return df

def read_table(path: Path, mode: str) -> pd.DataFrame:
    if path.suffix.lower() in [".xls", ".xlsx"]:
        df = _read_excel_first_table(path)
        return preprocess_df(df, mode)
    # HTML 등 폴백은 현재 사용 안 함
    raise ValueError(f"unsupported file type: {path.suffix}")

def save_excel(path: Path, df: pd.DataFrame):
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as xw:
        df.to_excel(xw, index=False, sheet_name="data")

# ---------- Google (옵션) ----------
DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID", "").strip()
DRIVE_RETENTION_DAYS = int(os.getenv("DRIVE_RETENTION_DAYS", "3") or "3")
SHEET_ID = os.getenv("SHEET_ID", "").strip()

def load_sa_credentials(sa_path: Path):
    try:
        from google.oauth2.service_account import Credentials
        scopes = [
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/spreadsheets"
        ]
        data = json.loads(Path(sa_path).read_text(encoding="utf-8"))
        creds = Credentials.from_service_account_info(data, scopes=scopes)
        debug("  - SA loaded.")
        return creds
    except Exception as e:
        debug(f"  ! service account load failed: {e}")
        return None

def drive_upload_and_cleanup(creds, file_path: Path):
    if ARTIFACTS_ONLY or not creds or not DRIVE_FOLDER_ID:
        debug("  - skip Drive upload (Artifacts mode).")
        return
    try:
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaFileUpload
        svc = build("drive", "v3", credentials=creds, cache_discovery=False)

        media = MediaFileUpload(file_path.as_posix(), mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        meta = {"name": file_path.name, "parents": [DRIVE_FOLDER_ID]}
        svc.files().create(body=meta, media_body=media, fields="id,name").execute()
        debug(f"  - uploaded to Drive: {file_path.name}")

        if DRIVE_RETENTION_DAYS > 0:
            from dateutil import parser as dtp
            cutoff = time.time() - DRIVE_RETENTION_DAYS * 86400
            q = f"'{DRIVE_FOLDER_ID}' in parents and trashed=false"
            items = svc.files().list(q=q, fields="files(id,name,createdTime)").execute().get("files", [])
            for it in items:
                try:
                    ts = dtp.parse(it.get("createdTime","")).timestamp()
                except Exception:
                    continue
                if ts < cutoff:
                    try:
                        svc.files().delete(fileId=it["id"]).execute()
                        debug(f"  - old removed: {it['name']}")
                    except Exception:
                        pass
    except Exception as e:
        debug(f"  ! drive error: {e}")

def sheets_write(creds, *args, **kwargs):
    # 이번 단계에서는 시트 쓰기 보류(아티팩트 우선)
    if ARTIFACTS_ONLY or not creds or not SHEET_ID:
        debug("  - skip Sheets write (Artifacts mode).")
        return
    # (필요 시 기존 구현/요구 규칙에 맞춰 write 로직 추가)

# ---------- 한 덩어리 처리 ----------
def fetch_and_process(driver: webdriver.Chrome,
                      sido: Optional[str],
                      start: date,
                      end: date,
                      outname: str,
                      mode: str,           # 'national' or 'seoul'
                      creds) -> None:
    driver.get(URL)
    time.sleep(0.8)

    set_dates(driver, start, end)
    debug(f"  - set_dates: {start} ~ {end}")

    if sido:
        ok = select_sido(driver, sido)
        debug(f"  - select_sido({sido}): {ok}")

    got_file: Optional[Path] = None
    for attempt in range(1, CLICK_RETRY_MAX + 1):
        before = set(p for p in TMP_DL.glob("*") if p.is_file())
        ok = click_download(driver, "excel")
        debug(f"  - click_download(excel) / attempt {attempt}: {ok}")
        if not ok:
            time.sleep(CLICK_RETRY_WAIT)
            # 5회마다 폼 재세팅으로 복구 시도
            if attempt % 5 == 0:
                driver.get(URL); time.sleep(0.8)
                set_dates(driver, start, end)
                if sido: select_sido(driver, sido)
            continue
        try:
            got = wait_download(TMP_DL, before, timeout=DOWNLOAD_TIMEOUT)
            got_file = got
            debug(f"  - got file: {got_file}  size={got_file.stat().st_size:,}  ext={got_file.suffix}")
            break
        except TimeoutError:
            debug(f"  - warn: 다운로드 시작 감지 실패(시도 {attempt}/{CLICK_RETRY_MAX})")
            if attempt % 5 == 0:
                driver.get(URL); time.sleep(0.8)
                set_dates(driver, start, end)
                if sido: select_sido(driver, sido)
            # 즉시 다음 시도
            continue

    if not got_file:
        raise RuntimeError("다운로드 시작 감지 실패(최대 시도 초과)")

    df = read_table(got_file, mode=mode)
    out = SAVE_DIR / outname
    save_excel(out, df)
    debug(f"완료: {out}")

    drive_upload_and_cleanup(creds, out)
    # sheets_write(creds, ...)  # 아티팩트 모드에서는 스킵

# ---------- 메인 ----------
def main():
    sa_path = Path(os.getenv("SA_PATH", "sa.json"))
    creds = load_sa_credentials(sa_path) if sa_path.exists() else None

    driver = build_driver(TMP_DL)
    try:
        t = today_kst()

        # 전국: 현재 달 포함 12개월 (오래된 → 최신 순으로 진행)
        bases = [shift_months(month_first(t), -i) for i in range(0, 12)]
        bases.sort()
        for base in bases:
            start = base
            end = t if (base.year, base.month) == (t.year, t.month) else (shift_months(base, +1) - timedelta(days=1))
            name = f"전국 {yymm(base)}_{yymmdd(t)}.xlsx"
            debug(f"[전국] {start} ~ {end} → {name}")
            fetch_and_process(driver, None, start, end, name, mode="national", creds=creds)

        # 서울: 전년도 10월 1일 ~ 오늘
        if t.month >= 10:
            seoul_start = date(t.year - 1, 10, 1)
        else:
            seoul_start = date(t.year - 2, 10, 1)
        seoul_end = t
        seoul_name = f"서울시 {yymmdd(t)}.xlsx"
        debug(f"[서울] {seoul_start} ~ {seoul_end} → {seoul_name}")
        fetch_and_process(driver, "서울특별시", seoul_start, seoul_end, seoul_name, mode="seoul", creds=creds)

    finally:
        try:
            driver.quit()
        except Exception:
            pass

if __name__ == "__main__":
    main()
