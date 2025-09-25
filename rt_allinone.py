# -*- coding: utf-8 -*-
"""
rt_allinone.py
- rt.molit.go.kr 조건별 자료제공 페이지에서 월별/서울 데이터를 자동 다운로드
- 다운받은 원본을 전처리(행/열 정리, 숫자화, 주소 분리) 후 엑셀 저장 + 간단 피벗
- 전국: 최근 3개월(당월 포함, 당월은 오늘까지) -> "전국 YYMM_YYMMDD.xlsx"
- 서울: 전년도 10월 1일 ~ 오늘(한 번에) -> "서울시 YYMMDD.xlsx"
- (CI 환경) Google Drive 업로드/보관일수 정리 + 구글시트 갱신(있으면/되면 수행, 실패해도 다운로드·전처리는 계속)
"""

from __future__ import annotations
import os, re, sys, json, time, tempfile, shutil
from datetime import date, timedelta
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import numpy as np

# ---------- 환경/경로 ----------
URL = "https://rt.molit.go.kr/pt/xls/xls.do?mobileAt="

SAVE_DIR = Path(os.getenv("OUT_DIR", "output")).resolve()   # 결과 저장 (로컬: D:\OneDrive.. 대신 repo 하위)
TMP_DL   = (Path.cwd() / "_rt_downloads").resolve()         # 임시 다운로드 폴더
SAVE_DIR.mkdir(parents=True, exist_ok=True)
TMP_DL.mkdir(parents=True, exist_ok=True)

DOWNLOAD_TIMEOUT = int(os.getenv("DOWNLOAD_TIMEOUT", "120"))  # 1회 다운로드 대기
CLICK_RETRY_MAX  = int(os.getenv("CLICK_RETRY_MAX", "10"))    # 월별 시도 횟수
CLICK_RETRY_WAIT = int(os.getenv("CLICK_RETRY_WAIT", "30"))   # 시도 간 대기(초)

IS_CI = os.getenv("CI", "") == "1"

# ---------- 날짜 유틸 ----------
def today_kst() -> date:
    return date.today()

def month_first(d: date) -> date:
    return date(d.year, d.month, 1)

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

# ---------- Selenium ----------
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import (
    TimeoutException, ElementClickInterceptedException,
    ElementNotInteractableException, NoSuchElementException,
)
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.alert import Alert

def debug(msg: str):
    # 깔끔한 실시간 로그
    sys.stdout.write(msg.rstrip() + "\n")
    sys.stdout.flush()

def build_driver(download_dir: Path) -> webdriver.Chrome:
    opts = Options()

    # GitHub Actions/리눅스 헤드리스에서 안정적으로 돌기 위한 필수 옵션
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-notifications")
    opts.add_argument("--window-size=1400,900")
    opts.add_argument("--lang=ko-KR")
    # 헤드리스 탐지 우회(일반 UA)
    opts.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
    # 다운로드 허용/제약 해제
    prefs = {
        "download.default_directory": str(download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "safebrowsing.disable_download_protection": True,
        "download_restrictions": 0,
    }
    opts.add_experimental_option("prefs", prefs)

    # Actions에서 지정한 바이너리 경로가 있으면 사용
    chrome_bin = os.getenv("CHROME_BIN")
    if chrome_bin:
        opts.binary_location = chrome_bin

    # 임시 사용자 프로필(세션 충돌 방지)
    tmp_profile = Path(tempfile.mkdtemp(prefix="chrome_prof_"))
    opts.add_argument(f"--user-data-dir={tmp_profile.as_posix()}")

    # chromedriver 경로(있으면) 우선
    chromedriver_bin = os.getenv("CHROMEDRIVER_BIN")
    if chromedriver_bin and Path(chromedriver_bin).exists():
        service = Service(chromedriver_bin)
    else:
        # 로컬 환경에서도 동작
        from webdriver_manager.chrome import ChromeDriverManager
        service = Service(ChromeDriverManager().install())

    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(90)

    # Headless 다운로드 허용(CDP)
    try:
        driver.execute_cdp_cmd("Page.setDownloadBehavior", {
            "behavior": "allow",
            "downloadPath": str(download_dir),
        })
    except Exception as e:
        debug(f"  - warn: setDownloadBehavior failed: {e}")

    # 종료 시 임시 프로필 제거
    import atexit
    atexit.register(lambda: shutil.rmtree(tmp_profile, ignore_errors=True))
    return driver

# ---------- 페이지 조작 ----------
def find_date_inputs(driver: webdriver.Chrome) -> Tuple:
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
    raise RuntimeError("날짜 입력 박스를 찾지 못했습니다.")

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
    # 실제로 값이 들어갔는지 확인
    assert s_el.get_attribute("value") == start.isoformat()
    assert e_el.get_attribute("value") == end.isoformat()

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

def _try_accept_alert(driver: webdriver.Chrome, wait=2.0):
    try:
        WebDriverWait(driver, wait).until(EC.alert_is_present())
        Alert(driver).accept()
        time.sleep(0.3)
        return True
    except TimeoutException:
        return False
    except Exception:
        return False

def _click_by_locators(driver: webdriver.Chrome, label: str) -> bool:
    """여러 locator로 'EXCEL 다운' 시도"""
    locators = [
        (By.XPATH, f"//button[normalize-space()='{label}']"),
        (By.XPATH, f"//a[normalize-space()='{label}']"),
        (By.XPATH, f"//*[self::a or self::button][contains(normalize-space(), 'EXCEL')]"),
        (By.XPATH, f"//*[contains(@onclick,'excel') or contains(@onclick,'xls')][contains(.,'EXCEL')]"),
    ]
    for by, sel in locators:
        els = driver.find_elements(by, sel)
        if not els:
            continue
        btn = els[0]
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            time.sleep(0.2)
            try:
                btn.click()
            except (ElementClickInterceptedException, ElementNotInteractableException):
                driver.execute_script("arguments[0].click();", btn)
            return True
        except Exception:
            continue
    return False

def click_download(driver: webdriver.Chrome, kind="excel") -> bool:
    """EXCEL 다운 클릭. 안되면 JS 함수 호출까지"""
    label = "EXCEL 다운" if kind == "excel" else "CSV 다운"

    # 남은 경고창 정리
    _try_accept_alert(driver, wait=1.0)

    # 1) 버튼/링크 클릭
    if _click_by_locators(driver, label):
        _try_accept_alert(driver, wait=3.0)  # “처리중입니다” 알림 확인
        return True

    # 2) JS 함수 직접 호출(페이지에서 흔히 쓰는 함수명 후보)
    js_funcs = ["excelDown", "xlsDown", "excelDownload", "fnExcel", "fnExcelDown", "fncExcel"]
    for fn in js_funcs:
        try:
            ok = driver.execute_script(
                f"if (typeof {fn} === 'function') {{ {fn}(); return true; }} return false;"
            )
            if ok:
                _try_accept_alert(driver, wait=3.0)
                return True
        except Exception:
            pass

    return False

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

# ---------- 읽기/전처리 ----------
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
            tt = t.iloc[hdr + 1 :].copy()
            tt.columns = t.iloc[hdr].astype(str).str.strip()
            return tt
    return tables[0]

def read_table(path: Path) -> pd.DataFrame:
    ext = path.suffix.lower()
    if ext in (".xlsx", ".xls"):
        try:
            df0 = pd.read_excel(path, header=None, dtype=str, engine="openpyxl" if ext == ".xlsx" else None)
        except Exception:
            return _read_html_table(path)
        hdr_idx = None
        max_scan = min(80, len(df0))
        for i in range(max_scan):
            row = df0.iloc[i].astype(str).str.strip().tolist()
            if row and (row[0].strip().upper() == "NO"):
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
        df = df.rename(columns={"시군구 ": "시군구"})
    must_rename = {}
    for c in df.columns:
        k = str(c).replace(" ", "")
        if k == "거래금액(만원)" and c != "거래금액(만원)": must_rename[c] = "거래금액(만원)"
        if k == "전용면적(㎡)" and c != "전용면적(㎡)": must_rename[c] = "전용면적(㎡)"
    if must_rename:
        df = df.rename(columns=must_rename)
    # NO 열 제거
    for c in list(df.columns):
        if str(c).strip().upper() == "NO":
            df = df[df[c].notna()].drop(columns=[c])
    # 숫자화
    for c in ["거래금액(만원)", "전용면적(㎡)"]:
        if c in df.columns:
            df[c] = (
                df[c].astype(str)
                .str.replace(",", "", regex=False)
                .str.replace(" ", "", regex=False)
                .str.replace("-", "", regex=False)
                .replace({"": np.nan})
            )
            df[c] = pd.to_numeric(df[c], errors="coerce")
    # 시군구 쪼개기
    if "시군구" in df.columns:
        parts = df["시군구"].astype(str).str.split(expand=True, n=2)
        for i, name in enumerate(["광역", "구", "법정동"]):
            if parts.shape[1] > i:
                df[name] = parts[i].fillna("")
            else:
                df[name] = ""
    # 계약년/월 분리(서울만)
    if split_month and "계약년월" in df.columns:
        s = df["계약년월"].astype(str).str.replace(r"\D", "", regex=True)
        df["계약년"] = s.str.slice(0, 4)
        df["계약월"] = s.str.slice(4, 6)
    return df.reset_index(drop=True)

def pivot_national(df: pd.DataFrame) -> pd.DataFrame:
    if "광역" in df.columns:
        pv = df.pivot_table(index="광역", values="거래금액(만원)", aggfunc="count").rename(columns={"거래금액(만원)": "건수"})
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

# ---------- 구글(있으면 실행) ----------
# - 서비스 계정 JSON이 깨지거나 권한 없으면 조용히 스킵(로그만)
DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID", "").strip()
DRIVE_RETENTION_DAYS = int(os.getenv("DRIVE_RETENTION_DAYS", "3") or "3")
SHEET_ID = os.getenv("SHEET_ID", "").strip()

def load_sa_credentials(sa_path: Path):
    try:
        from google.oauth2.service_account import Credentials
        scopes = [
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/spreadsheets",
        ]
        creds = Credentials.from_service_account_file(str(sa_path), scopes=scopes)
        debug("  - SA loaded.")
        return creds
    except Exception as e:
        debug(f"  ! service account load failed: {e}")
        return None

def drive_upload_and_cleanup(creds, file_path: Path):
    if not (creds and DRIVE_FOLDER_ID):
        return
    try:
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaFileUpload
        svc = build("drive", "v3", credentials=creds, cache_discovery=False)
        media = MediaFileUpload(str(file_path), mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", resumable=False)
        meta = {"name": file_path.name, "parents": [DRIVE_FOLDER_ID]}
        svc.files().create(body=meta, media_body=media, fields="id,name").execute()
        debug(f"  - uploaded to Drive: {file_path.name}")
        # 보관일수 초과 삭제
        if DRIVE_RETENTION_DAYS > 0:
            q = f"'{DRIVE_FOLDER_ID}' in parents and trashed=false"
            items = svc.files().list(q=q, fields="files(id,name,createdTime)").execute().get("files", [])
            cutoff = time.time() - DRIVE_RETENTION_DAYS * 86400
            for it in items:
                # createdTime -> epoch
                ct = it.get("createdTime", "")
                try:
                    from datetime import datetime
                    from dateutil import parser as dtp  # optional
                    ts = dtp.parse(ct).timestamp()
                except Exception:
                    # 대충 보관 개수 과하면 최근만 두고 삭제하는 식으로 바꿔도 됨
                    continue
                if ts < cutoff:
                    try:
                        svc.files().delete(fileId=it["id"]).execute()
                        debug(f"  - deleted old: {it['name']}")
                    except Exception:
                        pass
    except Exception as e:
        debug(f"  ! drive error: {e}")

def sheets_write(creds, outname: str, pivot: pd.DataFrame, mode: str, today_str: str):
    if not (creds and SHEET_ID and isinstance(pivot, pd.DataFrame) and not pivot.empty):
        return
    try:
        import gspread
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SHEET_ID)

        if mode == "national":
            # 시트명 예: "전국 25년 07월"
            y, m = "20" + outname[3:5], outname[5:7]
            title = f"전국 {y[-2:]}년 {m}월"
            try:
                ws = sh.worksheet(title)
            except Exception:
                ws = sh.add_worksheet(title=title, rows="200", cols="30")
            # today_str 행 찾기 (첫열에 날짜), 없으면 append
            vals = ws.get_all_values()
            row_idx = None
            for i, row in enumerate(vals, start=1):
                if row and row[0].strip() == today_str:
                    row_idx = i; break
            if row_idx is None:
                row_idx = len(vals) + 1
                ws.update_cell(row_idx, 1, today_str)
            # 광역, 건수 쓰기 (2열부터)
            # 헤더행 1행: 날짜 | 광역1 | 광역2 | ...
            if len(vals) == 0:
                ws.update_cell(1, 1, "날짜")
                for j, r in enumerate(pivot["광역"].tolist(), start=2):
                    ws.update_cell(1, j, r)
            # 값행
            counts = pivot.set_index("광역")["건수"].to_dict()
            header = ws.row_values(1)[1:]
            # 없는 광역은 뒤에 추가
            existing = header
            for region in counts.keys():
                if region not in existing:
                    ws.update_cell(1, len(existing)+2, region)
                    existing.append(region)
            # 헤더 최신화
            header = ws.row_values(1)[1:]
            # 각 열별 숫자 업데이트
            for j, region in enumerate(header, start=2):
                val = counts.get(region, 0)
                ws.update_cell(row_idx, j, int(val))

        else:  # seoul
            # 각 월(열) 헤더가 '01'~'12' 라고 가정
            title = f"서울 {today_str[:2]}년 {today_str[2:4]}월"
            try:
                ws = sh.worksheet(title)
            except Exception:
                ws = sh.add_worksheet(title=title, rows="200", cols="30")
            vals = ws.get_all_values()
            # 첫 열: 날짜, 이후 구들 헤더 or 반대로 쓰고 있다면 프로젝트 규칙에 맞춰 수정
            if len(vals) == 0:
                ws.update_cell(1, 1, "날짜")
                # 구 헤더
                for j, g in enumerate(pivot["구"].tolist(), start=2):
                    ws.update_cell(1, j, g)
            # 날짜 행 찾기
            row_idx = None
            for i, row in enumerate(vals, start=1):
                if row and row[0].strip() == today_str:
                    row_idx = i; break
            if row_idx is None:
                row_idx = len(vals) + 1
                ws.update_cell(row_idx, 1, today_str)
            # 월별 합계(피벗 열은 '01'~'12')
            month_cols = [c for c in pivot.columns if c.isdigit()]
            month_now = today_str[2:4]
            if month_now not in month_cols:
                return
            ser = pivot.set_index("구")[month_now]
            header = ws.row_values(1)[1:]
            # 없는 구는 뒤에 추가
            for g in ser.index:
                if g not in header:
                    ws.update_cell(1, len(header)+2, g)
                    header.append(g)
            # 값 채우기
            header = ws.row_values(1)[1:]
            for j, g in enumerate(header, start=2):
                val = ser.get(g, 0)
                ws.update_cell(row_idx, j, int(val))

    except Exception as e:
        debug(f"  ! sheets error: {e}")

# ---------- 한 번의 처리 ----------
def fetch_and_process(driver: webdriver.Chrome,
                      sido: Optional[str],
                      start: date, end: date,
                      outname: str,
                      pivot_mode: str,
                      creds) -> None:
    """pivot_mode: 'national' or 'seoul'"""
    # 페이지 진입
    driver.get(URL)
    time.sleep(0.8)

    # 날짜 설정
    set_dates(driver, start, end)
    debug(f"  - set_dates: {start} ~ {end}")

    # 시도 선택(전국은 None)
    if sido:
        ok = select_sido(driver, sido)
        debug(f"  - select_sido({sido}): {ok}")

    # 다운로드(재시도 루프)
    kind = "excel"
    got_file: Optional[Path] = None
    for attempt in range(1, CLICK_RETRY_MAX + 1):
        before = set(TMP_DL.glob("*"))
        ok = click_download(driver, kind)
        debug(f"  - click_download({kind}) / attempt {attempt}: {ok}")
        if not ok:
            time.sleep(1.0)
        # 다운로드 시작/완료 대기
        try:
            got = wait_download(TMP_DL, before, timeout=DOWNLOAD_TIMEOUT)
            got_file = got
            size = got.stat().st_size
            debug(f"  - got file: {got}  size={size:,}  ext={got.suffix.lower()}")
            break
        except TimeoutError:
            debug(f"  - warn: 다운로드 시작 감지 실패(시도 {attempt}/{CLICK_RETRY_MAX})")
            # 새로고침 후 다시 설정
            driver.get(URL)
            time.sleep(0.8)
            set_dates(driver, start, end)
            if sido:
                select_sido(driver, sido)
            time.sleep(1.0)
            continue

    if not got_file:
        raise RuntimeError("다운로드 시작 감지 실패(최대 시도 초과)")

    # 읽기/전처리/피벗/저장
    df_raw = read_table(got_file)
    split_month = (pivot_mode == "seoul")
    df = clean_df(df_raw, split_month=split_month)
    debug(f"  - parsed: rows={len(df)}  cols={len(df.columns)}")

    if pivot_mode == "national":
        pv = pivot_national(df)
    else:
        pv = pivot_seoul(df)

    out = SAVE_DIR / outname
    save_excel(out, df, pv)
    debug(f"완료: {out}")

    # (옵션) 드라이브 업로드 + 보관 정리
    drive_upload_and_cleanup(creds, out)

    # (옵션) 구글시트 쓰기
    today_str = yymmdd(today_kst())
    sheets_write(creds, outname.replace(".xlsx", ""), pv, pivot_mode, today_str)

# ---------- 메인 ----------
def main():
    # 서비스 계정 로드(실패해도 계속 진행)
    sa_path = Path(os.getenv("SA_PATH", "sa.json"))
    creds = load_sa_credentials(sa_path) if sa_path.exists() else None

    driver = build_driver(TMP_DL)
    try:
        t = today_kst()

        # 전국: 최근 3개월(당월 포함), 당월은 오늘까지
        months = [shift_months(month_first(t), k) for k in [0, -1, -2]]
        months.sort()
        for base in months:
            start = base
            if base.month == t.month and base.year == t.year:
                end = t  # 당월은 오늘까지
            else:
                end = shift_months(base, +1) - timedelta(days=1)
            name = f"전국 {yymm(base)}_{yymmdd(t)}.xlsx"
            debug(f"[전국] {start} ~ {end} → {name}")
            fetch_and_process(driver, None, start, end, name, pivot_mode="national", creds=creds)
            time.sleep(1.0)

        # 서울: 전년도 10월 1일 ~ 오늘
        start_seoul = date(t.year - 1, 10, 1)
        if start_seoul > t:
            start_seoul = date(t.year, 1, 1)
        name_seoul = f"서울시 {yymmdd(t)}.xlsx"
        debug(f"[서울] {start_seoul} ~ {t} → {name_seoul}")
        fetch_and_process(driver, "서울특별시", start_seoul, t, name_seoul, pivot_mode="seoul", creds=creds)

    finally:
        try:
            driver.quit()
        except Exception:
            pass

if __name__ == "__main__":
    main()
