# -*- coding: utf-8 -*-
"""
국토부 실거래가 > 조건별 자료제공 페이지 자동화
- 크롬(헤드리스)로 월별/서울기간 데이터 내려받아 전처리 후 엑셀 저장
- 전국: 최근 3개월(당월 포함, 당월은 오늘까지) → "전국 YYMM_YYMMDD.xlsx"
- 서울: 전년도 10월 1일 ~ 오늘 → "서울시 YYMMDD.xlsx"

전처리 규칙 (요청 반영):
- 헤더행 자동 탐지로 불필요 상단 제거, NO 열 제거
- '시군구' 분리:
  · 전국: 광역/구/법정동/리
  · 서울: 시/구/법정동
- '계약년월' → '계약년','계약월' 분리(계약년월 열은 삭제)
- '거래금액(만원)','전용면적(㎡)' 숫자화
"""

from __future__ import annotations

import os, re, sys, time, tempfile, shutil
from pathlib import Path
from typing import Optional, Tuple, List
from datetime import datetime, timedelta, date

import numpy as np
import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ---------------- 기본 설정 ----------------

URL = "https://rt.molit.go.kr/pt/xls/xls.do?mobileAt="

SAVE_DIR = Path(os.getenv("OUT_DIR", "output")).resolve()
TMP_DL   = (Path.cwd() / "_rt_downloads").resolve()
SAVE_DIR.mkdir(parents=True, exist_ok=True)
TMP_DL.mkdir(parents=True, exist_ok=True)

DOWNLOAD_TIMEOUT = int(os.getenv("DOWNLOAD_TIMEOUT", "120"))
CLICK_RETRY_MAX  = int(os.getenv("CLICK_RETRY_MAX", "10"))
CLICK_RETRY_WAIT = int(os.getenv("CLICK_RETRY_WAIT", "30"))

IS_CI = os.getenv("CI", "") == "1"

# 구글(옵션)
DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID", "").strip()
DRIVE_RETENTION_DAYS = int(os.getenv("DRIVE_RETENTION_DAYS", "3") or "3")
SHEET_ID = os.getenv("SHEET_ID", "").strip()

def debug(msg: str):
    sys.stdout.write(msg.rstrip() + "\n")
    sys.stdout.flush()

# ---------------- 날짜 유틸 ----------------

def today_kst() -> date:
    # 런너가 UTC일 수 있으니 간단 보정(+9h)
    return (datetime.utcnow() + timedelta(hours=9)).date()

def month_first(d: date) -> date:
    return d.replace(day=1)

def shift_months(d: date, k: int) -> date:
    y, m = d.year, d.month + k
    y += (m - 1) // 12
    m = (m - 1) % 12 + 1
    return date(y, m, 1)

def yymm(d: date) -> str:
    return d.strftime("%y%m")

def yymmdd(d: date) -> str:
    return d.strftime("%y%m%d")

# ---------------- 크롬 드라이버 ----------------

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
        "profile.default_content_settings.popups": 0,
    }
    opts.add_experimental_option("prefs", prefs)

    # Actions가 지정해준 바이너리/드라이버 경로가 있으면 사용
    chrome_bin = os.getenv("CHROME_BIN")
    if chrome_bin:
        opts.binary_location = chrome_bin

    chromedriver_bin = os.getenv("CHROMEDRIVER_BIN")
    if chromedriver_bin and Path(chromedriver_bin).exists():
        service = Service(chromedriver_bin)
    else:
        from webdriver_manager.chrome import ChromeDriverManager
        service = Service(ChromeDriverManager().install())

    driver = webdriver.Chrome(service=service, options=opts)

    # 다운로드 허가(헤드리스)
    try:
        driver.execute_cdp_cmd(
            "Page.setDownloadBehavior",
            {"behavior": "allow", "downloadPath": str(download_dir)}
        )
    except Exception as e:
        debug(f"  - warn: setDownloadBehavior failed: {e}")

    # 임시 프로필 정리
    tmp_profile = Path(tempfile.mkdtemp(prefix="chrome_prof_"))
    opts.add_argument(f"--user-data-dir={tmp_profile.as_posix()}")
    import atexit
    atexit.register(lambda: shutil.rmtree(tmp_profile, ignore_errors=True))

    return driver

# ---------------- 페이지 조작 ----------------

def _looks_like_date_input(el) -> bool:
    typ = (el.get_attribute("type") or "").lower()
    ph  = (el.get_attribute("placeholder") or "").lower()
    val = (el.get_attribute("value") or "").lower()
    name= (el.get_attribute("name") or "").lower()
    id_ = (el.get_attribute("id") or "").lower()
    txt = " ".join([ph, val, name, id_])
    return (typ in ("date", "text", "")) and (
        re.search(r"\d{4}-\d{2}-\d{2}", ph) or
        re.search(r"\d{4}-\d{2}-\d{2}", val) or
        "yyyy" in ph or "yyyy-mm-dd" in ph or
        "start" in name or "end" in name or
        "from" in name or "to" in name or
        "start" in id_ or "end" in id_ or
        "from" in id_ or "to" in id_
    )

def find_date_inputs(driver: webdriver.Chrome) -> Tuple:
    inputs = driver.find_elements(By.CSS_SELECTOR, "input")
    cands = [el for el in inputs if _looks_like_date_input(el)]
    if len(cands) >= 2:
        return cands[0], cands[1]
    dates = [e for e in inputs if (e.get_attribute("type") or "").lower() == "date"]
    if len(dates) >= 2:
        return dates[0], dates[1]
    text_inputs = [e for e in inputs if (e.get_attribute("type") or "").lower() in ("text", "")]
    if len(text_inputs) >= 2:
        return text_inputs[0], text_inputs[1]
    raise RuntimeError("날짜 입력 박스를 찾지 못했습니다.")

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

def _type_and_verify(el, val: str) -> bool:
    try:
        el.click()
        el.send_keys(Keys.CONTROL, "a")
        el.send_keys(Keys.DELETE)
        el.send_keys(val)
        time.sleep(0.2)
        el.send_keys(Keys.TAB)
        time.sleep(0.2)
        cur = (el.get_attribute("value") or "").strip()
        return cur == val
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
    time.sleep(0.2)

def select_sido(driver: webdriver.Chrome, wanted: str) -> bool:
    selects = driver.find_elements(By.TAG_NAME, "select")
    for sel in selects:
        try:
            s = Select(sel)
            options = [o.text.strip() for o in s.options]
            if any(k in " ".join(options) for k in ("시도", "도", "서울특별시", "경기도")):
                for i, o in enumerate(s.options):
                    if wanted in o.text:
                        s.select_by_index(i)
                        time.sleep(0.2)
                        return True
        except Exception:
            continue
    return False

def _click_by_locators(driver: webdriver.Chrome, label: str) -> bool:
    locators = [
        (By.XPATH, f"//button[normalize-space()='{label}']"),
        (By.XPATH, f"//a[normalize-space()='{label}']"),
        (By.CSS_SELECTOR, "button, a"),
    ]
    for by, q in locators:
        try:
            if by == By.CSS_SELECTOR and q == "button, a":
                elems = driver.find_elements(by, q)
                for el in elems:
                    t = (el.text or "").strip()
                    if t == label:
                        el.click(); return True
            else:
                el = WebDriverWait(driver, 2).until(EC.element_to_be_clickable((by, q)))
                el.click(); return True
        except Exception:
            pass
    return False

def click_download(driver: webdriver.Chrome, kind="excel") -> bool:
    label = "EXCEL 다운" if kind == "excel" else "CSV 다운"
    # 버튼/링크 시도
    if _click_by_locators(driver, label):
        return True
    # JS 함수 직접 호출 후보
    js_funcs = ["excelDown", "xlsDown", "excelDownload", "fnExcel", "fnExcelDown", "fncExcel"]
    for fn in js_funcs:
        try:
            driver.execute_script(f"if (typeof {fn}==='function') {{ {fn}(); }}")
            return True
        except Exception:
            pass
    return False

def wait_download(folder: Path, before: List[Path], timeout=DOWNLOAD_TIMEOUT) -> Path:
    start = time.time()
    while time.time() - start < timeout:
        files = list(folder.glob("*"))
        new = [f for f in files if f not in before]
        # .crdownload 이면 완료까지 대기
        done = [f for f in new if not str(f).endswith(".crdownload")]
        if done:
            return sorted(done, key=lambda p: p.stat().st_mtime)[-1]
        time.sleep(1.0)
    raise TimeoutError("다운로드 시작/완료 감지 실패")

# ---------------- 파일 읽기 & 전처리 ----------------

EXPECTED_HEADER_KEYS = [
    "시군구","번지","본번","부번","단지명","전용면적(㎡)","계약년월","계약일","거래금액(만원)","동","층",
    "매수자","매도자","건축년도","도로명","해제사유발생일","거래유형","중개사소재지","등기일자","주택유형"
]

def _detect_header_row(df: pd.DataFrame) -> int:
    """
    엑셀 첫 수십행을 스캔하여 '시군구','거래금액(만원)' 같은 키가 다수 포함된 행을 헤더로 간주
    """
    max_scan = min(len(df), 50)
    best_idx, best_score = 0, -1
    for i in range(max_scan):
        row = df.iloc[i].astype(str).tolist()
        score = sum(1 for key in EXPECTED_HEADER_KEYS if any(key in str(x) for x in row))
        if score > best_score:
            best_score, best_idx = score, i
    return best_idx

def read_table(xlsx_path: Path) -> pd.DataFrame:
    # 우선 전체를 헤더 없이 불러서 헤더행 탐지
    temp = pd.read_excel(xlsx_path, header=None, engine="openpyxl")
    hdr = _detect_header_row(temp)
    df = pd.read_excel(xlsx_path, header=hdr, engine="openpyxl")
    # NO 열 제거
    for c in list(df.columns):
        if str(c).strip().upper() == "NO":
            df = df[df[c].notna()].drop(columns=[c])
    # 완전 공백 행 제거
    df = df.dropna(how="all")
    return df

def _split_sigungu(df: pd.DataFrame, mode: str) -> pd.DataFrame:
    if "시군구" not in df.columns:
        return df
    parts = df["시군구"].astype(str).str.split(expand=True)
    if mode == "seoul":
        names = ["시", "구", "법정동"]
    else:
        names = ["광역", "구", "법정동", "리"]
    for i, name in enumerate(names):
        df[name] = parts[i] if parts.shape[1] > i else ""
    df = df.drop(columns=["시군구"])
    return df

def _split_contract_ym(df: pd.DataFrame) -> pd.DataFrame:
    if "계약년월" not in df.columns:
        return df
    s = df["계약년월"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(6)
    df["계약년"] = s.str.slice(0, 4)
    df["계약월"] = s.str.slice(4, 6)
    df = df.drop(columns=["계약년월"])
    return df

def _to_numeric(df: pd.DataFrame, col: str):
    if col in df.columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.replace(" ", "", regex=False)
            .replace({"": np.nan})
        )
        df[col] = pd.to_numeric(df[col], errors="coerce")

def clean_df(df: pd.DataFrame, mode: str) -> pd.DataFrame:
    # 관심 컬럼만(있을 때)
    keep = [c for c in EXPECTED_HEADER_KEYS if c in df.columns]
    if keep:
        df = df[keep + [c for c in df.columns if c not in keep]]  # 우선순위 보장

    # 시군구 분리
    df = _split_sigungu(df, mode)

    # 계약년월 분리
    df = _split_contract_ym(df)

    # 숫자화
    _to_numeric(df, "거래금액(만원)")
    _to_numeric(df, "전용면적(㎡)")

    return df.reset_index(drop=True)

# ---------------- 저장 & (선택) 구글 ----------------

def save_excel(path: Path, df: pd.DataFrame):
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as xw:
        df.to_excel(xw, index=False, sheet_name="data")

# (선택) Drive/Sheets — 실패해도 본작업엔 영향 없도록 try/except
def load_sa_credentials(sa_path: Path):
    try:
        import json
        from google.oauth2 import service_account
        info = json.loads(sa_path.read_text(encoding="utf-8"))
        scopes = [
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/spreadsheets",
        ]
        return service_account.Credentials.from_service_account_info(info, scopes=scopes)
    except Exception as e:
        debug(f"  ! service account load failed: {e}")
        return None

def drive_upload_and_cleanup(creds, file_path: Path):
    if not (creds and DRIVE_FOLDER_ID):
        debug("  - skip Drive upload (Artifacts mode).")
        return
    try:
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaFileUpload
        svc = build("drive", "v3", credentials=creds)
        meta = {"name": file_path.name, "parents": [DRIVE_FOLDER_ID]}
        media = MediaFileUpload(str(file_path), mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        svc.files().create(body=meta, media_body=media, fields="id").execute()
        debug(f"  - uploaded to Drive: {file_path.name}")
        # 보관일수 정리
        if DRIVE_RETENTION_DAYS > 0:
            q = f"'{DRIVE_FOLDER_ID}' in parents and trashed=false"
            items = svc.files().list(q=q, fields="files(id,name,createdTime)").execute().get("files", [])
            import dateutil.parser as dtp
            cutoff = time.time() - DRIVE_RETENTION_DAYS * 86400
            for it in items:
                try:
                    ts = dtp.parse(it.get("createdTime","")).timestamp()
                except Exception:
                    continue
                if ts < cutoff:
                    try: svc.files().delete(fileId=it["id"]).execute()
                    except Exception: pass
    except Exception as e:
        debug(f"  ! drive error: {e}")

# ---------------- 파이프라인 ----------------

def fetch_and_process(driver: webdriver.Chrome,
                      sido: Optional[str],
                      start: date,
                      end: date,
                      outname: str,
                      mode: str,
                      creds):
    # 페이지 진입 & 기간/지역 세팅
    driver.get(URL); time.sleep(0.8)
    set_dates(driver, start, end)
    debug(f"  - set_dates: {start} ~ {end}")
    if sido:
        ok = select_sido(driver, sido)
        debug(f"  - select_sido({sido}): {ok}")

    # 다운로드 시도
    got_file: Optional[Path] = None
    for attempt in range(1, CLICK_RETRY_MAX + 1):
        before = list(TMP_DL.glob("*"))
        ok = click_download(driver, "excel")
        debug(f"  - click_download(excel) / attempt {attempt}: {ok}")
        if not ok:
            time.sleep(1.0)
        try:
            got = wait_download(TMP_DL, before, timeout=DOWNLOAD_TIMEOUT)
            got_file = got
            debug(f"  - got file: {got}  size={got.stat().st_size:,}  ext={got.suffix}")
            break
        except TimeoutError:
            debug(f"  - warn: 다운로드 시작 감지 실패(시도 {attempt}/{CLICK_RETRY_MAX})")
            driver.get(URL); time.sleep(0.8)
            set_dates(driver, start, end)
            if sido: select_sido(driver, sido)
            time.sleep(1.0)

    if not got_file:
        raise RuntimeError("다운로드 시작 감지 실패(최대 시도 초과)")

    # 읽기 & 전처리 & 저장
    df_raw = read_table(got_file)
    df = clean_df(df_raw, mode=("seoul" if sido else "national"))
    out = SAVE_DIR / outname
    save_excel(out, df)
    debug(f"완료: {out}")

    # (선택) Drive 업로드
    drive_upload_and_cleanup(creds, out)

def main():
    # 서비스 계정 로드(없거나 실패해도 계속)
    sa_path = Path(os.getenv("SA_PATH", "sa.json"))
    creds = load_sa_credentials(sa_path) if sa_path.exists() else None

    driver = build_driver(TMP_DL)
    try:
        t = today_kst()

        # 전국: 최근 3개월(당월은 오늘까지)
        bases = sorted([shift_months(month_first(t), k) for k in (0, -1, -2)])
        for base in bases:
            end = t if (base.year, base.month) == (t.year, t.month) else (shift_months(base, +1) - timedelta(days=1))
            name = f"전국 {yymm(base)}_{yymmdd(t)}.xlsx"
            debug(f"[전국] {base} ~ {end} → {name}")
            fetch_and_process(driver, None, base, end, name, "national", creds)

        # 서울: 전년도 10월 1일 ~ 오늘
        start_seoul = date(t.year - 1, 10, 1)
        name_seoul = f"서울시 {yymmdd(t)}.xlsx"
        debug(f"[서울] {start_seoul} ~ {t} → {name_seoul}")
        fetch_and_process(driver, "서울특별시", start_seoul, t, name_seoul, "seoul", creds)

    finally:
        try: driver.quit()
        except Exception: pass

if __name__ == "__main__":
    main()
