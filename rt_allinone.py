import os
import sys
import time
import json
import glob
import shutil
import traceback
from pathlib import Path
from datetime import datetime, date, timedelta

import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# =========================
# 설정 (안정판)
# =========================
BASE_URL = os.environ.get("MOLIT_URL", "https://rt.molit.go.kr/")
OUTPUT_DIR = Path("output")
TMP_DL = Path("_rt_downloads")
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
TMP_DL.mkdir(exist_ok=True, parents=True)

CLICK_RETRY_MAX = 10
WAIT_BETWEEN_CLICK = 5      # 클릭 실패 또는 재시도 사이 텀(초) – 성공 로그와 유사
WAIT_DOWNLOAD_START = 30    # 다운로드 시작 감지 대기
WAIT_DOWNLOAD_FINISH = 240  # 다운로드 완료 감지 대기

# =========================
# 공통 유틸
# =========================
def log(msg: str):
    print(msg, flush=True)

def human_size(p: Path):
    try:
        return f"{p.stat().st_size:,}"
    except FileNotFoundError:
        return "?"

def newest_file(dirpath: Path):
    files = [p for p in dirpath.glob("*") if p.is_file()]
    if not files:
        return None
    return max(files, key=lambda p: p.stat().st_mtime)

def list_files(dirpath: Path):
    return set(str(p) for p in dirpath.glob("*") if p.is_file())

def wait_download_start(dirpath: Path, before: set, timeout: int) -> bool:
    """클릭 후 '새 파일(.crdownload 또는 .xlsx)' 등장 감지"""
    end = time.time() + timeout
    while time.time() < end:
        after = list_files(dirpath)
        new_files = [Path(p) for p in after - before]
        for nf in new_files:
            if nf.suffix in (".crdownload", ".xlsx"):
                return True
        time.sleep(1)
    return False

def wait_download_finish(dirpath: Path, timeout: int) -> Path | None:
    """가장 최신 파일이 안정적인 .xlsx가 될 때까지 대기"""
    end = time.time() + timeout
    last = None
    while time.time() < end:
        last = newest_file(dirpath)
        if last and last.suffix.lower() == ".xlsx" and last.exists():
            s0 = last.stat().st_size
            time.sleep(1.0)
            s1 = last.stat().st_size
            if s0 == s1 and s1 > 0:
                return last
        time.sleep(1)
    return None

# =========================
# 브라우저 준비
# =========================
def build_driver(download_dir: Path):
    opts = Options()
    chrome_bin = os.environ.get("CHROME_BIN")
    if chrome_bin:
        opts.binary_location = chrome_bin

    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1600,1200")
    opts.add_argument("--lang=ko-KR")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-notifications")
    opts.add_argument("--disable-popup-blocking")

    prefs = {
        "profile.default_content_settings.popups": 0,
        "download.default_directory": str(download_dir.resolve()),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "profile.default_content_setting_values.automatic_downloads": 1,
    }
    opts.add_experimental_option("prefs", prefs)

    svc_path = os.environ.get("CHROMEDRIVER_BIN")
    service = Service(executable_path=svc_path) if svc_path else Service()
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(60)
    return driver

def go_home(driver):
    driver.get(BASE_URL)
    WebDriverWait(driver, 30).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )
    time.sleep(1)

# =========================
# 프레임/네비게이션 보조
# =========================
def debug_list_frames(driver, prefix=""):
    """프레임 목록 및 식별 정보 로깅(진단용)"""
    try:
        frames = driver.find_elements(By.TAG_NAME, "iframe")
        frames2 = driver.find_elements(By.TAG_NAME, "frame")
        allf = frames + frames2
        log(f"{prefix} · frames detected: {len(allf)}")
        for idx, fr in enumerate(allf):
            try:
                fid = fr.get_attribute("id") or ""
                fname = fr.get_attribute("name") or ""
                fsrc = fr.get_attribute("src") or ""
                log(f"{prefix}    - frame[{idx}] id={fid} name={fname} src={fsrc[:80]}")
            except Exception:
                pass
    except Exception:
        pass

def _has_form_elements_here(driver) -> bool:
    """현재 컨텍스트에서 날짜 입력 또는 엑셀 버튼 존재여부 판별"""
    try:
        cands = driver.find_elements(By.XPATH, "//input[@type='date' or contains(@class,'date') or contains(@id,'date')]")
        cands += driver.find_elements(By.XPATH, "//input[@type='text']")
        for el in cands:
            # 화면에 보이는 후보가 하나라도 있으면 충분
            if el.is_displayed():
                return True
    except Exception:
        pass
    # 다운로드 버튼 후보
    try:
        btn = find_download_button(driver)
        if btn:
            return True
    except Exception:
        pass
    return False

def switch_into_form_frame(driver, max_depth=2) -> bool:
    """
    프레임(iframe/frame) 깊이 2까지 순회하며,
    날짜 입력/다운로드 버튼이 있는 컨텍스트로 전환.
    """
    driver.switch_to.default_content()
    debug_list_frames(driver, prefix="    ")

    # 깊이 0 검사
    if _has_form_elements_here(driver):
        log("    · form context found at root")
        return True

    # 깊이 1
    lvl1 = driver.find_elements(By.TAG_NAME, "iframe") + driver.find_elements(By.TAG_NAME, "frame")
    for i, fr in enumerate(lvl1):
        try:
            driver.switch_to.default_content()
            driver.switch_to.frame(fr)
            if _has_form_elements_here(driver):
                log(f"    · form context found at frame[{i}]")
                return True

            if max_depth >= 2:
                # 깊이 2
                lvl2 = driver.find_elements(By.TAG_NAME, "iframe") + driver.find_elements(By.TAG_NAME, "frame")
                for j, fr2 in enumerate(lvl2):
                    try:
                        driver.switch_to.default_content()
                        driver.switch_to.frame(fr)
                        driver.switch_to.frame(fr2)
                        if _has_form_elements_here(driver):
                            log(f"    · form context found at frame[{i}] -> frame[{j}]")
                            return True
                    except Exception:
                        continue
        except Exception:
            continue

    driver.switch_to.default_content()
    return False

def navigate_to_search_ui(driver):
    """
    홈에서 '실거래/실거래가/아파트(매매)' 와 유사한 링크를 따라가 보며
    프레임 내부 폼 UI로 진입 시도.
    """
    # 1) 텍스트 링크 클릭 시도
    #    (사이트가 바뀌더라도 '실거래' / '실거래가' / '아파트(매매)' 키워드에 걸리도록 범용 XPATH)
    keywords = ["실거래", "실거래가", "아파트(매매)"]
    for kw in keywords:
        try:
            els = driver.find_elements(By.XPATH, f"//a[contains(., '{kw}') or contains(@title,'{kw}')]")
            if not els:
                continue
            driver.execute_script("arguments[0].click();", els[0])
            time.sleep(1)
            if switch_into_form_frame(driver):
                return True
        except Exception:
            pass

    # 2) 프레임만 바뀌는 경우도 있으니 단순 프레임 스캔
    if switch_into_form_frame(driver):
        return True

    # 3) 마지막 시도로 전체 DOM에서 '검색/조회' 버튼 유무만 확인
    try:
        btn = driver.find_element(By.XPATH, "//*[self::button or self::a][contains(.,'검색') or contains(.,'조회')]")
        if btn:
            if switch_into_form_frame(driver):
                return True
    except Exception:
        pass

    return False

# =========================
# 페이지 조작
# =========================
def find_date_inputs(driver):
    cands = driver.find_elements(By.XPATH, "//input[@type='date' or contains(@class,'date') or contains(@id,'date')]")
    cands += driver.find_elements(By.XPATH, "//input[@type='text']")
    # 중복 제거
    uniq = []
    seen = set()
    for el in cands:
        try:
            key = (el.tag_name, el.get_attribute("id"), el.get_attribute("name"), el.get_attribute("class"))
        except Exception:
            continue
        if key in seen:
            continue
        seen.add(key)
        uniq.append(el)
    return uniq

def set_dates(driver, start: date, end: date):
    s = start.isoformat()
    e = end.isoformat()
    log(f"  - set_dates: {s} ~ {e}")

    # 먼저 현재 컨텍스트에 폼이 없으면 프레임 진입/네비게이션 시도
    if not switch_into_form_frame(driver):
        log("    · no form at current page → try navigate_to_search_ui")
        if not navigate_to_search_ui(driver):
            # 그래도 실패면 프레임 목록만 찍고 실패 처리
            debug_list_frames(driver, prefix="    ")
            raise RuntimeError("기간 설정 실패: 날짜 입력 필드를 찾지 못함")

    cands = find_date_inputs(driver)
    log(f"    · input candidates: {len(cands)} (scored)")

    pairs = []
    for i, a in enumerate(cands):
        for j, b in enumerate(cands):
            if i >= j:
                continue
            # 값 초기화
            try:
                driver.execute_script("arguments[0].value=''; arguments[1].value='';", a, b)
            except Exception:
                pass
            try:
                a.clear(); b.clear()
            except Exception:
                pass

            ok = False
            try:
                a.send_keys(s)
                b.send_keys(e)
                va = a.get_attribute("value") or ""
                vb = b.get_attribute("value") or ""
                ok = (va == s and vb == e)
                log(f"    · probe pair → value check: {va or '????'} / {vb or '????'} → {ok}")
                if ok:
                    pairs.append((i, j))
            except Exception:
                # JS 강제 설정 시도
                try:
                    driver.execute_script(
                        "arguments[0].value=arguments[2]; arguments[1].value=arguments[3];",
                        a, b, s, e
                    )
                    va = a.get_attribute("value") or ""
                    vb = b.get_attribute("value") or ""
                    ok = (va == s and vb == e)
                    log(f"    · probe pair(JS) → value check: {va or '????'} / {vb or '????'} → {ok}")
                    if ok:
                        pairs.append((i, j))
                except Exception:
                    pass

    if not pairs:
        # 혹시 루트로 빠졌으면 한 번 더 프레임 스캔
        if not switch_into_form_frame(driver):
            debug_list_frames(driver, prefix="    ")
        raise RuntimeError("기간 설정 실패: 날짜 입력 필드를 찾지 못함")

    si, ei = pairs[0]
    log(f"    · selected pair index: {si},{ei}")

def select_sido(driver, name="서울특별시"):
    try:
        el = WebDriverWait(driver, 3).until(
            EC.presence_of_element_located((By.XPATH, "//select[contains(@name,'sido') or contains(@id,'sido')]"))
        )
        for opt in el.find_elements(By.TAG_NAME, "option"):
            if name in (opt.text or ""):
                opt.click()
                log(f"  - select_sido({name}): True")
                return True
    except Exception:
        pass

    try:
        el = driver.find_element(By.XPATH, f"//*[self::button or self::a or self::label][contains(., '{name}')]")
        driver.execute_script("arguments[0].click();", el)
        log(f"  - select_sido({name}): True")
        return True
    except Exception:
        log(f"  - select_sido({name}): False")
        return False

def find_download_button(driver):
    xpaths = [
        "//*[self::a or self::button][contains(., '엑셀') or contains(., 'Excel') or contains(., '다운')]",
        "//a[contains(@class,'excel') or contains(@href,'excel') or contains(@onclick,'excel')]",
        "//button[contains(@class,'excel') or contains(@onclick,'excel')]",
    ]
    tried = set()
    for xp in xpaths:
        for el in driver.find_elements(By.XPATH, xp):
            try:
                key = (el.tag_name, (el.get_attribute("outerHTML") or "")[:100])
            except Exception:
                key = None
            if key and key in tried:
                continue
            tried.add(key)
            try:
                if el.is_displayed():
                    return el
            except Exception:
                continue
    return None

def click_download(driver, kind="excel") -> bool:
    el = find_download_button(driver)
    if not el:
        return False
    try:
        el.click()
        return True
    except Exception:
        try:
            driver.execute_script("arguments[0].click();", el)
            return True
        except Exception:
            return False

# =========================
# 파싱 / 저장
# =========================
def parse_xlsx(path: Path):
    try:
        df = pd.read_excel(path, engine="openpyxl")
        rows, cols = df.shape
        log(f"  - parsed: rows={rows}  cols={cols}")
        return df
    except Exception as e:
        log(str(e))
        return None

def save_output(df: pd.DataFrame, out_path: Path):
    with pd.ExcelWriter(out_path, engine="openpyxl") as w:
        df.to_excel(w, index=False)

# =========================
# Sheets / Drive (옵션)
# =========================
def load_sa(path: Path):
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        log("  - SA loaded.")
        return data
    except Exception as e:
        log(f"  ! service account load failed: {e}")
        return None

def try_write_sheets(df: pd.DataFrame, title: str, creds_json: dict, sheet_id: str):
    if not (creds_json and sheet_id):
        return
    try:
        import gspread
        from google.oauth2.service_account import Credentials

        scope = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(creds_json, scopes=scope)
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(sheet_id)

        ws_title = title[:100]
        try:
            ws = sh.worksheet(ws_title)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=ws_title, rows=max(1000, len(df)+5), cols=max(20, len(df.columns)+2))

        values = [list(df.columns)] + df.fillna("").astype(str).values.tolist()
        ws.clear()
        ws.update("A1", values)
        log(f"  - sheets ok: {ws_title}")
    except Exception as e:
        log(f"  ! sheets error: {e}")

def try_upload_drive(file_path: Path, creds_json: dict, folder_id: str):
    if not (creds_json and folder_id):
        log("  - skip Drive upload (Artifacts mode).")
        return
    try:
        from googleapiclient.discovery import build
        from google.oauth2.service_account import Credentials
        from googleapiclient.http import MediaFileUpload

        scopes = ["https://www.googleapis.com/auth/drive.file"]
        creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
        service = build("drive", "v3", credentials=creds)

        file_metadata = {"name": file_path.name, "parents": [folder_id]}
        media = MediaFileUpload(str(file_path), mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        r = service.files().create(body=file_metadata, media_body=media, fields="id,name").execute()
        log(f"  - drive ok: {r.get('name')} ({r.get('id')})")
    except Exception as e:
        log(f"  ! drive error: {e}")

# =========================
# 메인 플로우
# =========================
def fetch_and_process(driver, period_name: str, start: date, end: date, out_title: str, creds_json: dict, sheet_id: str, folder_id: str):
    log(f"[{period_name}] {start} ~ {end} → {out_title}")

    # 1) 기간 설정 / (필요 시) 시도 선택
    set_dates(driver, start, end)
    if period_name == "서울":
        select_sido(driver, "서울특별시")

    # 2) 다운로드 클릭(여러번 시도)
    for attempt in range(1, CLICK_RETRY_MAX+1):
        before = list_files(TMP_DL)
        ok_click = click_download(driver, "excel")
        log(f"  - click_download(excel) / attempt {attempt}: {ok_click}")
        if not ok_click:
            time.sleep(WAIT_BETWEEN_CLICK)
            continue

        # 3) 시작 감지
        if not wait_download_start(TMP_DL, before, WAIT_DOWNLOAD_START):
            log(f"  - warn: 다운로드 시작 감지 실패(시도 {attempt}/{CLICK_RETRY_MAX})")
            time.sleep(WAIT_BETWEEN_CLICK)
            continue

        # 4) 완료 감지
        got = wait_download_finish(TMP_DL, WAIT_DOWNLOAD_FINISH)
        if not got:
            log(f"  - warn: 다운로드 완료 대기 초과(시도 {attempt}/{CLICK_RETRY_MAX})")
            time.sleep(WAIT_BETWEEN_CLICK)
            continue

        log(f"  - got file: {got}  size={human_size(got)}  ext={got.suffix}")
        # 5) 파싱/저장
        df = parse_xlsx(got)
        if df is None:
            break

        out_path = OUTPUT_DIR / out_title
        save_output(df, out_path)
        log(f"완료: {out_path}")

        # 6) 드라이브/시트(있으면 시도, 없으면 건너뜀)
        try_upload_drive(out_path, creds_json, folder_id)
        try_write_sheets(df, out_title, creds_json, sheet_id)
        break

def main():
    # 서비스계정/시크릿 로딩(없어도 실행은 진행)
    sa_path = Path(os.environ.get("SA_PATH", "sa.json"))
    creds_json = load_sa(sa_path)
    sheet_id = os.environ.get("SHEET_ID", "").strip() or None
    folder_id = os.environ.get("DRIVE_FOLDER_ID", "").strip() or None

    driver = build_driver(TMP_DL)
    try:
        go_home(driver)

        # 홈에서 검색 UI로 진입 시도(프레임 안착)
        navigate_to_search_ui(driver)  # 실패해도 set_dates 내부에서 다시 시도

        today = date.today()
        y = today.year
        m = today.month
        last_month = (today.replace(day=1) - timedelta(days=1))
        two_months_ago = (last_month.replace(day=1) - timedelta(days=1))

        def month_range(y, m):
            s = date(y, m, 1)
            if m == 12:
                e = date(y, 12, 31)
            else:
                e = date(y, m+1, 1) - timedelta(days=1)
            return s, e

        # 두 달 전 / 한 달 전 / 이번 달(오늘까지)
        s2, e2 = month_range(two_months_ago.year, two_months_ago.month)
        s1, e1 = month_range(last_month.year, last_month.month)
        s0 = today.replace(day=1)
        e0 = today

        stamp = today.strftime("%y%m%d")
        def name_for(prefix, d):
            return f"{prefix} {d.strftime('%y%m')}_{stamp}.xlsx"

        # 전국(두 달 전)
        fetch_and_process(driver, "전국", s2, e2, name_for("전국", s2), creds_json, sheet_id, folder_id)
        # 전국(한 달 전)
        fetch_and_process(driver, "전국", s1, e1, name_for("전국", s1), creds_json, sheet_id, folder_id)
        # 전국(이번 달)
        fetch_and_process(driver, "전국", s0, e0, name_for("전국", s0), creds_json, sheet_id, folder_id)

        # 서울: 직전년도 10/1 ~ 오늘
        start_seoul = (today.replace(day=1) - timedelta(days=1)).replace(month=10, day=1)
        if start_seoul > today:
            start_seoul = start_seoul.replace(year=start_seoul.year - 1)
        fetch_and_process(driver, "서울", start_seoul, e0, f"서울시 {stamp}.xlsx", creds_json, sheet_id, folder_id)

    finally:
        try:
            driver.quit()
        except Exception:
            pass

if __name__ == "__main__":
    main()
