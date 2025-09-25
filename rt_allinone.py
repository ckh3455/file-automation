name: molit-daily

on:
  schedule:
    - cron: "5 0 * * *"   # 매일 09:05 KST
  workflow_dispatch:

jobs:
  run:
    runs-on: ubuntu-latest
    timeout-minutes: 30
    env:
      CI: "1"
      PYTHONUNBUFFERED: "1"

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Install Chromium & Chromedriver
        run: |
          set -euxo pipefail
          sudo apt-get update
          sudo apt-get install -y chromium-browser chromium-chromedriver
          echo "CHROME_BIN=/usr/bin/chromium-browser" >> $GITHUB_ENV
          echo "CHROMEDRIVER_BIN=/usr/bin/chromedriver" >> $GITHUB_ENV

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install deps
        run: |
          set -euxo pipefail
          python -m pip install --upgrade pip
          pip install -r requirements.txt --default-timeout=120 -v 2>&1 | tee pip-install.log

      # 서비스계정 JSON을 파일로 저장 (jq 검사 생략: 설치/권한 이슈 피하기)
      - name: Write service account json
        run: |
          set -euxo pipefail
          printf "%s" "${{ secrets.GDRIVE_SA_JSON }}" > sa.json

      - name: Run job
        timeout-minutes: 20
        env:
          TZ: Asia/Seoul
          SA_PATH: ${{ github.workspace }}/sa.json
          SHEET_ID: ${{ secrets.SHEET_ID }}         # 시트 쓰기가 필요 없으면 rt_allinone.py에서 USE_SHEETS 가드로 끄기
          USE_DRIVE: "0"                             # 🔴 Drive 업로드 끔 (Artifacts 모드)
        run: |
          set -euxo pipefail
          python rt_allinone.py 2>&1 | tee runlog.txt

      # ✅ 결과 엑셀을 GitHub Artifacts로 보관 (3일)
      - name: Upload outputs as artifact (keep 3 days)
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: outputs-${{ github.run_id }}
          path: output/*.xlsx
          retention-days: 3
          if-no-files-found: warn

      # (선택) 설치/실행 로그 업로드
      - name: Upload logs
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: run-logs-${{ github.run_id }}
          path: |
            pip-install.log
            runlog.txt
          if-no-files-found: warn
