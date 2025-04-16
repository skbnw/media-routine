import os
import time
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException
from bs4 import BeautifulSoup
import csv

# 実行日と対象日範囲
today = datetime.today()
start_date = today - timedelta(days=7)
end_date = today + timedelta(days=7)

# チャンネル種別とURL
channels = {
    "td": "https://bangumi.org/epg/td?broad_cast_date={date}&ggm_group_id=42",
    "bs": "https://bangumi.org/epg/bs?broad_cast_date={date}"
}

# ベース保存ディレクトリ
base_dir = "./files"

# エラーログパス
error_log_path = "error_log.txt"
open(error_log_path, "a").close()  # エラー用ログファイルが存在しない場合に備えて作成

# Chromeドライバー設定
options = Options()
options.add_argument('--headless')
options.add_argument('--disable-gpu')
options.add_argument('--no-sandbox')
driver = webdriver.Chrome(options=options)

try:
    for i in range((end_date - start_date).days + 1):
        target_date = start_date + timedelta(days=i)
        date_str = target_date.strftime("%Y%m%d")
        subfolder = target_date.strftime("%Y/%m/%d")

        for ch_key, url_template in channels.items():
            url = url_template.format(date=date_str)
            html_dir = os.path.join(base_dir, ch_key, "html", subfolder)
            csv_dir = os.path.join(base_dir, ch_key, "csv", subfolder)
            os.makedirs(html_dir, exist_ok=True)
            os.makedirs(csv_dir, exist_ok=True)

            html_path = os.path.join(html_dir, f"{ch_key}_{date_str}.html")
            csv_path = os.path.join(csv_dir, f"{ch_key}_{date_str}_programs.csv")

            print("=" * 50)
            print(f"[{ch_key.upper()}] {date_str} → アクセス中: {url}")
            print("-" * 50)

            success = False
            for attempt in range(1, 4):
                try:
                    driver.get(url)
                    time.sleep(10)
                    html = driver.page_source

                    with open(html_path, "w", encoding="utf-8-sig") as f:
                        f.write(html)

                    print(f"  ✅ HTML保存完了: {html_path}")
                    success = True
                    break
                except WebDriverException as e:
                    print(f"  ⚠️ エラー（{attempt}回目）: {e}")
                    time.sleep(5)

            if not success:
                with open(error_log_path, "a", encoding="utf-8") as log:
                    log.write(f"{datetime.now()} - {ch_key}_{date_str} - URL: {url} - Failed\n")
                print(f"  ❌ 取得失敗 → ログ記録済み: {error_log_path}")
                continue

            # CSV抽出処理
            with open(html_path, "r", encoding="utf-8") as file:
                html = file.read()

            soup = BeautifulSoup(html, "html.parser")
            csv_data = [["channel", "start_time", "end_time", "program_title", "program_detail", "link"]]

            for j in range(1, 13):
                ul_id = f"program_line_{j}"
                ul_element = soup.find("ul", id=ul_id)
                if ul_element:
                    programs = ul_element.find_all("li")
                    for program in programs:
                        start_time = str(program.get("s", ""))
                        end_time = str(program.get("e", ""))
                        a_tag = program.find("a", class_="title_link")
                        channel = ul_id
                        title = detail = link = ""

                        if a_tag:
                            title_element = a_tag.find("p", class_="program_title")
                            detail_element = a_tag.find("p", class_="program_detail")
                            if title_element:
                                title = title_element.text.strip()
                            if detail_element:
                                detail = detail_element.text.strip()
                            link = a_tag.get("href", "")

                        csv_data.append([channel, start_time, end_time, title, detail, link])
                else:
                    print(f"  ℹ️ {ul_id} が見つかりません")

            with open(csv_path, "w", encoding="utf-8-sig", newline="") as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerows(csv_data)

            print(f"  ✅ CSV保存完了: {csv_path}")
            print("=" * 50)

finally:
    driver.quit()
