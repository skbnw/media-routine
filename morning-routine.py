import os
import time
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException
from bs4 import BeautifulSoup
import csv

# 実行日と対象日範囲
now = datetime.now()
today = now.date()
start_date = today - timedelta(days=1)
end_date = today + timedelta(days=7)

# 実行タイムスタンプ（ファイル名に使用）
timestamp = now.strftime("_%Y%m%d-%H%M")

# 対象地域（例: 東京）
REGION_NAME = "東京"

# チャンネル種別とURL
channels = {
    "td": "https://bangumi.org/epg/td?broad_cast_date={date}&ggm_group_id=42",
    "bs": "https://bangumi.org/epg/bs?broad_cast_date={date}"
}

# ベース保存ディレクトリ
base_dir = "./files"

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

        for ch_key, url_template in channels.items():
            url = url_template.format(date=date_str)
            html_dir = os.path.join(base_dir, ch_key, "html", target_date.strftime("%Y/%m/%d"))
            csv_dir = os.path.join(base_dir, ch_key, "csv", target_date.strftime("%Y/%m/%d"))
            os.makedirs(html_dir, exist_ok=True)
            os.makedirs(csv_dir, exist_ok=True)

            html_path = os.path.join(html_dir, f"{ch_key}_{date_str}{timestamp}.html")
            csv_path = os.path.join(csv_dir, f"{ch_key}_{date_str}_programs{timestamp}.csv")

            print("=" * 50)
            print(f"[{ch_key.upper()}] {date_str} → アクセス中: {url}")

            success = False
            for attempt in range(3):
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
                    print(f"  ⚠️ エラー（{attempt+1}回目）: {e}")
                    time.sleep(5)

            if not success:
                print(f"  ❌ 最大リトライ失敗: {url}")
                continue

            # CSV抽出処理
            with open(html_path, "r", encoding="utf-8") as file:
                html = file.read()

            soup = BeautifulSoup(html, "html.parser")
            csv_data = [["channel_id", "channel_name", "start_time", "end_time", "program_title", "program_detail", "link", "region"]]

            channels_ul = soup.find_all("li", class_="js_channel topmost")
            channel_names = [tag.text.strip() for tag in channels_ul]

            for j in range(1, 13):
                ul_id = f"program_line_{j}"
                ul_element = soup.find("ul", id=ul_id)
                if ul_element:
                    programs = ul_element.find_all("li")
                    channel_name = channel_names[j-1] if j-1 < len(channel_names) else ""
                    for program in programs:
                        start_time = str(program.get("s", ""))
                        end_time = str(program.get("e", ""))
                        a_tag = program.find("a", class_="title_link")
                        title = detail = link = ""
                        if a_tag:
                            title_element = a_tag.find("p", class_="program_title")
                            detail_element = a_tag.find("p", class_="program_detail")
                            if title_element:
                                title = title_element.text.strip()
                            if detail_element:
                                detail = detail_element.text.strip()
                            link = a_tag.get("href", "")
                        csv_data.append([ul_id, channel_name, start_time, end_time, title, detail, link, REGION_NAME])
                else:
                    print(f"  ℹ️ {ul_id} が見つかりません")

            with open(csv_path, "w", encoding="utf-8-sig", newline="") as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerows(csv_data)

            print(f"  ✅ CSV保存完了: {csv_path}")

finally:
    driver.quit()
    print("✅ 全作業完了")
