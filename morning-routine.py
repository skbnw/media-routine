import os
import time
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException
from bs4 import BeautifulSoup
import csv
import pandas as pd

# 実行日と対象日範囲
today = datetime.today()
start_date = today - timedelta(days=1)
end_date = today + timedelta(days=7)

# チャンネル種別とURL
channels = {
    "td": "https://bangumi.org/epg/td?broad_cast_date={date}&ggm_group_id=42",
    "bs": "https://bangumi.org/epg/bs?broad_cast_date={date}"
}

# ディレクトリ設定
base_dir = "./files"
previous_dir = "./previous_run"
os.makedirs(base_dir, exist_ok=True)
os.makedirs(previous_dir, exist_ok=True)

# ログファイル
error_log_path = "error_log.txt"
diff_log_path = "diff_detected.log"
open(error_log_path, "a").close()
open(diff_log_path, "a").close()

# Chromeドライバー設定
options = Options()
options.add_argument('--headless')
options.add_argument('--disable-gpu')
options.add_argument('--no-sandbox')
driver = webdriver.Chrome(options=options)

# フォーマット関数
def format_time(t):
    return f"{t[:4]}-{t[4:6]}-{t[6:8]}_{t[8:10]}{t[10:12]}"

try:
    for i in range((end_date - start_date).days + 1):
        target_date = start_date + timedelta(days=i)
        date_str = target_date.strftime("%Y%m%d")
        subfolder = target_date.strftime("%Y/%m/%d")

        for ch_key, url_template in channels.items():
            url = url_template.format(date=date_str)
            html_dir = os.path.join(base_dir, ch_key, "html", subfolder)
            csv_dir = os.path.join(base_dir, ch_key, "csv", subfolder)
            prev_csv_path = os.path.join(previous_dir, ch_key, "csv", subfolder, f"{ch_key}_{date_str}_programs.csv")

            os.makedirs(html_dir, exist_ok=True)
            os.makedirs(csv_dir, exist_ok=True)

            html_path = os.path.join(html_dir, f"{ch_key}_{date_str}.html")
            csv_path = os.path.join(csv_dir, f"{ch_key}_{date_str}_programs.csv")
            diff_path = csv_path.replace(".csv", "_diff.csv")

            print("=" * 50)
            print(f"[{ch_key.upper()}] {date_str} → アクセス中: {url}")
            print("-" * 50)

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
                    print(f"  ⚠️ エラー（{attempt + 1}回目）: {e}")
                    time.sleep(5)

            if not success:
                with open(error_log_path, "a", encoding="utf-8") as log:
                    log.write(f"{datetime.now()} - {ch_key}_{date_str} - URL: {url} - Failed\n")
                print(f"  ❌ 取得失敗 → ログ記録済み")
                continue

            soup = BeautifulSoup(html, "html.parser")

            # チャンネル名のマッピング
            channel_name_map = {}
            for idx, p in enumerate(soup.select("li.js_channel.topmost > p"), start=1):
                channel_name_map[f"program_line_{idx}"] = p.get_text(strip=True)

            csv_data = [["channel_id", "channel_name", "start_time", "end_time", "program_title", "program_detail", "link"]]
            program_count = 0

            for j in range(1, 13):
                ul_id = f"program_line_{j}"
                ul = soup.find("ul", id=ul_id)
                if ul:
                    for program in ul.find_all("li"):
                        s_raw = program.get("s", "")
                        e_raw = program.get("e", "")
                        start_time = format_time(s_raw)
                        end_time = format_time(e_raw)

                        a_tag = program.find("a", class_="title_link")
                        title = a_tag.find("p", class_="program_title").text.strip() if a_tag else ""
                        detail = a_tag.find("p", class_="program_detail").text.strip() if a_tag else ""
                        link = a_tag.get("href", "") if a_tag else ""

                        channel_id = ul_id
                        channel_name = channel_name_map.get(ul_id, "")
                        csv_data.append([channel_id, channel_name, start_time, end_time, title, detail, link])
                        program_count += 1
                else:
                    print(f"  ℹ️ {ul_id} が見つかりません")

            # 差分チェック
            new_df = pd.DataFrame(csv_data[1:], columns=csv_data[0])
            if os.path.exists(prev_csv_path):
                old_df = pd.read_csv(prev_csv_path)
                merged = pd.merge(new_df, old_df, how='outer', indicator=True)
                diff = merged[merged['_merge'] != 'both']
                if not diff.empty:
                    diff.to_csv(diff_path, index=False, encoding='utf-8-sig')
                    print(f"  🔄 差分検知: {len(diff)}件 → {diff_path}")
                    with open(diff_log_path, "a", encoding="utf-8") as log:
                        log.write(f"{datetime.now()} - {ch_key}_{date_str} - 差分あり\n")

            # CSV保存
            with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
                writer = csv.writer(f)
                writer.writerows(csv_data)

            print(f"  ✅ CSV保存完了: {csv_path}")
            print("=" * 50)

finally:
    driver.quit()
