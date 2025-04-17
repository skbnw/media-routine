import os
import time
import sqlite3
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

# ベース保存ディレクトリ
base_dir = "./files"
db_dir = "./database"
previous_dir = "./previous_run"
os.makedirs(db_dir, exist_ok=True)
db_path = os.path.join(db_dir, "programs.sqlite")

# エラーログパス
error_log_path = "error_log.txt"
open(error_log_path, "a").close()

# 差分検知ログパス
diff_detected_path = "diff_detected.log"
open(diff_detected_path, "a").close()

# Chromeドライバー設定
options = Options()
options.add_argument('--headless')
options.add_argument('--disable-gpu')
options.add_argument('--no-sandbox')
driver = webdriver.Chrome(options=options)

# SQLiteの初期化
def init_db():
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS programs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id TEXT,
            channel_name TEXT,
            start_time TEXT,
            end_time TEXT,
            program_title TEXT,
            program_detail TEXT,
            link TEXT,
            is_confirmed INTEGER DEFAULT 0,
            is_changed INTEGER DEFAULT 0,
            last_updated TEXT,
            UNIQUE(channel_id, start_time, program_title)
        )
    ''')
    conn.commit()
    conn.close()

# データをSQLiteに保存
def save_to_db(rows):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    now = datetime.now().isoformat()
    for row in rows:
        cur.execute('''
            INSERT OR REPLACE INTO programs (
                channel_id, channel_name, start_time, end_time, 
                program_title, program_detail, link, 
                is_confirmed, is_changed, last_updated
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', row + [0, 0, now])
    conn.commit()
    conn.close()

def format_time(t):
    return f"{t[:4]}-{t[4:6]}-{t[6:8]}_{t[8:10]}{t[10:12]}"

init_db()

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
            diff_path = csv_path.replace(".csv", "_diff.csv")
            prev_csv_path = os.path.join(previous_dir, ch_key, "csv", subfolder, f"{ch_key}_{date_str}_programs.csv")

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

            soup = BeautifulSoup(html, "html.parser")

            # チャンネル名のマッピングを取得
            channel_name_map = {}
            channel_name_elements = soup.select("li.js_channel.topmost > p")
            for idx, p in enumerate(channel_name_elements, start=1):
                ul_id = f"program_line_{idx}"
                channel_name = p.get_text(strip=True)
                channel_name_map[ul_id] = channel_name

            csv_data = [["channel_id", "channel_name", "start_time", "end_time", "program_title", "program_detail", "link"]]
            db_rows = []
            program_count = 0

            for j in range(1, 13):
                ul_id = f"program_line_{j}"
                ul_element = soup.find("ul", id=ul_id)
                if ul_element:
                    programs = ul_element.find_all("li")
                    for program in programs:
                        start_raw = str(program.get("s", ""))
                        end_raw = str(program.get("e", ""))
                        start_time = format_time(start_raw)
                        end_time = format_time(end_raw)

                        a_tag = program.find("a", class_="title_link")
                        channel_id = ul_id
                        channel_name = channel_name_map.get(ul_id, ul_id)
                        title = detail = link = ""

                        if a_tag:
                            title_element = a_tag.find("p", class_="program_title")
                            detail_element = a_tag.find("p", class_="program_detail")
                            if title_element:
                                title = title_element.text.strip()
                            if detail_element:
                                detail = detail_element.text.strip()
                            link = a_tag.get("href", "")

                        row = [channel_id, channel_name, start_time, end_time, title, detail, link]
                        csv_data.append(row)
                        db_rows.append(row)
                        program_count += 1
                else:
                    print(f"  ℹ️ {ul_id} が見つかりません")

            if program_count < 10:
                print(f"  ⚠️ 異常検知: 番組数が少なすぎます（{program_count} 件）")
                with open(error_log_path, "a", encoding="utf-8") as log:
                    log.write(f"{datetime.now()} - {ch_key}_{date_str} - 番組数異常: {program_count}\n")

            # 差分チェック（前回Artifactと比較）
            new_df = pd.DataFrame(csv_data[1:], columns=csv_data[0])
            if os.path.exists(prev_csv_path):
                old_df = pd.read_csv(prev_csv_path)
                merged = pd.merge(new_df, old_df, how='outer', indicator=True)
                diff = merged[merged['_merge'] != 'both']
                if not diff.empty:
                    diff.to_csv(diff_path, index=False, encoding='utf-8-sig')
                    print(f"  🔄 差分検知: {len(diff)}件 → {diff_path} に保存")
                    with open(diff_detected_path, "a", encoding="utf-8") as log:
                        log.write(f"{datetime.now()} - {ch_key}_{date_str} - 差分あり\n")

            # CSV保存
            with open(csv_path, "w", encoding="utf-8-sig", newline="") as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerows(csv_data)

            # SQLite保存
            save_to_db(db_rows)

            print(f"  ✅ CSV保存完了: {csv_path}")
            print(f"  ✅ DB保存完了: {db_path}")
            print("=" * 50)

finally:
    driver.quit()
