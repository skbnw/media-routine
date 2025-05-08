import os
import time
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, WebDriverException
from urllib3.exceptions import ReadTimeoutError as Urllib3ReadTimeoutError # エイリアスで明確化
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
options.add_argument('--window-size=1920,1080') # 念のためウィンドウサイズ指定

print("WebDriverを初期化中...")
driver = webdriver.Chrome(options=options)
# driver.implicitly_wait(10) # 暗黙的な待機 (必要に応じて)
print("WebDriverの初期化完了。")

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
            for attempt in range(3): # 最大3回リトライ
                try:
                    # ページ全体の読み込みタイムアウトを設定 (driver.get()自体がこの時間待つ)
                    driver.set_page_load_timeout(60) # 60秒
                    driver.get(url)

                    # 特定の要素が表示されるまで待機 (ページ内の動的コンテンツ読み込みを想定)
                    # 'program_line_1' のIDを持つ要素が表示されるまで最大20秒待機
                    # このIDはページの構造によって変更が必要な場合があります
                    WebDriverWait(driver, 20).until(
                        EC.presence_of_element_located((By.ID, "program_line_1"))
                    )

                    html = driver.page_source
                    with open(html_path, "w", encoding="utf-8-sig") as f:
                        f.write(html)
                    print(f"  ✅ HTML保存完了: {html_path}")
                    success = True
                    break # 成功したらリトライループを抜ける

                except TimeoutException as e_timeout: # Seleniumのページロードタイムアウト or WebDriverWaitのタイムアウト
                    print(f"  ⚠️ Selenium TimeoutException（{attempt+1}/{3}回目）: {url} - {str(e_timeout).splitlines()[0]}")
                    if attempt < 2:
                        print(f"     リトライします... ({10 * (attempt + 1)}秒後)")
                        time.sleep(10 * (attempt + 1)) # リトライ間隔を少しずつ増やす
                    else:
                        print(f"     最大リトライ回数に達しました。")
                except Urllib3ReadTimeoutError as e_readtimeout: # urllib3から直接発生する可能性のある読み取りタイムアウト
                    print(f"  ⚠️ Urllib3 ReadTimeoutError（{attempt+1}/{3}回目）: {url} - {str(e_readtimeout).splitlines()[0]}")
                    if attempt < 2:
                        print(f"     リトライします... ({10 * (attempt + 1)}秒後)")
                        time.sleep(10 * (attempt + 1))
                    else:
                        print(f"     最大リトライ回数に達しました。")
                except WebDriverException as e_wd: # その他のSelenium関連エラー
                    print(f"  ⚠️ WebDriverException（{attempt+1}/{3}回目）: {url} - {str(e_wd).splitlines()[0]}")
                    if attempt < 2:
                        print(f"     リトライします... ({10 * (attempt + 1)}秒後)")
                        time.sleep(10 * (attempt + 1))
                    else:
                        print(f"     最大リトライ回数に達しました。")
                except Exception as e_generic: # 予期しないその他のエラー
                    print(f"  ⚠️ 予期せぬエラー（{attempt+1}/{3}回目）: {url} - {str(e_generic).splitlines()[0]}")
                    # 予期せぬエラーの場合はリトライせずにループを抜けるか、状況に応じて判断
                    # ここではリトライ対象外としておく
                    print(f"     予期せぬエラーのため、このURLの処理を中断します。")
                    success = False # 念のため
                    break # このURLの試行を中断


            if not success:
                print(f"  ❌ HTML取得失敗（最大リトライ超過または予期せぬエラー）: {url}")
                continue # 次のチャンネルまたは日付へ

            # CSV抽出処理 (HTML取得が成功した場合のみ実行)
            print(f"  抽出処理開始: {html_path}")
            with open(html_path, "r", encoding="utf-8") as file:
                html_content_for_soup = file.read()

            soup = BeautifulSoup(html_content_for_soup, "html.parser")
            csv_data = [["channel_id", "channel_name", "start_time", "end_time", "program_title", "program_detail", "link", "region"]]

            channels_ul_tags = soup.find_all("li", class_="js_channel topmost") # 変数名を変更
            channel_names = [tag.text.strip() for tag in channels_ul_tags]

            found_program_data = False
            for j in range(1, 13): #チャンネル数に応じて調整が必要な場合あり
                ul_id = f"program_line_{j}"
                ul_element = soup.find("ul", id=ul_id)
                if ul_element:
                    programs = ul_element.find_all("li")
                    # チャンネル名を取得 (インデックス範囲チェックを強化)
                    current_channel_name = channel_names[j-1] if 0 <= j-1 < len(channel_names) else f"不明なチャンネル {j}"

                    for program in programs:
                        found_program_data = True # 何かしらの番組データが見つかった
                        start_time_val = str(program.get("s", "")) # 変数名を変更
                        end_time_val = str(program.get("e", ""))   # 変数名を変更
                        a_tag = program.find("a", class_="title_link")
                        title = detail = link = ""
                        if a_tag:
                            title_element = a_tag.find("p", class_="program_title")
                            detail_element = a_tag.find("p", class_="program_detail")
                            if title_element:
                                title = title_element.text.strip()
                            if detail_element:
                                detail = detail_element.text.strip()
                            link = "https://bangumi.org" + a_tag.get("href", "") if a_tag.get("href", "").startswith("/") else a_tag.get("href", "")
                        csv_data.append([ul_id, current_channel_name, start_time_val, end_time_val, title, detail, link, REGION_NAME])
                else:
                    # このメッセージは多数表示される可能性があるので、必要に応じてコメントアウト
                    # print(f"  ℹ️ {ul_id} が見つかりません (通常、そのチャンネルの番組がない場合に発生します)")
                    pass # 見つからない場合は何もしない

            if not found_program_data and len(csv_data) == 1: # ヘッダー行のみの場合
                 print(f"  ℹ️ 番組データが見つかりませんでした: {html_path}")

            with open(csv_path, "w", encoding="utf-8-sig", newline="") as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerows(csv_data)
            print(f"  ✅ CSV保存完了: {csv_path}")

finally:
    if 'driver' in locals() and driver is not None:
        print("WebDriverを終了します。")
        driver.quit()
    print("✅ 全作業完了")
