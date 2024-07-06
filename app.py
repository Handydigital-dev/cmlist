import streamlit as st
import csv
from collections import defaultdict
import re
import io
import pandas as pd
import time
import paramiko
import os
from dotenv import load_dotenv
import openpyxl
from datetime import datetime

st.set_page_config(layout="wide", page_title="AICS競合リスト作成ツール")

# Load environment variables from .env file
load_dotenv()

# Get and validate environment variables
MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')
EC2_HOSTNAME = os.getenv('EC2_HOSTNAME')
EC2_USERNAME = os.getenv('EC2_USERNAME')
EC2_PRIVATE_KEY = os.getenv('EC2_PRIVATE_KEY')

# Check if all required environment variables are set
if not all([MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, EC2_HOSTNAME, EC2_USERNAME, EC2_PRIVATE_KEY]):
    st.error("必須の環境変数が設定されていません。")
    st.stop()

# Add styles
st.markdown("""
<style>
    .stButton>button {
        width: 100%;
        height: 3em;
        margin-top: 1em;
    }
    .stProgress > div > div > div > div {
        background-color: #4CAF50;
    }
    .result-table {
        font-size: 14px;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data
def load_correspondence_table():
    correspondence = {}
    try:
        with open('correspondenceTable.csv', 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header
            for row in reader:
                input_category, output_category = row
                correspondence[input_category.strip()] = output_category.strip()
    except Exception as e:
        st.error(f"対応表の読み込みに失敗しました: {str(e)}")
    return correspondence

def categorize_ads(ad_info, correspondence):
    categories = defaultdict(list)
    if not isinstance(ad_info, str):
        return categories

    lines = ad_info.replace('\r\n', '\n').split('\n')
    current_category = None
    
    for line in lines:
        line = line.strip()
        if '：' in line:
            category, status = line.split('：', 1)
            category = category.strip()
            status = status.strip()

            output_category = 'その他'
            for input_cat, output_cat in correspondence.items():
                if input_cat == category:
                    output_category = output_cat
                    break

            current_category = output_category

            if 'あり' in status:
                process_status(status, current_category, categories)
        elif current_category and 'あり' in line:
            process_status(line, current_category, categories)

    return categories

def process_status(status, category, categories):
    client_info = status.split('あり', 1)[1].strip()
    if client_info:
        matches = re.findall(r'(.+?)『(.+?)』', client_info)
        if matches:
            for brand, product in matches:
                formatted_info = f"{brand.strip()}『{product.strip()}』"
                categories[category].append(formatted_info)
        else:
            categories[category].append(client_info)

def generate_output_excel(talent_data, correspondence, selected_categories=None):
    output_categories = [
        '飲料・アルコール', '食品・菓子・外食', '小売・コンビニ', '化粧品・美容・ヘアケア',
        'アパレル・アクセサリ', '医薬品・医薬部外品・健康食品', 'メガネ・コンタクト',
        'バス・トイレタリー・生活用品', '家電・電子機器', 'ゲーム・おもちゃ・楽器',
        'レジャー・エンタメ・ギャンブル', '不動産・住宅関連', '自動車・電車・航空',
        '金融・保険（決済・クレジットカード含む）', 'その他金融関連（ポイント訴求・公営ギャンブル含む）',
        '教育', '人材派遣・求人', '運輸・運送', '通信', 'エネルギー・公共インフラ',
        '介護・福祉', '官公庁・団体', 'その他'
    ]

    if selected_categories:
        output_categories = [cat for cat in output_categories if cat in selected_categories]

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(['タレント名', '年齢', '性別', '個人/グループ'] + output_categories + ['事務所URL'])

    for talent_name, talent_info in talent_data.items():
        row = [talent_name, talent_info['age'], talent_info['gender'], talent_info['is_group']]
        ad_categories = categorize_ads(talent_info['ad_info'], correspondence)
        
        for category in output_categories:
            cell_content = '\n'.join(ad_categories[category])
            cell_content = cell_content.replace('\\r', ' ').replace('\\t', ' ')
            row.append(cell_content)
        
        row.append(talent_info['agency_url'])
        ws.append(row)

    for row in ws.iter_rows():
        for cell in row:
            cell.alignment = openpyxl.styles.Alignment(wrapText=True, vertical='top')
    
    for column in ws.columns:
        max_length = 0
        column_letter = column[0].column_letter
        for cell in column:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(cell.value)
            except:
                pass
        adjusted_width = (max_length + 2) * 1.2
        ws.column_dimensions[column_letter].width = adjusted_width

    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    return output

def execute_mysql_command(ssh_client, mysql_command):
    try:
        create_config_command = (
            f"echo '[client]\nuser={MYSQL_USER}\npassword={MYSQL_PASSWORD}\nhost={MYSQL_HOST}' > ~/.my.cnf && chmod 600 ~/.my.cnf"
        )
        ssh_client.exec_command(create_config_command)
        stdin, stdout, stderr = ssh_client.exec_command(f"mysql --defaults-file=~/.my.cnf {MYSQL_DATABASE} -e \"{mysql_command}\"")
        
        result = stdout.read().decode('utf-8')
        error = stderr.read().decode('utf-8')

        ssh_client.exec_command("rm ~/.my.cnf")

        if error and "Warning" not in error:
            st.error(f"MySQLコマンド実行エラー: {error}")
            return None
        return result
    except Exception as e:
        st.error(f"MySQLコマンドの実行に失敗しました: {str(e)}")
        return None

def connect_to_ec2_and_execute_query(selected_types, selected_genders, start_date, row_limit, talent_names=None):
    try:
        private_key_path = "/tmp/temp_key.pem"
        with open(private_key_path, "w") as key_file:
            key_file.write(EC2_PRIVATE_KEY)

        os.chmod(private_key_path, 0o600)

        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            ssh_client.connect(hostname=EC2_HOSTNAME, username=EC2_USERNAME, key_filename=private_key_path)
            st.sidebar.success("EC2サーバーに接続しました。")
        except Exception as e:
            st.sidebar.error(f"EC2サーバーへの接続に失敗しました: {str(e)}")
            return None

        type_condition = "AND is_group IN ({})".format(", ".join(selected_types)) if selected_types else ""
        gender_condition = "AND gender_cd IN ({})".format(", ".join(selected_genders)) if selected_genders else ""
        
        if talent_names:
            name_condition = "AND name IN ({})".format(", ".join(f"'{name}'" for name in talent_names))
        else:
            name_condition = ""

        mysql_query = f"""
        SELECT 
            id,
            name,
            COALESCE(
               (YEAR(CURDATE()) - born_date_yyyy) -
               (DATE_FORMAT(CURDATE(), '%m%d') < CONCAT(LPAD(born_date_mm, 2, '0'), LPAD(born_date_dd, 2, '0'))),
                ''
            ) AS age,
            CASE
                WHEN is_group = 0 THEN
                    CASE
                        WHEN gender_cd = 1 THEN '男性'
                        WHEN gender_cd = 2 THEN '女性'
                        WHEN gender_cd = 3 THEN 'その他'
                        ELSE '不明'
                    END
                ELSE
                    CASE
                        WHEN gender_cd = 1 THEN '男性のみ'
                        WHEN gender_cd = 2 THEN '女性のみ'
                        WHEN gender_cd = 3 THEN '混成'
                        ELSE '不明'
                    END
            END AS gender,
            CASE
                WHEN is_group = 0 THEN '個人'
                ELSE 'グループ'
            END AS is_group,
            memo_cm,
            other_blog_url
        FROM talents 
        WHERE deleted IS NULL 
            AND modified >= '{start_date}'
            {type_condition}
            {gender_condition}
            {name_condition}
        ORDER BY
            total_score DESC,
            instagram_follower_count DESC,
            twitter_follower_count DESC,
            youtube_subscriber_count DESC,
            tiktok_follower_count DESC
            LIMIT {row_limit};
        """

        st.sidebar.info("クエリを実行しています...")
        result = execute_mysql_command(ssh_client, mysql_query)
        
        if result is None:
            st.sidebar.error("クエリ結果が空です。")
            return None

        lines = result.strip().split('\n')
        headers = lines[0].split('\t')
        data = [line.split('\t') for line in lines[1:]]

        df = pd.DataFrame(data, columns=headers)
        st.sidebar.success(f"取得したレコード数: {len(df)}")

        talent_data = {}
        for _, row in df.iterrows():
            ad_info = row['memo_cm']
            ad_info = ad_info.replace('\\n', '\\r\n')

            talent_data[row['name']] = {
                'id': row['id'],
                'age': row['age'],
                'gender': row['gender'],
                'is_group': row['is_group'],
                'ad_info': ad_info,
                'agency_url': row['other_blog_url']
            }

        return talent_data

    except Exception as e:
        st.sidebar.error(f"予期せぬエラーが発生しました: {str(e)}")
        return None
    finally:
        if ssh_client:
            ssh_client.close()
            st.sidebar.info("EC2サーバーとの接続を閉じました。")
        if os.path.exists(private_key_path):
            os.remove(private_key_path)

st.title('AICS競合リスト作成ツール')

st.info('このアプリケーションは、タレントの広告出演情報を分類し、エクセルファイルとして出力します。')
st.info('サイドバーに条件を入力して検索ボタンを押してください。条件検索とタレント検索は独立した検索になります。')
st.warning('注意: 大きなデータセットの処理には時間がかかる場合があります。')

correspondence = load_correspondence_table()

if 'processed_data' not in st.session_state:
    st.session_state.processed_data = None
if 'raw_talent_data' not in st.session_state:
    st.session_state.raw_talent_data = None

st.sidebar.subheader('🔍 AICSから条件で検索')

# 個人/グループ選択
type_options = {
    '0': '個人',
    '1': 'グループ'
}
selected_types = st.sidebar.multiselect('個人/グループを選択してください', options=list(type_options.keys()), format_func=lambda x: type_options[x])

# 性別選択
gender_options = {
    '1': '男性' if '0' in selected_types else '男性のみ',
    '2': '女性' if '0' in selected_types else '女性のみ',
    '3': 'その他' if '0' in selected_types else '混成'
}

if '0' in selected_types and '1' in selected_types:
    gender_options = {
        '1': '男性/男性のみ',
        '2': '女性/女性のみ',
        '3': 'その他/混成'
    }

selected_genders = st.sidebar.multiselect('性別を選択してください', options=list(gender_options.keys()), format_func=lambda x: gender_options[x])

# 日付選択
start_date = st.sidebar.date_input('最終編集日を選択してください', value=datetime(2023, 1, 1))

# 行数リミット選択
row_limit = st.sidebar.number_input('取得する行数を入力してください', min_value=1, max_value=10000, value=1000, step=100)

if st.sidebar.button('🔍 条件検索', key='condition_search_button'):
    progress_bar = st.sidebar.progress(0)
    status_text = st.sidebar.empty()

    status_text.text('処理を開始しています...')
    progress_bar.progress(10)
    time.sleep(0.5)

    st.session_state.raw_talent_data = connect_to_ec2_and_execute_query(selected_types, selected_genders, start_date, row_limit)

    if st.session_state.raw_talent_data is None:
        st.sidebar.error("データの取得に失敗しました。")
        status_text.empty()
        progress_bar.empty()
    else:
        status_text.text('データを解析しています...')
        progress_bar.progress(40)
        time.sleep(0.5)

        output = generate_output_excel(st.session_state.raw_talent_data, correspondence)
        status_text.text('結果を生成しています...')
        progress_bar.progress(70)
        time.sleep(0.5)

        df = pd.read_excel(output)
        st.session_state.processed_data = df
        status_text.text('処理が完了しました。結果を表示しています...')
        progress_bar.progress(100)
        time.sleep(0.5)

        status_text.empty()
        progress_bar.empty()

# タレント名直接指定フォーム
st.sidebar.subheader('🎭 タレント名で直接検索')
talent_names_input = st.sidebar.text_area("タレント名を入力してください（1行に1人）", 
                                          height=150,
                                          help="例:\nサンドウィッチマン\n大泉洋\n阿部寛\n堺雅人\nムロツヨシ\n福山雅治")

if st.sidebar.button('🔍 タレント名で検索', key='talent_search_button'):
    if talent_names_input:
        talent_names = [name.strip() for name in talent_names_input.split('\n') if name.strip()]
        if talent_names:
            progress_bar = st.sidebar.progress(0)
            status_text = st.sidebar.empty()

            status_text.text('タレント名で検索しています...')
            progress_bar.progress(10)
            time.sleep(0.5)

            st.session_state.raw_talent_data = connect_to_ec2_and_execute_query(
                [], [], datetime(2000, 1, 1), len(talent_names), talent_names)

            if st.session_state.raw_talent_data is None:
                st.sidebar.error("データの取得に失敗しました。")
                status_text.empty()
                progress_bar.empty()
            else:
                status_text.text('データを解析しています...')
                progress_bar.progress(40)
                time.sleep(0.5)

                output = generate_output_excel(st.session_state.raw_talent_data, correspondence)
                status_text.text('結果を生成しています...')
                progress_bar.progress(70)
                time.sleep(0.5)

                df = pd.read_excel(output)
                st.session_state.processed_data = df
                status_text.text('処理が完了しました。結果を表示しています...')
                progress_bar.progress(100)
                time.sleep(0.5)

                status_text.empty()
                progress_bar.empty()
        else:
            st.sidebar.warning("有効なタレント名が入力されていません。")
    else:
        st.sidebar.warning("タレント名を入力してください。")

# 検索結果の表示
if st.session_state.processed_data is not None:
    st.subheader('📊 検索結果')
    
    df = st.session_state.processed_data
    page_size = 50
    page_number = st.number_input('ページ番号', min_value=1, max_value=len(df)//page_size + 1, value=1)
    start_idx = (page_number - 1) * page_size
    end_idx = start_idx + page_size

    st.write(f"全 {len(df)} 件中 {start_idx+1} - {min(end_idx, len(df))} 件を表示")
    st.dataframe(df.iloc[start_idx:end_idx], height=400)

    st.subheader('📥 検索結果のダウンロード')
    categories = df.columns[4:-1].tolist()  # Exclude 'タレント名', '年齢', '性別', '個人/グループ', and '事務所URL'
    selected_categories = st.multiselect('ダウンロードするカテゴリーを選択してください', categories, default=categories)

    if selected_categories:
        with st.spinner('選択されたカテゴリーのデータを準備中...'):
            filtered_output = generate_output_excel(
                st.session_state.raw_talent_data,
                correspondence,
                selected_categories
            )
        st.success('データの準備が完了しました。ダウンロードボタンが利用可能です。')
        st.download_button(
            label="🔽 検索結果をダウンロード",
            data=filtered_output,
            file_name="search_output.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )