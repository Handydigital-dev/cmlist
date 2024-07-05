import streamlit as st
import openpyxl
import csv
from collections import defaultdict
import re
import io
import pandas as pd
import time
import paramiko
import os
from dotenv import load_dotenv
import mysql.connector

st.set_page_config(layout="wide")

# .envファイルから環境変数を読み込む
load_dotenv()

# 環境変数の取得とバリデーション
MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')
EC2_HOSTNAME = os.getenv('EC2_HOSTNAME')
EC2_USERNAME = os.getenv('EC2_USERNAME')
EC2_PRIVATE_KEY = os.getenv('EC2_PRIVATE_KEY')

# 環境変数のデバッグ出力
st.write("MYSQL_HOST:", MYSQL_HOST)
st.write("MYSQL_USER:", MYSQL_USER)
st.write("MYSQL_PASSWORD:", MYSQL_PASSWORD)
st.write("MYSQL_DATABASE:", MYSQL_DATABASE)
st.write("EC2_HOSTNAME:", EC2_HOSTNAME)
st.write("EC2_USERNAME:", EC2_USERNAME)
st.write("EC2_PRIVATE_KEY:", EC2_PRIVATE_KEY[:10] + "..." if EC2_PRIVATE_KEY else "None")


# 環境変数が設定されていない場合、エラーメッセージを表示して停止
if not all([MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, EC2_HOSTNAME, EC2_USERNAME, EC2_PRIVATE_KEY]):
    st.error("必須の環境変数が設定されていません。")
    st.stop()

# スタイルの追加
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
            next(reader)  # ヘッダーをスキップ
            for row in reader:
                input_category, output_category = row
                correspondence[input_category.strip()] = output_category.strip()
    except Exception as e:
        st.error(f"対応表の読み込みに失敗しました: {str(e)}")
    return correspondence

def parse_input_excel(file_content):
    talent_data = {}
    try:
        wb = openpyxl.load_workbook(io.BytesIO(file_content))
        ws = wb.active
        for row in ws.iter_rows(min_row=2, values_only=True):
            if len(row) >= 4:
                talent_id, talent_name, age, ad_info = row[0], row[1], row[2], row[3]
                talent_data[talent_name] = {'id': talent_id, 'age': age, 'ad_info': ad_info}
    except Exception as e:
        st.error(f"Excelファイルの解析に失敗しました: {str(e)}")
    return talent_data

def validate_excel_file(file_content):
    try:
        wb = openpyxl.load_workbook(io.BytesIO(file_content))
        if 'Sheet1' not in wb.sheetnames:
            st.error("アップロードされたExcelファイルに'Sheet1'がありません。")
            return False
        return True
    except Exception as e:
        st.error(f"Excelファイルの読み込みに失敗しました: {str(e)}")
        return False

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

            # デバッグ情報
            #st.write(f"Processing category: {category}")

            # 対応するカテゴリーを見つける
            output_category = 'その他'
            for input_cat, output_cat in correspondence.items():
                if input_cat == category:  # 完全一致でチェック
                    output_category = output_cat
                    break

            # デバッグ情報
            #st.write(f"Matched to output category: {output_category}")

            current_category = output_category

            if 'あり' in status:
                process_status(status, current_category, categories)
        elif current_category and 'あり' in line:
            # 前の行の続きの場合
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
            # 『』がない場合はclient_infoをそのまま使用
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
    ws.append(['タレント名', '年齢'] + output_categories)

    for talent_name, talent_info in talent_data.items():
        row = [talent_name, talent_info['age']]
        ad_categories = categorize_ads(talent_info['ad_info'], correspondence)
        
        for category in output_categories:
            cell_content = '\n'.join(ad_categories[category])
            # \r と \t を空白に置換
            cell_content = cell_content.replace('\\r', ' ').replace('\\t', ' ')
            row.append(cell_content)
        
        ws.append(row)

    # セルの書式設定を調整
    for row in ws.iter_rows():
        for cell in row:
            cell.alignment = openpyxl.styles.Alignment(wrapText=True, vertical='top')
    
    # 列幅の自動調整
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
        # MySQLの設定ファイルを作成するコマンド
        create_config_command = (
            f"echo '[client]\nuser={MYSQL_USER}\npassword={MYSQL_PASSWORD}\nhost={MYSQL_HOST}' > ~/.my.cnf && chmod 600 ~/.my.cnf"
        )
        # 設定ファイルを作成
        ssh_client.exec_command(create_config_command)
        stdin, stdout, stderr = ssh_client.exec_command(f"mysql --defaults-file=~/.my.cnf {MYSQL_DATABASE} -e \"{mysql_command}\"")
        
        result = stdout.read().decode('utf-8')
        error = stderr.read().decode('utf-8')

        ssh_client.exec_command("rm ~/.my.cnf")

        if error and "Warning" not in error:  # 警告以外のエラーがある場合
            st.error(f"MySQLコマンド実行エラー: {error}")
            return None
        return result
    except Exception as e:
        st.error(f"MySQLコマンドの実行に失敗しました: {str(e)}")
        return None

def connect_to_ec2_and_execute_query():
    try:
        # 秘密鍵の一時ファイルを作成
        private_key_path = "/tmp/temp_key.pem"
        with open(private_key_path, "w") as key_file:
            key_file.write(EC2_PRIVATE_KEY)

        os.chmod(private_key_path, 0o600)

        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            ssh_client.connect(hostname=EC2_HOSTNAME, username=EC2_USERNAME, key_filename=private_key_path)
            st.write("EC2サーバーに接続しました。")
        except Exception as e:
            st.error(f"EC2サーバーへの接続に失敗しました: {str(e)}")
            return None

        # MySQLクエリの構築
        mysql_query = """
        SELECT 
            id,
            name,
            COALESCE(
               (YEAR(CURDATE()) - born_date_yyyy) -
               (DATE_FORMAT(CURDATE(), '%m%d') < CONCAT(LPAD(born_date_mm, 2, '0'), LPAD(born_date_dd, 2, '0'))),
                ''
            ) AS age,
            memo_cm 
        FROM talents 
        WHERE deleted IS NULL 
            AND modified >= '2023-01-01 00:00:00'
        ORDER BY
            total_score DESC,
            instagram_follower_count DESC,
            twitter_follower_count DESC,
            youtube_subscriber_count DESC,
            tiktok_follower_count DESC
            LIMIT 100;
        """

        # MySQLコマンドの実行
        st.write("クエリを実行しています...")
        result = execute_mysql_command(ssh_client, mysql_query)
        
        if result is None:
            st.error("クエリ結果が空です。")
            return None

        # 結果の処理
        lines = result.strip().split('\n')
        headers = lines[0].split('\t')
        data = [line.split('\t') for line in lines[1:]]
        
        df = pd.DataFrame(data, columns=headers)
        st.write(f"取得したレコード数: {len(df)}")

        # raw データの表示（デバッグ用）
        st.write("Raw データ:")
        st.write(df)

        # データの処理と形式の変換
        # なぜうまくいったかわからないが、
        talent_data = {}
        for _, row in df.iterrows():
            ad_info = row['memo_cm']
            # 改行を \r\nに変換するとうまくいかないが下記だとうまくいく　理由不明
            ad_info = ad_info.replace('\\n', '\\r\n')

            talent_data[row['name']] = {
                'id': row['id'],
                'age': row['age'],
                'ad_info': ad_info
            }

        # 処理後のデータのサンプルを表示（デバッグ用）
        # st.write("処理後のデータサンプル:")
        # for name, info in list(talent_data.items())[:2]:
        #     st.write(f"Name: {name}")
        #     st.write(f"Age: {info['age']}")
        #     st.write(f"Ad Info:\n{info['ad_info']}")
        #     st.write("---")

        return talent_data

    except Exception as e:
        st.error(f"予期せぬエラーが発生しました: {str(e)}")
        return None
    finally:
        if ssh_client:
            ssh_client.close()
            st.write("EC2サーバーとの接続を閉じました。")
        # 秘密鍵の一時ファイルを削除
        if os.path.exists(private_key_path):
            os.remove(private_key_path)

st.title('広告ジャンル分類処理アプリケーション')

correspondence = load_correspondence_table()

# correspondenceテーブルの内容を表示（デバッグ用）
#st.write("Correspondence Table:")
#st.write(correspondence)

# セッション状態の初期化
if 'processed_data' not in st.session_state:
    st.session_state.processed_data = None
if 'raw_talent_data' not in st.session_state:
    st.session_state.raw_talent_data = None

# データソースの選択
data_source = st.radio(
    "データソースを選択してください",
    ('RDS MySQL', 'Excelファイル')
)

if data_source == 'Excelファイル':
    st.subheader('📁 base_input.xlsxをアップロード')
    input_file = st.file_uploader("base_input.xlsxを選択してください", type="xlsx")
else:
    st.subheader('🌐 RDS MySQLからデータを取得')
    input_file = None  # MySQLを使用する場合はinput_fileをNoneに設定

if (input_file is not None or data_source == 'RDS MySQL') and st.session_state.processed_data is None:
    if st.button('🚀 処理開始', key='process_button'):
        progress_bar = st.progress(0)
        status_text = st.empty()

        # 処理開始
        status_text.text('処理を開始しています...')
        progress_bar.progress(10)
        time.sleep(0.5)

        if data_source == 'Excelファイル':
            if validate_excel_file(input_file.read()):
                st.session_state.raw_talent_data = parse_input_excel(input_file.read())
        else:
            st.session_state.raw_talent_data = connect_to_ec2_and_execute_query()

        if st.session_state.raw_talent_data is None:
            st.error("データの取得に失敗しました。")
            status_text.empty()
            progress_bar.empty()
            st.stop()

        status_text.text('データを解析しています...')
        progress_bar.progress(40)
        time.sleep(0.5)

        # データのサンプルを表示（デバッグ用）
        # st.write("取得したデータのサンプル:")
        # for name, info in list(st.session_state.raw_talent_data.items())[:5]:
        #     st.write(f"Name: {name}")
        #     st.write(f"Age: {info['age']}")
        #     st.write(f"Ad Info: {info['ad_info']}")
        #     st.write("---")

        output = generate_output_excel(st.session_state.raw_talent_data, correspondence)
        status_text.text('結果を生成しています...')
        progress_bar.progress(70)
        time.sleep(0.5)

        # 結果の表示
        df = pd.read_excel(output)
        st.session_state.processed_data = df
        status_text.text('処理が完了しました。結果を表示しています...')
        progress_bar.progress(100)
        time.sleep(0.5)

        status_text.empty()
        progress_bar.empty()

if st.session_state.processed_data is not None:
    st.subheader('📊 処理結果')
    
    # ページネーション機能の追加
    df = st.session_state.processed_data
    page_size = 50
    page_number = st.number_input('ページ番号', min_value=1, max_value=len(df)//page_size + 1, value=1)
    start_idx = (page_number - 1) * page_size
    end_idx = start_idx + page_size

    st.write(f"全 {len(df)} 件中 {start_idx+1} - {min(end_idx, len(df))} 件を表示")
    st.dataframe(df.iloc[start_idx:end_idx], height=400)

# カテゴリー選択
    st.subheader('📥 結果のダウンロード')
    categories = df.columns[2:].tolist()  # タレント名と年齢を除外
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
            label="🔽 選択したカテゴリーの結果をダウンロード",
            data=filtered_output,
            file_name="processed_output.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

st.sidebar.title('📌 アプリケーション情報')
st.sidebar.info('このアプリケーションは、タレントの広告出演情報を分類し、エクセルファイルとして出力します。')
st.sidebar.warning('注意: 大きなファイルの処理には時間がかかる場合があります。')
