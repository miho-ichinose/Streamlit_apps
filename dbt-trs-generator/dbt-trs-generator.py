import streamlit as st
import pandas as pd
import re
from io import BytesIO

def convert_excel_type_to_snowflake(excel_type):
    """Excel設計書の型をSnowflakeの型に変換"""
    type_mapping = {
        '文字列': 'varchar',
        'VARCHAR': 'varchar',
        'CHAR': 'varchar',
        'TEXT': 'varchar',
        '数値': 'number',
        'NUMBER': 'number',
        'INT': 'number',
        'INTEGER': 'number',
        'DECIMAL': 'number',
        'NUMERIC': 'number',
        'FLOAT': 'float',
        'DOUBLE': 'double',
        '日付': 'date',
        'DATE': 'date',
        '日時': 'timestamp',
        'DATETIME': 'timestamp',
        'TIMESTAMP': 'timestamp',
        '時刻': 'time',
        'TIME': 'time',
        'BOOLEAN': 'boolean',
        'BOOL': 'boolean'
    }
    
    if not excel_type:
        return 'varchar(100)'
    
    excel_type_upper = str(excel_type).upper().strip()
    
    # 括弧付きの型を処理（例：VARCHAR(100)）
    match = re.match(r'([A-Z]+)\((\d+)\)', excel_type_upper)
    if match:
        base_type = match.group(1)
        size = match.group(2)
        if base_type in ['VARCHAR', 'CHAR']:
            return f'varchar({size})'
        elif base_type in ['NUMBER', 'DECIMAL', 'NUMERIC']:
            return f'number({size})'
    
    # 日本語の型名を処理
    for key, value in type_mapping.items():
        if key in excel_type_upper or excel_type_upper in key:
            if value == 'varchar' and '(' not in excel_type:
                return 'varchar(100)'  # デフォルトサイズ
            return value
    
    return 'varchar(100)'  # デフォルト

def generate_dbt_sql_with_mapping(df, source_schema, source_table, model_name, model_description,
                                 physical_col, type_col, logical_col=None, desc_col=None, 
                                 include_comments=True, selected_rows=None):
    """マッピング設定に基づいてdbt SQLを生成"""
    
    if not physical_col or not type_col:
        st.error("物理名とデータ型のカラムを選択してください。")
        return None, None
    
    # 使用するカラムを選択
    cols_to_use = [physical_col, type_col]
    if logical_col and logical_col != '未選択':
        cols_to_use.append(logical_col)
    if desc_col and desc_col != '未選択':
        cols_to_use.append(desc_col)
    
    # 選択された行のみをフィルタリング
    if selected_rows is not None:
        # selected_rowsの長さをdfに合わせる
        selected_mask = selected_rows[:len(df)] + [False] * max(0, len(df) - len(selected_rows))
        selected_mask = selected_mask[:len(df)]
        df_filtered = df[selected_mask].copy()
    else:
        df_filtered = df.copy()
    
    # NaNを除外してデータを取得
    df_clean = df_filtered[cols_to_use].dropna(subset=[physical_col])
    
    sql = f"""with
    -- 取り込みエラー回避のための最低限の処理を行う　※''がエラーとなる型で取り込む場合は、nullに変換する
    source_data as (
        select"""
    
    # カラムの処理
    columns = []
    col_index = 1  # c1から開始
    for idx, row in df_clean.iterrows():
        col_name = str(row[physical_col]).strip()
        data_type = str(row[type_col]).strip() if pd.notna(row[type_col]) else 'varchar(100)'
        
        if not col_name or col_name == 'nan':
            continue
            
        snowflake_type = convert_excel_type_to_snowflake(data_type)
        
        # 数値型と日付型の場合はnullif処理を追加
        if any(t in snowflake_type.lower() for t in ['number', 'int', 'decimal', 'float', 'double', 'date', 'timestamp']):
            column_def = f"            nullif(value:c{col_index}, '')::{snowflake_type} as {col_name}"
        else:
            column_def = f"            value:c{col_index}::{snowflake_type} as {col_name}"
        
        columns.append(column_def)
        col_index += 1  # 次のカラムインデックス
    
    sql += "\n" + ",\n".join(columns)
    sql += f"""
        from {{{{ source("{source_schema}", "{source_table}") }}}}
    )

-- 重複レコードや欠損値、揺らぎの矯正、コード体系の統一等を行う
select"""
    
    # SELECT句のカラムリスト
    select_columns = []
    for idx, row in df_clean.iterrows():
        col_name = str(row[physical_col]).strip()
        if col_name and col_name != 'nan':
            # 特定のカラムに対する変換ロジックの例（genderの場合）
            if 'gender' in col_name.lower() or '性別' in col_name:
                select_columns.append(f"""    case
        when {col_name} is null
        then 0
        when {col_name} = '男'
        then 1
        when {col_name} = '女'
        then 2
        else 9
    end as {col_name}""")
            else:
                select_columns.append(f"    {col_name}")
    
    sql += "\n" + ",\n".join(select_columns)
    sql += "\nfrom source_data"
    
    # models.yml用のスキーマ定義を生成
    schema_yml = None
    if include_comments and (logical_col and logical_col != '未選択' or desc_col and desc_col != '未選択'):
        yml_lines = []
        yml_lines.append(f"  - name: {model_name}")
        
        # モデルの説明を追加
        if model_description and model_description.strip():
            # 改行を削除して1行にする
            clean_description = model_description.strip().replace('\n', ' ').replace('\r', ' ')
            clean_description = ' '.join(clean_description.split())
            clean_description = clean_description.replace('"', '\\"')
            yml_lines.append(f'    description: "{clean_description}"')
        else:
            yml_lines.append(f'    description: "{model_name}のテーブル定義"')
        
        yml_lines.append("    columns:")
        
        for idx, row in df_clean.iterrows():
            col_name = str(row[physical_col]).strip()
            if col_name and col_name != 'nan':
                yml_lines.append(f"      - name: {col_name}")
                
                # 説明を構築
                description_parts = []
                
                # 論理名/項目名を追加
                if logical_col and logical_col != '未選択' and logical_col in row.index and pd.notna(row[logical_col]):
                    logical_name = str(row[logical_col]).strip()
                    if logical_name and logical_name != 'nan':
                        description_parts.append(logical_name)
                
                # 説明を追加
                if desc_col and desc_col != '未選択' and desc_col in row.index and pd.notna(row[desc_col]):
                    description = str(row[desc_col]).strip()
                    if description and description != 'nan':
                        description_parts.append(description)
                
                if description_parts:
                    # YAMLのダブルクォート内でのエスケープ処理
                    description_text = ': '.join(description_parts) if len(description_parts) > 1 else description_parts[0]
                    # 改行を削除
                    description_text = description_text.replace('\n', ' ').replace('\r', ' ')
                    # 連続するスペースを1つに
                    description_text = ' '.join(description_text.split())
                    # ダブルクォートをエスケープ
                    description_text = description_text.replace('"', '\\"')
                    yml_lines.append(f'        description: "{description_text}"')
                else:
                    yml_lines.append(f'        description: "{col_name}"')
                
                # data_testsはコメントアウトして、必要に応じて手動で追加できるようにする
                yml_lines.append("        # data_tests:")
                yml_lines.append("        #   - not_null")
                if 'id' in col_name.lower() or 'key' in col_name.lower():
                    yml_lines.append("        #   - unique")
        
        if len(yml_lines) > 3:  # ヘッダー3行以上の内容がある場合
            schema_yml = '\n'.join(yml_lines)
    
    return sql, schema_yml

def auto_detect_columns(df):
    """カラムを自動検出"""
    detected = {
        'physical': None,
        'type': None,
        'logical': None,
        'desc': None
    }
    
    for col in df.columns:
        col_str = str(col).strip().lower()
        # 物理名
        if any(keyword in col_str for keyword in ['物理名', '物理', 'カラム名', 'physical', 'column_name', 'field']):
            detected['physical'] = col
        # データ型
        elif any(keyword in col_str for keyword in ['データ型', 'データタイプ', '型', 'type', 'datatype']):
            detected['type'] = col
        # 論理名/項目名
        elif any(keyword in col_str for keyword in ['論理名', '項目名', '項目', 'logical', 'item', 'name']):
            detected['logical'] = col
        # 説明
        elif any(keyword in col_str for keyword in ['説明', '備考', 'コメント', 'description', 'comment', 'remarks', 'note']):
            detected['desc'] = col
    
    return detected

def main():
    st.set_page_config(page_title="dbt SQL Generator", layout="wide")
    
    st.title("📊 Excel設計書からdbt SQL生成ツール")
    st.markdown("Excelのテーブル設計書をアップロードして、Snowflake外部テーブルから通常テーブルを作成するdbt SQLを生成します。")
    
    # 必要なパッケージのチェック
    packages_info = []
    try:
        import openpyxl
        packages_info.append("✅ openpyxl (for .xlsx files)")
    except ImportError:
        packages_info.append("❌ openpyxl (for .xlsx files) - `pip install openpyxl`")
    
    try:
        import xlrd
        packages_info.append("✅ xlrd (for .xls files)")
    except ImportError:
        packages_info.append("⚠️ xlrd (for .xls files) - `pip install xlrd` (optional)")
    
    with st.expander("📦 パッケージ情報"):
        for info in packages_info:
            st.write(info)
    
    # サイドバーで設定
    with st.sidebar:
        st.header("⚙️ 設定")
        source_schema = st.text_input("Source Schema", value="test", help="dbtのsource schemaを入力")
        source_table = st.text_input("Source Table", value="customer_master", help="外部テーブル名を入力")
        model_name = st.text_input("Model Name", value="trs_customer_master", help="生成するモデル名を入力")
        model_description = st.text_area(
            "Model Description", 
            value="", 
            help="モデルの説明（models.ymlのdescriptionに使用）",
            placeholder="例: 顧客マスタ"
        )
        
        st.markdown("---")
        st.markdown("### 📝 必要なExcelカラム")
        st.markdown("""
        - **物理名** (必須): カラムの物理名
        - **データ型** (必須): データ型
        - **論理名/項目名** (任意): コメント用
        - **説明/備考** (任意): コメント用
        """)
    
    # メインエリア
    st.header("📁 Excelファイルアップロード")
    uploaded_file = st.file_uploader(
        "テーブル設計書のExcelファイルを選択",
        type=['xlsx', 'xls'],
        accept_multiple_files=False,
        help="物理名とデータ型のカラムを含むExcelファイルをアップロードしてください"
    )
    
    if uploaded_file is not None:
        try:
            # ファイル形式を判定してエンジンを選択
            file_extension = uploaded_file.name.split('.')[-1].lower()
            
            if file_extension == 'xlsx':
                engine = 'openpyxl'
            elif file_extension == 'xls':
                engine = 'xlrd'
            else:
                st.error("サポートされていないファイル形式です。.xlsx または .xls ファイルをアップロードしてください。")
                st.stop()
            
            # Excelファイルを読み込む
            try:
                excel_file = pd.ExcelFile(uploaded_file, engine=engine)
            except ImportError as e:
                if 'xlrd' in str(e):
                    st.error("⚠️ .xlsファイルを読み込むには xlrd をインストールしてください：")
                    st.code("pip install xlrd", language="bash")
                    st.stop()
                else:
                    raise e
            
            # シート選択
            sheet_names = excel_file.sheet_names
            selected_sheet = st.selectbox("シートを選択", sheet_names)
            
            # 選択したシートを読み込む
            df = pd.read_excel(uploaded_file, sheet_name=selected_sheet, engine=engine)
            
            # データプレビュー
            st.subheader("📋 データプレビュー")
            st.dataframe(df.head(20), height=300)
            
            # カラムマッピング設定
            st.subheader("🔧 カラムマッピング設定")
            st.info("データプレビューを確認して、各用途に対応するカラムを選択してください。")
            
            # 自動検出
            detected = auto_detect_columns(df)
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                physical_options = list(df.columns)
                physical_default = physical_options.index(detected['physical']) if detected['physical'] else 0
                physical_col = st.selectbox(
                    "物理名カラム *必須", 
                    options=physical_options,
                    index=physical_default,
                    help="カラムの物理名が入っている列"
                )
            
            with col2:
                type_options = list(df.columns)
                type_default = type_options.index(detected['type']) if detected['type'] else 0
                type_col = st.selectbox(
                    "データ型カラム *必須",
                    options=type_options,
                    index=type_default,
                    help="データ型が入っている列"
                )
            
            with col3:
                logical_options = ['未選択'] + list(df.columns)
                logical_default = logical_options.index(detected['logical']) if detected['logical'] else 0
                logical_col = st.selectbox(
                    "論理名/項目名カラム",
                    options=logical_options,
                    index=logical_default,
                    help="論理名や項目名が入っている列（コメント生成用）"
                )
            
            with col4:
                desc_options = ['未選択'] + list(df.columns)
                desc_default = desc_options.index(detected['desc']) if detected['desc'] else 0
                desc_col = st.selectbox(
                    "説明/備考カラム",
                    options=desc_options,
                    index=desc_default,
                    help="説明や備考が入っている列（コメント生成用）"
                )
            
            # マッピング結果のプレビューと行選択
            if st.checkbox("マッピング結果をプレビュー・編集", value=True):
                st.subheader("📊 マッピング結果プレビュー")
                st.info("✏️ 生成対象に含めたくない行のチェックを外してください")
                
                preview_cols = [physical_col, type_col]
                preview_labels = ['物理名', 'データ型']
                
                if logical_col != '未選択':
                    preview_cols.append(logical_col)
                    preview_labels.append('論理名/項目名')
                
                if desc_col != '未選択':
                    preview_cols.append(desc_col)
                    preview_labels.append('説明/備考')
                
                # プレビュー用データフレームを作成
                preview_df = df[preview_cols].copy()
                preview_df.columns = preview_labels
                
                # セッションステートで選択状態を管理
                if 'selected_rows' not in st.session_state:
                    # 初期状態：物理名がNaNでない行を選択
                    st.session_state.selected_rows = preview_df['物理名'].notna().tolist()
                
                # 選択状態をリセットするボタン
                col1, col2, col3 = st.columns([1, 1, 3])
                with col1:
                    if st.button("✅ すべて選択"):
                        st.session_state.selected_rows = [True] * len(preview_df)
                        st.rerun()
                with col2:
                    if st.button("❌ すべて解除"):
                        st.session_state.selected_rows = [False] * len(preview_df)
                        st.rerun()
                
                # 表示する行数の選択
                display_rows = st.selectbox(
                    "表示行数",
                    options=[10, 20, 50, 100, "すべて"],
                    index=1
                )
                
                if display_rows == "すべて":
                    display_limit = len(preview_df)
                else:
                    display_limit = min(display_rows, len(preview_df))
                
                # 選択チェックボックスを含むデータフレームを作成
                display_df = preview_df.head(display_limit).copy()
                
                # 選択状態を表示用に準備
                selected_indices = []
                for i in range(display_limit):
                    # セッションステートの長さを調整
                    if i >= len(st.session_state.selected_rows):
                        st.session_state.selected_rows.append(True)
                
                # データエディタを使用して選択可能なテーブルを表示
                edited_df = pd.DataFrame()
                edited_df['選択'] = [st.session_state.selected_rows[i] if i < len(st.session_state.selected_rows) else True for i in range(display_limit)]
                
                # プレビューデータを結合
                for col in display_df.columns:
                    edited_df[col] = display_df[col].values
                
                # データエディタで表示（チェックボックス付き）
                edited_result = st.data_editor(
                    edited_df,
                    column_config={
                        "選択": st.column_config.CheckboxColumn(
                            "選択",
                            help="SQL生成に含める行を選択",
                            default=True,
                        )
                    },
                    disabled=[col for col in edited_df.columns if col != '選択'],
                    hide_index=False,
                    use_container_width=True,
                    height=min(600, 35 * display_limit + 35),
                    key="data_editor"
                )
                
                # 編集結果をセッションステートに反映
                if edited_result is not None:
                    for i in range(len(edited_result)):
                        if i < len(st.session_state.selected_rows):
                            st.session_state.selected_rows[i] = edited_result['選択'].iloc[i]
                
                # 選択された行数を表示
                selected_count = sum(edited_result['選択']) if edited_result is not None else 0
                st.success(f"📊 {selected_count} / {display_limit} 行が選択されています")
                
                if display_limit < len(preview_df):
                    st.warning(f"⚠️ 全 {len(preview_df)} 行中 {display_limit} 行を表示しています")
            
            st.markdown("---")
            
            # SQL生成
            st.subheader("🔄 SQL生成")
            
            # コメント生成オプション
            include_comments = st.checkbox(
                "📝 models.yml定義も生成", 
                value=True,
                disabled=(logical_col == '未選択' and desc_col == '未選択'),
                help="項目名と説明からdbtのmodels.yml定義を生成します"
            )
            
            if st.button("🚀 SQL生成", type="primary", use_container_width=True):
                # 選択された行を取得
                selected_rows = st.session_state.get('selected_rows', None)
                
                sql, schema_yml = generate_dbt_sql_with_mapping(
                    df, source_schema, source_table, model_name, model_description,
                    physical_col, type_col, 
                    logical_col if logical_col != '未選択' else None,
                    desc_col if desc_col != '未選択' else None,
                    include_comments,
                    selected_rows
                )
                
                if sql:
                    # メインSQLを表示
                    st.subheader("📄 dbt Model SQL")
                    st.code(sql, language='sql', line_numbers=True)
                    
                    # models.ymlを表示
                    if schema_yml and include_comments:
                        st.subheader("📝 models.yml定義")
                        st.info("このYAML定義をmodels.ymlファイルに追加してください。")
                        st.code(schema_yml, language='yaml', line_numbers=True)
                    
                    # ダウンロードボタン
                    col1, col2 = st.columns(2)
                    with col1:
                        st.download_button(
                            label="📥 Model SQLをダウンロード",
                            data=sql,
                            file_name=f"{model_name}.sql",
                            mime="text/plain",
                            use_container_width=True
                        )
                    
                    with col2:
                        if schema_yml and include_comments:
                            st.download_button(
                                label="📥 models.yml定義をダウンロード",
                                data=schema_yml,
                                file_name=f"{model_name}_schema.yml",
                                mime="text/yaml",
                                use_container_width=True
                            )
                    
                    # 統計情報
                    st.subheader("📈 生成情報")
                    col_count = len([line for line in sql.split('\n') if 'as ' in line and 'select' not in line.lower()])
                    metrics_col1, metrics_col2 = st.columns(2)
                    with metrics_col1:
                        st.metric("生成されたカラム数", col_count)
                    with metrics_col2:
                        if schema_yml:
                            yml_col_count = len([line for line in schema_yml.split('\n') if '- name:' in line and 'columns:' not in line])
                            st.metric("スキーマ定義カラム数", yml_col_count)
            
        except Exception as e:
            st.error(f"ファイルの処理エラー: {str(e)}")
    else:
        st.info("👆 Excelファイルをアップロードしてください")
    
    # 使い方
    with st.expander("📖 使い方"):
        st.markdown("""
        ### 使用方法
        1. **Excelファイル準備**: テーブル設計書のExcelファイルを用意
        2. **ファイルアップロード**: Excelファイルをアップロード
        3. **シート選択**: 対象のシートを選択
        4. **カラムマッピング**: 各用途に対応するカラムを選択
           - 物理名（必須）: カラムの物理名
           - データ型（必須）: Snowflakeのデータ型
           - 論理名/項目名（任意）: コメント生成用
           - 説明/備考（任意）: コメント生成用
        5. **設定確認**: サイドバーでschema, table名を設定
        6. **SQL生成**: 「SQL生成」ボタンをクリック
        7. **ダウンロード**: 生成されたSQLをダウンロード
        
        ### サポートされるデータ型
        - VARCHAR(n), CHAR(n) → varchar(n)
        - NUMBER, INT, DECIMAL → number
        - DATE → date
        - TIMESTAMP, DATETIME → timestamp
        - FLOAT → float
        - BOOLEAN → boolean
        
        ### 特殊処理
        - 数値型・日付型: `nullif(value:column, '')` で空文字をNULLに変換
        - gender/性別カラム: 自動的にコード変換ロジックを追加
        
        ### dbtでの活用
        生成されたmodels.yml定義は、既存のmodels.ymlファイルに追加してください：
        
        ```yaml
        version: 2
        
        models:
          - name: stg_customer
            description: "stg_customerのテーブル定義"
            columns:
              - name: serial_number
                description: "シリアル番号: 顧客を一意に識別する番号"
                # data_tests:
                #   - not_null
                #   - unique
              - name: full_name
                description: "氏名: 顧客の氏名"
                # data_tests:
                #   - not_null
        ```
        
        **data_testsについて**:
        - data_testsはコメントアウトして生成されます
        - 実際のデータやビジネスルールに応じて、必要なテストのコメントを外してください
        - IDやKEYを含むカラムには`unique`テストの候補も含まれます
        """)

if __name__ == "__main__":
    main()