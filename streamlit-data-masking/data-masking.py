# Streamlit データマスキング専用ツール (Masker)
# ------------------------------------------------------------
# 使い方:
# 1) pip install streamlit pandas openpyxl xlsxwriter
# 2) このファイルを data_masking.py として保存
# 3) streamlit run data_masking.py
#
# 機能(マスキング特化):
# - 入力: CSV/TSV/Excel 読み込み（UTF-8-SIG/UTF-8/CP932の順で自動判定）
# - 列ごとにマスキング方式を選択
#   * 全マスク(****)
#   * 先頭N/末尾M以外マスク（中央を*）
#   * SHA256ハッシュ（任意ソルト）
#   * トークン化（安定マッピング）/ マップのJSON入出力
#   * 正規表現で一致部分のみマスク
#   * プリセット（Email/Phone/CreditCard/住所）
# - 出力: CSV/TSV/Excel、トークンマップ(JSON)、実行ポリシー(JSON)
# ------------------------------------------------------------

import io
import re
import json
import hashlib
import pandas as pd
import streamlit as st
import os
import time

st.set_page_config(page_title="データマスキング専用ツール", layout="wide")
st.title("🔒 データマスキング専用ツール")

# -------------------------------
# Utility
# -------------------------------

def read_table_from_path(path: str, sheet_name=None, header=0) -> pd.DataFrame:
    lower = path.lower()
    # Excel
    if lower.endswith((".xlsx", ".xls")):
        return pd.read_excel(path, sheet_name=sheet_name, header=header)
    # Gzip圧縮CSV/TSV
    if lower.endswith(".gz"):
        sep = "\t" if lower.endswith(".tsv.gz") or ".tsv" in lower else ","
        return pd.read_csv(path, compression="gzip", sep=sep, header=header)
    # 通常CSV/TSV
    sep = "\t" if lower.endswith(".tsv") else ","
    try:
        return pd.read_csv(path, sep=sep, header=header)
    except Exception:
        return pd.read_csv(path, sep=sep, engine="python", header=header)


def read_preview_from_path(path: str, sheet_name=None, header=0, nrows: int = 200) -> pd.DataFrame:
    """先頭 nrows 行だけを高速に読む軽量プレビュー用。Excelは pandas の対応状況によりフル読み込み + head にフォールバック。"""
    lower = path.lower()
    try:
        if lower.endswith((".xlsx", ".xls")):
            try:
                return pd.read_excel(path, sheet_name=sheet_name, header=header, nrows=nrows)
            except TypeError:
                df_full = pd.read_excel(path, sheet_name=sheet_name, header=header)
                return df_full.head(nrows)
        if lower.endswith(".gz"):
            sep = "\t" if lower.endswith(".tsv.gz") or ".tsv" in lower else ","
            return pd.read_csv(path, compression="gzip", sep=sep, header=header, nrows=nrows)
        sep = "\t" if lower.endswith(".tsv") else ","
        return pd.read_csv(path, sep=sep, header=header, nrows=nrows)
    except Exception:
        df = read_table_from_path(path, sheet_name=sheet_name, header=header)
        return df.head(nrows)

def sha256_hash(x: str, salt: str = "") -> str:
    return hashlib.sha256((salt + x).encode()).hexdigest()


# トークン化: セッション内で安定。既存マップを適用し、未登録のみ新規採番。
if "token_map" not in st.session_state:
    st.session_state.token_map = {}


def tokenize_series(series: pd.Series, prefix: str = "TKN_") -> pd.Series:
    tm = st.session_state.token_map
    out = []
    counter = len(tm) + 1
    for v in series.astype(str).fillna(""):
        if v == "":
            out.append("")
            continue
        if v not in tm:
            tm[v] = f"{prefix}{counter:07d}"
            counter += 1
        out.append(tm[v])
    return pd.Series(out, index=series.index)


def mask_full(s: pd.Series) -> pd.Series:
    return s.astype(str).apply(lambda x: "****" if x else "")


def mask_keep_head_tail(s: pd.Series, head: int = 0, tail: int = 0) -> pd.Series:
    s = s.astype(str).fillna("")
    return s.apply(lambda x: (x[:head] + ("*" * max(0, len(x) - head - tail)) + x[-tail:]) if x else "")


def mask_regex_sub(s: pd.Series, pattern: str, repl_char: str = "*") -> pd.Series:
    try:
        reg = re.compile(pattern)
    except re.error as e:
        st.error(f"正規表現エラー: {e}")
        return s
    return s.astype(str).apply(lambda x: reg.sub(lambda m: repl_char * len(m.group(0)), x) if x else "")


# 住所マスキング関数
def mask_address_to_prefecture(address: str) -> str:
    """住所を都道府県レベルまでにマスク（郵便番号は削除）"""
    if not address:
        return ""
    
    # 郵便番号を除去（先頭の数字とハイフンのパターン）
    address_clean = re.sub(r'^\d{3}-?\d{4}\s*', '', address.strip())
    
    # 都道府県のパターン（住所の任意の位置から探す）
    prefecture_pattern = r'(東京都|北海道|(?:京都|大阪)府|(?:神奈川|埼玉|千葉|愛知|兵庫|福岡|北海道|青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|新潟|富山|石川|福井|山梨|長野|岐阜|静岡|三重|滋賀|奈良|和歌山|鳥取|島根|岡山|広島|山口|徳島|香川|愛媛|高知|佐賀|長崎|熊本|大分|宮崎|鹿児島|沖縄)県)'
    
    # 住所全体から都道府県を探す
    match = re.search(prefecture_pattern, address_clean)
    
    if match:
        return match.group(1)
    else:
        return ""


def mask_address_to_city(address: str) -> str:
    """住所を市区町村レベルまでにマスク（郵便番号は削除）"""
    if not address:
        return ""
    
    # 郵便番号を除去
    address_clean = re.sub(r'^\d{3}-?\d{4}\s*', '', address.strip())
    
    # 都道府県のパターン
    prefecture_pattern = r'(東京都|北海道|(?:京都|大阪)府|(?:神奈川|埼玉|千葉|愛知|兵庫|福岡|北海道|青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|新潟|富山|石川|福井|山梨|長野|岐阜|静岡|三重|滋賀|奈良|和歌山|鳥取|島根|岡山|広島|山口|徳島|香川|愛媛|高知|佐賀|長崎|熊本|大分|宮崎|鹿児島|沖縄)県)'
    
    # 都道府県を探す
    match_pref = re.search(prefecture_pattern, address_clean)
    
    if match_pref:
        prefecture = match_pref.group(1)
        # 都道府県の後の部分を取得
        pref_end = match_pref.end()
        rest = address_clean[pref_end:]
        
        # 市区町村のパターン（郡部も含む）
        city_patterns = [
            r'^(.+?市)',  # 市
            r'^(.+?区)',  # 区（政令指定都市の区）
            r'^(.+?町)',  # 町
            r'^(.+?村)',  # 村
            r'^(.+?郡.+?[町村])',  # 郡部の町村
        ]
        
        # 市区町村を抽出
        city = ""
        for pattern in city_patterns:
            match_city = re.match(pattern, rest)
            if match_city:
                city = match_city.group(1)
                break
        
        if city:
            return prefecture + city
        else:
            return prefecture
    else:
        # 都道府県が見つからない場合、市区町村だけを探す
        city_patterns = [
            r'(.+?[市区町村])',  # 一般的な市区町村
            r'(.+?郡.+?[町村])',  # 郡部の町村
        ]
        
        for pattern in city_patterns:
            match_city = re.search(pattern, address_clean)
            if match_city:
                city = match_city.group(1)
                return city
        
        return ""


def mask_address_custom(address: str, level: str = "prefecture") -> str:
    """住所を指定レベルまでマスク"""
    if level == "prefecture":
        return mask_address_to_prefecture(address)
    elif level == "city":
        return mask_address_to_city(address)
    else:
        return address


# ---- 追加: チャンク処理ユーティリティ ----

def _sep_from_path(path: str) -> str:
    lower = path.lower()
    return "\t" if lower.endswith(".tsv") or lower.endswith(".tsv.gz") else ","


# プリセット関数（住所マスキングを追加）
PRESETS = {
    "Email(ドメインをマスク)": {
        "func": lambda x: re.sub(r"([A-Za-z0-9._%+-]+)@([A-Za-z0-9.-]+)", lambda m: m.group(1) + "@" + "*" * len(m.group(2)), x),
        "has_level": False
    },
    "Phone(中間をマスク)": {
        "func": lambda x: re.sub(r"(\d{2,4})([- ]?)(\d{2,4})([- ]?)(\d{3,4})", lambda m: m.group(1) + m.group(2) + "*" * len(m.group(3)) + m.group(4) + m.group(5), x),
        "has_level": False
    },
    "CreditCard(最後4桁残し)": {
        "func": lambda x: (lambda digits: ("*" * max(0, len(digits) - 4)) + digits[-4:])(re.sub(r"[^\d]", "", x)),
        "has_level": False
    },
    "住所(都道府県まで)": {
        "func": lambda x: mask_address_to_prefecture(x),
        "has_level": False
    },
    "住所(市区町村まで)": {
        "func": lambda x: mask_address_to_city(x),
        "has_level": False
    },
}


def _apply_masking(chunk: pd.DataFrame, cols: list, method: str, head_n: int, tail_n: int, salt: str, regex_pat: str, preset: str) -> pd.DataFrame:
    if not cols:
        return chunk
    for c in cols:
        if c not in chunk.columns:
            continue
        s = chunk[c].astype(str).fillna("")
        if method == "全マスク(****)":
            chunk[c] = s.apply(lambda x: "****" if x else "")
        elif method == "先頭N/末尾M以外マスク":
            chunk[c] = s.apply(lambda x: (x[:head_n] + ("*" * max(0, len(x) - head_n - tail_n)) + x[-tail_n:]) if x else "")
        elif method == "SHA256ハッシュ":
            chunk[c] = s.apply(lambda x: sha256_hash(x, salt=salt) if x else "")
        elif method == "トークン化(一意マッピング)":
            # 既存のtoken_mapを使って安定マッピング
            tm = st.session_state.token_map
            out = []
            counter = len(tm) + 1
            for v in s:
                if v == "":
                    out.append("")
                    continue
                if v not in tm:
                    tm[v] = f"TKN_{counter:07d}"
                    counter += 1
                out.append(tm[v])
            chunk[c] = pd.Series(out, index=s.index)
        elif method == "正規表現で一致部分のみマスク":
            if regex_pat:
                try:
                    reg = re.compile(regex_pat)
                    chunk[c] = s.apply(lambda x: reg.sub(lambda m: "*" * len(m.group(0)), x) if x else "")
                except re.error as e:
                    st.error(f"正規表現エラー: {e}")
            else:
                st.error("正規表現パターンを入力してください。")
        elif method == "プリセット: Email/Phone/Card/住所":
            if preset in PRESETS:
                fn = PRESETS[preset]["func"]
                chunk[c] = s.apply(lambda x: fn(x) if x else "")
    return chunk


def stream_mask_save(path: str, header_has_names: bool, cols: list, method: str, head_n: int, tail_n: int, salt: str, regex_pat: str, preset: str, chunksize: int, out_dir: str = None, gzip_save: bool = False) -> str:
    """大容量向け: チャンク処理でマスクしながらCSV/TSVに追記保存する。戻り値は出力ファイルパス。"""
    sep = _sep_from_path(path)
    header = 0 if header_has_names else None

    base = os.path.basename(path)
    lower = base.lower()
    for suf in [".csv.gz",".tsv.gz",".xlsx",".xls",".csv",".tsv"]:
        if lower.endswith(suf):
            base = base[: -len(suf)]
            break
    ext = ".tsv" if sep == "\t" else ".csv"
    out_name = f"{base}_masked{ext}"
    if gzip_save and ext == ".csv":
        out_name += ".gz"
    out_dir = out_dir or os.path.dirname(path)
    out_path = os.path.join(out_dir, out_name)

    # Excelはチャンク未対応のためフルロード警告
    if path.lower().endswith((".xlsx",".xls")):
        st.warning("Excelはストリーミング分割読み込みに対応していないため、フルロードで処理します。時間がかかる場合はCSVに変換してください。")
        df_full = read_table_from_path(path, header=header)
        df_full = _apply_masking(df_full, cols, method, head_n, tail_n, salt, regex_pat, preset)
        df_full.to_csv(out_path, index=False, sep=sep, encoding="utf-8-sig")
        return out_path

    # CSV/TSV: ストリーミング
    write_header = True
    processed = 0
    start = time.time()
    progress = st.progress(0, text="処理開始…")
    status = st.empty()

    # 圧縮設定
    compression = "gzip" if gzip_save and ext == ".csv" else None

    for chunk in pd.read_csv(path, sep=sep, header=header, chunksize=chunksize, dtype=str, keep_default_na=False, na_filter=False):
        chunk = _apply_masking(chunk, cols, method, head_n, tail_n, salt, regex_pat, preset)
        chunk.to_csv(out_path, mode="a", index=False, sep=sep, header=write_header, encoding="utf-8-sig", compression=compression)
        write_header = False
        processed += len(chunk)
        elapsed = time.time() - start
        status.text(f"処理行数: {processed:,}  | 経過: {elapsed:.1f}s  | 出力: {os.path.basename(out_path)}")
        # 進捗は不確定なので疑似的にゆっくり上げる
        p = min(100, int((elapsed % 10) * 10))
        progress.progress(p, text="処理中…")

    progress.progress(100, text="完了")
    return out_path


# -------------------------------
# Input
# -------------------------------
# 読み込みオプション（サイドバー）
header_has_names = st.sidebar.checkbox("ヘッダ行あり（1行目は列名）", value=True)
light_nrows = st.sidebar.number_input("プレビュー行数（nrows）", min_value=5, max_value=5000, value=200, step=5)
left, right = st.columns([2, 1])
with left:
    # ローカルファイルパス指定のみ（アップロード機能は削除）
    path = st.text_input("ローカルファイルパスを入力 (CSV/TSV/Excel/.gz 対応)")
    sheet = None
    if path and path.lower().endswith((".xlsx", ".xls")):
        sheet = st.text_input("Excelのシート名（未指定なら先頭）", value="") or None

    if not path:
        st.info("ローカルファイルパスを入力してください。例: C:/data/bigfile.csv.gz")
        st.stop()

    # 存在確認 & ファイルサイズ表示
    if not os.path.exists(path):
        st.error("指定のパスにファイルが見つかりません。存在を確認してください。")
        st.stop()
    try:
        size_mb = os.path.getsize(path) / (1024 * 1024)
        st.caption(f"📄 {os.path.basename(path)} | サイズ: {size_mb:.2f} MB")
    except Exception:
        pass

    # 読み込み（軽量プレビュー専用）
    t0 = time.time()
    try:
        with st.spinner("プレビューを読み込み中…"):
            df = read_preview_from_path(path, sheet_name=sheet, header=0 if header_has_names else None, nrows=int(light_nrows))
    except Exception as e:
        st.exception(e)
        st.stop()
    dt = time.time() - t0
    st.success(f"プレビュー読み込み: {len(df)}行 × {df.shape[1]}列 | 所要 {dt:.2f} 秒（先頭のみ）")

    # プレビュー表示行数（読み込んだ範囲内で調整）
    max_rows = max(5, min(200, len(df)))
    n_preview = st.slider("プレビュー表示行数", 5, max_rows, min(50, max_rows), 5)
    st.dataframe(df.head(n_preview), use_container_width=True)

with right:
    st.subheader("列情報")
    info = pd.DataFrame({
        "column": df.columns,
        "dtype": [str(t) for t in df.dtypes],
        "nulls": df.isna().sum().values,
        "unique": [df[c].nunique(dropna=True) for c in df.columns],
    })
    st.dataframe(info, use_container_width=True, height=400)

# -------------------------------
# Sidebar: Masking Config
# -------------------------------
st.sidebar.header("🛡️ マスキング設定")
cols = st.sidebar.multiselect("対象列(複数可)", options=list(df.columns))
method = st.sidebar.selectbox(
    "方式",
    [
        "全マスク(****)",
        "先頭N/末尾M以外マスク",
        "SHA256ハッシュ",
        "トークン化(一意マッピング)",
        "正規表現で一致部分のみマスク",
        "プリセット: Email/Phone/Card/住所",
    ],
)

head_n = st.sidebar.number_input("先頭に残す文字数", 0, 100, 2)
_tail_n = st.sidebar.number_input("末尾に残す文字数", 0, 100, 2)
salt = st.sidebar.text_input("ハッシュ用ソルト(任意)")
regex_pat = st.sidebar.text_input("正規表現パターン (一致箇所を*)")
preset = st.sidebar.selectbox("プリセット", list(PRESETS.keys())) if method == "プリセット: Email/Phone/Card/住所" else None

# 保存前プレビュー適用ボタン（保存はしない）
preview_btn = st.sidebar.button("👀 プレビューにマスク適用（保存なし）")

# トークンマップ I/O
st.sidebar.subheader("トークンマップ（ローカル）")
map_path = st.sidebar.text_input("既存マップのローカルパス(JSON)")
if st.sidebar.button("マップを読み込む", disabled=not map_path):
    try:
        with open(map_path, "r", encoding="utf-8") as f:
            st.session_state.token_map.update(json.load(f))
        st.sidebar.success("トークンマップを読み込みました。")
    except Exception as e:
        st.sidebar.error(f"読み込み失敗: {e}")


# -------------------------------
# Preview masked (no save)
# -------------------------------
if 'preview_masked' not in st.session_state:
    st.session_state.preview_masked = None

if 'preview_btn' in locals() and preview_btn:
    st.session_state.preview_masked = _apply_masking(
        df.copy(),
        cols=cols,
        method=method,
        head_n=int(head_n),
        tail_n=int(_tail_n),
        salt=salt,
        regex_pat=regex_pat,
        preset=preset if method == "プリセット: Email/Phone/Card/住所" else None,
    )

if st.session_state.preview_masked is not None:
    st.subheader("プレビュー（マスキング適用後）")
    st.dataframe(st.session_state.preview_masked.head(n_preview), use_container_width=True)

# -------------------------------
# Apply Masking
# -------------------------------
# ストリーミング処理（大容量向け）
st.sidebar.header("🚚 ストリーミング処理（大容量向け）")
chunksize = st.sidebar.number_input("chunksize (行)", min_value=1000, max_value=2_000_000, value=100_000, step=10_000)
out_dir = st.sidebar.text_input("出力先フォルダ（未指定なら入力ファイルと同じ）", value=os.path.dirname(path))
gzip_save = st.sidebar.checkbox("CSVはgzipで保存（.csv.gz）", value=False)
run_stream = st.sidebar.button("▶ ストリーミングでマスクして保存")


# ストリーミング実行
if run_stream:
    if not cols:
        st.warning("対象列を選択してください。")
    else:
        with st.spinner("ストリーミング保存中…"):
            out_path = stream_mask_save(
                path=path,
                header_has_names=header_has_names,
                cols=cols,
                method=method,
                head_n=int(head_n),
                tail_n=int(_tail_n),
                salt=salt,
                regex_pat=regex_pat,
                preset=preset if method == "プリセット: Email/Phone/Card/住所" else None,
                chunksize=int(chunksize),
                out_dir=out_dir or None,
                gzip_save=bool(gzip_save),
            )
        st.success(f"✅ ストリーミング保存が完了: {out_path}")
        try:
            size_mb = os.path.getsize(out_path) / (1024*1024)
            st.caption(f"出力サイズ: {size_mb:.2f} MB")
        except Exception:
            pass
        # 出力の先頭だけ確認
        try:
            n_preview_out = st.slider("出力プレビュー行数", 5, 200, 20, 5, key="outprev")
            st.dataframe(pd.read_csv(out_path, sep=_sep_from_path(out_path), nrows=n_preview_out), use_container_width=True)
        except Exception as e:
            st.info(f"出力プレビュー読込をスキップ: {e}")

# -------------------------------
# Download buttons
# -------------------------------
st.subheader("補助ダウンロード")
st.download_button(
    "📥 トークンマップ(JSON)",
    data=json.dumps(st.session_state.token_map, ensure_ascii=False, indent=2),
    file_name="token_map.json",
    mime="application/json",
)

# 監査用: 実行ポリシーの保存
st.subheader("ポリシー(実行設定)の保存")
policy = {
    "columns": cols,
    "method": method,
    "head_n": int(head_n),
    "tail_n": int(_tail_n),
    "salt": salt,
    "regex": regex_pat,
    "preset": preset,
}
st.download_button(
    "📄 実行ポリシー(JSON)を保存",
    data=json.dumps(policy, ensure_ascii=False, indent=2),
    file_name="mask_policy.json",
    mime="application/json",
)

st.caption("© Data Masking App | Streamlit + pandas")