# Append_yesterday_to_merged.py

import streamlit as st
import pandas as pd
import numpy as np
import io
from datetime import datetime

st.set_page_config(page_title="➕ Append Yesterday → merged_leaderboards", layout="wide")
st.title("➕ Append Yesterday → merged_leaderboards")

@st.cache_data(show_spinner=False)
def safe_read(f):
    name = str(getattr(f, "name", f)).lower()
    if name.endswith(".parquet"):
        return pd.read_parquet(f)
    try:
        return pd.read_csv(f, low_memory=False)
    except UnicodeDecodeError:
        return pd.read_csv(f, encoding="latin1", low_memory=False)

def to_ymd(s):
    if pd.isna(s): return pd.NaT
    try:
        return pd.to_datetime(s, errors="coerce").normalize()
    except Exception:
        return pd.NaT

def normalize_cols(df):
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    # normalize common date col names into 'game_date'
    if "game_date" not in df.columns:
        for alt in ["date", "game date", "Game Date"]:
            if alt in df.columns:
                df["game_date"] = df[alt]
                break
    # normalize common name col names into 'player_name'
    if "player_name" not in df.columns:
        for alt in ["Player", "player name", "name", "Name"]:
            if alt in df.columns:
                df["player_name"] = df[alt]
                break
    return df

def dedupe_concat(merged_df: pd.DataFrame, yday_df: pd.DataFrame) -> pd.DataFrame:
    # concat first
    out = pd.concat([merged_df, yday_df], ignore_index=True, sort=False)

    # ----- SAFE getters (fix for your traceback) -----
    def _get_str_col(df, col, default=""):
        if col in df.columns:
            return df[col].astype("string")
        # fallback series must match df length and index
        return pd.Series([default] * len(df), index=df.index, dtype="string")

    def _get_date_col(df, col):
        if col in df.columns:
            return pd.to_datetime(df[col], errors="coerce").dt.normalize()
        return pd.Series([pd.NaT] * len(df), index=df.index, dtype="datetime64[ns]")

    # unify key columns (do not change existing content)
    key_date  = _get_date_col(out, "game_date")
    key_name  = _get_str_col(out, "player_name", "")
    key_batid = _get_str_col(out, "batter_id", "")

    # build a dedupe key (prefer batter_id if available)
    has_any_batid = key_batid.fillna("").str.len().gt(0)
    use_batid = has_any_batid.any()

    if use_batid:
        dedupe_key = key_date.astype("string") + " | " + key_batid.fillna("")
    else:
        dedupe_key = key_date.astype("string") + " | " + key_name.fillna("")

    out["_dedupe_key"] = dedupe_key
    # keep last occurrence (yesterday’s rows should overwrite older duplicates)
    out = out.drop_duplicates(subset=["_dedupe_key"], keep="last").drop(columns=["_dedupe_key"])
    return out

st.subheader("Inputs")
merged_file = st.file_uploader("Merged leaderboard CSV (existing)", type=["csv", "parquet"])
yday_file   = st.file_uploader("Yesterday's leaderboard CSV", type=["csv", "parquet"])

if not merged_file or not yday_file:
    st.info("Upload both the existing merged file and yesterday’s CSV/Parquet.")
    st.stop()

with st.spinner("Reading and normalizing files..."):
    merged_df = safe_read(merged_file)
    yday_df   = safe_read(yday_file)

    merged_df = normalize_cols(merged_df)
    yday_df   = normalize_cols(yday_df)

    # Ensure game_date is proper datetime (keep original columns intact)
    if "game_date" in merged_df.columns:
        merged_df["game_date"] = pd.to_datetime(merged_df["game_date"], errors="coerce").dt.normalize()
    if "game_date" in yday_df.columns:
        yday_df["game_date"] = pd.to_datetime(yday_df["game_date"], errors="coerce").dt.normalize()

st.write(f"Existing merged rows: {len(merged_df):,} | Yesterday rows: {len(yday_df):,}")

with st.spinner("Appending & de-duplicating..."):
    out = dedupe_concat(merged_df, yday_df)

st.success(f"Done. New merged size: {len(out):,} rows")
st.dataframe(out.head(30), use_container_width=True)

buf = io.BytesIO()
# keep same extension as input merged if it was parquet; else CSV
if str(getattr(merged_file, "name", "")).lower().endswith(".parquet"):
    out.to_parquet(buf, index=False)
    st.download_button(
        "⬇️ Download merged_leaderboards.parquet",
        data=buf.getvalue(),
        file_name="merged_leaderboards.parquet",
        mime="application/octet-stream"
    )
else:
    csv = out.to_csv(index=False)
    st.download_button(
        "⬇️ Download merged_leaderboards.csv",
        data=csv,
        file_name="merged_leaderboards.csv",
        mime="text/csv"
    )
