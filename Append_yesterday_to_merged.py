import streamlit as st
import pandas as pd
import io
import re

st.set_page_config(page_title="Append Yesterday → merged_leaderboards", layout="centered")
st.title("➕ Append Yesterday to merged_leaderboards")

# ---------- utils ----------
def safe_read(f):
    name = str(getattr(f, "name", f)).lower()
    if name.endswith(".parquet"):
        return pd.read_parquet(f)
    try:
        return pd.read_csv(f, low_memory=False)
    except UnicodeDecodeError:
        return pd.read_csv(f, encoding="latin1", low_memory=False)

def to_ymd(s, fallback_year=None):
    if pd.isna(s): return pd.NaT
    ss = str(s).strip()
    # already iso?
    try:
        return pd.to_datetime(ss, errors="raise").normalize()
    except Exception:
        pass
    # formats like 8_15 or 8/15
    m = re.match(r"^\s*(\d{1,2})[^\d]+(\d{1,2})\s*$", ss)
    if m and fallback_year:
        mm = int(m.group(1)); dd = int(m.group(2))
        try:
            return pd.to_datetime(f"{int(fallback_year):04d}-{mm:02d}-{dd:02d}").normalize()
        except Exception:
            return pd.NaT
    return pd.NaT

def normalize(df, season_year=None):
    df = df.copy()
    # standardize column casing
    df.columns = [str(c).strip() for c in df.columns]
    # harmonize date
    date_col = "game_date" if "game_date" in df.columns else None
    for cand in ["date","Date","GAME_DATE","game day","game day "]:
        if (not date_col) and cand in df.columns:
            date_col = cand
    if not date_col:
        st.error("Could not find a date column (expected 'game_date').")
        st.stop()
    df["game_date"] = df[date_col].apply(lambda s: to_ymd(s, season_year))
    # keep name/id if present
    if "player_name" not in df.columns:
        # try a few common variants
        for cand in ["player name","Player","PLAYER_NAME","name","Name"]:
            if cand in df.columns:
                df["player_name"] = df[cand]
                break
    # sanitize batter_id to string (avoid 123.0)
    if "batter_id" in df.columns:
        df["batter_id"] = df["batter_id"].astype(str).str.replace(r"\.0$", "", regex=True)
        df.loc[df["batter_id"].isin(["", "nan", "NaN"]), "batter_id"] = pd.NA
    return df

def dedupe_concat(merged_old, yday):
    # union columns, then align
    all_cols = list(dict.fromkeys(list(merged_old.columns) + list(yday.columns)))
    A = merged_old.reindex(columns=all_cols)
    B = yday.reindex(columns=all_cols)
    out = pd.concat([A, B], ignore_index=True)

    # de-dupe priority: (game_date, batter_id) if batter_id exists & not NA
    if "batter_id" in out.columns:
        # Build a key where batter_id present; else fallback to name
        key_id = out["batter_id"].astype("string")
        key_name = out.get("player_name", pd.Series([""], index=out.index)).astype("string")
        key = out["game_date"].astype("string") + "||" + key_id.fillna("") + "||" + key_name.fillna("")
    else:
        # fallback only on player_name + date
        key = out["game_date"].astype("string") + "||" + out.get("player_name", pd.Series([""], index=out.index)).astype("string")

    # keep last occurrence (i.e., yesterday’s rows overwrite prior)
    out["_key"] = key
    out = out.drop_duplicates(subset=["_key"], keep="last").drop(columns=["_key"])
    return out

# ---------- UI ----------
col_left, col_right = st.columns(2)
with col_left:
    merged_file = st.file_uploader("Upload current merged_leaderboards CSV", type=["csv"], key="merged")
with col_right:
    yday_file = st.file_uploader("Upload yesterday's leaderboard CSV", type=["csv"], key="yday")

season_year = st.number_input("Season year (only used if yesterday file date looks like '8_15')",
                              min_value=2015, max_value=2100, value=2025, step=1)

if merged_file and yday_file:
    with st.spinner("Reading and normalizing..."):
        merged_df = safe_read(merged_file)
        yday_df   = safe_read(yday_file)
        merged_df = normalize(merged_df, season_year)
        yday_df   = normalize(yday_df, season_year)

        if "game_date" not in merged_df.columns or "game_date" not in yday_df.columns:
            st.error("Missing 'game_date' after normalization.")
            st.stop()

        # show quick ranges
        st.write(f"📦 merged rows: {len(merged_df):,} | yesterday rows: {len(yday_df):,}")
        try:
            md_min, md_max = merged_df["game_date"].min(), merged_df["game_date"].max()
            yd_min, yd_max = yday_df["game_date"].min(), yday_df["game_date"].max()
            st.write(f"merged date range: {str(md_min.date())} → {str(md_max.date())}")
            st.write(f"yesterday date range: {str(yd_min.date())} → {str(yd_max.date())}")
        except Exception:
            pass

        out = dedupe_concat(merged_df, yday_df)

        # pretty print date
        out["game_date"] = pd.to_datetime(out["game_date"], errors="coerce").dt.strftime("%Y-%m-%d")

        st.success(f"✅ New merged size: {len(out):,} rows (de-duplicated)")
        st.dataframe(out.head(50), use_container_width=True)

        # Offer download (same filename you use in the Learner app)
        csv_buf = io.StringIO()
        out.to_csv(csv_buf, index=False)
        st.download_button(
            "⬇️ Download updated merged_leaderboards_complete.csv",
            data=csv_buf.getvalue(),
            file_name="merged_leaderboards_complete.csv",
            mime="text/csv",
        )

        # sanity: show how many rows at yesterday's max date are present
        try:
            latest_day = pd.to_datetime(yday_df["game_date"]).max().normalize()
            n_latest = (pd.to_datetime(out["game_date"]) == latest_day).sum()
            st.info(f"Rows at latest date {str(latest_day.date())}: {n_latest}")
        except Exception:
            pass
else:
    st.info("Upload both CSVs to append and re-download.")
