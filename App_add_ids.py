# app_add_ids.py
# =====================================================================
# 🔧 Add batter_id to a merged leaderboard using 1..N "today features" CSVs
# - Upload leaderboard (no batter_id) + multiple mapping CSVs (with batter_id)
# - Deterministic join on (game_date, player_name[, team]) with safe normalization
# - Produces merged_leaderboards_with_ids.csv + diagnostics
# =====================================================================

import streamlit as st
import pandas as pd
import numpy as np
import io, re
from unidecode import unidecode

st.set_page_config(page_title="🔧 Add batter_id to Leaderboard", layout="wide")
st.title("🔧 Add batter_id to Leaderboard (multi-CSV mapper)")

# ------------------------ Helpers ------------------------
@st.cache_data(show_spinner=False)
def read_any(f):
    name = str(getattr(f, "name", f)).lower()
    if name.endswith(".parquet"):
        return pd.read_parquet(f)
    try:
        return pd.read_csv(f, low_memory=False)
    except UnicodeDecodeError:
        return pd.read_csv(f, encoding="latin1", low_memory=False)

def to_date_ymd(s, season_year=None):
    if pd.isna(s): return pd.NaT
    ss = str(s).strip()
    # YYYY-MM-DD or other pd-recognized
    try:
        return pd.to_datetime(ss, errors="raise").normalize()
    except Exception:
        pass
    # M_D like 8_13
    m = re.match(r"^\s*(\d{1,2})[^\d]+(\d{1,2})\s*$", ss)
    if m and season_year:
        mm = int(m.group(1)); dd = int(m.group(2))
        try:
            return pd.to_datetime(f"{int(season_year):04d}-{mm:02d}-{dd:02d}").normalize()
        except Exception:
            return pd.NaT
    return pd.NaT

def clean_name(s):
    if pd.isna(s): return ""
    s = unidecode(str(s)).upper().strip()
    s = re.sub(r"[\.\-']", " ", s)
    s = re.sub(r"\s+", " ", s)
    for suf in [", JR", " JR", " JR.", ", SR", " SR", " SR.", " II", " III", " IV"]:
        if s.endswith(suf): s = s[: -len(suf)]
    return s.strip()

def std_team(s):
    if pd.isna(s): return ""
    return str(s).strip().upper()

def safe_str_id(s):
    # turn 123.0 or floats into clean string ids
    if pd.isna(s): return ""
    try:
        v = str(s).strip()
        if v.endswith(".0"): v = v[:-2]
        return v
    except Exception:
        return str(s)

# ------------------------ UI ------------------------
st.markdown("**Inputs**")
leaderboard_f = st.file_uploader("Merged leaderboard CSV (the one without batter_id)", type=["csv"])
map_files = st.file_uploader("Upload 1..N of your 'today features' CSVs (these contain batter_id)", type=["csv"], accept_multiple_files=True)
season_year = st.number_input("Season year (only used if leaderboard game_date looks like '8_13')", 2015, 2100, 2025, 1)

if not leaderboard_f:
    st.info("Upload your merged leaderboard CSV to begin.")
    st.stop()

lb = read_any(leaderboard_f)
st.write(f"Leaderboard rows: {len(lb):,}")

if not map_files:
    st.info("Upload one or more 'today features' CSVs that include `game_date`, `player_name`, `batter_id`.")
    st.stop()

maps = [read_any(f) for f in map_files]
st.write(f"Mapping CSVs uploaded: {len(maps)}")

# ------------------------ Normalize Leaderboard ------------------------
req_lb_cols = ["game_date", "player_name"]
for c in req_lb_cols:
    if c not in lb.columns:
        st.error(f"Leaderboard missing required column: `{c}`")
        st.stop()

lb = lb.copy()
lb["game_date_norm"] = lb["game_date"].apply(lambda s: to_date_ymd(s, season_year))
lb["player_name_norm"] = lb["player_name"].astype(str).apply(clean_name)
lb["team_code_std"] = lb["team_code"].astype(str).apply(std_team) if "team_code" in lb.columns else ""

if lb["game_date_norm"].isna().all():
    st.error("Could not parse any leaderboard dates. Ensure 'game_date' looks like YYYY-MM-DD or '8_13' (with season year set).")
    st.stop()

# ------------------------ Build ID Map from all mapping CSVs ------------------------
def normalize_map(df_in):
    df = df_in.copy()
    # Flexible column discovery
    # date
    date_col = None
    for c in df.columns:
        if str(c).lower().replace(" ", "_") in ("game_date","date"):
            date_col = c; break
    if date_col is None:
        raise ValueError("Mapping CSV missing a 'game_date' column.")
    df["game_date_norm"] = df[date_col].apply(lambda s: to_date_ymd(s, season_year))

    # player_name
    name_col = None
    for c in df.columns:
        if str(c).lower().replace(" ", "_") in ("player_name","name","batter_name"):
            name_col = c; break
    if name_col is None:
        raise ValueError("Mapping CSV missing a 'player_name' column.")
    df["player_name_norm"] = df[name_col].astype(str).apply(clean_name)

    # team_code (optional)
    team_col = None
    for c in df.columns:
        if str(c).lower().replace(" ", "_") in ("team_code","team","team_abbr","tm"):
            team_col = c; break
    df["team_code_std"] = df[team_col].astype(str).apply(std_team) if team_col else ""

    # batter_id
    bid_col = None
    for c in df.columns:
        if str(c).lower().replace(" ", "_") in ("batter_id","mlb_id","player_id","id"):
            bid_col = c; break
    if bid_col is None:
        raise ValueError("Mapping CSV missing a 'batter_id' (or mlb_id/player_id) column.")
    df["batter_id"] = df[bid_col].apply(safe_str_id)

    out = df.loc[:, ["game_date_norm","player_name_norm","team_code_std","batter_id"]].dropna(subset=["game_date_norm","player_name_norm"])
    # keep last occurrence per key if duplicates
    out = out.drop_duplicates(subset=["game_date_norm","player_name_norm","team_code_std"], keep="last")
    return out

idmaps = []
errors = []
for i, m in enumerate(maps, 1):
    try:
        idmaps.append(normalize_map(m))
    except Exception as e:
        errors.append(f"Map {i}: {e}")

if errors:
    st.warning("Some mapping files were skipped:\n\n- " + "\n- ".join(errors))

if not idmaps:
    st.error("No valid mapping tables were built from the uploaded CSVs.")
    st.stop()

idmap = pd.concat(idmaps, ignore_index=True)
idmap = idmap.drop_duplicates(subset=["game_date_norm","player_name_norm","team_code_std"], keep="last")

st.write(f"Built ID map rows: {len(idmap):,}")

# ------------------------ Two-stage merge ------------------------
# 1) Strict with team
m1 = lb.merge(
    idmap,
    on=["game_date_norm","player_name_norm","team_code_std"],
    how="left",
    suffixes=("", "_y")
)

matched_1 = m1["batter_id"].notna().sum()

# 2) Fill remaining with team-agnostic
need = m1["batter_id"].isna()
if need.any():
    fill = lb.loc[need, ["game_date_norm","player_name_norm"]].merge(
        idmap.drop(columns=["team_code_std"]),
        on=["game_date_norm","player_name_norm"],
        how="left"
    )["batter_id"]
    m1.loc[need, "batter_id"] = fill.values

matched_total = m1["batter_id"].notna().sum()
st.success(f"✅ Matched rows: {matched_total} / {len(m1)} (strict team stage matched {matched_1})")

# ------------------------ Diagnostics ------------------------
st.subheader("Diagnostics")
st.write("Leaderboard date range:", str(lb["game_date_norm"].min().date()), "→", str(lb["game_date_norm"].max().date()))
st.write("ID map date range:", str(idmap["game_date_norm"].min().date()), "→", str(idmap["game_date_norm"].max().date()))

if matched_total == 0:
    st.error("No matches were found. Check that your mapping CSVs and leaderboard share the SAME date format and names.")
else:
    sample = m1[m1["batter_id"].notna()].head(20)
    st.write("Sample of matched rows:")
    st.dataframe(sample[["game_date","player_name","team_code_std","batter_id"]], use_container_width=True)

# Unmatched suggestions (per date, show name counts)
unmatched = m1[m1["batter_id"].isna()].copy()
st.write(f"Unmatched rows: {len(unmatched)}")
if len(unmatched):
    st.dataframe(unmatched[["game_date","player_name","team_code_std"]].head(50), use_container_width=True)

# ------------------------ Output ------------------------
# Put batter_id next to existing columns; keep original schema + new column
out = m1.drop(columns=["game_date_norm","player_name_norm"], errors="ignore").copy()
# ensure batter_id string
out["batter_id"] = out["batter_id"].fillna("").apply(safe_str_id)

csv_buf = io.StringIO()
out.to_csv(csv_buf, index=False)
st.download_button(
    "⬇️ Download merged_leaderboards_with_ids.csv",
    data=csv_buf.getvalue(),
    file_name="merged_leaderboards_with_ids.csv",
    mime="text/csv"
)

st.caption("Now feed merged_leaderboards_with_ids.csv into your Learner app — it will match by batter_id+date first and avoid fuzzy headaches.")
