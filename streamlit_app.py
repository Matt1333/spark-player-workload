"""
Spark Player Workload - interactive dashboard.

Reads the CSV outputs produced by src/analysis.py (Spark) and lets you explore
workload and injury-risk indicators for 2024-25 big-five-league players.
No Spark needed at runtime: the heavy lifting was done upstream.
"""

import glob
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

ROOT = Path(__file__).parent
OUT = ROOT / "output" / "player_workload"

st.set_page_config(
    page_title="Spark Player Workload",
    page_icon="⚽",
    layout="wide",
)

POS_LABELS = {"GK": "Goalkeeper", "DF": "Defender", "MF": "Midfielder", "FW": "Forward"}
POS_COLORS = {
    "Defender": "#2563eb",
    "Midfielder": "#16a34a",
    "Forward": "#dc2626",
    "Goalkeeper": "#9333ea",
}


# --------------------------------------------------------------------------- data
@st.cache_data
def load(folder: str) -> pd.DataFrame:
    files = glob.glob(str(OUT / folder / "part-*.csv"))
    if not files:
        raise FileNotFoundError(f"No Spark output found in {OUT / folder}")
    return pd.read_csv(files[0], encoding="utf-8", encoding_errors="ignore")


def tidy(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "Comp" in df:
        df["League"] = df["Comp"].str.replace(r"^[a-z]{2,3} ", "", regex=True)
    if "Nation" in df:
        df["Nation"] = df["Nation"].str.split().str[-1]
    if "Pos_main" in df:
        df["Position"] = df["Pos_main"].map(POS_LABELS).fillna(df["Pos_main"])
    numeric = (
        "workload_index", "injury_risk_score", "running_load", "defensive_load",
        "duel_load", "Min", "MP", "Age", "avg_workload_index", "avg_injury_risk_score",
    )
    for c in numeric:
        if c in df:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


full = tidy(load("full_player_workload"))
teams = tidy(load("team_summary"))

# --------------------------------------------------------------------------- sidebar
st.sidebar.title("Filters")
leagues = sorted(full["League"].dropna().unique())
sel_leagues = st.sidebar.multiselect("League", leagues, default=leagues)
positions = [p for p in POS_LABELS.values() if p in full["Position"].unique()]
sel_pos = st.sidebar.multiselect("Position", positions, default=positions)
min_minutes = st.sidebar.slider(
    "Minimum minutes played",
    int(full["Min"].min()), int(full["Min"].max()), 900, step=90,
)
top_n = st.sidebar.slider("Players in rankings", 10, 50, 20, step=5)

f = full[
    full["League"].isin(sel_leagues)
    & full["Position"].isin(sel_pos)
    & (full["Min"] >= min_minutes)
]

# --------------------------------------------------------------------------- header
st.title("⚽ Spark Player Workload")
st.caption(
    "Who carried the heaviest physical load in 2024-25, and who is most exposed to overuse? "
    "Metrics computed with Apache Spark on 2,854 players from the big five leagues."
)

k1, k2, k3, k4 = st.columns(4)
k1.metric("Players in selection", f"{len(f):,}")
k2.metric("Avg workload index", f"{f['workload_index'].mean():.1f}" if len(f) else "-")
top_player = f.sort_values("workload_index", ascending=False).head(1)
k3.metric("Highest workload", top_player["Player"].iloc[0] if len(f) else "-")
risk_player = f.sort_values("injury_risk_score", ascending=False).head(1)
k4.metric("Highest injury risk", risk_player["Player"].iloc[0] if len(f) else "-")

st.divider()

# --------------------------------------------------------------------------- tabs
tab_risk, tab_rank, tab_pos, tab_team, tab_method = st.tabs(
    ["Risk map", "Rankings", "By position", "By team", "How the score works"]
)

with tab_risk:
    st.subheader("Load vs. minutes: the overuse quadrant")
    st.caption(
        "Top-right is where injuries happen: players who are intense *and* play a lot. "
        "Bubble size = injury-risk score."
    )
    if len(f):
        fig = px.scatter(
            f,
            x="Min", y="workload_index",
            size="injury_risk_score", color="Position",
            color_discrete_map=POS_COLORS,
            hover_name="Player",
            hover_data={
                "Squad": True, "League": True, "Min": True,
                "workload_index": ":.1f", "injury_risk_score": ":.1f",
                "Position": False,
            },
            labels={"Min": "Minutes played", "workload_index": "Workload index"},
            height=560,
        )
        fig.update_layout(legend_title_text="", margin=dict(l=10, r=10, t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No player matches the current filters.")

with tab_rank:
    st.subheader(f"Top {top_n} players by workload index")
    top = f.sort_values("workload_index", ascending=False).head(top_n)
    if len(top):
        fig = px.bar(
            top.iloc[::-1],
            x="workload_index", y="Player", orientation="h",
            color="Position", color_discrete_map=POS_COLORS,
            hover_data={"Squad": True, "League": True, "Min": True, "Position": False},
            labels={"workload_index": "Workload index", "Player": ""},
            height=max(400, 22 * len(top)),
        )
        fig.update_layout(legend_title_text="", margin=dict(l=10, r=10, t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(
            top[[
                "Player", "Squad", "League", "Position", "Age", "MP", "Min",
                "running_load", "defensive_load", "duel_load",
                "workload_index", "injury_risk_score",
            ]].round(2).reset_index(drop=True),
            use_container_width=True, hide_index=True,
        )
    else:
        st.info("No player matches the current filters.")

with tab_pos:
    st.subheader("Top 10 per position")
    if sel_pos:
        cols = st.columns(len(sel_pos))
        for col, pos in zip(cols, sel_pos):
            sub = (
                f[f["Position"] == pos]
                .sort_values("workload_index", ascending=False)
                .head(10)[["Player", "Squad", "workload_index"]]
                .round(1).reset_index(drop=True)
            )
            sub.index = sub.index + 1
            col.markdown(f"**{pos}**")
            col.dataframe(sub, use_container_width=True)
    else:
        st.info("Select at least one position.")

with tab_team:
    st.subheader("Average workload per club")
    st.caption("Computed on every player of the club with at least 5 full-match equivalents.")
    t = teams[teams["League"].isin(sel_leagues)].sort_values("avg_workload_index", ascending=False)
    if len(t):
        fig = px.bar(
            t, x="Squad", y="avg_workload_index", color="League",
            hover_data={"avg_injury_risk_score": ":.1f", "num_players": True},
            labels={"avg_workload_index": "Average workload index", "Squad": ""},
            height=520,
        )
        fig.update_layout(
            xaxis_tickangle=-60, legend_title_text="",
            margin=dict(l=10, r=10, t=10, b=10),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Select at least one league.")

with tab_method:
    st.markdown(
        """
**Source.** Season statistics 2024-25 (FBref-style), 2,854 players, Premier League, La Liga,
Serie A, Bundesliga, Ligue 1. Players with fewer than 5 full-match equivalents are dropped
(1,987 kept).

**Per-90 load metrics**

| Metric | Definition |
|---|---|
| `running_load` | progressive carrying distance / 90s |
| `defensive_load` | (tackles + interceptions) / 90s |
| `duel_load` | fouls committed / 90s |

**Workload index** = 0.4 x running_load + 0.4 x defensive_load + 0.2 x duel_load

**Injury-risk score** = workload_index x playing-time factor, i.e. intensity scaled by exposure.

**Limitations, stated plainly.** No tracking data (distance, sprints), so progressive carries
stand in for running volume and the index favours ball-carriers. Weights are a judgement call,
not fitted against injury records. Season-level only: rolling windows would be needed for a
medical staff.

Processing was done with Apache Spark (PySpark, window functions) inside Docker; this dashboard
reads the resulting CSVs. Source code on
[GitHub](https://github.com/Matt1333/spark-player-workload).
"""
    )
