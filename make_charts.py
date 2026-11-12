"""Draw the README charts from the small CSV summaries written by pipeline.py.

Only small aggregated tables are read here (pandas); no Spark needed.

Usage:
    python make_charts.py            # reads reports/, writes reports/figures/
"""

import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib.ticker import FuncFormatter, PercentFormatter  # noqa: E402

# Validated categorical slots 1-2 (blue, orange) on a light surface.
SURFACE = "#fcfcfb"
TEXT = "#0b0b0b"
TEXT_2 = "#52514e"
GRID = "#e4e3df"
BLUE = "#2a78d6"
ORANGE = "#eb6834"


def _style(ax, title, subtitle):
    ax.set_facecolor(SURFACE)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=TEXT_2, labelsize=9, length=0)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    fig = ax.figure
    fig.text(0.012, 0.965, title, fontsize=13, color=TEXT, fontweight="bold", va="top")
    fig.text(0.012, 0.905, subtitle, fontsize=9, color=TEXT_2, va="top")


def _new_fig(w=10, h=5):
    fig, ax = plt.subplots(figsize=(w, h), dpi=150)
    fig.patch.set_facecolor(SURFACE)
    return fig, ax


def chart_monthly_share(monthly: pd.DataFrame, out: Path):
    m = monthly.copy()
    m["month_start"] = pd.to_datetime(m["month_start"])
    fig, ax = _new_fig()
    ax.plot(m["month_start"], m["ai_share"], color=BLUE, linewidth=2, label="AI-related papers")
    ax.plot(m["month_start"], m["genai_llm_share"], color=ORANGE, linewidth=2,
            label="Papers mentioning GenAI / LLM terms")
    last = m.iloc[-1]
    for col, color in (("ai_share", BLUE), ("genai_llm_share", ORANGE)):
        ax.plot(last["month_start"], last[col], "o", color=color, markersize=6,
                markeredgecolor=SURFACE, markeredgewidth=2)
        ax.annotate(f"{last[col]:.1%}", (last["month_start"], last[col]), xytext=(8, 0),
                    textcoords="offset points", va="center", fontsize=9, color=TEXT)
    first = m.iloc[0]
    ax.annotate(f"{first['ai_share']:.1%}", (first["month_start"], first["ai_share"]), xytext=(6, 14),
                textcoords="offset points", ha="left", fontsize=9, color=TEXT)
    ax.yaxis.set_major_formatter(PercentFormatter(1.0, decimals=0))
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.set_ylim(0, max(m["ai_share"].max() * 1.15, 0.05))
    ax.set_xlim(m["month_start"].min().to_pydatetime(), (m["month_start"].max() + pd.Timedelta(days=75)).to_pydatetime())
    ax.legend(frameon=False, loc="upper left", fontsize=9, labelcolor=TEXT)
    _style(ax, "Share of new arXiv papers that are AI-related, by month",
           f"Denominator: all papers first submitted (v1) in the month. "
           f"{m['month_start'].min():%b %Y} – {m['month_start'].max():%b %Y}, rule-based flags.")
    fig.tight_layout(rect=(0, 0, 1, 0.86))
    fig.savefig(out, facecolor=SURFACE)
    plt.close(fig)


def chart_monthly_counts(monthly: pd.DataFrame, out: Path):
    m = monthly.copy()
    m["month_start"] = pd.to_datetime(m["month_start"])
    fig, ax = _new_fig()
    ax.plot(m["month_start"], m["total_papers"], color=BLUE, linewidth=2, label="All new papers")
    ax.plot(m["month_start"], m["ai_papers"], color=ORANGE, linewidth=2, label="AI-related papers")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v / 1000:.0f}k"))
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.set_ylim(0, m["total_papers"].max() * 1.12)
    last = m.iloc[-1]
    for col, color in (("total_papers", BLUE), ("ai_papers", ORANGE)):
        ax.annotate(f"{last[col]:,.0f}", (last["month_start"], last[col]), xytext=(8, 0),
                    textcoords="offset points", va="center", fontsize=9, color=TEXT)
    ax.set_xlim(m["month_start"].min().to_pydatetime(), (m["month_start"].max() + pd.Timedelta(days=100)).to_pydatetime())
    ax.legend(frameon=False, loc="upper left", fontsize=9, labelcolor=TEXT)
    _style(ax, "New arXiv papers per month: all vs AI-related",
           "Counts of unique papers by v1 submission month (UTC).")
    fig.tight_layout(rect=(0, 0, 1, 0.86))
    fig.savefig(out, facecolor=SURFACE)
    plt.close(fig)


def chart_field_comparison(fields: pd.DataFrame, out: Path):
    f = fields[fields["subject_group"] != "Unknown"].sort_values("ai_share_2026")
    months = int(f["months_compared"].iloc[0])
    label = f"Jan–{pd.Timestamp(2026, months, 1):%b}"
    fig, ax = _new_fig(10, 5.2)
    y = range(len(f))
    for i, (_, r) in enumerate(f.iterrows()):
        ax.plot([r["ai_share_2025"], r["ai_share_2026"]], [i, i], color=GRID, linewidth=2, zorder=1)
    ax.scatter(f["ai_share_2025"], y, s=60, color=BLUE, label=f"{label} 2025", zorder=2,
               edgecolor=SURFACE, linewidth=2)
    ax.scatter(f["ai_share_2026"], y, s=60, color=ORANGE, label=f"{label} 2026", zorder=3,
               edgecolor=SURFACE, linewidth=2)
    for i, (_, r) in enumerate(f.iterrows()):
        x = max(r["ai_share_2025"], r["ai_share_2026"])
        ax.annotate(f"{r['ai_share_change_pp']:+.1f} pp  (n={r['total_2026']:,.0f})", (x, i),
                    xytext=(10, 0), textcoords="offset points", va="center", fontsize=8.5, color=TEXT_2)
    ax.set_yticks(list(y))
    ax.set_yticklabels(f["subject_group"], color=TEXT, fontsize=9)
    ax.xaxis.set_major_formatter(PercentFormatter(1.0, decimals=0))
    ax.set_xlim(0, f[["ai_share_2025", "ai_share_2026"]].max().max() * 1.35)
    ax.grid(axis="x", color=GRID, linewidth=0.8)
    ax.grid(axis="y", visible=False)
    ax.legend(frameon=False, loc="lower right", fontsize=9, labelcolor=TEXT)
    _style(ax, "AI-related share of papers by arXiv subject group, same months of 2025 and 2026",
           f"Unique (paper, subject group) pairs; cross-listed papers count in each group. "
           f"Label: change in percentage points, 2026 denominator.")
    fig.tight_layout(rect=(0, 0, 1, 0.86))
    fig.savefig(out, facecolor=SURFACE)
    plt.close(fig)


def make_charts(reports_dir):
    reports_dir = Path(reports_dir)
    figs = reports_dir / "figures"
    figs.mkdir(parents=True, exist_ok=True)
    monthly = pd.read_csv(reports_dir / "monthly_ai_trends.csv")
    fields = pd.read_csv(reports_dir / "field_comparison_2025_2026.csv")
    chart_monthly_share(monthly, figs / "monthly_ai_share.png")
    chart_monthly_counts(monthly, figs / "monthly_paper_counts.png")
    chart_field_comparison(fields, figs / "field_ai_share_2025_vs_2026.png")
    return figs


if __name__ == "__main__":
    out = make_charts(sys.argv[1] if len(sys.argv) > 1 else Path(__file__).parent / "reports")
    print("charts written to", out.name)
