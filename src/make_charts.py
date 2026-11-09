"""Vẽ biểu đồ từ results/results.json (dùng trong báo cáo)."""
import json
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

res_dir = sys.argv[1] if len(sys.argv) > 1 else "results"
R = json.load(open(f"{res_dir}/results.json"))
plt.rcParams.update({"font.size": 10, "axes.spines.top": False, "axes.spines.right": False})
BLUE, ORANGE, GREY = "#2563eb", "#ea7c1c", "#9ca3af"

# --- Hình E3: pruning ---
e3 = R["E3_pruning"]
labels = [r["config"].replace(", ", ",\n").replace(" + ", "\n+ ") for r in e3]
vals = [r["median_ms"] for r in e3]
fig, ax = plt.subplots(figsize=(7.2, 3.0))
bars = ax.bar(labels, vals, color=[BLUE, GREY, GREY, ORANGE, ORANGE][:len(vals)])
for b, r in zip(bars, e3):
    rg = eval(r["row_groups"]) if r.get("row_groups") else ("?", "?")
    ax.text(b.get_x() + b.get_width() / 2, b.get_height(), f"{b.get_height():.1f} ms\nRG đọc: {rg[1]}/{rg[0]}",
            ha="center", va="bottom", fontsize=8)
ax.set_ylabel("Thời gian Q2 (ms, median)")
ax.set_ylim(0, max(vals) * 1.3)
ax.tick_params(axis="x", labelsize=8)
fig.tight_layout()
fig.savefig(f"{res_dir}/fig_e3_pruning.png", dpi=200)

# --- Hình E5: DataFusion vs DuckDB ---
e5 = R["E5_vs_duckdb"]
threads = sorted({r["threads"] for r in e5})
fig, axes = plt.subplots(1, len(threads), figsize=(7.2, 2.8), sharey=True)
if len(threads) == 1:
    axes = [axes]
for ax, t in zip(axes, threads):
    rows = [r for r in e5 if r["threads"] == t]
    x = range(len(rows))
    ax.bar([i - 0.2 for i in x], [r["datafusion_s"] * 1000 for r in rows], 0.4, label="DataFusion", color=ORANGE)
    ax.bar([i + 0.2 for i in x], [r["duckdb_s"] * 1000 for r in rows], 0.4, label="DuckDB", color=BLUE)
    ax.set_xticks(list(x), [r["query"] for r in rows])
    ax.set_title(f"{t} thread", fontsize=10)
axes[0].set_ylabel("Thời gian (ms, median)")
axes[0].legend(frameon=False, fontsize=8)
fig.tight_layout()
fig.savefig(f"{res_dir}/fig_e5_vs_duckdb.png", dpi=200)
print("charts saved")
