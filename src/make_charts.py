"""Vẽ biểu đồ từ results.json của cùng một lần chạy. Không gắn số đo cố định."""
import ast
import json
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def log(msg: str) -> None:
    text = msg + "\n"
    encoding = getattr(sys.stderr, "encoding", None) or "utf-8"
    data = text.encode(encoding, errors="backslashreplace")
    buffer = getattr(sys.stderr, "buffer", None)
    if buffer is not None:
        buffer.write(data)
        buffer.flush()
    else:
        sys.stderr.write(data.decode(encoding, errors="replace"))


def load_results(res_dir: Path) -> dict:
    path = res_dir / "results.json"
    if not path.is_file():
        log(
            f"Khong thay {path}. Hay chay truoc: "
            "python src/run_experiments.py --data <thu-muc-parquet> --out "
            f"{res_dir}"
        )
        raise SystemExit(1)
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def row_group_pair(row: dict) -> tuple[int, int]:
    value = row.get("row_groups")
    if isinstance(value, dict) and {"total", "matched"} <= set(value):
        return int(value["total"]), int(value["matched"])
    if isinstance(value, (list, tuple)) and len(value) == 2:
        return int(value[0]), int(value[1])
    if isinstance(value, str):
        parsed = ast.literal_eval(value)
        if isinstance(parsed, (list, tuple)) and len(parsed) == 2:
            return int(parsed[0]), int(parsed[1])
    raise SystemExit(f"FAIL row_groups khong dung cau truc trong results.json: {value!r}")


def main() -> None:
    res_dir = Path(sys.argv[1] if len(sys.argv) > 1 else "results")
    payload = load_results(res_dir)
    plt.rcParams.update({"font.size": 10, "axes.spines.top": False, "axes.spines.right": False})
    blue, orange, grey = "#2563eb", "#ea7c1c", "#9ca3af"

    e3 = payload["E3_pruning"]
    labels = [row["config"].replace(", ", ",\n").replace(" + ", "\n+ ") for row in e3]
    values = [row["median_ms"] for row in e3]
    fig, ax = plt.subplots(figsize=(7.2, 3.2))
    bars = ax.bar(labels, values, color=[blue, grey, grey, orange, orange][:len(values)])
    for bar, row in zip(bars, e3):
        total, matched = row_group_pair(row)
        ax.text(
            bar.get_x() + bar.get_width() / 2, bar.get_height(),
            f"{bar.get_height():.1f} ms\nRG doc: {matched}/{total}",
            ha="center", va="bottom", fontsize=8,
        )
    ax.set_ylabel("Thoi gian Q2 (ms, median)")
    ax.set_ylim(0, max(values) * 1.35)
    ax.tick_params(axis="x", labelsize=8)
    ax.set_title("E3, target_partitions=1 (khong phai so loi bi khoa)")
    fig.tight_layout()
    fig.savefig(res_dir / "fig_e3_pruning.png", dpi=200)
    plt.close(fig)

    e5 = payload["E5_vs_duckdb"]
    levels = []
    for row in e5:
        level = row.get("parallelism", row.get("threads"))
        if level not in levels:
            levels.append(level)
    fig, axes = plt.subplots(1, len(levels), figsize=(7.4, 3.2), sharey=True)
    if len(levels) == 1:
        axes = [axes]
    for ax, level in zip(axes, levels):
        rows = [row for row in e5 if row.get("parallelism", row.get("threads")) == level]
        xpos = range(len(rows))
        ax.bar(
            [i - 0.2 for i in xpos],
            [row["datafusion_s"] * 1000.0 for row in rows],
            0.4, label="DataFusion partitions", color=orange,
        )
        ax.bar(
            [i + 0.2 for i in xpos],
            [row["duckdb_s"] * 1000.0 for row in rows],
            0.4, label="DuckDB threads", color=blue,
        )
        ax.set_xticks(list(xpos), [row["query"] for row in rows])
        ax.set_title(f"muc song song {level}", fontsize=10)
    axes[0].set_ylabel("Thoi gian (ms, median)")
    axes[0].legend(frameon=False, fontsize=7)
    fig.suptitle(
        "N = DataFusion target_partitions va DuckDB threads, khong phai CPU affinity",
        fontsize=8,
    )
    fig.tight_layout()
    fig.savefig(res_dir / "fig_e5_vs_duckdb.png", dpi=200)
    plt.close(fig)
    log(f"charts saved in {res_dir}")


if __name__ == "__main__":
    main()
