"""Cài đặt minh họa - Apache Arrow DataFusion (Lamb et al., SIGMOD-Companion 2024).

Năm thí nghiệm trên dữ liệu bán hàng giả lập dạng Parquet:
  E1  Tính đúng so với baseline pandas và DuckDB.
  E2  EXPLAIN: projection / predicate pushdown.
  E3  Pruning Parquet (mục 6.8): sorted, random, có hoặc không có thống kê.
  E4  UDF Python so với CASE WHEN (mục 7.1).
  E5  So với DuckDB tại các mức song song đã chọn (mục 8).

Mức song song N có nghĩa là DataFusion target_partitions=N và DuckDB threads=N.
Đây không phải CPU affinity và không phải giới hạn cứng N lõi.

Chạy:
  python src/run_experiments.py --data data --out results --runs 7 --parallelism 1 2
"""
import argparse
import hashlib
import json
import os
import platform
import re
import statistics
import sys
import time
from datetime import datetime

import duckdb
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import datafusion
from datafusion import SessionConfig, SessionContext, udf

# Sai số đã công bố trong báo cáo: tương đối 1e-9, cộng thêm dung sai tuyệt đối 1e-6
# để một khác biệt làm tròn rất nhỏ trên tổng lớn không bị rtol phóng đại,
# và một lệch tuyệt đối ~1e-6 trên giá trị nhỏ vẫn bị chặn bởi atol.
FLOAT_RTOL = 1e-9
FLOAT_ATOL = 1e-6
WARMUP_RUNS = 1

QUERIES = {
    "Q1": """SELECT region, COUNT(*) AS n_orders, SUM(amount) AS revenue
             FROM sales GROUP BY region ORDER BY region""",
    "Q2": """SELECT category, COUNT(*) AS n_orders, SUM(amount) AS revenue
             FROM sales
             WHERE order_date BETWEEN DATE '2025-03-01' AND DATE '2025-03-31'
             GROUP BY category ORDER BY category""",
    "Q3": """SELECT customer_id, SUM(amount) AS total
             FROM sales GROUP BY customer_id
             ORDER BY total DESC, customer_id LIMIT 10""",
    "Q4": """SELECT year, region, AVG(unit_price) AS avg_price, SUM(quantity) AS qty
             FROM sales WHERE category = 'Electronics' AND quantity >= 5
             GROUP BY year, region ORDER BY year, region""",
}

TIER_UDF_SQL = """SELECT order_tier(amount) AS tier, COUNT(*) AS n_orders, SUM(amount) AS revenue
                  FROM sales GROUP BY tier ORDER BY tier"""
TIER_CASE_SQL = """SELECT CASE WHEN amount >= 2000 THEN 'A-VIP'
                               WHEN amount >= 500  THEN 'B-High'
                               WHEN amount >= 100  THEN 'C-Medium'
                               ELSE 'D-Low' END AS tier,
                          COUNT(*) AS n_orders, SUM(amount) AS revenue
                   FROM sales GROUP BY tier ORDER BY tier"""


def log(msg: str = "") -> None:
    """In tiến trình. Không làm sập chương trình khi console là cp1252."""
    text = msg + "\n"
    encoding = getattr(sys.stdout, "encoding", None) or "utf-8"
    data = text.encode(encoding, errors="backslashreplace")
    buffer = getattr(sys.stdout, "buffer", None)
    if buffer is not None:
        buffer.write(data)
        buffer.flush()
    else:
        sys.stdout.write(data.decode(encoding, errors="replace"))


def write_text(path: str, text: str) -> None:
    with open(path, "w", encoding="utf-8", newline="\n") as handle:
        handle.write(text)


def read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as handle:
        return handle.read()


def write_json(path: str, payload) -> None:
    with open(path, "w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)
        handle.write("\n")


def order_tier_arrow(amount: pa.Array) -> pa.Array:
    """UDF theo cột: nhận và trả Arrow Array (hợp đồng mục 7.1)."""
    filled = pc.fill_null(amount, 0.0)
    return pc.case_when(
        pc.make_struct(
            pc.greater_equal(filled, 2000),
            pc.greater_equal(filled, 500),
            pc.greater_equal(filled, 100),
        ),
        pa.scalar("A-VIP"),
        pa.scalar("B-High"),
        pa.scalar("C-Medium"),
        pa.scalar("D-Low"),
    )


def order_tier_rowwise(amount: pa.Array) -> pa.Array:
    """Cùng quy tắc phân hạng, nhưng vòng lặp Python từng dòng."""
    out = []
    for value in amount.to_pylist():
        if value is None:
            value = 0.0
        out.append(
            "A-VIP" if value >= 2000
            else "B-High" if value >= 500
            else "C-Medium" if value >= 100
            else "D-Low"
        )
    return pa.array(out, pa.string())


def make_ctx(path: str, partitions: int, settings: dict | None = None,
             tier_impl=order_tier_arrow) -> SessionContext:
    cfg = SessionConfig().with_target_partitions(partitions)
    for key, value in (settings or {}).items():
        cfg = cfg.set(key, value)
    ctx = SessionContext(cfg)
    ctx.register_parquet("sales", path)
    ctx.register_udf(udf(
        tier_impl, [pa.float64()], pa.string(), "immutable", name="order_tier",
    ))
    return ctx


def timed(fn, runs: int):
    """Một lần warm-up, rồi `runs` mẫu. Median tính từ các mẫu, không từ EXPLAIN."""
    if runs < 1:
        raise ValueError("--runs phải là số nguyên dương")
    fn()
    samples = []
    for _ in range(runs):
        start = time.perf_counter()
        fn()
        samples.append(time.perf_counter() - start)
    return statistics.median(samples), samples


def pandas_baseline(df: pd.DataFrame) -> dict:
    """Baseline không dùng SQL. Tên cột trùng alias của QUERIES / TIER_CASE_SQL."""
    out = {}
    grouped = df.groupby("region", sort=False).agg(
        n_orders=("order_id", "size"), revenue=("amount", "sum"),
    ).reset_index()
    out["Q1"] = grouped.sort_values("region", kind="mergesort").reset_index(drop=True)
    mask = (
        (df.order_date >= pd.Timestamp("2025-03-01").date())
        & (df.order_date <= pd.Timestamp("2025-03-31").date())
    )
    grouped = df[mask].groupby("category", sort=False).agg(
        n_orders=("order_id", "size"), revenue=("amount", "sum"),
    ).reset_index()
    out["Q2"] = grouped.sort_values("category", kind="mergesort").reset_index(drop=True)
    grouped = df.groupby("customer_id", sort=False).agg(total=("amount", "sum")).reset_index()
    out["Q3"] = grouped.sort_values(
        ["total", "customer_id"], ascending=[False, True], kind="mergesort",
    ).head(10).reset_index(drop=True)
    mask = (df.category == "Electronics") & (df.quantity >= 5)
    grouped = df[mask].groupby(["year", "region"], sort=False).agg(
        avg_price=("unit_price", "mean"), qty=("quantity", "sum"),
    ).reset_index()
    out["Q4"] = grouped.sort_values(["year", "region"], kind="mergesort").reset_index(drop=True)
    tier = np.select(
        [df.amount >= 2000, df.amount >= 500, df.amount >= 100],
        ["A-VIP", "B-High", "C-Medium"],
        default="D-Low",
    )
    grouped = df.assign(tier=tier).groupby("tier", sort=False).agg(
        n_orders=("order_id", "size"), revenue=("amount", "sum"),
    ).reset_index()
    out["Q5"] = grouped.sort_values("tier", kind="mergesort").reset_index(drop=True)
    return out


def _na_mask(series: pd.Series) -> np.ndarray:
    return series.isna().to_numpy()


def _integer_like(series: pd.Series) -> bool:
    if pd.api.types.is_bool_dtype(series):
        return False
    if pd.api.types.is_integer_dtype(series):
        return True
    if pd.api.types.is_float_dtype(series):
        values = series.to_numpy(dtype=float, na_value=np.nan)
        finite = values[~np.isnan(values)]
        if finite.size == 0:
            return True
        return bool(np.all(np.floor(finite) == finite))
    return False


def frame_mismatch(left: pd.DataFrame, right: pd.DataFrame) -> str | None:
    """None nếu hai kết quả khớp. Không đổi tên cột và không sắp xếp lại dòng.

    Số nguyên so đúng từng giá trị. Số thực dùng rtol=1e-9 và atol=1e-6:
    |a-b| <= atol + rtol*|b|. Null chỉ khớp null. int64 và int32 được coi là
    cùng nghĩa nếu giá trị bằng nhau.
    """
    left = left.reset_index(drop=True)
    right = right.reset_index(drop=True)
    if len(left) != len(right):
        return f"so dong {len(left)} va {len(right)}"
    if list(left.columns) != list(right.columns):
        return f"cot {list(left.columns)} va {list(right.columns)}"
    for name in left.columns:
        a = left[name]
        b = right[name]
        if not np.array_equal(_na_mask(a), _na_mask(b)):
            return f"null khac nhau o cot {name}"
        if pd.api.types.is_bool_dtype(a) or pd.api.types.is_bool_dtype(b):
            if not a.astype("boolean").equals(b.astype("boolean")):
                return f"bool khac nhau o cot {name}"
            continue
        if _integer_like(a) and _integer_like(b) and not (
            pd.api.types.is_float_dtype(a) and pd.api.types.is_float_dtype(b)
            and not (pd.api.types.is_integer_dtype(a) or pd.api.types.is_integer_dtype(b))
        ):
            # Cả hai là số nguyên về nghĩa (kể cả float không có phần thập phân
            # khi bên kia là integer). Hai cột float thì đi nhánh allclose.
            if pd.api.types.is_float_dtype(a) and pd.api.types.is_float_dtype(b):
                pass
            else:
                av = pd.to_numeric(a, errors="coerce").to_numpy(dtype=float)
                bv = pd.to_numeric(b, errors="coerce").to_numpy(dtype=float)
                if np.isnan(av).any() or np.isnan(bv).any():
                    return f"gia tri khong phai so o cot {name}"
                if not np.array_equal(av, bv):
                    return f"integer khac nhau o cot {name}"
                continue
        if pd.api.types.is_float_dtype(a) or pd.api.types.is_float_dtype(b):
            av = pd.to_numeric(a, errors="coerce").to_numpy(dtype=float)
            bv = pd.to_numeric(b, errors="coerce").to_numpy(dtype=float)
            ok = np.allclose(av, bv, rtol=FLOAT_RTOL, atol=FLOAT_ATOL, equal_nan=True)
            if not ok:
                return f"float vuot nguong rtol={FLOAT_RTOL} atol={FLOAT_ATOL} o cot {name}"
            continue
        a_text = pd.api.types.is_string_dtype(a) or a.dtype == object
        b_text = pd.api.types.is_string_dtype(b) or b.dtype == object
        if a_text and b_text:
            if a.astype("string").equals(b.astype("string")):
                continue
            return f"gia tri khac nhau o cot {name}"
        if pd.api.types.is_datetime64_any_dtype(a) or pd.api.types.is_datetime64_any_dtype(b):
            if pd.to_datetime(a).equals(pd.to_datetime(b)):
                continue
            return f"thoi gian khac nhau o cot {name}"
        if not a.equals(b):
            return f"gia tri khac nhau o cot {name}"
    return None


def same(left: pd.DataFrame, right: pd.DataFrame) -> bool:
    return frame_mismatch(left, right) is None


def finish(failures: list[str]) -> None:
    if not failures:
        return
    for item in failures:
        log("FAIL " + item)
    raise SystemExit(1)


def parse_metrics(plan_text: str) -> dict:
    patterns = {
        "row_groups": r"row_groups_pruned_statistics=(\d+) total → (\d+) matched",
        "pages": r"page_index_pages_pruned=(\d+) total → (\d+) matched",
        "pushdown_rows_pruned": r"pushdown_rows_pruned=([\d.]+ ?[KM]?)",
        "bytes_scanned": r"bytes_scanned=([\d.]+ ?[KMG]?)",
        "scan_output_rows": r"DataSourceExec.*?output_rows=([\d.]+ ?[KM]?)",
    }
    parsed = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, plan_text, re.S)
        if not match:
            continue
        if len(match.groups()) > 1:
            parsed[key] = {"total": int(match.group(1)), "matched": int(match.group(2))}
        else:
            parsed[key] = match.group(1)
    return parsed


def parse_parallelism(values: list[int] | None, logical: int) -> list[int]:
    if not values:
        return [1, 2] if logical >= 2 else [1]
    chosen = []
    for value in values:
        if value < 1:
            raise SystemExit("FAIL --parallelism chi nhan so nguyen duong")
        if value not in chosen:
            chosen.append(value)
    return chosen


def _timing_block(median_s: float, samples: list[float], runs: int) -> dict:
    return {
        "warmup_runs": WARMUP_RUNS,
        "measured_runs": runs,
        "median_s": median_s,
        "median_ms": median_s * 1000.0,
        "samples_s": samples,
        "samples_ms": [item * 1000.0 for item in samples],
    }


def _file_sha256(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        digest.update(handle.read())
    return digest.hexdigest()


def _memory_bytes() -> dict:
    if os.name != "nt":
        return {}
    import ctypes

    class MEMORYSTATUSEX(ctypes.Structure):
        _fields_ = [
            ("dwLength", ctypes.c_ulong),
            ("dwMemoryLoad", ctypes.c_ulong),
            ("ullTotalPhys", ctypes.c_ulonglong),
            ("ullAvailPhys", ctypes.c_ulonglong),
            ("ullTotalPageFile", ctypes.c_ulonglong),
            ("ullAvailPageFile", ctypes.c_ulonglong),
            ("ullTotalVirtual", ctypes.c_ulonglong),
            ("ullAvailVirtual", ctypes.c_ulonglong),
            ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
        ]

    status = MEMORYSTATUSEX()
    status.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
    if not ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
        return {}
    return {
        "ram_total_bytes": int(status.ullTotalPhys),
        "ram_available_bytes": int(status.ullAvailPhys),
    }


def _source_hashes() -> dict:
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
    names = [
        os.path.join("src", "gen_data.py"),
        os.path.join("src", "run_experiments.py"),
        os.path.join("src", "make_charts.py"),
        "requirements.txt",
    ]
    found = {}
    for name in names:
        path = os.path.join(root, name)
        if os.path.isfile(path):
            found[name.replace("\\", "/")] = _file_sha256(path)
    return found


def _git_note() -> dict:
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
    try:
        import subprocess
        head = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=root, text=True, capture_output=True, check=False,
        )
        status = subprocess.run(
            ["git", "status", "--porcelain"], cwd=root, text=True, capture_output=True, check=False,
        )
    except OSError:
        return {}
    head_text = (head.stdout or "").strip()
    dirty = bool((status.stdout or "").strip())
    return {
        "git_head": head_text or None,
        "git_worktree_dirty": dirty,
        "git_head_describes_measured_source": bool(head_text) and not dirty,
    }


def build_provenance(args, logical: int, levels: list[int], n_rows: int) -> dict:
    return {
        "measured_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "command": sys.argv,
        "python": platform.python_version(),
        "datafusion": datafusion.__version__,
        "duckdb": duckdb.__version__,
        "pyarrow": pa.__version__,
        "pandas": pd.__version__,
        "numpy": np.__version__,
        "os": platform.platform(),
        "machine": platform.machine(),
        "logical_processors": logical,
        "parallelism_levels": levels,
        "parallelism_meaning": (
            "DataFusion target_partitions=N va DuckDB threads=N. "
            "Khong phai CPU affinity va khong phai gioi han cung N loi."
        ),
        "seed": args.seed,
        "shuffle_seed": 7,
        "rows": n_rows,
        "warmup_runs": WARMUP_RUNS,
        "measured_runs": args.runs,
        "float_rtol": FLOAT_RTOL,
        "float_atol": FLOAT_ATOL,
        "bytes_scanned_meaning": (
            "Chi so bytes_scanned do engine bao cao trong EXPLAIN ANALYZE, "
            "khong phai so byte vat ly da doc tu dia."
        ),
        "cache": "Cac mau do sau 1 lan warm-up; du lieu nho co the nam trong OS cache.",
        **_git_note(),
        "source_sha256": _source_hashes(),
        **_memory_bytes(),
    }


def register_duckdb(con, path: str, threads: int) -> None:
    con.execute(f"SET threads={int(threads)}")
    # Dấu nháy đơn bọc đường dẫn. Backslash của Windows đã được kiểm chứng với DuckDB 2.0.
    escaped = path.replace("'", "''")
    con.execute(f"CREATE VIEW sales AS SELECT * FROM read_parquet('{escaped}')")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default="data")
    parser.add_argument("--out", default="results")
    parser.add_argument("--runs", type=int, default=7)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--parallelism", type=int, nargs="+", default=None,
        help="Cac muc N: DataFusion target_partitions va DuckDB threads. Mac dinh 1 va 2.",
    )
    args = parser.parse_args()
    if args.runs < 1:
        raise SystemExit("FAIL --runs phai la so nguyen duong")

    os.makedirs(args.out, exist_ok=True)
    sorted_path = os.path.join(args.data, "sales_sorted.parquet")
    random_path = os.path.join(args.data, "sales_random.parquet")
    nostats_path = os.path.join(args.data, "sales_sorted_nostats.parquet")
    logical = os.cpu_count() or 1
    levels = parse_parallelism(args.parallelism, logical)
    n_rows = pq.ParquetFile(sorted_path).metadata.num_rows
    provenance = build_provenance(args, logical, levels, n_rows)
    report = {"provenance": provenance, "env": {
        "python": provenance["python"],
        "datafusion": provenance["datafusion"],
        "duckdb": provenance["duckdb"],
        "pyarrow": provenance["pyarrow"],
        "pandas": provenance["pandas"],
        "numpy": provenance["numpy"],
        "logical_processors": logical,
        "parallelism_levels": levels,
        "os": provenance["os"],
        "rows": n_rows,
        "seed": args.seed,
        "warmup_runs": WARMUP_RUNS,
        "measured_runs": args.runs,
    }}
    log("Environment: " + json.dumps(report["env"], ensure_ascii=True))

    log("\n[E1] Correctness")
    e1_parts = max(levels)
    ctx = make_ctx(sorted_path, e1_parts)
    con = duckdb.connect()
    register_duckdb(con, sorted_path, e1_parts)
    base = pandas_baseline(pq.read_table(sorted_path).to_pandas())
    e1 = {}
    failures = []
    for name, sql in {**QUERIES, "Q5": TIER_UDF_SQL}.items():
        df_res = ctx.sql(sql).to_pandas()
        duck_sql = TIER_CASE_SQL if name == "Q5" else sql
        dk_res = con.execute(duck_sql).df()
        pandas_reason = frame_mismatch(base[name], df_res)
        duck_reason = frame_mismatch(dk_res, df_res)
        e1[name] = {
            "rows": len(df_res),
            "vs_pandas": pandas_reason is None,
            "vs_duckdb": duck_reason is None,
        }
        if pandas_reason:
            failures.append(f"E1 {name} vs pandas: {pandas_reason}")
        if duck_reason:
            failures.append(f"E1 {name} vs duckdb: {duck_reason}")
        log(
            f"  {name}: rows={len(df_res)} vs_pandas={e1[name]['vs_pandas']} "
            f"vs_duckdb={e1[name]['vs_duckdb']}"
        )
        df_res.to_csv(
            os.path.join(args.out, f"result_{name}.csv"),
            index=False, encoding="utf-8", lineterminator="\n",
        )
    report["E1_correctness"] = e1
    if failures:
        report["E1_failures"] = failures
        write_json(os.path.join(args.out, "results.json"), report)
        finish(failures)

    log("\n[E2] Query plan of Q2")
    ctx1 = make_ctx(sorted_path, 1)
    plan = ctx1.sql("EXPLAIN " + QUERIES["Q2"]).to_pylist()
    plan_txt = "\n\n".join(f"{row['plan_type']}\n{row['plan']}" for row in plan)
    write_text(os.path.join(args.out, "explain_Q2.txt"), plan_txt + "\n")
    log(plan_txt)

    log("\n[E3] Parquet pruning on Q2 (target_partitions=1)")
    e3 = []
    late = {"datafusion.execution.parquet.pushdown_filters": "true"}
    configs = [
        ("(a) sorted, co thong ke", "(a) sorted, có thống kê", sorted_path, None),
        ("(b) sorted, khong thong ke", "(b) sorted, không thống kê", nostats_path, None),
        ("(c) random, co thong ke", "(c) random, có thống kê", random_path, None),
        ("(d) random + late materialization", "(d) random + late materialization", random_path, late),
        ("(e) sorted + late materialization", "(e) sorted + late materialization", sorted_path, late),
    ]
    for ascii_label, label, path, settings in configs:
        scan = make_ctx(path, 1, settings)
        median_s, samples = timed(lambda: scan.sql(QUERIES["Q2"]).to_arrow_table(), args.runs)
        analyzed = "\n".join(
            row["plan"] for row in scan.sql("EXPLAIN ANALYZE " + QUERIES["Q2"]).to_pylist()
        )
        tag = label[1]
        write_text(os.path.join(args.out, f"explain_analyze_Q2_{tag}.txt"), analyzed + "\n")
        metrics = parse_metrics(analyzed)
        row = {
            "config": label,
            "target_partitions": 1,
            **_timing_block(median_s, samples, args.runs),
            **metrics,
        }
        e3.append(row)
        log("  " + ascii_label + " " + json.dumps(
            {"median_ms": row["median_ms"], **{k: metrics[k] for k in metrics}},
            ensure_ascii=True,
        ))
    report["E3_pruning"] = e3

    log("\n[E4] UDF vs CASE WHEN")
    e4 = []
    impls = [
        ("Built-in CASE WHEN", TIER_CASE_SQL, order_tier_arrow),
        ("Python UDF (Arrow vectorized)", TIER_UDF_SQL, order_tier_arrow),
        ("Python UDF (row-by-row loop)", TIER_UDF_SQL, order_tier_rowwise),
    ]
    for partitions in levels:
        for label, sql, impl in impls:
            scan = make_ctx(sorted_path, partitions, tier_impl=impl)
            result = scan.sql(sql).to_pandas()
            reason = frame_mismatch(base["Q5"], result)
            if reason:
                failures.append(f"E4 {label} partitions={partitions}: {reason}")
            median_s, samples = timed(lambda: scan.sql(sql).to_arrow_table(), args.runs)
            row = {
                "impl": label,
                "target_partitions": partitions,
                "correct": reason is None,
                **_timing_block(median_s, samples, args.runs),
            }
            e4.append(row)
            log(
                f"  partitions={partitions} {label} median_ms={row['median_ms']:.3f} "
                f"n={len(samples)} correct={row['correct']}"
            )
    report["E4_udf"] = e4
    if failures:
        report["E4_failures"] = failures
        write_json(os.path.join(args.out, "results.json"), report)
        finish(failures)

    log("\n[E5] DataFusion vs DuckDB")
    e5 = []
    for threads in levels:
        scan = make_ctx(sorted_path, threads)
        duck = duckdb.connect()
        register_duckdb(duck, sorted_path, threads)
        for name, sql in QUERIES.items():
            df_median, df_samples = timed(lambda: scan.sql(sql).to_arrow_table(), args.runs)
            dk_median, dk_samples = timed(lambda: duck.sql(sql).to_arrow_table(), args.runs)
            ratio = df_median / dk_median
            row = {
                "parallelism": threads,
                "datafusion_target_partitions": threads,
                "duckdb_threads": threads,
                "query": name,
                "datafusion_s": df_median,
                "duckdb_s": dk_median,
                "ratio_df_over_duck": ratio,
                "warmup_runs": WARMUP_RUNS,
                "measured_runs": args.runs,
                "datafusion_samples_s": df_samples,
                "duckdb_samples_s": dk_samples,
            }
            e5.append(row)
            log(
                f"  parallelism={threads} {name} "
                f"datafusion_s={df_median:.6f} duckdb_s={dk_median:.6f} ratio={ratio:.6f}"
            )
    report["E5_vs_duckdb"] = e5
    report["notes"] = {
        "q2_pruning": (
            "Tren file sorted, ca DataFusion va DuckDB deu co the bo row group. "
            "Chenh thoi gian Q2 khong duoc giai thich bang viec chi DataFusion co pruning."
        ),
        "random_layout": (
            "File random co bytes_scanned va do nen cot order_date khac file sorted. "
            "bytes_scanned la chi so engine, khong chung minh I/O dia la nguyen nhan chinh."
        ),
        "scope": "So lieu chi mo ta may, workload va cau hinh da do trong provenance.",
    }

    write_json(os.path.join(args.out, "results.json"), report)
    log(f"\nSaved results to {args.out}/results.json")


if __name__ == "__main__":
    main()
