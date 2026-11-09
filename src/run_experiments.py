"""Cài đặt minh họa - Apache Arrow DataFusion (Lamb et al., SIGMOD-Companion 2024).

Thực hiện 5 thí nghiệm (E1..E5) trên dữ liệu bán hàng giả lập dạng Parquet:
  E1  Tính đúng: so kết quả DataFusion với baseline pandas và DuckDB.
  E2  Quan sát query plan (EXPLAIN): projection / predicate pushdown.
  E3  Pruning (Mục 6.8 bài báo): dữ liệu sorted vs random, bật/tắt pruning.
  E4  Mở rộng bằng UDF (Mục 7.1): Python scalar UDF vs CASE WHEN built-in.
  E5  Hiệu năng so với DuckDB, 1 và nhiều thread (Mục 8, quy mô nhỏ).

Chạy:  python src/run_experiments.py --data data --out results --runs 5
"""
import argparse
import json
import os
import platform
import re
import statistics
import time

import duckdb
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import datafusion
from datafusion import SessionConfig, SessionContext, udf

# --------------------------------------------------------------------------
# Truy vấn workload
# --------------------------------------------------------------------------
QUERIES = {
    # Q1: tổng hợp toàn bảng, ít nhóm (low-cardinality group by)
    "Q1": """SELECT region, COUNT(*) AS n_orders, SUM(amount) AS revenue
             FROM sales GROUP BY region ORDER BY region""",
    # Q2: điều kiện lọc chọn lọc cao trên cột ngày -> pruning
    "Q2": """SELECT category, COUNT(*) AS n_orders, SUM(amount) AS revenue
             FROM sales
             WHERE order_date BETWEEN DATE '2025-03-01' AND DATE '2025-03-31'
             GROUP BY category ORDER BY category""",
    # Q3: group by nhiều nhóm (~500K khách hàng) + Top-K
    "Q3": """SELECT customer_id, SUM(amount) AS total
             FROM sales GROUP BY customer_id
             ORDER BY total DESC, customer_id LIMIT 10""",
    # Q4: lọc + group by 2 cột
    "Q4": """SELECT year, region, AVG(unit_price) AS avg_price, SUM(quantity) AS qty
             FROM sales WHERE category = 'Electronics' AND quantity >= 5
             GROUP BY year, region ORDER BY year, region""",
}

# Q5: logic nghiệp vụ phân hạng đơn hàng - viết bằng UDF và bằng CASE WHEN
TIER_UDF_SQL = """SELECT order_tier(amount) AS tier, COUNT(*) AS n_orders, SUM(amount) AS revenue
                  FROM sales GROUP BY tier ORDER BY tier"""
TIER_CASE_SQL = """SELECT CASE WHEN amount >= 2000 THEN 'A-VIP'
                               WHEN amount >= 500  THEN 'B-High'
                               WHEN amount >= 100  THEN 'C-Medium'
                               ELSE 'D-Low' END AS tier,
                          COUNT(*) AS n_orders, SUM(amount) AS revenue
                   FROM sales GROUP BY tier ORDER BY tier"""


def order_tier_arrow(amount: pa.Array) -> pa.Array:
    """Scalar UDF vector hóa: nhận một Arrow Array, trả về Arrow Array.

    Đây đúng là hợp đồng mà Mục 7.1 bài báo mô tả: hàm nhận ColumnarValue
    (Arrow Array) và trả về ColumnarValue, nên không tốn chi phí chuyển đổi
    định dạng giữa engine và mã người dùng.
    """
    a = pc.fill_null(amount, 0.0)
    return pc.case_when(
        pc.make_struct(pc.greater_equal(a, 2000), pc.greater_equal(a, 500),
                       pc.greater_equal(a, 100)),
        pa.scalar("A-VIP"), pa.scalar("B-High"), pa.scalar("C-Medium"), pa.scalar("D-Low"),
    )


def order_tier_rowwise(amount: pa.Array) -> pa.Array:
    """Phiên bản 'ngây thơ' xử lý từng dòng bằng vòng lặp Python (để so sánh)."""
    out = []
    for v in amount.to_pylist():
        v = v or 0.0
        out.append("A-VIP" if v >= 2000 else "B-High" if v >= 500 else "C-Medium" if v >= 100 else "D-Low")
    return pa.array(out, pa.string())


def make_ctx(path: str, partitions: int, settings: dict | None = None,
             tier_impl=order_tier_arrow) -> SessionContext:
    cfg = SessionConfig().with_target_partitions(partitions)
    for k, v in (settings or {}).items():
        cfg = cfg.set(k, v)
    ctx = SessionContext(cfg)
    ctx.register_parquet("sales", path)
    ctx.register_udf(udf(tier_impl, [pa.float64()], pa.string(), "immutable", name="order_tier"))
    return ctx


def timed(fn, runs: int):
    fn()  # warm-up (đọc metadata, cache hệ điều hành)
    ts = []
    for _ in range(runs):
        t0 = time.perf_counter()
        fn()
        ts.append(time.perf_counter() - t0)
    return statistics.median(ts), ts


# --------------------------------------------------------------------------
# Baseline pandas (tính độc lập, không dùng SQL)
# --------------------------------------------------------------------------
def pandas_baseline(df: pd.DataFrame) -> dict:
    out = {}
    g = df.groupby("region").agg(n_orders=("order_id", "size"), revenue=("amount", "sum")).reset_index()
    out["Q1"] = g.sort_values("region").reset_index(drop=True)
    m = (df.order_date >= pd.Timestamp("2025-03-01").date()) & (df.order_date <= pd.Timestamp("2025-03-31").date())
    g = df[m].groupby("category").agg(n_orders=("order_id", "size"), revenue=("amount", "sum")).reset_index()
    out["Q2"] = g.sort_values("category").reset_index(drop=True)
    g = df.groupby("customer_id").agg(total=("amount", "sum")).reset_index()
    out["Q3"] = g.sort_values(["total", "customer_id"], ascending=[False, True]).head(10).reset_index(drop=True)
    m = (df.category == "Electronics") & (df.quantity >= 5)
    g = df[m].groupby(["year", "region"]).agg(avg_price=("unit_price", "mean"), qty=("quantity", "sum")).reset_index()
    out["Q4"] = g.sort_values(["year", "region"]).reset_index(drop=True)
    tier = np.select([df.amount >= 2000, df.amount >= 500, df.amount >= 100],
                     ["A-VIP", "B-High", "C-Medium"], default="D-Low")
    g = df.assign(tier=tier).groupby("tier").agg(n_orders=("order_id", "size"), revenue=("amount", "sum")).reset_index()
    out["Q5"] = g.sort_values("tier").reset_index(drop=True)
    return out


def same(a: pd.DataFrame, b: pd.DataFrame) -> bool:
    a = a.reset_index(drop=True).copy()
    b = b.reset_index(drop=True).copy()
    b.columns = a.columns
    if a.shape != b.shape:
        return False
    for c in a.columns:
        if pd.api.types.is_float_dtype(a[c]) or pd.api.types.is_float_dtype(b[c]):
            if not np.allclose(a[c].astype(float), b[c].astype(float), rtol=1e-9, atol=1e-6):
                return False
        elif not (a[c].astype(str).values == b[c].astype(str).values).all():
            return False
    return True


def parse_metrics(plan_text: str) -> dict:
    keys = {
        "row_groups": r"row_groups_pruned_statistics=(\d+) total → (\d+) matched",
        "pages": r"page_index_pages_pruned=(\d+) total → (\d+) matched",
        "pushdown_rows_pruned": r"pushdown_rows_pruned=([\d.]+ ?[KM]?)",
        "bytes_scanned": r"bytes_scanned=([\d.]+ ?[KMG]?)",
        "scan_output_rows": r"DataSourceExec.*?output_rows=([\d.]+ ?[KM]?)",
    }
    out = {}
    for k, pat in keys.items():
        m = re.search(pat, plan_text, re.S)
        if m:
            out[k] = m.groups() if len(m.groups()) > 1 else m.group(1)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="data")
    ap.add_argument("--out", default="results")
    ap.add_argument("--runs", type=int, default=5)
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)
    sorted_path = os.path.join(args.data, "sales_sorted.parquet")
    random_path = os.path.join(args.data, "sales_random.parquet")
    ncpu = os.cpu_count()
    report = {"env": {
        "python": platform.python_version(), "datafusion": datafusion.__version__,
        "duckdb": duckdb.__version__, "pyarrow": pa.__version__, "pandas": pd.__version__,
        "cpu_count": ncpu, "machine": platform.machine(), "os": platform.platform(),
        "rows": pq.ParquetFile(sorted_path).metadata.num_rows,
    }}
    print("Environment:", json.dumps(report["env"], indent=1))

    # ---------------- E1: tính đúng ----------------
    print("\n[E1] Correctness")
    ctx = make_ctx(sorted_path, ncpu)
    con = duckdb.connect()
    con.execute(f"CREATE VIEW sales AS SELECT * FROM read_parquet('{sorted_path}')")
    base = pandas_baseline(pq.read_table(sorted_path).to_pandas())
    e1 = {}
    for name, sql in {**QUERIES, "Q5": TIER_UDF_SQL}.items():
        df_res = ctx.sql(sql).to_pandas()
        duck_sql = TIER_CASE_SQL if name == "Q5" else sql
        dk_res = con.execute(duck_sql).df()
        e1[name] = {"rows": len(df_res), "vs_pandas": same(base[name], df_res), "vs_duckdb": same(dk_res, df_res)}
        print(f"  {name}: rows={len(df_res):>3}  DataFusion==pandas: {e1[name]['vs_pandas']}  "
              f"DataFusion==DuckDB: {e1[name]['vs_duckdb']}")
        df_res.to_csv(os.path.join(args.out, f"result_{name}.csv"), index=False)
    report["E1_correctness"] = e1

    # ---------------- E2: EXPLAIN ----------------
    print("\n[E2] Query plan of Q2")
    ctx1 = make_ctx(sorted_path, 1)
    plan = ctx1.sql("EXPLAIN " + QUERIES["Q2"]).to_pylist()
    plan_txt = "\n\n".join(f"{r['plan_type']}\n{r['plan']}" for r in plan)
    with open(os.path.join(args.out, "explain_Q2.txt"), "w") as f:
        f.write(plan_txt)
    print(plan_txt)

    # ---------------- E3: pruning ----------------
    print("\n[E3] Parquet pruning / late materialization on Q2 (1 partition)")
    e3 = []
    nostats_path = os.path.join(args.data, "sales_sorted_nostats.parquet")
    late = {"datafusion.execution.parquet.pushdown_filters": "true"}
    for label, path, settings in [
            ("(a) sorted, có thống kê", sorted_path, None),
            ("(b) sorted, không thống kê", nostats_path, None),
            ("(c) random, có thống kê", random_path, None),
            ("(d) random + late materialization", random_path, late),
            ("(e) sorted + late materialization", sorted_path, late)]:
        c = make_ctx(path, 1, settings)
        med, ts = timed(lambda: c.sql(QUERIES["Q2"]).to_arrow_table(), args.runs)
        analyzed = "\n".join(r["plan"] for r in c.sql("EXPLAIN ANALYZE " + QUERIES["Q2"]).to_pylist())
        tag = label[1]
        with open(os.path.join(args.out, f"explain_analyze_Q2_{tag}.txt"), "w") as f:
            f.write(analyzed)
        m = parse_metrics(analyzed)
        row = {"config": label, "median_ms": round(med * 1000, 2), **{k: str(v) for k, v in m.items()}}
        e3.append(row)
        print("  ", row)
    report["E3_pruning"] = e3

    # ---------------- E4: UDF ----------------
    print("\n[E4] Extensibility: scalar UDF vs built-in CASE WHEN (all cores)")
    e4 = []
    for label, sql, impl in [("Built-in CASE WHEN", TIER_CASE_SQL, order_tier_arrow),
                             ("Python UDF (Arrow vectorized)", TIER_UDF_SQL, order_tier_arrow),
                             ("Python UDF (row-by-row loop)", TIER_UDF_SQL, order_tier_rowwise)]:
        c = make_ctx(sorted_path, ncpu, tier_impl=impl)
        res = c.sql(sql).to_pandas()
        med, _ = timed(lambda: c.sql(sql).to_arrow_table(), max(3, args.runs if "loop" not in label else 3))
        ok = same(base["Q5"], res)
        e4.append({"impl": label, "median_ms": round(med * 1000, 1), "correct": ok})
        print("  ", e4[-1])
    report["E4_udf"] = e4

    # ---------------- E5: DataFusion vs DuckDB ----------------
    print("\n[E5] DataFusion vs DuckDB (median of runs, seconds)")
    e5 = []
    for threads in sorted({1, ncpu}):
        c = make_ctx(sorted_path, threads)
        d = duckdb.connect()
        d.execute(f"SET threads={threads}")
        d.execute(f"CREATE VIEW sales AS SELECT * FROM read_parquet('{sorted_path}')")
        for name, sql in QUERIES.items():
            mdf, _ = timed(lambda: c.sql(sql).to_arrow_table(), args.runs)
            mdk, _ = timed(lambda: d.sql(sql).to_arrow_table(), args.runs)
            row = {"threads": threads, "query": name, "datafusion_s": round(mdf, 4),
                   "duckdb_s": round(mdk, 4), "ratio_df_over_duck": round(mdf / mdk, 2)}
            e5.append(row)
            print("  ", row)
    report["E5_vs_duckdb"] = e5

    with open(os.path.join(args.out, "results.json"), "w") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print(f"\nSaved results to {args.out}/results.json")


if __name__ == "__main__":
    main()
