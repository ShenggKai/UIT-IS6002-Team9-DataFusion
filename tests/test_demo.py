"""Kiểm tra tính đúng, same(), encoding UTF-8 và số mẫu --runs.

Chạy từ thư mục source_code:
  python -m unittest discover -s tests -v
"""
import datetime
import decimal
import os
import pathlib
import subprocess
import sys
import tempfile
import unittest

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import run_experiments as exp  # noqa: E402

ROWS = [
    (1, 101, "North", "Electronics", datetime.date(2025, 2, 28), 2025, 5, 20.0, 100.0),
    (2, 101, "North", "Electronics", datetime.date(2025, 3, 1), 2025, 5, 20.0, 100.0),
    (3, 102, "South", "Books", datetime.date(2025, 3, 15), 2025, 2, 50.0, 100.0),
    (4, 103, "Central", "Home", datetime.date(2025, 3, 31), 2025, 1, 500.0, 500.0),
    (5, 103, "Central", "Home", datetime.date(2025, 4, 1), 2025, 1, 500.0, 500.0),
    (6, 104, "Highlands", "Fashion", datetime.date(2024, 6, 1), 2024, 1, 99.99, 99.99),
    (7, 104, "Highlands", "Fashion", datetime.date(2024, 6, 2), 2024, 1, 100.00, 100.00),
    (8, 105, "Mekong", "Grocery", datetime.date(2024, 6, 3), 2024, 1, 499.99, 499.99),
    (9, 105, "Mekong", "Grocery", datetime.date(2024, 6, 4), 2024, 1, 500.00, 500.00),
    (10, 106, "North", "Sports", datetime.date(2024, 6, 5), 2024, 1, 1999.99, 1999.99),
    (11, 106, "North", "Sports", datetime.date(2024, 6, 6), 2024, 1, 2000.00, 2000.00),
    (12, 107, "South", "Beauty", datetime.date(2024, 6, 7), 2024, 2, 1500.00, 3000.00),
    (13, 108, "North", "Electronics", datetime.date(2023, 5, 1), 2023, 4, 25.0, 100.0),
    (14, 108, "North", "Electronics", datetime.date(2023, 5, 2), 2023, 5, 25.0, 125.0),
    (15, 109, "South", "Electronics", datetime.date(2024, 5, 1), 2024, 6, 30.0, 180.0),
    (16, 109, "South", "Fashion", datetime.date(2024, 5, 2), 2024, 10, 30.0, 300.0),
    (17, 202, "Central", "Toys", datetime.date(2023, 1, 1), 2023, 5, 1000.0, 5000.0),
    (18, 201, "North", "Toys", datetime.date(2023, 1, 2), 2023, 5, 1000.0, 5000.0),
    (19, 203, "Mekong", "Grocery", datetime.date(2023, 1, 3), 2023, 1, 6000.0, 6000.0),
    (20, 204, "Highlands", "Grocery", datetime.date(2023, 1, 4), 2023, 1, 10.0, 10.0),
    (21, 205, "North", "Books", datetime.date(2023, 8, 1), 2023, 2, 50.0, 100.0),
    (22, 205, "South", "Books", datetime.date(2024, 8, 1), 2024, 2, 50.0, 100.0),
    (23, 206, "Central", "Books", datetime.date(2025, 8, 1), 2025, 2, 50.0, 100.0),
    (24, 206, "Highlands", "Books", datetime.date(2025, 3, 10), 2025, 3, 50.0, 150.0),
    (25, 207, "Mekong", "Books", datetime.date(2024, 3, 10), 2024, 3, 50.0, 150.0),
]


def _money(values) -> float:
    total = sum(decimal.Decimal(str(value)) for value in values)
    return float(total)


def expected_frames():
    frame = pd.DataFrame(ROWS, columns=[
        "order_id", "customer_id", "region", "category", "order_date",
        "year", "quantity", "unit_price", "amount",
    ])
    q1 = []
    for region, group in frame.groupby("region", sort=True):
        q1.append({
            "region": region,
            "n_orders": int(len(group)),
            "revenue": _money(group["amount"]),
        })
    q2 = pd.DataFrame([
        {"category": "Books", "n_orders": 2, "revenue": 250.0},
        {"category": "Electronics", "n_orders": 1, "revenue": 100.0},
        {"category": "Home", "n_orders": 1, "revenue": 500.0},
    ])
    totals = {}
    for _, row in frame.iterrows():
        cid = int(row["customer_id"])
        totals[cid] = totals.get(cid, decimal.Decimal("0")) + decimal.Decimal(str(row["amount"]))
    top = sorted(totals.items(), key=lambda item: (-item[1], item[0]))[:10]
    q3 = pd.DataFrame([
        {"customer_id": cid, "total": float(total)} for cid, total in top
    ])
    q4 = pd.DataFrame([
        {"year": 2023, "region": "North", "avg_price": 25.0, "qty": 5},
        {"year": 2024, "region": "South", "avg_price": 30.0, "qty": 6},
        {"year": 2025, "region": "North", "avg_price": 20.0, "qty": 10},
    ])
    q5 = pd.DataFrame([
        {"tier": "A-VIP", "n_orders": 5, "revenue": 21000.0},
        {"tier": "B-High", "n_orders": 4, "revenue": 3499.99},
        {"tier": "C-Medium", "n_orders": 14, "revenue": 2204.99},
        {"tier": "D-Low", "n_orders": 2, "revenue": 109.99},
    ])
    return frame, {
        "Q1": pd.DataFrame(q1),
        "Q2": q2,
        "Q3": q3,
        "Q4": q4,
        "Q5": q5,
    }


def write_handcrafted(path: pathlib.Path) -> None:
    schema = pa.schema([
        ("order_id", pa.int64()),
        ("customer_id", pa.int64()),
        ("region", pa.string()),
        ("category", pa.string()),
        ("order_date", pa.date32()),
        ("year", pa.int32()),
        ("quantity", pa.int32()),
        ("unit_price", pa.float64()),
        ("amount", pa.float64()),
    ])
    table = pa.Table.from_arrays([
        pa.array([row[i] for row in ROWS], type=schema.field(i).type) for i in range(9)
    ], schema=schema)
    pq.write_table(table, path)


class SameTests(unittest.TestCase):
    def setUp(self):
        self.base = pd.DataFrame({
            "region": ["Central", "North"],
            "n_orders": [3, 4],
            "revenue": [5.0e8, 4.0e8],
        })

    def test_rename_fails(self):
        renamed = self.base.rename(columns={"revenue": "doanh_thu"})
        self.assertIsNotNone(exp.frame_mismatch(self.base, renamed))

    def test_count_off_by_one_fails(self):
        bad = self.base.copy()
        bad.loc[0, "n_orders"] += 1
        self.assertIn("integer", exp.frame_mismatch(self.base, bad))

    def test_row_order_fails(self):
        swapped = self.base.iloc[[1, 0]].reset_index(drop=True)
        self.assertIsNotNone(exp.frame_mismatch(self.base, swapped))

    def test_float_inside_tolerance_passes(self):
        near = self.base.copy()
        near.loc[0, "revenue"] += 0.4
        self.assertIsNone(exp.frame_mismatch(self.base, near))

    def test_float_outside_tolerance_fails(self):
        far = self.base.copy()
        far.loc[0, "revenue"] += 1.0
        self.assertIsNotNone(exp.frame_mismatch(self.base, far))

    def test_int32_matches_int64(self):
        other = self.base.copy()
        other["n_orders"] = other["n_orders"].astype("int32")
        self.assertIsNone(exp.frame_mismatch(self.base, other))

    def test_null_matches_null_not_zero(self):
        left = pd.DataFrame({"amount": [1.0, None]})
        right = pd.DataFrame({"amount": [1.0, None]})
        zero = pd.DataFrame({"amount": [1.0, 0.0]})
        self.assertIsNone(exp.frame_mismatch(left, right))
        self.assertIsNotNone(exp.frame_mismatch(left, zero))

    def test_string_does_not_equal_int(self):
        left = pd.DataFrame({"region": ["1"]})
        right = pd.DataFrame({"region": [1]})
        self.assertIsNotNone(exp.frame_mismatch(left, right))

    def test_finish_exits_nonzero(self):
        with self.assertRaises(SystemExit) as caught:
            exp.finish(["E1 Q1 vs pandas: cot"])
        self.assertEqual(caught.exception.code, 1)


class CorrectnessTests(unittest.TestCase):
    def test_queries_null_and_empty(self):
        frame, expected = expected_frames()
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "sales handcrafted.parquet"
            write_handcrafted(path)
            ctx_arrow = exp.make_ctx(str(path), 1, tier_impl=exp.order_tier_arrow)
            ctx_row = exp.make_ctx(str(path), 1, tier_impl=exp.order_tier_rowwise)
            baseline = exp.pandas_baseline(frame)
            for name in ("Q1", "Q2", "Q3", "Q4"):
                got = ctx_arrow.sql(exp.QUERIES[name]).to_pandas()
                self.assertIsNone(exp.frame_mismatch(expected[name], got), name)
                self.assertIsNone(exp.frame_mismatch(expected[name], baseline[name]), name)
            for impl_ctx, sql in (
                (ctx_arrow, exp.TIER_UDF_SQL),
                (ctx_row, exp.TIER_UDF_SQL),
                (ctx_arrow, exp.TIER_CASE_SQL),
            ):
                got = impl_ctx.sql(sql).to_pandas()
                self.assertIsNone(exp.frame_mismatch(expected["Q5"], got))
            self.assertIsNone(exp.frame_mismatch(expected["Q5"], baseline["Q5"]))

            empty_sql = """SELECT category, COUNT(*) AS n_orders, SUM(amount) AS revenue
                           FROM sales
                           WHERE order_date BETWEEN DATE '2026-01-01' AND DATE '2026-01-31'
                           GROUP BY category ORDER BY category"""
            empty = ctx_arrow.sql(empty_sql).to_pandas()
            self.assertEqual(len(empty), 0)
            self.assertEqual(list(empty.columns), ["category", "n_orders", "revenue"])

            null_amount = pa.array([10.0, None], type=pa.float64())
            null_table = pa.table({
                "order_id": pa.array([1, 2], type=pa.int64()),
                "customer_id": pa.array([1, 1], type=pa.int64()),
                "region": pa.array(["North", "North"]),
                "category": pa.array(["A", "A"]),
                "order_date": pa.array([datetime.date(2025, 1, 1)] * 2, type=pa.date32()),
                "year": pa.array([2025, 2025], type=pa.int32()),
                "quantity": pa.array([1, 1], type=pa.int32()),
                "unit_price": null_amount,
                "amount": null_amount,
            })
            null_path = pathlib.Path(tmp) / "sales null.parquet"
            pq.write_table(null_table, null_path)
            arrow_ctx = exp.make_ctx(str(null_path), 1, tier_impl=exp.order_tier_arrow)
            row_ctx = exp.make_ctx(str(null_path), 1, tier_impl=exp.order_tier_rowwise)
            arrow_res = arrow_ctx.sql(exp.TIER_UDF_SQL).to_pandas()
            row_res = row_ctx.sql(exp.TIER_UDF_SQL).to_pandas()
            case_res = arrow_ctx.sql(exp.TIER_CASE_SQL).to_pandas()
            self.assertIsNone(exp.frame_mismatch(arrow_res, row_res))
            self.assertIsNone(exp.frame_mismatch(arrow_res, case_res))
            low = arrow_res[arrow_res["tier"] == "D-Low"].iloc[0]
            self.assertEqual(int(low["n_orders"]), 2)


class EncodingTests(unittest.TestCase):
    def test_explain_unicode_without_pythonutf8(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = pathlib.Path(tmp) / "explain.txt"
            code = (
                "import pathlib, sys\n"
                "sys.path.insert(0, r'%s')\n"
                "import run_experiments as exp\n"
                "exp.write_text(r'%s', 'row_groups 39 total \\u2192 2 matched\\n')\n"
                "print('utf8_mode', sys.flags.utf8_mode)\n"
            ) % (str(ROOT / "src").replace("\\", "\\\\"), str(target).replace("\\", "\\\\"))
            env = os.environ.copy()
            env["PYTHONUTF8"] = "0"
            env.pop("PYTHONIOENCODING", None)
            env["PYTHONDONTWRITEBYTECODE"] = "1"
            proc = subprocess.run(
                [sys.executable, "-X", "utf8=0", "-c", code],
                cwd=ROOT, env=env, text=True, capture_output=True, check=False,
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertIn("utf8_mode 0", proc.stdout)
            self.assertIn("→", target.read_text(encoding="utf-8"))


class RunCountTests(unittest.TestCase):
    def test_runs_apply_to_every_method(self):
        with tempfile.TemporaryDirectory() as tmp:
            data = pathlib.Path(tmp) / "data with spaces"
            out = pathlib.Path(tmp) / "out with spaces"
            env = os.environ.copy()
            env["PYTHONUTF8"] = "0"
            env.pop("PYTHONIOENCODING", None)
            env["PYTHONDONTWRITEBYTECODE"] = "1"
            gen = subprocess.run(
                [sys.executable, "-X", "utf8=0", str(ROOT / "src" / "gen_data.py"),
                 "--rows", "2000", "--row-group-size", "500", "--seed", "42",
                 "--out", str(data)],
                cwd=ROOT, env=env, text=True, capture_output=True, check=False,
            )
            self.assertEqual(gen.returncode, 0, gen.stderr)
            run = subprocess.run(
                [sys.executable, "-X", "utf8=0", str(ROOT / "src" / "run_experiments.py"),
                 "--data", str(data), "--out", str(out), "--runs", "3",
                 "--parallelism", "1", "2", "--seed", "42"],
                cwd=ROOT, env=env, text=True, capture_output=True, check=False,
            )
            self.assertEqual(run.returncode, 0, run.stdout + "\n" + run.stderr)
            import json
            payload = json.loads((out / "results.json").read_text(encoding="utf-8"))
            self.assertEqual(payload["provenance"]["measured_runs"], 3)
            self.assertEqual(payload["provenance"]["warmup_runs"], 1)
            self.assertEqual(payload["env"]["logical_processors"] >= 1, True)
            for row in payload["E3_pruning"]:
                self.assertEqual(len(row["samples_s"]), 3)
            labels = [row["impl"] for row in payload["E4_udf"]]
            self.assertEqual(labels.count("Python UDF (row-by-row loop)"), 2)
            for row in payload["E4_udf"]:
                self.assertEqual(row["measured_runs"], 3)
                self.assertEqual(len(row["samples_s"]), 3)
            for row in payload["E5_vs_duckdb"]:
                self.assertEqual(len(row["datafusion_samples_s"]), 3)
                self.assertEqual(len(row["duckdb_samples_s"]), 3)
            chart = subprocess.run(
                [sys.executable, "-X", "utf8=0", str(ROOT / "src" / "make_charts.py"), str(out)],
                cwd=ROOT, env=env, text=True, capture_output=True, check=False,
            )
            self.assertEqual(chart.returncode, 0, chart.stderr)
            self.assertTrue((out / "fig_e3_pruning.png").is_file())
            self.assertTrue((out / "fig_e5_vs_duckdb.png").is_file())
            missing = subprocess.run(
                [sys.executable, str(ROOT / "src" / "make_charts.py"), str(pathlib.Path(tmp) / "missing")],
                cwd=ROOT, env=env, text=True, capture_output=True, check=False,
            )
            self.assertEqual(missing.returncode, 1)
            self.assertIn("run_experiments.py", missing.stderr)


if __name__ == "__main__":
    unittest.main()
