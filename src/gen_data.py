"""Sinh bộ dữ liệu bán hàng giả lập và lưu thành Parquet.

Tạo 3 file có cùng nội dung:
  - sales_sorted.parquet         : sắp xếp theo order_date (dữ liệu "clustered")
  - sales_random.parquet         : xáo trộn ngẫu nhiên
  - sales_sorted_nostats.parquet : sắp xếp nhưng không ghi thống kê min/max

Mục đích: kiểm chứng nhận định ở Mục 6.8 của bài báo rằng pruning dựa trên
metadata min/max của Parquet hiệu quả nhất khi cột điều kiện được sắp xếp.

Chạy:  python src/gen_data.py --rows 5000000 --out data
"""
import argparse
import os

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

REGIONS = np.array(["North", "South", "Central", "Highlands", "Mekong"])
CATEGORIES = np.array(["Electronics", "Fashion", "Grocery", "Home", "Books", "Sports", "Beauty", "Toys"])


def build_table(n_rows: int, seed: int = 42) -> pa.Table:
    rng = np.random.default_rng(seed)
    # 3 năm dữ liệu: 2023-01-01 .. 2025-12-31
    start = np.datetime64("2023-01-01")
    days = rng.integers(0, 1096, n_rows)
    order_date = (start + days).astype("datetime64[D]")
    quantity = rng.integers(1, 11, n_rows)
    # giá đơn vị phân phối log-normal (đơn vị: nghìn đồng)
    unit_price = np.round(rng.lognormal(mean=4.0, sigma=1.0, size=n_rows), 2)
    amount = np.round(quantity * unit_price, 2)
    return pa.table({
        "order_id": pa.array(np.arange(1, n_rows + 1, dtype=np.int64)),
        "customer_id": pa.array(rng.integers(1, 500_001, n_rows, dtype=np.int64)),
        "region": pa.array(REGIONS[rng.integers(0, len(REGIONS), n_rows)]),
        "category": pa.array(CATEGORIES[rng.integers(0, len(CATEGORIES), n_rows)]),
        "order_date": pa.array(order_date, type=pa.date32()),
        "year": pa.array(order_date.astype("datetime64[Y]").astype(int) + 1970, type=pa.int32()),
        "quantity": pa.array(quantity, type=pa.int32()),
        "unit_price": pa.array(unit_price),
        "amount": pa.array(amount),
    })


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=5_000_000)
    ap.add_argument("--row-group-size", type=int, default=128 * 1024)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default="data")
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)

    table = build_table(args.rows, seed=args.seed)
    sorted_tbl = table.sort_by([("order_date", "ascending")])
    # bản xáo trộn: cùng dữ liệu, thứ tự ngẫu nhiên
    perm = np.random.default_rng(7).permutation(args.rows)
    random_tbl = table.take(pa.array(perm))

    # sales_sorted_nostats: giống sales_sorted nhưng KHÔNG ghi thống kê min/max
    # và page index -> engine không có metadata để bỏ qua dữ liệu.
    for name, t, stats in [("sales_sorted", sorted_tbl, True), ("sales_random", random_tbl, True),
                           ("sales_sorted_nostats", sorted_tbl, False)]:
        path = os.path.join(args.out, f"{name}.parquet")
        pq.write_table(t, path, row_group_size=args.row_group_size, compression="snappy",
                       write_statistics=stats, write_page_index=stats)
        md = pq.ParquetFile(path).metadata
        print(f"{path}: {md.num_rows:,} rows, {md.num_row_groups} row groups, "
              f"{os.path.getsize(path)/1e6:.1f} MB")


if __name__ == "__main__":
    main()
