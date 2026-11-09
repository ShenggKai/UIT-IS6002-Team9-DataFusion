# Team 9 – Cài đặt minh họa Apache Arrow DataFusion

Mã nguồn minh họa cho đồ án môn **Hệ Cơ Sở Dữ Liệu Tiên Tiến (IS6002.26.1.CH.01, UIT)**, dựa trên bài báo:

> Lamb, A. et al. (2024). *Apache Arrow DataFusion: A Fast, Embeddable, Modular Analytic Query Engine*. SIGMOD-Companion '24. https://doi.org/10.1145/3626246.3653368

Nhóm 9: Đặng Yến Linh · Mai Nguyễn An Bình · Nguyễn Quốc Đạt · Nguyễn Thị Hồng Trúc

## Mục tiêu

Kiểm chứng ở quy mô nhỏ ba nhận định của bài báo, dùng DataFusion qua Python binding:

| Câu hỏi | Liên hệ bài báo | Thí nghiệm |
|---|---|---|
| Truy vấn SQL trên Parquet cho kết quả đúng? Query plan có thể hiện pushdown? | Mục 5, 6.1 | E1, E2 |
| Pruning theo metadata Parquet hiệu quả đến đâu và phụ thuộc gì (thống kê, thứ tự sắp xếp, late materialization)? | Mục 6.8 | E3 |
| Mở rộng bằng UDF tốn bao nhiêu so với hàm built-in? | Mục 7.1 | E4 |
| Hiệu năng so với DuckDB (1 và nhiều thread)? | Mục 8 | E5 |

## Cấu trúc

```
src/gen_data.py         sinh 5 triệu dòng bán hàng giả lập -> 3 file Parquet (sorted / random / sorted-không-thống-kê)
src/run_experiments.py  chạy E1..E5, lưu results/results.json, EXPLAIN, kết quả CSV
src/make_charts.py      vẽ biểu đồ từ results.json
results/                kết quả đã chạy (dùng trong báo cáo)
run_all.sh              chạy toàn bộ
```

## Chạy lại

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
./run_all.sh          # ~1 phút; dữ liệu ~330 MB được sinh vào data/ (không commit)
```

## Kết quả (5.000.000 dòng, 39 row group, 2 vCPU, median 7 lần chạy)

Môi trường: Python 3.13, DataFusion 55.0.0, DuckDB 2.0.0, PyArrow 26.0.1, Linux x86_64, 2 vCPU.

**E1 – Tính đúng:** Q1–Q5 của DataFusion khớp hoàn toàn với baseline pandas và DuckDB.

**E3 – Pruning trên Q2 (lọc 1 tháng, 1 partition):**

| Cấu hình | Thời gian | Row group đọc | Bytes đọc |
|---|---|---|---|
| (a) sorted, có thống kê | 4,7 ms | 2/39 | 0,93 MB |
| (b) sorted, không thống kê | 56,6 ms | 39/39 | 22,8 MB |
| (c) random, có thống kê | 75,9 ms | 39/39 | 29,8 MB |
| (d) random + late materialization | 177,5 ms | 39/39 | 29,8 MB |
| (e) sorted + late materialization | 7,0 ms | 2/39 | 0,93 MB |

**E4 – UDF (Q5 phân hạng đơn hàng, 2 thread):** CASE WHEN built-in 218 ms · Python UDF vector hóa bằng Arrow 383 ms · Python UDF vòng lặp từng dòng 711 ms — cả ba cho cùng kết quả.

**E5 – So với DuckDB:** xem `results/results.json` và `results/fig_e5_vs_duckdb.png`.

> Lưu ý: đây là minh họa trên máy ảo dùng chung 2 vCPU; chênh lệch dưới ~1,5 lần ở mức vài chục ms có thể nằm trong nhiễu đo. Kết quả không nhằm tái lập benchmark của bài báo.
