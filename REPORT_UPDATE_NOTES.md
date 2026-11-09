# Ghi chú cập nhật báo cáo môn học

PDF trong `docs/` không được sửa bởi phiên này. Bộ số dưới đây lấy từ `results/results.json` của lần chạy 2026-11-09T15:49:45+07:00.

Môi trường của bộ số này: Windows 11 (`10.0.26300`), Python 3.13.13, DataFusion 55.0.0, DuckDB 2.0.0, PyArrow 26.0.1, pandas 3.0.5, NumPy 2.5.4, 12 luồng logic, RAM khoảng 16,83 GB. Seed 42, 5.000.000 dòng, 1 lần warm-up, 7 mẫu cho mọi phương pháp. Mức song song 1 và 2 là `target_partitions` / `threads`, không phải máy 2 vCPU và không phải CPU affinity.

Tỷ lệ E5 là trường `ratio_df_over_duck`, tính từ median giây chưa làm tròn. Hình cùng lần chạy:

![E3](results/fig_e3_pruning.png)

![E5](results/fig_e5_vs_duckdb.png)

## E3 – Q2, target_partitions = 1, median 7 mẫu

| Cấu hình | Median (ms) | Row group matched/total | bytes_scanned |
|---|---:|---:|---|
| (a) sorted, có thống kê | 7,666 | 2/39 | 934,7 K |
| (b) sorted, không thống kê | 65,786 | 39/39 | 22,76 M |
| (c) random, có thống kê | 101,003 | 39/39 | 29,81 M |
| (d) random + late materialization | 165,477 | 39/39 | 29,81 M |
| (e) sorted + late materialization | 6,073 | 2/39 | 934,7 K |

Page index của (a) và (e): 14 total, 8 matched, tức loại 6/14 trang. `bytes_scanned` là chỉ số trong `EXPLAIN ANALYZE`.

## E4 – Q5, 7 mẫu sau warm-up

| Cách cài | Median ms, partitions=1 | Median ms, partitions=2 |
|---|---:|---:|
| CASE WHEN | 318,860 | 128,072 |
| UDF Arrow | 312,971 | 191,219 |
| UDF vòng lặp | 869,223 | 755,080 |

Ở partitions=2, UDF Arrow / CASE ≈ 1,49 và vòng lặp / CASE ≈ 5,90, tính từ hai median trong JSON. Ở partitions=1, UDF Arrow không chậm hơn CASE trên lần đo này (312,971 so với 318,860).

## E5 – median giây và tỷ lệ DF/DuckDB

| Query | DF 1 | DuckDB 1 | Tỷ lệ | DF 2 | DuckDB 2 | Tỷ lệ |
|---|---:|---:|---:|---:|---:|---:|
| Q1 | 0,07036 | 0,06292 | 1,118 | 0,03927 | 0,03883 | 1,011 |
| Q2 | 0,00485 | 0,00736 | 0,660 | 0,00478 | 0,00618 | 0,773 |
| Q3 | 0,25075 | 0,26428 | 0,949 | 0,16259 | 0,24113 | 0,674 |
| Q4 | 0,08985 | 0,06128 | 1,466 | 0,05084 | 0,03853 | 1,320 |

Từ partitions 1 lên 2, wall-time DataFusion của Q3 giảm khoảng 1,54 lần (0,25075 / 0,16259) và Q4 khoảng 1,77 lần (0,08985 / 0,05084).

## Câu cần thay trong báo cáo

Bỏ các số Linux 2 vCPU và các tỷ lệ gắn với chúng: E3 4,7 / 56,6 / 75,9 / 177,5 / 7,0 ms; E4 218 / 383 / 711 ms và các hệ số 1,8×, 3,3×; E5 bảng cũ (82/67, 6,0/7,3, 405/266, 118/52 và cột 2 thread tương ứng); câu tăng tốc 1,9×.

Thay bằng bảng ở trên, và các ý sau:

- Lần đo mẫu chạy trên Windows 11, 12 luồng logic. Cột mức song song là `target_partitions` và `threads`, không phải máy 2 vCPU.
- UDF vòng lặp cũng lấy median của 7 mẫu sau 1 warm-up, cùng `--runs` với CASE và UDF Arrow. Bản mã cũ chỉ đo vòng lặp 3 lần; bản này không còn nhánh đó.
- Trên file sorted, DuckDB cũng chỉ đọc 2/39 row group ở Q2 trong lần kiểm `EXPLAIN ANALYZE` trước đó. Không viết DataFusion nhanh hơn ở Q2 vì chỉ mình nó pruning. Trên lần chạy này Q2 của DataFusion có median thấp hơn (tỷ lệ 0,660 và 0,773).
- File random chậm hơn file sorted không thống kê (101,0 ms so với 65,8 ms) đi cùng `bytes_scanned` 29,81 M so với 22,76 M và cột ngày nén kém hơn. Chưa chứng minh I/O đĩa là nguyên nhân chính. Dữ liệu được đo sau warm-up.
- Q3 trên lần này DataFusion không chậm hơn DuckDB (tỷ lệ 0,949 và 0,674). Q4 DataFusion chậm hơn (1,466 và 1,320). Không kết luận một engine luôn nhanh hơn.
- “Extension có hiệu năng như built-in” trong bài báo không được minh họa bằng UDF Python. Ở partitions=2 UDF Arrow chậm hơn CASE; ở partitions=1 lần này thì không.

URL repo, khi đã có owner: `https://github.com/<owner>/UIT-IS6002-Team9-DataFusion`. Chưa điền owner.
