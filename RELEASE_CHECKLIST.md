# Checklist bàn giao mã nguồn và báo cáo

Danh mục dưới đây dùng để kiểm tra lần cuối trước khi đóng gói mã nguồn, công bố repository lên GitHub và nộp báo cáo môn học **Hệ Cơ Sở Dữ Liệu Tiên Tiến (IS6002)**.

## 1. Môi trường và kiểm thử tự động

- [x] File [`requirements.txt`](requirements.txt) đã ghim cố định phiên bản cho cả 6 thư viện sử dụng (`datafusion==55.0.0`, `duckdb==2.0.0`, `pyarrow==26.0.1`, `pandas==3.0.5`, `numpy==2.5.4`, `matplotlib==3.11.2`).
- [x] Bộ kiểm thử đơn vị tại [`tests/test_demo.py`](tests/test_demo.py) chạy thành công toàn bộ 12 bài kiểm thử (`.\.venv\Scripts\python.exe -m unittest discover -s tests -v`), bao gồm kiểm tra hàm đối chiếu kết quả, xử lý giá trị `null`, truy vấn rỗng, ghi file UTF-8 và áp dụng đúng số lần lặp `--runs`.
- [x] Các script chạy tự động [`run_all.ps1`](run_all.ps1) (Windows PowerShell) và [`run_all.sh`](run_all.sh) (Linux/macOS) mặc định xuất kết quả chạy lại vào thư mục `results-rerun/` để tránh ghi đè lên bộ kết quả mẫu trong `results/`.

## 2. Bộ kết quả mẫu đi kèm (`results/`)

- [x] Thư mục [`results/`](results/) chứa trọn vẹn đầu ra của cùng một lần chạy 5.000.000 dòng trên Windows 11 (`measured_at = 2026-11-09T15:49:45+07:00`, `seed = 42`, 1 lần warm-up, 7 mẫu đo ở mức song song 1 và 2):
  - [`results/results.json`](results/results.json): Lưu đầy đủ thông tin nguồn gốc lần chạy (`provenance`), mã băm SHA-256 của các file mã nguồn lúc đo và toàn bộ số liệu của `E1`–`E5`.
  - [`results/result_Q1.csv`](results/result_Q1.csv) đến [`results/result_Q5.csv`](results/result_Q5.csv): Kết quả truy vấn xuất ra CSV.
  - [`results/explain_Q2.txt`](results/explain_Q2.txt) và [`results/explain_analyze_Q2_a.txt`](results/explain_analyze_Q2_a.txt) … [`_e.txt`](results/explain_analyze_Q2_e.txt): Kế hoạch truy vấn và chỉ số thực thi chi tiết của `Q2`.
  - [`results/fig_e3_pruning.png`](results/fig_e3_pruning.png) và [`results/fig_e5_vs_duckdb.png`](results/fig_e5_vs_duckdb.png): Biểu đồ được vẽ trực tiếp từ `results/results.json`.
- [x] File [`.gitignore`](.gitignore) đã loại trừ các thư mục sinh tạm (`data/`, `data-small/`, `results-small/`, `results-rerun/`, `.venv/`, `__pycache__/`).

## 3. Công việc hoàn thiện trước khi nộp bài

- [ ] **Cập nhật báo cáo PDF:** Đối chiếu và thay thế các bảng số liệu (`E3`, `E4`, `E5`), thông số môi trường đo cùng hai hình biểu đồ trong báo cáo môn học theo hướng dẫn tại [`REPORT_UPDATE_NOTES.md`](REPORT_UPDATE_NOTES.md).
- [ ] **Tạo và đẩy lên GitHub repository:**
  - Khởi tạo repository trên tài khoản GitHub của nhóm (tên đề xuất: `UIT-IS6002-Team9-DataFusion`).
  - Đẩy mã nguồn và bộ kết quả mẫu trong `source_code/` lên nhánh chính.
  - Điền đường dẫn URL chính thức của repository (`https://github.com/<owner>/UIT-IS6002-Team9-DataFusion`) vào báo cáo môn học.
- [ ] **Chọn giấy phép mã nguồn (tùy chọn):** Bổ sung file `LICENSE` (ví dụ MIT hoặc Apache-2.0) nếu nhóm dự định để repository ở chế độ công khai (*public*).
- [ ] **Kiểm tra gói nén bàn giao (nếu nộp kèm file `.zip`):** Đảm bảo gói nén chỉ chứa mã nguồn, tài liệu và thư mục `results/`, không chứa môi trường ảo `.venv/` hay dữ liệu Parquet tạm trong `data/`.
