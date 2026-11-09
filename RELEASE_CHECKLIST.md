# Checklist bàn giao source

Bản này sẵn sàng để upload thủ công. Chưa commit, chưa push, chưa tạo remote.

## Đã sửa

| File | Lý do |
|---|---|
| `src/run_experiments.py` | UTF-8 khi ghi JSON/CSV/EXPLAIN; `--runs` cho mọi phương pháp kể cả UDF loop; lưu warm-up, số mẫu và mẫu thô; `same()` không ghi đè tên cột; E1/E4 lỗi thì exit 1; `--parallelism`; provenance |
| `src/make_charts.py` | Bỏ `eval`, dùng dict hoặc `ast.literal_eval`; thiếu JSON thì báo lệnh cần chạy; nhãn partitions/threads |
| `src/gen_data.py` | Thêm `--seed` (mặc định 42, cùng luồng số với trước) |
| `requirements.txt` | Ghim numpy 2.5.4 và matplotlib 3.11.2 cùng bốn gói đã kiểm |
| `run_all.ps1` | Chạy PowerShell từ vị trí script, dừng khi lệnh con lỗi, mặc định ghi `results-rerun` |
| `run_all.sh` | Cùng tham số; không được chạy lại trên Linux trong phiên này |
| `tests/test_demo.py` | Đúng kết quả, null, lọc rỗng, `same()`, UTF-8 không cần `PYTHONUTF8`, đủ số mẫu |
| `README.md` | Hướng dẫn Windows và giới hạn diễn đạt |
| `results/` | Thay bộ Linux bằng một lần Windows 5 triệu dòng |
| `.gitignore` | Thêm venv, log, staging, cache |

`run_all.sh` trước đó chỉ lệch mode file, không lệch nội dung, so với commit `70bd3e81e9e4d814387d1682d873eb82e737ff92`. Nội dung bash đã được cập nhật trong lần này.

## Đã chạy

- Cài sạch: `uv venv source_code/.venv --python 3.13.13` rồi `uv pip install -r requirements.txt`. Đủ bản ghim, kể cả numpy 2.5.4 và matplotlib 3.11.2.
- `python -X utf8=0 -m unittest discover -s tests -v` trên venv sạch: 12 tests, OK. Gồm đường dẫn có khoảng trắng và `PYTHONUTF8=0`.
- Full run, cùng interpreter, `PYTHONUTF8=0`, thư mục `staging/final win/`:
  - `gen_data.py --rows 5000000 --seed 42`
  - `run_experiments.py --runs 7 --parallelism 1 2 --seed 42`
  - `make_charts.py`
- E1 Q1–Q5: `vs_pandas` và `vs_duckdb` đều true. Exit code 0.
- Sau đó mới copy sang `source_code/results/`. Provenance ghi HEAD cũ không mô tả cây đã sửa, kèm SHA-256 các file nguồn lúc đo.

## Bộ kết quả

`source_code/results/`, sinh 2026-11-09T15:49:45+07:00. Máy Windows 11, 12 luồng logic, mức song song 1 và 2. Không trộn số Linux.

## Người dùng còn làm tay

- Tạo repository GitHub tên `UIT-IS6002-Team9-DataFusion` bằng tài khoản của nhóm.
- Upload nội dung zip, hoặc commit cây `source_code/` bằng chính mình. Không dùng commit `70bd3e8` như thể nó đã chứa các sửa này.
- Điền URL thật vào báo cáo sau khi repo tồn tại.
- Chọn LICENSE nếu muốn public. Bản này cố ý không thêm LICENSE.
- Sửa PDF theo `REPORT_UPDATE_NOTES.md`.
- Không bắt buộc đổi email placeholder hay trailer của commit cũ.

## Không chặn demo

- Chưa có LICENSE và chưa có owner GitHub.
- `run_all.sh` chưa được chạy trên Linux trong phiên này.
- Một lần đo, warm-cache, không affinity.
- Q3 trên lần Windows này không cùng chiều với bảng Linux cũ. Báo cáo cần nói theo bộ số mới nếu thay bảng.
