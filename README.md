# Cài đặt minh họa và Thực nghiệm Apache Arrow DataFusion (Nhóm 9)

Repository này chứa mã nguồn thực nghiệm và bộ kết quả mẫu cho đồ án môn **Hệ Cơ Sở Dữ Liệu Tiên Tiến (IS6002.26.1.CH.01)** tại Trường Đại học Công nghệ Thông tin – ĐHQG-HCM.

- **Thực hiện:** Nhóm 9 — Đặng Yến Linh · Mai Nguyễn An Bình · Nguyễn Quốc Đạt · Nguyễn Thị Hồng Trúc
- **Bài báo gốc:** Lamb, A. et al. (2024). *Apache Arrow DataFusion: A Fast, Embeddable, Modular Analytic Query Engine*. Companion of the 2024 International Conference on Management of Data (SIGMOD-Companion '24), 579–590. https://doi.org/10.1145/3626246.3653368

## 1. Giới thiệu đề tài

Apache Arrow DataFusion là một công cụ thực thi truy vấn phân tích (*analytic query engine*) khả nhúng, được viết bằng Rust và xây dựng trực tiếp trên định dạng bộ nhớ cột Apache Arrow. Trong bài báo tại SIGMOD 2024, nhóm tác giả trình bày kiến trúc mô-đun của DataFusion cùng ba đặc trưng kỹ thuật quan trọng: khả năng đẩy các phép chiếu và bộ lọc xuống tầng đọc dữ liệu Parquet (*pushdown* và *pruning*), khả năng mở rộng linh hoạt thông qua các hàm tự định nghĩa (*user-defined functions* – UDF) xử lý theo lô cột (*columnar batch*), và hiệu năng xử lý cạnh tranh với các hệ thống phân tích hiện đại trên dữ liệu định dạng cột.

Dự án này sử dụng thư viện Python binding của DataFusion (`datafusion`) để xây dựng một bộ thực nghiệm tự khép kín ở quy mô 5 triệu dòng dữ liệu bán hàng giả lập. Mục tiêu của đồ án không phải là tái lập lại các bộ benchmark quy mô lớn (như TPC-H hay ClickBench) trong bài báo gốc, mà là minh họa trực quan và kiểm chứng thực nghiệm cách các cơ chế của DataFusion vận hành thông qua kế hoạch thực thi truy vấn, chỉ số quét dữ liệu và thời gian phản hồi thực tế.

## 2. Nội dung thực nghiệm

### Cấu trúc mã nguồn

```text
.
├── src/
│   ├── gen_data.py          # Sinh dữ liệu bán hàng giả lập ra 3 biến thể file Parquet
│   ├── run_experiments.py   # Thực thi các thí nghiệm E1–E5, xuất kết quả CSV, EXPLAIN và JSON
│   └── make_charts.py       # Vẽ biểu đồ tổng hợp cho thí nghiệm E3 và E5 từ results.json
├── tests/
│   └── test_demo.py         # Bộ kiểm thử đơn vị (unit tests) cho hàm đối chiếu và luồng thực thi
├── results/                 # Bộ kết quả mẫu (5 triệu dòng, chạy trên Windows 11)
├── requirements.txt         # Danh sách các thư viện Python đã ghim phiên bản
├── run_all.ps1              # Script tự động hóa toàn bộ quy trình trên Windows PowerShell
├── run_all.sh               # Script tự động hóa tương đương cho môi trường Linux/macOS
├── REPORT_UPDATE_NOTES.md   # Bảng số liệu đối chiếu và gợi ý diễn giải cho báo cáo môn học
└── RELEASE_CHECKLIST.md     # Danh mục kiểm tra trước khi đóng gói và bàn giao
```

### Bộ dữ liệu giả lập và 3 biến thể Parquet

Script `src/gen_data.py` khởi tạo một bảng giao dịch bán hàng gồm 9 cột (`order_id`, `customer_id`, `region`, `category`, `order_date`, `year`, `quantity`, `unit_price`, `amount`) trải đều trong 3 năm từ `2023-01-01` đến `2025-12-31`. Ở cấu hình đầy đủ (5.000.000 dòng, kích thước mỗi row group là 131.072 dòng, nén Snappy), dữ liệu được chia thành 39 row group và xuất thành ba file Parquet có cùng tập bản ghi nhưng khác cách tổ chức vật lý và siêu dữ liệu (*metadata*):

1. `sales_sorted.parquet`: Các dòng được sắp xếp tăng dần theo ngày đặt hàng (`order_date`), đồng thời ghi đầy đủ thống kê `min/max` ở mức row group và chỉ mục trang (*page index*).
2. `sales_random.parquet`: Các dòng bị xáo trộn ngẫu nhiên (`shuffle_seed = 7`), vẫn ghi đầy đủ thống kê `min/max` và chỉ mục trang.
3. `sales_sorted_nostats.parquet`: Giữ nguyên thứ tự sắp xếp theo `order_date` như file thứ nhất nhưng tắt việc ghi thống kê `min/max` và chỉ mục trang (`write_statistics=False`, `write_page_index=False`).

Cách thiết kế ba file này cho phép tách bạch ảnh hưởng của việc có siêu dữ liệu thống kê với ảnh hưởng của thứ tự sắp xếp vật lý khi thực hiện cắt tỉa dữ liệu (*pruning*).

### Các câu truy vấn phân tích (`Q1`–`Q5`)

Bộ thực nghiệm sử dụng 5 câu truy vấn đại diện cho các mẫu tác vụ phân tích phổ biến:

- **`Q1` (Tổng hợp toàn bảng theo vùng):** Đếm số đơn hàng (`COUNT(*)`) và tính tổng doanh thu (`SUM(amount)`) theo từng vùng (`region`).
- **`Q2` (Lọc theo khoảng thời gian hẹp):** Tính số đơn và doanh thu theo nhóm mặt hàng (`category`) chỉ trong tháng 3/2025 (`order_date BETWEEN DATE '2025-03-01' AND DATE '2025-03-31'`). Đây là truy vấn có độ chọn lọc cao trên cột ngày, được dùng làm tâm điểm để đánh giá cơ chế pruning.
- **`Q3` (Gom nhóm lực lượng lớn và lấy Top-N):** Gom nhóm theo `customer_id` (500.000 mã khách hàng), tính tổng chi tiêu và lấy ra 10 khách hàng có tổng chi tiêu lớn nhất (`ORDER BY total DESC, customer_id LIMIT 10`).
- **`Q4` (Lọc đa điều kiện và tổng hợp nhiều chiều):** Lọc các đơn hàng thuộc nhóm `'Electronics'` có `quantity >= 5`, sau đó tính đơn giá trung bình (`AVG(unit_price)`) và tổng số lượng (`SUM(quantity)`) theo từng cặp `(year, region)`.
- **`Q5` (Phân hạng đơn hàng và gom nhóm):** Phân loại từng đơn hàng vào 4 hạng giá trị (`A-VIP` khi `amount >= 2000`, `B-High` khi `amount >= 500`, `C-Medium` khi `amount >= 100`, và `D-Low` cho các trường hợp còn lại), rồi đếm số đơn và cộng doanh thu theo từng hạng.

### Năm thí nghiệm (`E1`–`E5`) và liên hệ với bài báo

Script `src/run_experiments.py` thực hiện tuần tự 5 thí nghiệm:

| Ký hiệu | Tên thí nghiệm | Liên hệ bài báo | Nội dung thực hiện |
|---|---|---|---|
| **E1** | Kiểm tra tính đúng đắn (*Correctness*) | Mục 5 | Thực thi `Q1`–`Q5` bằng DataFusion và đối chiếu kết quả với hai chuẩn tham chiếu độc lập là `pandas` và `DuckDB`. Các cột số nguyên và chuỗi phải khớp tuyệt đối; các cột số thực được kiểm tra với dung sai tương đối `rtol = 1e-9` và dung sai tuyệt đối `atol = 1e-6`. |
| **E2** | Kế hoạch truy vấn (*Query Plan*) | Mục 5, 6.1 | Xuất kế hoạch logic (`logical_plan`) và kế hoạch vật lý (`physical_plan`) của `Q2` bằng lệnh `EXPLAIN` để kiểm tra cơ chế chỉ đọc các cột cần thiết (*projection pushdown*) và đẩy điều kiện lọc xuống tầng đọc Parquet (*predicate pushdown*). |
| **E3** | Cắt tỉa dữ liệu Parquet (*Parquet Pruning*) | Mục 6.8 | Đo thời gian chạy `Q2` (ở cấu hình 1 phân vùng thực thi) và phân tích chỉ số từ `EXPLAIN ANALYZE` trên 5 cấu hình `(a)`–`(e)`: kết hợp giữa 3 biến thể file Parquet và tùy chọn *late materialization* (`datafusion.execution.parquet.pushdown_filters = true`). |
| **E4** | Mở rộng bằng UDF so với biểu thức tích hợp | Mục 7.1 | So sánh thời gian thực thi `Q5` giữa 3 cách cài đặt: (1) biểu thức SQL `CASE WHEN` tích hợp sẵn, (2) hàm vô hướng Python UDF xử lý vector hóa trên mảng `pyarrow.Array`, và (3) Python UDF duyệt từng dòng bằng vòng lặp Python thuần. |
| **E5** | So sánh hiệu năng với DuckDB | Mục 8 | Đo và so sánh thời gian thực thi `Q1`–`Q4` trên `sales_sorted.parquet` giữa DataFusion và DuckDB ở các mức độ song song khác nhau (mặc định là `1` và `2`). |

## 3. Yêu cầu môi trường và cài đặt

Dự án được thiết kế và kiểm thử với **Python 3.13** (phiên bản kiểm chứng: `3.13.13` trên Windows 11, PowerShell). Toàn bộ các thư viện phụ thuộc đã được ghim phiên bản cụ thể trong [`requirements.txt`](requirements.txt) để đảm bảo tính nhất quán khi tái lập:

| Thư viện | Phiên bản | Vai trò trong thực nghiệm |
|---|---|---|
| `datafusion` | `55.0.0` | Engine thực thi truy vấn phân tích chính được khảo sát |
| `duckdb` | `2.0.0` | Engine phân tích đối chứng về tính đúng đắn (`E1`) và thời gian thực thi (`E5`) |
| `pyarrow` | `26.0.1` | Tạo dữ liệu Parquet, đọc metadata và xử lý mảng cột trong UDF vector hóa (`E4`) |
| `pandas` | `3.0.5` | Tính toán kết quả chuẩn tham chiếu độc lập không qua SQL (`E1`) |
| `numpy` | `2.5.4` | Sinh dữ liệu ngẫu nhiên có hạt giống (`seed`) và đối chiếu sai số mảng số thực |
| `matplotlib` | `3.11.2` | Vẽ biểu đồ tổng hợp kết quả thực nghiệm `E3` và `E5` |

### Cài đặt trên Windows PowerShell

Khuyến nghị sử dụng công cụ [`uv`](https://docs.astral.sh/uv/) để tạo môi trường ảo `.venv` đúng phiên bản Python 3.13 và cài đặt gói nhanh chóng:

```powershell
cd <thu-muc-source_code>
uv venv .venv --python 3.13.13
uv pip install --python .\.venv\Scripts\python.exe -r requirements.txt
```

Nếu không dùng `uv`, bạn có thể tạo môi trường ảo bằng Python 3.13 sẵn có trên máy (`py -3.13 -m venv .venv`) rồi cài đặt qua `.\.venv\Scripts\python.exe -m pip install -r requirements.txt`.

Trong các bước tiếp theo, các lệnh đều gọi trực tiếp trình thông dịch `.\.venv\Scripts\python.exe` để đảm bảo sử dụng đúng môi trường ảo của dự án (tránh gọi nhầm vào alias `python.exe` mặc định của thư mục `WindowsApps` trên Windows). Mã nguồn đã chỉ định rõ bảng mã UTF-8 khi đọc/ghi mọi file văn bản (JSON, CSV, EXPLAIN) nên không cần cấu hình thêm biến môi trường hệ thống.

## 4. Chạy thử nhanh và kiểm thử tự động

Trước khi sinh bộ dữ liệu 5 triệu dòng, bạn nên chạy bộ kiểm thử tự động và thử nghiệm quy trình trên một tập dữ liệu nhỏ (100.000 dòng) để xác nhận môi trường đã hoạt động ổn định.

### Chạy bộ kiểm thử đơn vị (Unit tests)

File [`tests/test_demo.py`](tests/test_demo.py) kiểm tra 4 nhóm chức năng: (1) hàm đối chiếu kết quả `frame_mismatch` trước các tình huống lệch tên cột, lệch thứ tự dòng, vượt ngưỡng sai số số thực hoặc nhầm lẫn giữa `null` và `0`; (2) tính đúng đắn của `Q1`–`Q5` và các biến thể UDF trên bộ dữ liệu kiểm thử 25 dòng có kết quả tính tay, kèm trường hợp truy vấn trả về tập rỗng và dữ liệu chứa `null`; (3) khả năng ghi ký tự Unicode trong kết quả `EXPLAIN` ngay cả khi tắt chế độ UTF-8 mặc định của Python; và (4) việc áp dụng đúng số lần lặp `--runs` cho mọi phương pháp trong luồng chạy đầu–cuối.

Chạy lệnh kiểm thử từ thư mục gốc của mã nguồn:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

Khi thành công, cả 12 bài kiểm thử đều hiển thị trạng thái `ok` và kết thúc bằng dòng `OK` (mã thoát bằng `0`).

### Chạy thử quy trình với dữ liệu nhỏ (100.000 dòng)

Bộ lệnh dưới đây sinh dữ liệu vào thư mục `data-small/` và xuất kết quả vào `results-small/` trong vài giây:

```powershell
.\.venv\Scripts\python.exe src\gen_data.py --rows 100000 --row-group-size 8192 --seed 42 --out data-small
.\.venv\Scripts\python.exe src\run_experiments.py --data data-small --out results-small --runs 3 --parallelism 1 2 --seed 42
.\.venv\Scripts\python.exe src\make_charts.py results-small
```

Chế độ chạy nhanh này dùng để kiểm tra toàn bộ đường ống xử lý từ khâu sinh file Parquet, thực thi truy vấn đến khâu vẽ biểu đồ. Vì khối lượng dữ liệu chỉ bằng 1/50 quy mô chuẩn, thời gian thực thi của lần chạy thử này không dùng để đánh giá hiệu năng hay đối chiếu với báo cáo.

## 5. Chạy bộ thực nghiệm đầy đủ (5 triệu dòng)

Để tái lập bộ thực nghiệm quy mô 5.000.000 dòng như trong báo cáo, bạn có thể chạy script tự động [`run_all.ps1`](run_all.ps1) trên Windows PowerShell. Script mặc định ghi kết quả mới vào thư mục `results-rerun/` để bạn dễ dàng so sánh mà không ghi đè lên bộ kết quả mẫu có sẵn trong `results/`:

```powershell
.\run_all.ps1 -Rows 5000000 -Runs 7 -Parallelism 1,2 -Data data -Out results-rerun
```

Nếu muốn chạy từng bước riêng lẻ (hoặc tùy chỉnh tham số), thực hiện lần lượt 3 lệnh sau:

```powershell
.\.venv\Scripts\python.exe src\gen_data.py --rows 5000000 --seed 42 --out data
.\.venv\Scripts\python.exe src\run_experiments.py --data data --out results-rerun --runs 7 --parallelism 1 2 --seed 42
.\.venv\Scripts\python.exe src\make_charts.py results-rerun
```

Trên môi trường Linux hoặc macOS, bạn có thể kích hoạt môi trường ảo và chạy script tương đương `./run_all.sh`.

**Giải thích các tham số chính:**
- `--rows` (trong `gen_data.py`): Số dòng giao dịch cần sinh (mặc định `5000000`, tạo ra khoảng 330 MB tổng dung lượng cho 3 file Parquet trong `data/`).
- `--row-group-size` (trong `gen_data.py`): Số dòng tối đa trong mỗi row group (mặc định `131072`, tạo ra 39 row group cho 5 triệu dòng).
- `--runs` (trong `run_experiments.py`): Số lần đo chính thức sau 1 lần chạy khởi động (*warm-up*). Chương trình áp dụng cùng giá trị `--runs` (mặc định `7`) cho mọi phương pháp, bao gồm cả UDF vòng lặp từng dòng, và lấy giá trị trung vị (*median*) để giảm nhiễu.
- `--parallelism` (trong `run_experiments.py`): Danh sách các mức độ song song `N` cần khảo sát (mặc định `1 2` trên máy có từ 2 luồng xử lý trở lên). Với mỗi mức `N`, chương trình đặt `target_partitions = N` cho DataFusion và `threads = N` cho DuckDB.
- `--seed`: Hạt giống bộ sinh số ngẫu nhiên (mặc định `42`) giúp dữ liệu sinh ra hoàn toàn giống nhau giữa các lần chạy.

Trong quá trình chạy `run_experiments.py`, nếu bất kỳ truy vấn nào ở `E1` hoặc `E4` trả về kết quả lệch với chuẩn tham chiếu, chương trình sẽ ghi nhận chi tiết lỗi vào `results.json`, in thông báo `FAIL` ra màn hình và dừng lại với mã thoát `1`.

## 6. Đầu ra và cách đọc bộ kết quả mẫu

### Danh mục các file đầu ra

Thư mục [`results/`](results/) lưu sẵn bộ kết quả mẫu của nhóm (và mọi thư mục chỉ định qua `--out` khi chạy lại cũng có cấu trúc tương tự):

| Đường dẫn | Nội dung và ý nghĩa |
|---|---|
| [`results/results.json`](results/results.json) | File tổng hợp toàn bộ thực nghiệm: thông tin nguồn gốc lần chạy (`provenance`, `env`), kết quả đối chiếu tính đúng (`E1_correctness`), cùng thời gian trung vị, toàn bộ mẫu đo thô và chỉ số của `E3_pruning`, `E4_udf`, `E5_vs_duckdb`. |
| [`results/result_Q1.csv`](results/result_Q1.csv) … [`result_Q5.csv`](results/result_Q5.csv) | Bảng kết quả đầu ra của DataFusion cho từng truy vấn `Q1`–`Q5`. |
| [`results/explain_Q2.txt`](results/explain_Q2.txt) | Kế hoạch logic và vật lý của `Q2` (thí nghiệm `E2`), minh họa `projection=[category, order_date, amount]` và biểu thức `pruning_predicate`. |
| [`results/explain_analyze_Q2_a.txt`](results/explain_analyze_Q2_a.txt) … [`_e.txt`](results/explain_analyze_Q2_e.txt) | Kế hoạch thực thi kèm chỉ số đo thực tế (`EXPLAIN ANALYZE`) của 5 cấu hình `(a)`–`(e)` trong thí nghiệm `E3`. Các lệnh này được gọi tách biệt sau khi đã hoàn tất phép đo thời gian trung vị. |
| [`results/fig_e3_pruning.png`](results/fig_e3_pruning.png) | Biểu đồ cột so sánh thời gian chạy và số row group phải đọc của 5 cấu hình trong thí nghiệm `E3`. |
| [`results/fig_e5_vs_duckdb.png`](results/fig_e5_vs_duckdb.png) | Biểu đồ cột ghép so sánh thời gian trung vị của DataFusion và DuckDB trên `Q1`–`Q4` ở mức song song 1 và 2. |

### Phân tích bộ kết quả mẫu trong `results/`

Bộ kết quả mẫu đi kèm được thu thập trên máy tính chạy **Windows 11 (`10.0.26300`, kiến trúc AMD64), bộ xử lý 6 nhân / 12 luồng logic (`logical_processors = 12`), RAM khoảng 16,8 GB**, với bộ dữ liệu chuẩn 5.000.000 dòng (39 row group), 1 lần chạy khởi động (*warm-up*) và 7 lần đo lấy trung vị. Toàn bộ thông số phần cứng, phiên bản thư viện, câu lệnh đã chạy và mã băm SHA-256 của mã nguồn tại thời điểm đo được lưu tự động trong trường `provenance` của [`results/results.json`](results/results.json).

#### Hiệu quả cắt tỉa dữ liệu Parquet trên `Q2` (Thí nghiệm `E3`)

![Kết quả thực nghiệm E3 trên bộ mẫu Windows](results/fig_e3_pruning.png)

Biểu đồ trên thể hiện thời gian thực thi trung vị (tính bằng mili-giây) và tỷ lệ row group thực tế phải đọc (`RG đọc: matched/total`) của truy vấn lọc theo tháng `Q2` khi cấu hình `target_partitions = 1`:

| Cấu hình | Thời gian trung vị (ms) | Row group đọc / tổng | Trang đọc / tổng (Page Index) | Dữ liệu quét (`bytes_scanned`) | Số dòng ra khỏi tầng quét |
|---|---:|---:|---:|---:|---:|
| **(a) sorted, có thống kê** | 7,67 | 2 / 39 | 8 / 14 | 934,7 KB | 151,1 K |
| **(b) sorted, không thống kê** | 65,79 | 39 / 39 | — | 22,76 MB | 5,00 M |
| **(c) random, có thống kê** | 101,00 | 39 / 39 | 267 / 267 | 29,81 MB | 5,00 M |
| **(d) random + late materialization** | 165,48 | 39 / 39 | 267 / 267 | 29,81 MB | 141,2 K |
| **(e) sorted + late materialization** | 6,07 | 2 / 39 | 8 / 14 | 934,7 KB | 141,2 K |

- **Vai trò của thống kê và thứ tự sắp xếp:** Ở cấu hình `(a)`, nhờ dữ liệu được sắp xếp theo `order_date` và có lưu thống kê `min/max`, DataFusion loại bỏ được 37/39 row group ngay từ bước kiểm tra metadata, sau đó tiếp tục dùng chỉ mục trang (*page index*) để bỏ thêm 6/14 trang trong 2 row group còn lại. Nhờ đó, lượng dữ liệu quét (`bytes_scanned`) chỉ còn 934,7 KB (thay vì 22,76 MB ở cấu hình `(b)` khi tắt thống kê) và thời gian chạy giảm từ 65,79 ms xuống 7,67 ms (nhanh hơn khoảng 8,6 lần).
- **Vì sao có thống kê nhưng dữ liệu xáo trộn vẫn không cắt tỉa được:** Ở cấu hình `(c)`, dù file Parquet có đầy đủ thống kê `min/max`, các ngày trong giai đoạn 2023–2025 bị phân tán ngẫu nhiên vào mọi row group. Khoảng `[min, max]` của cả 39 row group đều bao phủ tháng 3/2025, buộc engine phải đọc toàn bộ 39/39 row group và 267/267 trang. Thời gian của `(c)` cao hơn `(b)` (101,00 ms so với 65,79 ms) đi kèm với lượng `bytes_scanned` lớn hơn (29,81 MB so với 22,76 MB) do cột `order_date` khi bị xáo trộn có khả năng nén kém hơn so với khi các ngày giống nhau nằm liền kề.
- **Tác động của *late materialization* (`pushdown_filters = true`):** Khi dữ liệu đã sắp xếp ở `(e)`, việc đẩy bộ lọc vào ngay quá trình đọc Parquet giúp lọc bỏ thêm 9,88 nghìn dòng ở biên trang và giảm thời gian trung vị xuống còn 6,07 ms. Ngược lại, trên dữ liệu xáo trộn ở `(d)`, mặc dù `pushdown_filters` giúp giảm số dòng đầu ra khỏi `DataSourceExec` từ 5,00 triệu xuống 141,2 nghìn dòng (loại được 4,86 triệu dòng), nhưng vì các dòng thỏa điều kiện nằm rải rác ở hầu hết các trang, chi phí đánh giá bộ lọc và cắt trang trên toàn bộ 39 row group khiến tổng thời gian tăng lên 165,48 ms.

#### So sánh hàm tự định nghĩa Python UDF và `CASE WHEN` trên `Q5` (Thí nghiệm `E4`)

| Phương pháp cài đặt | Trung vị ở `target_partitions = 1` (ms) | Trung vị ở `target_partitions = 2` (ms) |
|---|---:|---:|
| **Biểu thức SQL tích hợp (`Built-in CASE WHEN`)** | 318,86 | 128,07 |
| **Python UDF xử lý vector hóa (`Arrow vectorized`)** | 312,97 | 191,22 |
| **Python UDF vòng lặp từng dòng (`row-by-row loop`)** | 869,22 | 755,08 |

- **Lợi ích của UDF vector hóa theo cột Arrow:** Khi viết UDF bằng Python nhưng thao tác trực tiếp trên mảng `pyarrow.Array` thông qua các hàm tính toán của PyArrow (`order_tier_arrow`), thời gian thực thi nhanh hơn gấp khoảng 2,8 lần (ở 1 partition) đến 3,9 lần (ở 2 partitions) so với việc chuyển mảng sang danh sách Python và lặp qua từng dòng (`order_tier_rowwise`).
- **So sánh với biểu thức `CASE WHEN` nội tại:** Ở `target_partitions = 1`, thời gian của UDF Arrow và `CASE WHEN` xấp xỉ nhau (312,97 ms và 318,86 ms). Tuy nhiên, khi tăng mức phân vùng lên `target_partitions = 2`, biểu thức `CASE WHEN` chạy thuần trong nhân Rust tăng tốc mạnh xuống còn 128,07 ms (nhanh hơn UDF Arrow khoảng 1,49 lần). Điều này phản ánh chi phí chuyển đổi ngữ cảnh giữa các luồng thực thi Rust của DataFusion và trình thông dịch Python (cùng khóa GIL của Python) khi gọi Python UDF song song.

#### So sánh thời gian thực thi với DuckDB trên `Q1`–`Q4` (Thí nghiệm `E5`)

![Kết quả thực nghiệm E5 trên bộ mẫu Windows](results/fig_e5_vs_duckdb.png)

Biểu đồ trên đặt cạnh nhau thời gian trung vị (ms) của DataFusion (`DataFusion partitions`) và DuckDB (`DuckDB threads`) cho 4 truy vấn `Q1`–`Q4` ở hai mức độ song song 1 và 2:

| Truy vấn | DataFusion — mức 1 (s) | DuckDB — mức 1 (s) | Tỷ lệ DF/Duck (mức 1) | DataFusion — mức 2 (s) | DuckDB — mức 2 (s) | Tỷ lệ DF/Duck (mức 2) |
|---|---:|---:|---:|---:|---:|---:|
| **Q1** | 0,07036 | 0,06292 | 1,118 | 0,03927 | 0,03883 | 1,011 |
| **Q2** | 0,00485 | 0,00736 | 0,660 | 0,00478 | 0,00618 | 0,773 |
| **Q3** | 0,25075 | 0,26428 | 0,949 | 0,16259 | 0,24113 | 0,674 |
| **Q4** | 0,08985 | 0,06128 | 1,466 | 0,05084 | 0,03853 | 1,320 |

- Khi nâng mức độ song song từ 1 lên 2, thời gian thực thi của DataFusion cải thiện rõ rệt trên các truy vấn nặng về quét và gom nhóm: giảm khoảng 1,79 lần ở `Q1`, 1,54 lần ở `Q3` và 1,77 lần ở `Q4`.
- Giữa hai hệ thống, tương quan nhanh/chậm phụ thuộc vào đặc điểm của từng truy vấn và cấu hình song song. Trên bộ dữ liệu và môi trường đo này, DataFusion đạt thời gian thấp hơn ở `Q2` (nhanh hơn khoảng 1,3 đến 1,5 lần) và `Q3` (nhanh hơn nhẹ ở mức 1 và nhanh hơn khoảng 1,48 lần ở mức 2), trong khi DuckDB nhanh hơn ở `Q4` và nhỉnh hơn nhẹ ở `Q1` khi chạy đơn luồng.

### Các điều kiện thực nghiệm cần lưu ý khi diễn giải kết quả

Để hiểu đúng ý nghĩa khoa học của các con số trên, người đọc cần đặt chúng trong ngữ cảnh thiết lập thực nghiệm của đồ án:

1. **Ý nghĩa của tham số mức song song `N`:** Trong thí nghiệm `E4` và `E5`, mức song song `N` là cấu hình ở tầng engine (`target_partitions = N` đối với DataFusion và `threads = N` đối với DuckDB). Hai tham số này điều khiển cách mỗi engine chia nhỏ công việc trong kế hoạch thực thi, không phải là cơ chế khóa cứng tiến trình vào `N` nhân vật lý của hệ điều hành (*CPU affinity*).
2. **Trạng thái bộ nhớ đệm (*warm-cache*) và chỉ số `bytes_scanned`:** Vì mỗi phép đo đều thực hiện 1 lần chạy làm nóng (*warm-up*) trước khi lấy 7 mẫu và dung lượng mỗi file Parquet chỉ khoảng 108–117 MB, dữ liệu đọc lại thường đã nằm trong bộ nhớ đệm trang của hệ điều hành (*OS page cache*). Ngoài ra, chỉ số `bytes_scanned` trong `E3` là con số do toán tử đọc Parquet của DataFusion báo cáo trong `EXPLAIN ANALYZE`, biểu thị lượng dữ liệu đi qua tầng quét của engine chứ không phải lượng I/O vật lý đọc trực tiếp từ ổ cứng.
3. **Cơ chế pruning trên `Q2` giữa hai engine:** Trên file `sales_sorted.parquet`, cả DataFusion và DuckDB đều tận dụng thống kê `min/max` của Parquet để bỏ qua các row group không thỏa điều kiện và chỉ đọc 2/39 row group. Do đó, sự chênh lệch vài mili-giây ở `Q2` trong thí nghiệm `E5` phản ánh chi phí điều phối thực thi và giải mã 2 row group còn lại của mỗi engine, chứ không phải vì chỉ một hệ thống có khả năng pruning.
4. **Phạm vi của Python UDF trong `E4`:** Trong bài báo SIGMOD 2024 (Mục 7.1), nhận định rằng các hàm mở rộng (*extension*) đạt hiệu năng tương đương hàm tích hợp (*built-in*) được phát biểu cho các hàm viết bằng Rust và biên dịch trực tiếp cùng công cụ thực thi của DataFusion. Trong bản minh họa bằng Python này, UDF vector hóa giúp tránh vòng lặp từng dòng nhưng vẫn phải đi qua giao diện Python–Rust, nên vẫn chịu thêm chi phí điều phối so với biểu thức `CASE WHEN` nội tại khi chạy đa phân vùng.

## 7. Tài liệu liên quan và nguồn tham khảo

- **[`REPORT_UPDATE_NOTES.md`](REPORT_UPDATE_NOTES.md):** Tài liệu tổng hợp các bảng số liệu trích từ `results/results.json` và các đoạn phân tích gợi ý để đồng bộ nội dung báo cáo môn học với bộ kết quả mẫu trên Windows 11.
- **[`RELEASE_CHECKLIST.md`](RELEASE_CHECKLIST.md):** Danh mục kiểm tra kỹ thuật dành cho nhóm trước khi đóng gói mã nguồn và nộp báo cáo.
- **Tài liệu chính thức:**
  - Bài báo gốc: [Apache Arrow DataFusion: A Fast, Embeddable, Modular Analytic Query Engine (SIGMOD-Companion '24)](https://doi.org/10.1145/3626246.3653368)
  - Tài liệu Apache Arrow DataFusion: https://datafusion.apache.org/
  - Tài liệu DataFusion Python Binding: https://datafusion.apache.org/python/
