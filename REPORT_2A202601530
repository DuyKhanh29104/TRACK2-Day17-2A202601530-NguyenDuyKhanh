# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Nguyễn Duy Khánh  **Lớp:** 3B  **Ngày:** 17/08/2026

---

## 0 · Kết quả `make verify`

<details>
<summary>Output `make verify` sau ba lượt chạy</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 75.5s
  run 2/3 … 56.9s
  run 3/3 … 57.3s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536.3×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | `gold_training_set` tăng dòng khi chạy lại cùng ngày hoặc cùng partition; cùng một `ticket_id` xuất hiện nhiều lần. |
| **Nguyên nhân** | Đây là bảng entity, grain là 1 dòng / ticket, nhưng incremental model không khai báo khóa nên dbt dùng cơ chế append/`INSERT`; bản ghi CDC `op = 'u'` còn có thể đưa cùng ticket vào các ngày ingest khác nhau. |
| **Cách khắc phục** | Trong `dbt/models/gold/gold_training_set.sql`, khai báo `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'`. Trong `dags/ai_training_pipeline.py`, đặt `catchup=False`, `max_active_runs=1` để tránh backfill và ghi đồng thời. Giữ nguyên filter `run_date` vì đó là cơ chế xử lý partition. |
| **Bằng chứng** | Baseline lỗi: 38.750 hàng; sau sửa: 12.480 hàng, không lặp. Checksum 3 lượt: `8dd7c98653`, `8dd7c98653`, `8dd7c98653`. |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` chỉ có 8.645 dòng thay vì 9.100; các cặp thiếu tập trung ở những ngày cũ. |
| **P99 độ trễ đo được** | **2,725833 ngày** (xấp xỉ 65,42 giờ); `max` đo được là 2,944688 ngày; tỷ lệ đến muộn hơn một ngày là 5,05%. |
| **Lookback đã chọn** | **3 ngày** — làm tròn lên từ P99 để bao phủ dữ liệu đến muộn trong phạm vi vận hành thông thường. |
| **Nguyên nhân** | Điều kiện `event_date > max(event_date)` chỉ nhận ngày sự kiện mới hơn ngày lớn nhất trong target. Một event xảy ra ngày 08-12 nhưng tới kho ngày 08-15 đã bị bỏ qua và không được xét lại. |
| **Cách khắc phục** | Trong `dbt/models/gold/gold_feature_daily.sql`, dùng `event_date >= max(event_date) - interval 3 day`, đồng thời khai báo `unique_key = ['event_date', 'customer_id']` và `incremental_strategy = 'merge'` để các ngày được tính lại thay thế bản ghi cũ. |
| **Bằng chứng** | Trước: 8.645 hàng; sau: 9.100 hàng. Checksum 3 lượt: `3db448685c` giống nhau. |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> P99 ít nhạy với outlier và phản ánh độ trễ mà 99% dữ liệu cần được phục vụ; lookback 3 ngày được trả giá ở mọi lượt chạy sau này bằng việc đọc và tính lại thêm các ngày trong window. Dùng `max` có thể an toàn hơn cho 100% dữ liệu quan sát được nhưng một outlier rất xa sẽ làm tăng chi phí mọi lượt chạy. Với bộ dữ liệu này, cả P99 và max đều được làm tròn thành 3 ngày, nhưng P99 là căn cứ vận hành ổn định hơn.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | `silver_tickets.priority` có `NULL`, `0`, `5`, `-1`; từ 2026-08-10 source chuyển một phần dữ liệu sang nhãn chữ. |
| **Nguyên nhân** | `try_cast` biến nhãn chữ thành `NULL` nhưng lại chấp nhận các số ngoài miền 1..4. Đây là schema evolution kết hợp với một số bản ghi lỗi thật. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | Số `1..4` → giữ nguyên; `urgent/high/medium/low` → map lần lượt thành `1/2/3/4`; `P1`, `unknown`, `0`, `5`, `-1`, chuỗi rỗng, `NULL` → trả `NULL` và đưa vào quarantine. |
| **Cách khắc phục** | Sửa `dbt/macros/normalize_priority.sql` bằng `CASE`; lọc bản ghi không chuẩn hóa được trước khi `row_number()` trong `silver_tickets.sql`; dùng cùng macro để lọc `quarantine_tickets.sql`; bật contract và test miền giá trị trong `dbt/models/silver/schema.yml`. |
| **Bằng chứng** | `quarantine_tickets` = **312** dòng, ổn định; `dbt test` **11/11 pass**; Silver có 12.480 dòng và priority chỉ thuộc 1..4, không `NULL`. |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để
pipeline dừng khi gặp bản ghi lỗi?

> Nên giữ dữ liệu thô ở Bronze để audit và điều tra nguyên nhân, sau đó phân loại ở Silver. Nếu Bronze từ chối row lỗi thì mất bản ghi gốc để replay và truy vết. Không nên dừng toàn bộ DAG vì vài trăm row lỗi sẽ chặn hàng chục nghìn row hợp lệ; quarantine giữ row lỗi cho người trực xử lý còn pipeline vẫn phục vụ dữ liệu sạch.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

### Bài A — Query dashboard chậm

| | |
|---|---|
| **Triệu chứng** | Dashboard đọc 5.000 file Parquet nhỏ; baseline là 5.000.000 `rows scanned` trên 130.683 dòng thật. |
| **Nguyên nhân** | Dataset không partition theo cột filter, các file nhỏ gây chi phí scan theo batch; filter `strftime(event_time, ...)` bọc cột trong function nên không sargable. |
| **Cách khắc phục** | `tools/compact.py` ghi dataset mới theo `event_date`, sắp xếp theo `event_date, customer_name, event_time`, dùng row group 4.096 dòng. `queries/dashboard.sql` bật `hive_partitioning` và lọc trực tiếp `event_date = DATE '2026-08-09'`. |
| **Bằng chứng** | `rows scanned`: **5.000.000 → 9.324**, giảm **536,3×**; files: **5.000 → 14**; rows on disk giữ **130.683**; result hash giữ nguyên `4379e4c5d9f3`. |

### Bài B — Consumer gặp sự cố giữa batch

| | |
|---|---|
| **Triệu chứng** | Commit offset trước khi ghi là at-most-once: nếu process chết tại điểm crash thì batch hiện tại bị mất. Ghi trước nhưng dùng `INSERT` thường sẽ tạo duplicate khi batch được replay. |
| **Nguyên nhân** | Offset transport và việc ghi database là hai thao tác độc lập; không có exactly-once ở tầng giao vận. Cần at-least-once kết hợp với phép ghi idempotent. |
| **Cách khắc phục** | Trong `ingest/consumer.py`, thêm `PRIMARY KEY (event_id)`, ghi và commit transaction database trước, đặt crash point trước `consumer.commit()`, sau đó dùng `ON CONFLICT (event_id) DO UPDATE` để replay cập nhật bản ghi cũ. |
| **Bằng chứng** | Kịch bản crash 20.000 message: kết quả cuối **20.000 dòng / 20.000 event_id phân biệt**, không mất và không trùng; message replay có thể cập nhật nội dung cũ. |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Xác định grain và natural key của bảng, sau đó kiểm tra materialization có idempotent khi chạy lại hay không. |
| 2 | Đo độ trễ giữa thời gian sự kiện và thời gian ingest; so sánh incremental window với P99 dữ liệu về muộn. |
| 3 | Đọc contract/schema của source, phân biệt schema evolution với dữ liệu lỗi, rồi kiểm tra có quarantine và test miền giá trị hay không. |
