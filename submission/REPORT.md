# K4-Track02-Day17 — Report cá nhân

**Họ tên / MSSV:** Nguyễn Ngọc Tuyền / 2A202603010.
**Repo bài nộp:** https://github.com/Tuienn/K4-Track02-Day17-Data-Pipeline-Engineering
**Commit bài nộp:** Commit chứa phiên bản REPORT và checksums này; tra SHA bằng `git log -1 --format=%H -- submission/REPORT.md`.
**AI đã dùng và phạm vi hỗ trợ:** OpenAI Codex (GPT-6 Astra): đọc tài liệu, phân tích baseline, sửa ba lỗi, chạy kiểm tra và soạn báo cáo; GPT-6 Luna (medium): review độc lập model dbt, không sửa code. Học viên cần review và giải thích các thay đổi trước khi nộp.
**Nguồn tham khảo khác:** README và tài liệu trong `docs/` của đề bài; không dùng nguồn ngoài.

## 1. Ba lỗi

**Silver — khóa thực thể**
- Triệu chứng: verify báo 24 dòng/12 ticket; T-91 có ba trạng thái; rerun sai checksum.
- Nguyên nhân: dedup trong batch nhưng INSERT nối thêm giữa các batch.
- Sửa: `pipeline/silver.py` MERGE theo `ticket_id`, chỉ UPDATE khi `s._lsn > t._lsn`; batch cũ hoặc giao lại không ghi đè trạng thái mới.
- Khái niệm: Silver có khóa, keyed upsert, idempotency và thứ tự CDC.

**Gold — dữ liệu đến muộn**
- Triệu chứng: feature khác full recompute; u05 ngày 08-12 chỉ có (2 events, 0 down), thay vì (5, 1).
- Nguyên nhân: lookback 0 bỏ qua event ngày 08-12 được ingest ngày 08-15.
- Sửa: `pipeline/config.py` đặt `LOOKBACK_DAYS = 3 = ceil(P99)` đo từ Bronze; logic Gold hiện có ghi lại partition theo event time.
- Khái niệm: event time/ingest time, lateness và lookback.

**CDC — delete**
- Triệu chứng: T-97 chưa thành tombstone; vẫn có 1 dòng ở snapshot mới nhất và 2 chunks RAG.
- Nguyên nhân: staging lấy khóa chỉ từ `after`, trong khi delete có `after = null`, nên bị lọc mất.
- Sửa: `pipeline/staging.py` COALESCE khóa từ `after`, `before`, Kafka key; vẫn lấy thuộc tính từ `after` để xóa PII. MERGE giữ tombstone và LSN; logic Gold hiện có loại ticket đã xóa.
- Khái niệm: CDC delete khác Kafka tombstone; xóa phải lan, chống hồi sinh khi replay.

## 2. Các con số

Bronze 43 records: P50=0, P95=2,90, P99=3,00, max=3 ngày → lookback=3. Verify 18/18; pytest 34 passed; dbt PASS=19; parity PARITY. Checksum Gold `39e115c510ecdf526800eac227158a4f`: C0=C1=C2=C3, PASS.

## 3. Lựa chọn công cụ / kỹ thuật

- MERGE theo khóa cho trạng thái ticket; overwrite-partition cho feature vì cần tính lại toàn bộ tổng hợp của ngày có event muộn.
- Tombstone giữ LSN chống replay làm hồi sinh ticket; đổi lại phải lưu metadata lâu dài.
- Snapshot dựng từ Bronze as-of, feedback chỉ đến ngày đó, giúp tái lập và tránh leakage; snapshot cũ không tự đổi khi dữ liệu mới tới.
- DuckDB phù hợp seed nhỏ, local zero-key; dbt bổ sung contract, unit test và microbatch. Spark chưa cần vì không có nhu cầu phân tán.

## 4. Hai câu hỏi suy ngẫm

1. Khi cần xóa dữ liệu thực, ưu tiên chính sách xóa: truy vết ticket qua Bronze, snapshot, transcript, cache/index và backup; thu hồi phiên bản bị ảnh hưởng, tạo bản đã làm sạch, tái huấn luyện nếu cần. Giữ audit metadata không chứa PII; bất biến không đồng nghĩa lưu dữ liệu cá nhân mãi mãi.
2. Đặt gate PII ở Bronze→Silver và kiểm tra trước Gold: regex kết hợp nhận diện tên tiếng Việt, pseudonym hóa user ID, quarantine trường hợp nghi ngờ. Đo precision/recall trên tập gán nhãn, tỷ lệ PII lọt qua và false positives; hạn chế quyền đọc Bronze.

## 5. Output thực tế

Các output dưới đây lấy từ lần chạy trên code đã sửa; chỉ bỏ mã màu ANSI và khoảng trắng cuối dòng của terminal.

```text
$ make verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt
```

```text
$ make test
..................................                                       [100%]
34 passed in 1.36s
```

```text
$ make rerun3
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums
```

```text
$ make lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3
```

```text
$ make dbt
cd dbt_project && DBT_PROFILES_DIR=. /home/tuienn/User/VinAI/Labs/2026-10-04/K4-Track02-Day17-Data-Pipeline-Engineering/.venv/bin/dbt build --event-time-start 2026-08-10 --event-time-end 2026-08-17
02:53:04  Running with dbt=1.12.5
02:53:04  Registered adapter: duckdb=1.11.0
02:53:04  Unable to do partial parsing because saved manifest not found. Starting full parse.
02:53:05  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
02:53:05
02:53:05  Concurrency: 1 threads (target='dev')
02:53:05
02:53:05  1 of 19 START sql view model main.stg_events ................................... [RUN]
02:53:05  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.06s]
02:53:05  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
02:53:05  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.02s]
02:53:05  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
02:53:05  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.06s]
02:53:05  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
02:53:05  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.08s]
02:53:05  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
02:53:05  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.06s]
02:53:05  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
02:53:05  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.02s]
02:53:05  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
02:53:05  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.01s]
02:53:05  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
02:53:05  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.01s]
02:53:05  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
02:53:05  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.01s]
02:53:05  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
02:53:05  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.01s]
02:53:05  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
02:53:05  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.01s]
02:53:05  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
02:53:05  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.01s]
02:53:05  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
02:53:05  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.01s]
02:53:05  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
02:53:05  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.01s]
02:53:05  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
02:53:05  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.01s]
02:53:05  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
02:53:05  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
02:53:05  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.02s]
02:53:05  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
02:53:05  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.03s]
02:53:05  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
02:53:05  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.02s]
02:53:05  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
02:53:05  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.02s]
02:53:05  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
02:53:05  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.02s]
02:53:05  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
02:53:06  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.02s]
02:53:06  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
02:53:06  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.02s]
02:53:06  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.15s]
02:53:06  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
02:53:06  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.01s]
02:53:06  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
02:53:06  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.01s]
02:53:06  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
02:53:06  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.01s]
02:53:06
02:53:06  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 0.66 seconds (0.66s).
02:53:06
02:53:06  Completed successfully
02:53:06
02:53:06  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19
```

```text
$ make parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```
