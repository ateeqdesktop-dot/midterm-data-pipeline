# Architecture

```text
Dirty CSV
   |
   v
File Router (size <= 200 MB?)
   |----------------------------|
   v                            v
Python Batch + insert_many    PySpark DataFrame + fixed String schema
   |                            |
   +-------------> orders_raw <-+
                         |
                         v
                 Transform + Quality
                    |           |
                    v           v
           orders_validated   orders_quarantine
           Unique order_id    error_codes/details
                    |
                    v
          reports/results.json
```

The design follows ELT: Raw loading is completed before quality rules run. The Python path uses `csv.DictReader` and bounded batches. The Spark path uses `SparkSession`, DataFrame API, fixed String schema, and configurable partitions. MongoDB setup creates indexes and the validated upsert path compares stable business payloads so rerunning the same input produces `unchanged_count` rather than duplicate records.
