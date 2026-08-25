"""PySpark loader for large files with an explicit schema."""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from metrics import RunMetrics, write_results

RAW_COLUMNS = [
    "order_id",
    "customer_id",
    "customer_email",
    "phone",
    "order_date",
    "items",
    "price",
    "quantity",
    "shipping_cost",
    "total",
    "status",
]


def _spark_clean_partition(
    rows: Iterator[Any], run_id: str, source_file: str
) -> Iterator[tuple[Any, ...]]:
    """Apply the deterministic Python quality service per Spark partition."""
    from quality_rules import clean_order

    for row_number, row in enumerate(rows, start=2):
        row_data = row.asDict()
        source_row_number = row_data.get("_source_row_id", row_number)
        raw = {key: row_data.get(key) for key in RAW_COLUMNS}
        cleaned = clean_order(raw)
        numeric_values: dict[str, float | None] = {}
        for field in ("price", "quantity", "shipping_cost", "total"):
            value = cleaned.get(field)
            try:
                numeric_values[field] = None if value in (None, "") else float(value)
            except (TypeError, ValueError):
                numeric_values[field] = None
        items = cleaned.get("items")
        raw_record = {key: "" if value is None else str(value) for key, value in raw.items()}
        yield (
            cleaned.get("order_id") or None,
            cleaned.get("customer_id") or None,
            cleaned.get("customer_email") or None,
            cleaned.get("phone") or None,
            cleaned.get("order_date") or None,
            json.dumps(items, ensure_ascii=False, separators=(",", ":"))
            if isinstance(items, list)
            else None,
            numeric_values["price"],
            numeric_values["quantity"],
            numeric_values["shipping_cost"],
            numeric_values["total"],
            cleaned.get("status") or None,
            cleaned["quality_status"],
            json.dumps(cleaned["corrections"], ensure_ascii=False, separators=(",", ":")),
            cleaned["error_codes"],
            json.dumps(cleaned["error_details"], ensure_ascii=False, separators=(",", ":")),
            run_id,
            source_file,
            str(source_row_number),
            raw_record,
        )


def _write_with_connector(
    frame: Any,
    *,
    uri: str,
    database: str,
    collection: str,
    operation_type: str,
    id_fields: str,
) -> None:
    """Write a partitioned DataFrame through the official MongoDB Spark Connector."""
    (
        frame.write.format("mongodb")
        .mode("append")
        .option("connection.uri", uri)
        .option("database", database)
        .option("collection", collection)
        .option("operationType", operation_type)
        .option("upsertDocument", "true")
        .option("idFieldList", id_fields)
        .option("maxBatchSize", "512")
        .option("ordered", "false")
        .save()
    )


def run_spark(
    input_path: Path,
    repo: Any,
    reports_path: Path,
    partitions: int = 4,
    master: str = "local[*]",
    connector_package: str = "org.mongodb.spark:mongo-spark-connector_2.13:10.5.0",
    mongo_uri: str = "mongodb://localhost:27017",
    mongo_database: str = "midterm_orders",
) -> RunMetrics:
    """Run the large-file path with fixed String input schema and partitioned processing."""
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        ArrayType,
        DoubleType,
        MapType,
        StringType,
        StructField,
        StructType,
    )
    from pyspark.sql.window import Window

    if partitions < 1:
        raise ValueError("partitions must be positive")

    started = time.perf_counter()
    run_id = uuid.uuid4().hex[:12]
    size_mb = input_path.stat().st_size / (1024 * 1024)
    metrics = RunMetrics(
        run_id, input_path.name, round(size_mb, 6), "pyspark", partitions=partitions
    )
    raw_schema = StructType([StructField(column, StringType(), True) for column in RAW_COLUMNS])
    output_schema = StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("customer_email", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("order_date", StringType(), True),
            StructField("items", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("quantity", DoubleType(), True),
            StructField("shipping_cost", DoubleType(), True),
            StructField("total", DoubleType(), True),
            StructField("status", StringType(), True),
            StructField("quality_status", StringType(), True),
            StructField("corrections_json", StringType(), True),
            StructField("error_codes", ArrayType(StringType()), True),
            StructField("error_details_json", StringType(), True),
            StructField("run_id", StringType(), False),
            StructField("source_file", StringType(), False),
            StructField("source_row_number", StringType(), False),
            StructField("raw_record", MapType(StringType(), StringType()), True),
        ]
    )
    builder = SparkSession.builder.appName("midterm-orders-pipeline").master(master)
    if connector_package:
        builder = builder.config("spark.jars.packages", connector_package)
    spark = builder.getOrCreate()
    try:
        frame = (
            spark.read.option("header", True)
            .option("escape", '"')
            .schema(raw_schema)
            .csv(str(input_path))
            .withColumn("_source_row_id", F.monotonically_increasing_id() + F.lit(2))
            .repartition(partitions)
        )
        metrics.rows_read = frame.count()
        metrics.raw_loaded = metrics.rows_read
        metrics.partitions = frame.rdd.getNumPartitions()

        # Distributed quality transformation: no driver-side list is created for production Mongo runs.
        cleaned_rdd = frame.rdd.mapPartitions(
            lambda rows: _spark_clean_partition(rows, run_id, input_path.name)
        )
        processed = spark.createDataFrame(cleaned_rdd, schema=output_schema).cache()
        duplicate_window = Window.partitionBy("order_id").orderBy(F.col("source_row_number"))
        processed = (
            processed.withColumn(
                "duplicate_rank",
                F.when(
                    F.col("order_id").isNotNull() & (F.col("order_id") != ""),
                    F.row_number().over(duplicate_window),
                ).otherwise(F.lit(0)),
            )
            .withColumn(
                "error_codes",
                F.when(
                    F.col("duplicate_rank") > 1,
                    F.array_union(F.col("error_codes"), F.array(F.lit("DUPLICATE_ORDER_ID"))),
                ).otherwise(F.col("error_codes")),
            )
            .withColumn(
                "quality_status",
                F.when(F.col("duplicate_rank") > 1, F.lit("quarantined")).otherwise(
                    F.col("quality_status")
                ),
            )
        )
        aggregate = {
            row["quality_status"]: row["count"]
            for row in processed.groupBy("quality_status").count().collect()
        }
        metrics.valid_count = int(aggregate.get("valid", 0))
        metrics.corrected_count = int(aggregate.get("corrected", 0))
        metrics.quarantine_count = int(aggregate.get("quarantined", 0))
        error_counts = (
            processed.select(F.explode("error_codes").alias("code"))
            .groupBy("code")
            .count()
            .collect()
        )
        metrics.error_case_counts = {row["code"]: int(row["count"]) for row in error_counts}

        is_real_mongo = repo.__class__.__name__ == "MongoRepository"
        if is_real_mongo:
            raw_frame = (
                frame.withColumn(
                    "_id", F.concat(F.lit(f"{run_id}:"), F.col("_source_row_id").cast("string"))
                )
                .withColumn("source_row_number", F.col("_source_row_id").cast("long"))
                .withColumn("run_id", F.lit(run_id))
                .withColumn("source_file", F.lit(input_path.name))
                .withColumn("engine_used", F.lit("pyspark"))
                .withColumn("raw_record", F.struct(*[F.col(column) for column in RAW_COLUMNS]))
                .drop("_source_row_id")
            )
            _write_with_connector(
                raw_frame,
                uri=mongo_uri,
                database=mongo_database,
                collection="orders_raw",
                operation_type="insert",
                id_fields="_id",
            )
            valid_frame = processed.filter(F.col("quality_status") != "quarantined").drop(
                "duplicate_rank"
            )
            quarantine_frame = processed.filter(F.col("quality_status") == "quarantined").drop(
                "duplicate_rank"
            )
            _write_with_connector(
                valid_frame,
                uri=mongo_uri,
                database=mongo_database,
                collection="orders_validated",
                operation_type="replace",
                id_fields="order_id",
            )
            _write_with_connector(
                quarantine_frame,
                uri=mongo_uri,
                database=mongo_database,
                collection="orders_quarantine",
                operation_type="replace",
                id_fields="source_file,source_row_number",
            )
            metrics.inserted_count = metrics.valid_count + metrics.corrected_count
            metrics.notes.append(
                "Distributed DataFrame quality transformation and MongoDB Spark Connector writes were used."
            )
        else:
            # Offline/mongomock fallback is deliberately bounded to the demo file; production uses connector branch above.
            from quality_rules import clean_order

            raw_batch: list[dict[str, Any]] = []
            valid: list[dict[str, Any]] = []
            quarantine: list[dict[str, Any]] = []
            seen_ids: set[str] = set()
            for row_number, row in enumerate(frame.toLocalIterator(), start=2):
                raw_record = row.asDict()
                raw_batch.append(
                    {
                        "run_id": run_id,
                        "source_file": input_path.name,
                        "source_row_number": row_number,
                        "ingested_at": time.time(),
                        "engine_used": "pyspark",
                        "raw_record": raw_record,
                    }
                )
                cleaned = clean_order(raw_record)
                cleaned.update(
                    {
                        "run_id": run_id,
                        "source_file": input_path.name,
                        "source_row_number": row_number,
                        "raw_record": raw_record,
                    }
                )
                if cleaned["order_id"] and cleaned["order_id"] in seen_ids:
                    cleaned["quality_status"] = "quarantined"
                    cleaned["error_codes"] = ["DUPLICATE_ORDER_ID"]
                    cleaned["error_details"] = [
                        {"code": "DUPLICATE_ORDER_ID", "detail": "duplicate in this run"}
                    ]
                if cleaned["order_id"]:
                    seen_ids.add(cleaned["order_id"])
                if cleaned["quality_status"] == "quarantined":
                    quarantine.append(cleaned)
                else:
                    valid.append(cleaned)
            repo.insert_raw(raw_batch)
            outcome = repo.upsert_final(valid, quarantine)
            metrics.inserted_count = outcome["inserted_count"]
            metrics.updated_count = outcome["updated_count"]
            metrics.unchanged_count = outcome["unchanged_count"]
            metrics.notes.append(
                "Local MemoryRepository fallback collected demo rows; real Mongo backend uses distributed connector writes."
            )
        metrics.finalize(time.perf_counter() - started)
        write_results(metrics, reports_path)
        return metrics
    finally:
        spark.stop()
