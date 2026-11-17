"""CDC batch MERGE into silver with batch log and quarantine (P2-A5-S3)."""

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

import silver_transforms

LANDING_PATH = "/Volumes/workspace/bronze/landing"
BRONZE_SCHEMA = silver_transforms.BRONZE_SCHEMA
SILVER_SCHEMA = silver_transforms.SILVER_SCHEMA
LOG_TABLE = f"{SILVER_SCHEMA}.batch_log"
QUARANTINE_TABLE = f"{SILVER_SCHEMA}.quarantine"

KEY_COLUMNS = {
    "client": "client_id",
    "account": "account_id",
    "card": "card_id",
    "loan": "loan_id",
}

# TARGET: workspace.silver.<table> (t), SOURCE: vw_source_<table> (s)
MERGE_SQL = {}

MERGE_SQL["card"] = """
MERGE INTO workspace.silver.card AS t
USING vw_source_card AS s
ON t.card_id = s.card_id
-- D: card cancelled, hard delete
WHEN MATCHED AND s.op_type = 'D' THEN DELETE
-- U: only a newer change_ts overwrites
WHEN MATCHED AND s.op_type = 'U' AND s.change_ts > t._change_ts THEN UPDATE SET
  t.disp_id     = s.disp_id,
  t.type        = s.type,
  t.issued_date = s.issued_date,
  t._change_ts  = s.change_ts
-- I: new card
WHEN NOT MATCHED AND s.op_type = 'I' THEN INSERT
  (card_id, disp_id, type, issued_date, _change_ts)
  VALUES (s.card_id, s.disp_id, s.type, s.issued_date, s.change_ts)
"""

MERGE_SQL["client"] = """
MERGE INTO workspace.silver.client AS t
USING vw_source_client AS s
ON t.client_id = s.client_id
-- U: address change; only a newer change_ts overwrites
WHEN MATCHED AND s.op_type = 'U' AND s.change_ts > t._change_ts THEN UPDATE SET
  t.birth_number = s.birth_number,
  t.district_id  = s.district_id,
  t._change_ts   = s.change_ts
-- I: new client
WHEN NOT MATCHED AND s.op_type = 'I' THEN INSERT
  (client_id, birth_number, district_id, _change_ts)
  VALUES (s.client_id, s.birth_number, s.district_id, s.change_ts)
"""

MERGE_SQL["account"] = """
MERGE INTO workspace.silver.account AS t
USING vw_source_account AS s
ON t.account_id = s.account_id
-- U: frequency change; only a newer change_ts overwrites
WHEN MATCHED AND s.op_type = 'U' AND s.change_ts > t._change_ts THEN UPDATE SET
  t.district_id = s.district_id,
  t.frequency   = s.frequency,
  t.open_date   = s.open_date,
  t._change_ts  = s.change_ts
"""

MERGE_SQL["loan"] = """
MERGE INTO workspace.silver.loan AS t
USING vw_source_loan AS s
ON t.loan_id = s.loan_id
-- U: loan status change; only a newer change_ts overwrites
WHEN MATCHED AND s.op_type = 'U' AND s.change_ts > t._change_ts THEN UPDATE SET
  t.account_id = s.account_id,
  t.loan_date  = s.loan_date,
  t.amount     = s.amount,
  t.duration   = s.duration,
  t.payments   = s.payments,
  t.status     = s.status,
  t._change_ts = s.change_ts
"""


def read_batch(spark, table_name, batch_id):
    """Read one landing batch with an explicit schema; change_ts as timestamp."""
    df_bronze = spark.table(f"{BRONZE_SCHEMA}.{table_name}")

    fields = []
    for column_name in df_bronze.columns:
        # Bronze metadata columns (_ingest_ts, _source_file) are not in landing
        if not column_name.startswith("_"):
            fields.append(StructField(column_name, StringType(), True))
    fields.append(StructField("op_type", StringType(), True))
    fields.append(StructField("change_ts", StringType(), True))
    fields.append(StructField("batch_id", IntegerType(), True))
    schema = StructType(fields)

    batch_path = f"{LANDING_PATH}/{table_name}/batch_{batch_id:04d}/"
    df_batch = spark.read.schema(schema).json(batch_path)
    df_batch = df_batch.withColumn("change_ts", F.to_timestamp("change_ts"))
    return df_batch


def prepare_source(spark, table_name, batch_id):
    """Dedup a batch, cast to silver types and register vw_source_<table>."""
    key_column = KEY_COLUMNS[table_name]
    df_batch = read_batch(spark, table_name, batch_id)

    # Dedup rule 1: latest change_ts per key gets rn = 1
    window_latest = Window.partitionBy(key_column).orderBy(F.col("change_ts").desc())
    df_ranked = df_batch.withColumn("rn", F.row_number().over(window_latest))

    # Dedup rule 2 prep: does the key have an I in this batch (1 = yes)
    window_key = Window.partitionBy(key_column)
    is_insert = F.when(F.col("op_type") == "I", 1).otherwise(0)
    df_ranked = df_ranked.withColumn("has_insert", F.max(is_insert).over(window_key))

    df_dedup = df_ranked.filter(F.col("rn") == 1)

    # Dedup rule 2: I -> U means the key is not in target; insert with latest values
    final_op_type = (
        F.when((F.col("has_insert") == 1) & (F.col("op_type") != "D"), "I")
        .otherwise(F.col("op_type"))
    )
    df_dedup = df_dedup.withColumn("op_type", final_op_type)
    df_dedup = df_dedup.drop("rn", "has_insert")

    # to_silver drops CDC columns; keep them aside and join back on key
    df_data = silver_transforms.to_silver(df_dedup, table_name)
    df_cdc = df_dedup.select(
        F.col(key_column).cast("int").alias(key_column),
        "op_type",
        "change_ts",
        "batch_id",
    )

    # Safe join: one row per key after dedup
    df_source = df_data.join(df_cdc, on=key_column, how="inner")

    # SOURCE view for MERGE ... USING
    view_name = f"vw_source_{table_name}"
    df_source.createOrReplaceTempView(view_name)
    return df_source


def get_batch_status(spark, table_name, batch_id):
    """Return the batch_log status for (table, batch), or None."""
    df_log = spark.sql(f"""
        SELECT status FROM {LOG_TABLE}
        WHERE table_name = '{table_name}' AND batch_id = {batch_id}
    """)
    rows = df_log.collect()
    if len(rows) == 0:
        return None
    return rows[0]["status"]


def write_log_started(spark, table_name, batch_id, rows_read):
    """Replace any old log row for (table, batch) with a STARTED row."""
    spark.sql(f"""
        DELETE FROM {LOG_TABLE}
        WHERE table_name = '{table_name}' AND batch_id = {batch_id}
    """)
    spark.sql(f"""
        INSERT INTO {LOG_TABLE}
        (table_name, batch_id, status, rows_read, started_at)
        VALUES ('{table_name}', {batch_id}, 'STARTED', {rows_read}, current_timestamp())
    """)


def quarantine_missing_keys(spark, table_name, batch_id, df_source):
    """Write U/D rows whose key is not in target to quarantine; return the count."""
    key_column = KEY_COLUMNS[table_name]

    # SOURCE side: only U / D (I is expected to be missing in target)
    df_source_ud = df_source.filter(F.col("op_type").isin("U", "D"))

    # TARGET side: current silver keys
    df_target_keys = spark.table(f"{SILVER_SCHEMA}.{table_name}").select(key_column)

    # left_anti: keys in SOURCE but not in TARGET
    df_missing = df_source_ud.join(df_target_keys, on=key_column, how="left_anti")

    df_quarantine = df_missing.select(
        F.lit(table_name).alias("table_name"),
        F.col("batch_id").cast("int").alias("batch_id"),
        F.col(key_column).cast("int").alias("key_value"),
        F.col("op_type"),
        F.lit("key not found in target").alias("reason"),
        F.col("change_ts"),
    )

    # Count before MERGE; target changes after MERGE
    missing_count = df_quarantine.count()

    # Re-run after FAILED must not duplicate quarantine rows
    spark.sql(f"""
        DELETE FROM {QUARANTINE_TABLE}
        WHERE table_name = '{table_name}' AND batch_id = {batch_id}
    """)
    if missing_count > 0:
        df_quarantine.write.format("delta").mode("append").saveAsTable(QUARANTINE_TABLE)

    return missing_count


def run_batch(spark, table_name, batch_id):
    """Apply one batch with log + quarantine; skip if already SUCCESS."""
    # Batch-level guard: a processed batch never reaches MERGE
    status = get_batch_status(spark, table_name, batch_id)
    if status == "SUCCESS":
        print(f"{table_name} batch {batch_id}: SKIPPED (SUCCESS in batch_log)")
        return "SKIPPED"

    rows_read = read_batch(spark, table_name, batch_id).count()
    write_log_started(spark, table_name, batch_id, rows_read)

    try:
        df_source = prepare_source(spark, table_name, batch_id)
        rows_skipped = quarantine_missing_keys(spark, table_name, batch_id, df_source)

        # MERGE: SOURCE vw_source_<table> -> TARGET workspace.silver.<table>
        df_merge_result = spark.sql(MERGE_SQL[table_name])
        merge_row = df_merge_result.collect()[0]
        rows_updated = merge_row["num_updated_rows"]
        rows_deleted = merge_row["num_deleted_rows"]
        rows_inserted = merge_row["num_inserted_rows"]

        spark.sql(f"""
            UPDATE {LOG_TABLE}
            SET status = 'SUCCESS',
                rows_inserted = {rows_inserted},
                rows_updated = {rows_updated},
                rows_deleted = {rows_deleted},
                rows_skipped = {rows_skipped},
                finished_at = current_timestamp()
            WHERE table_name = '{table_name}' AND batch_id = {batch_id}
        """)
    except Exception as error:
        spark.sql(f"""
            UPDATE {LOG_TABLE}
            SET status = 'FAILED', finished_at = current_timestamp()
            WHERE table_name = '{table_name}' AND batch_id = {batch_id}
        """)
        print(f"{table_name} batch {batch_id}: FAILED -> {error}")
        raise

    print(
        f"{table_name} batch {batch_id}: SUCCESS | read={rows_read}, "
        f"upd={rows_updated}, del={rows_deleted}, ins={rows_inserted}, skipped={rows_skipped}"
    )
    return "SUCCESS"


def reset_silver(spark):
    """Reset silver CDC tables to the bronze snapshot, re-enable CDF, clear log and quarantine."""
    initial_ts = F.to_timestamp(F.lit(silver_transforms.INITIAL_CHANGE_TS))

    for table_name in silver_transforms.CDC_TABLES:
        df_bronze = spark.table(f"{BRONZE_SCHEMA}.{table_name}")
        df_silver = silver_transforms.to_silver(df_bronze, table_name)
        df_silver = df_silver.withColumn("_change_ts", initial_ts)
        (
            df_silver.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(f"{SILVER_SCHEMA}.{table_name}")
        )
        row_count = spark.table(f"{SILVER_SCHEMA}.{table_name}").count()
        print(f"silver.{table_name}: reset, rows={row_count}")

    # Overwrite recreates the table; CDF must be on again before any MERGE
    spark.sql(f"ALTER TABLE {SILVER_SCHEMA}.card SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    print("silver.card: CDF enabled")

    # Old log rows would make run_batch skip every batch after reset
    spark.sql(f"DELETE FROM {LOG_TABLE}")
    spark.sql(f"DELETE FROM {QUARANTINE_TABLE}")
    print("batch_log and quarantine: cleared")