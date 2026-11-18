"""Performance tuning data generator: multiplies a bronze table with shifted IDs and dates."""

from pyspark.sql import functions as F

from silver_transforms import berka_date

BRONZE_SCHEMA = "workspace.bronze"
PERF_SCHEMA = "workspace.perf"
ID_OFFSET = 10_000_000
SHIFT_YEARS = 6

def to_perf_trans(df):
    """Cast bronze trans string columns to typed columns."""
    k_symbol_trimmed = F.trim(F.col("k_symbol"))
    # Some k_symbol values contain only spaces; they become NULL
    k_symbol_clean = F.when(k_symbol_trimmed == "", None).otherwise(k_symbol_trimmed)

    df_typed = df.select(
        F.col("trans_id").cast("int").alias("trans_id"),
        F.col("account_id").cast("int").alias("account_id"),
        berka_date("date").alias("trans_date"),
        F.col("type"),
        F.col("operation"),
        F.col("amount").cast("decimal(12,2)").alias("amount"),
        F.col("balance").cast("decimal(12,2)").alias("balance"),
        k_symbol_clean.alias("k_symbol"),
        F.col("bank"),
        F.col("account"),
    )
    return df_typed



def multiply_rows(spark, df, factor, id_column, id_offset, date_columns, shift_years):
    """Repeat each row `factor` times. copy_no 0 is the original row.

    Copy n: id + n * id_offset, every date column + n * shift_years years.
    """
    df_copy_numbers = spark.range(factor).withColumnRenamed("id", "copy_no")
    # Each row is joined with every copy number: rows x factor
    df_multiplied = df.crossJoin(df_copy_numbers)

    # id becomes bigint (long) so large offsets cannot overflow
    new_id = F.col(id_column) + F.col("copy_no") * id_offset
    df_multiplied = df_multiplied.withColumn(id_column, new_id)

    months_to_add = (F.col("copy_no") * 12 * shift_years).cast("int")
    for date_column in date_columns:
        shifted_date = F.add_months(F.col(date_column), months_to_add)
        df_multiplied = df_multiplied.withColumn(date_column, shifted_date)

    return df_multiplied


def add_skew(df, key_column, hot_value, skew_ratio, seed):
    """Move about skew_ratio of the copied rows (copy_no > 0) to one hot key value.

    Original rows (copy_no 0) are not changed.
    """
    is_copy = F.col("copy_no") > 0
    # rand gives each row a number in [0, 1); about skew_ratio of rows fall below it
    is_selected = F.rand(seed) < skew_ratio
    new_key = F.when(is_copy & is_selected, F.lit(hot_value)).otherwise(F.col(key_column))
    df_skewed = df.withColumn(key_column, new_key)
    return df_skewed



def write_table(df, table_name):
    """Overwrite a managed Delta table in the perf schema."""
    full_name = PERF_SCHEMA + "." + table_name
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_name)
    )
    return full_name


def run(spark, factor=5, skew_ratio=0.3, hot_account_id=8261, seed=42):
    """Build perf.trans_big (multiplied) and perf.trans_skewed (multiplied + hot account)."""
    df_trans_bronze = spark.table(BRONZE_SCHEMA + ".trans")
    df_trans = to_perf_trans(df_trans_bronze)

    df_trans_big = multiply_rows(
        spark, df_trans, factor, "trans_id", ID_OFFSET, ["trans_date"], SHIFT_YEARS
    )
    write_table(df_trans_big, "trans_big")

    # skew_ratio = 0 means no skewed table
    if skew_ratio > 0:
        df_trans_skewed = add_skew(df_trans_big, "account_id", hot_account_id, skew_ratio, seed)
        write_table(df_trans_skewed, "trans_skewed")

