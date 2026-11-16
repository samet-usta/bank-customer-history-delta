"""Bronze -> silver type conversions for the CDC tables (client, account, card, loan)."""

from pyspark.sql import functions as F

BRONZE_SCHEMA = "workspace.bronze"
SILVER_SCHEMA = "workspace.silver"
CDC_TABLES = ["client", "account", "card", "loan"]
INITIAL_CHANGE_TS = "1998-12-31 00:00:00"


def berka_date(column_name):
    """Convert a Berka YYMMDD string (first 6 chars) to a date."""
    # 'yy' pattern maps 93 to 2093; all Berka years are 19xx, so add "19" explicitly.
    yymmdd = F.substring(F.col(column_name), 1, 6)
    yyyymmdd = F.concat(F.lit("19"), yymmdd)
    return F.to_date(yyyymmdd, "yyyyMMdd")


def to_silver_client(df):
    """Cast client columns to silver types."""
    df_silver = df.select(
        F.col("client_id").cast("int").alias("client_id"),
        F.col("birth_number"),
        F.col("district_id").cast("int").alias("district_id"),
    )
    return df_silver


def to_silver_account(df):
    """Cast account columns to silver types."""
    df_silver = df.select(
        F.col("account_id").cast("int").alias("account_id"),
        F.col("district_id").cast("int").alias("district_id"),
        F.col("frequency"),
        berka_date("date").alias("open_date"),
    )
    return df_silver


def to_silver_card(df):
    """Cast card columns to silver types."""
    df_silver = df.select(
        F.col("card_id").cast("int").alias("card_id"),
        F.col("disp_id").cast("int").alias("disp_id"),
        F.col("type"),
        berka_date("issued").alias("issued_date"),
    )
    return df_silver


def to_silver_loan(df):
    """Cast loan columns to silver types."""
    df_silver = df.select(
        F.col("loan_id").cast("int").alias("loan_id"),
        F.col("account_id").cast("int").alias("account_id"),
        berka_date("date").alias("loan_date"),
        F.col("amount").cast("int").alias("amount"),
        F.col("duration").cast("int").alias("duration"),
        F.col("payments").cast("decimal(10,2)").alias("payments"),
        F.col("status"),
    )
    return df_silver


def to_silver(df, table_name):
    """Apply the silver conversion of the given table."""
    if table_name == "client":
        return to_silver_client(df)
    if table_name == "account":
        return to_silver_account(df)
    if table_name == "card":
        return to_silver_card(df)
    if table_name == "loan":
        return to_silver_loan(df)
    raise ValueError("Unknown table: " + table_name)

