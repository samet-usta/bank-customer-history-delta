"""CDC batch generator for the Berka bank dataset.

Reads the current state from bronze, simulates changes and writes
JSON lines batch files to the landing volume.
"""

import calendar
from datetime import datetime

import random
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

LANDING_PATH = "/Volumes/workspace/bronze/landing"
BRONZE_SCHEMA = "workspace.bronze"
FREQUENCIES = ["POPLATEK MESICNE", "POPLATEK TYDNE", "POPLATEK PO OBRATU"]
CDC_COLUMNS = ["op_type", "change_ts", "batch_id"]

# Edge case IDs are far above max ID, so they never exist
EDGE_CASE_ID_OFFSET = 100000

# Key column of each table; district and disp are read only as reference data
TABLE_KEYS = {
    "client": "client_id",
    "account": "account_id",
    "card": "card_id",
    "loan": "loan_id",
    "district": "A1",
    "disp": "disp_id",
}

METADATA_COLUMNS = ["_ingest_ts", "_source_file"]


def load_current_state(spark):
    """Read bronze tables into dictionaries: {table: {id: row_dict}}."""
    state = {}
    for table_name, key_column in TABLE_KEYS.items():
        df_bronze = spark.table(f"{BRONZE_SCHEMA}.{table_name}")
        df_data = df_bronze.drop(*METADATA_COLUMNS)
        rows = df_data.collect()

        table_state = {}
        for row in rows:
            row_dict = row.asDict()
            key = row_dict[key_column]
            table_state[key] = row_dict

        state[table_name] = table_state
    return state

def random_change_ts(batch_id, rng):
    """Random timestamp inside the calendar month of the batch (batch 1 = 1999-01)."""
    year = 1999 + (batch_id - 1) // 12
    month = (batch_id - 1) % 12 + 1
    days_in_month = calendar.monthrange(year, month)[1]

    day = rng.randint(1, days_in_month)
    hour = rng.randint(0, 23)
    minute = rng.randint(0, 59)
    second = rng.randint(0, 59)

    change_ts = datetime(year, month, day, hour, minute, second)
    return change_ts.isoformat()


def get_sorted_ids(table_state):
    """IDs sorted as numbers, so rng picks are the same in every run."""
    return sorted(table_state.keys(), key=int)


def get_max_id(table_state):
    max_id = 0
    for key in table_state:
        if int(key) > max_id:
            max_id = int(key)
    return max_id

def to_berka_date(date_value, with_time=False):
    """Berka date format: YYMMDD, or 'YYMMDD 00:00:00' for card.issued."""
    yy = date_value.year % 100
    berka_date = f"{yy:02d}{date_value.month:02d}{date_value.day:02d}"
    if with_time:
        berka_date = berka_date + " 00:00:00"
    return berka_date


def make_change(row, op_type, batch_id, rng):
    """Full row image plus CDC columns."""
    change = dict(row)
    change["op_type"] = op_type
    change["change_ts"] = random_change_ts(batch_id, rng)
    change["batch_id"] = batch_id
    return change


def random_birth_number(rng):
    """Berka format YYMMDD; for women the month is +50."""
    year = rng.randint(1940, 1980)
    month = rng.randint(1, 12)
    days_in_month = calendar.monthrange(year, month)[1]
    day = rng.randint(1, days_in_month)

    is_female = rng.random() < 0.5
    if is_female:
        month = month + 50

    yy = year % 100
    return f"{yy:02d}{month:02d}{day:02d}"


def generate_client_changes(state, batch_id, rng):
    """Address changes (U) and new clients (I)."""
    changes = []
    client_ids = get_sorted_ids(state["client"])
    district_ids = get_sorted_ids(state["district"])

    num_updates = rng.randint(5, 20)
    update_ids = rng.sample(client_ids, num_updates)
    for client_id in update_ids:
        row = dict(state["client"][client_id])
        old_district = row["district_id"]
        new_district = rng.choice(district_ids)
        while new_district == old_district:
            new_district = rng.choice(district_ids)
        row["district_id"] = new_district
        changes.append(make_change(row, "U", batch_id, rng))

    num_inserts = rng.randint(1, 5)
    next_id = get_max_id(state["client"]) + 1
    for i in range(num_inserts):
        row = {
            "client_id": str(next_id + i),
            "birth_number": random_birth_number(rng),
            "district_id": rng.choice(district_ids),
        }
        changes.append(make_change(row, "I", batch_id, rng))
    return changes


def generate_account_changes(state, batch_id, rng):
    """Statement frequency changes (U)."""
    changes = []
    account_ids = get_sorted_ids(state["account"])

    num_updates = rng.randint(5, 20)
    update_ids = rng.sample(account_ids, num_updates)
    for account_id in update_ids:
        row = dict(state["account"][account_id])
        old_frequency = row["frequency"]
        new_frequency = rng.choice(FREQUENCIES)
        while new_frequency == old_frequency:
            new_frequency = rng.choice(FREQUENCIES)
        row["frequency"] = new_frequency
        changes.append(make_change(row, "U", batch_id, rng))
    return changes


def generate_card_changes(state, batch_id, rng):
    """Upgrades classic -> gold (U), cancellations (D) and new cards (I)."""
    changes = []
    card_ids = get_sorted_ids(state["card"])

    classic_ids = []
    for card_id in card_ids:
        if state["card"][card_id]["type"] == "classic":
            classic_ids.append(card_id)

    num_updates = rng.randint(3, 10)
    update_ids = rng.sample(classic_ids, num_updates)
    for card_id in update_ids:
        row = dict(state["card"][card_id])
        row["type"] = "gold"
        changes.append(make_change(row, "U", batch_id, rng))

    # A card is not updated and deleted in the same batch
    delete_candidates = []
    for card_id in card_ids:
        if card_id not in update_ids:
            delete_candidates.append(card_id)

    num_deletes = rng.randint(1, 5)
    delete_ids = rng.sample(delete_candidates, num_deletes)
    for card_id in delete_ids:
        row = dict(state["card"][card_id])
        changes.append(make_change(row, "D", batch_id, rng))

    disp_ids_with_card = set()
    for card_row in state["card"].values():
        disp_ids_with_card.add(card_row["disp_id"])

    free_disp_ids = []
    for disp_id in get_sorted_ids(state["disp"]):
        disp_row = state["disp"][disp_id]
        if disp_row["type"] == "OWNER" and disp_id not in disp_ids_with_card:
            free_disp_ids.append(disp_id)

    num_inserts = rng.randint(1, 5)
    new_disp_ids = rng.sample(free_disp_ids, num_inserts)
    next_id = get_max_id(state["card"]) + 1
    for i in range(num_inserts):
        change_ts = random_change_ts(batch_id, rng)
        issued_date = datetime.fromisoformat(change_ts)
        row = {
            "card_id": str(next_id + i),
            "disp_id": new_disp_ids[i],
            "type": rng.choice(["classic", "junior"]),
            "issued": to_berka_date(issued_date, with_time=True),
        }
        change = make_change(row, "I", batch_id, rng)
        # The card is issued on the day of the change
        change["change_ts"] = change_ts
        changes.append(change)
    return changes


def generate_loan_changes(state, batch_id, rng):
    """Running loans change status: C -> A (finished) or C -> D (in debt) (U)."""
    changes = []

    running_ids = []
    for loan_id in get_sorted_ids(state["loan"]):
        if state["loan"][loan_id]["status"] == "C":
            running_ids.append(loan_id)

    num_updates = rng.randint(5, 15)
    update_ids = rng.sample(running_ids, num_updates)
    for loan_id in update_ids:
        row = dict(state["loan"][loan_id])
        row["status"] = rng.choice(["A", "D"])
        changes.append(make_change(row, "U", batch_id, rng))
    return changes

def get_change_ts(change):
    """Sort key: changes are applied in change_ts order."""
    return change["change_ts"]


def apply_to_state(state, table_name, changes):
    """Apply one batch of one table to the in-memory state."""
    key_column = TABLE_KEYS[table_name]
    table_state = state[table_name]
    ordered_changes = sorted(changes, key=get_change_ts)

    for change in ordered_changes:
        key = change[key_column]
        op_type = change["op_type"]

        row = {}
        for column, value in change.items():
            if column not in CDC_COLUMNS:
                row[column] = value

        if op_type == "I":
            table_state[key] = row
        elif op_type == "U":
            # An update for a missing key changes nothing, like in the source system
            if key in table_state:
                table_state[key] = row
        elif op_type == "D":
            if key in table_state:
                del table_state[key]


def ordered_change_ts_pair(batch_id, rng):
    """Two different change_ts values in the batch month, the earlier one first."""
    first_ts = random_change_ts(batch_id, rng)
    second_ts = random_change_ts(batch_id, rng)
    while second_ts == first_ts:
        second_ts = random_change_ts(batch_id, rng)

    if second_ts < first_ts:
        temp_ts = first_ts
        first_ts = second_ts
        second_ts = temp_ts

    return first_ts, second_ts


def add_same_key_twice(state, batch_id, rng, batch_changes):
    """Two clients move twice (U -> U), one new card is upgraded (I -> U)."""
    district_ids = get_sorted_ids(state["district"])

    client_updates = []
    for change in batch_changes["client"]:
        if change["op_type"] == "U":
            client_updates.append(change)

    moved_twice = rng.sample(client_updates, 2)
    for first_change in moved_twice:
        first_ts, second_ts = ordered_change_ts_pair(batch_id, rng)
        first_change["change_ts"] = first_ts

        second_change = dict(first_change)
        new_district = rng.choice(district_ids)
        while new_district == first_change["district_id"]:
            new_district = rng.choice(district_ids)
        second_change["district_id"] = new_district
        second_change["change_ts"] = second_ts
        batch_changes["client"].append(second_change)
        print("Edge case U -> U, client_id:", first_change["client_id"])

    card_inserts = []
    for change in batch_changes["card"]:
        if change["op_type"] == "I":
            card_inserts.append(change)

    insert_change = rng.choice(card_inserts)
    first_ts, second_ts = ordered_change_ts_pair(batch_id, rng)
    insert_change["change_ts"] = first_ts
    insert_change["issued"] = to_berka_date(datetime.fromisoformat(first_ts), with_time=True)

    upgrade_change = dict(insert_change)
    upgrade_change["op_type"] = "U"
    upgrade_change["type"] = "gold"
    upgrade_change["change_ts"] = second_ts
    batch_changes["card"].append(upgrade_change)
    print("Edge case I -> U, card_id:", insert_change["card_id"])


def add_missing_key_changes(state, batch_id, rng, batch_changes):
    """Delete for a missing card, update for a missing loan."""
    card_ids = get_sorted_ids(state["card"])
    missing_card = dict(state["card"][rng.choice(card_ids)])
    missing_card["card_id"] = str(get_max_id(state["card"]) + EDGE_CASE_ID_OFFSET)
    batch_changes["card"].append(make_change(missing_card, "D", batch_id, rng))
    print("Edge case D on missing key, card_id:", missing_card["card_id"])

    loan_ids = get_sorted_ids(state["loan"])
    missing_loan = dict(state["loan"][rng.choice(loan_ids)])
    missing_loan["loan_id"] = str(get_max_id(state["loan"]) + EDGE_CASE_ID_OFFSET)
    missing_loan["status"] = "A"
    batch_changes["loan"].append(make_change(missing_loan, "U", batch_id, rng))
    print("Edge case U on missing key, loan_id:", missing_loan["loan_id"])


def add_edge_cases(state, batch_id, rng, batch_changes):
    """Batch 2: same key twice. Batch 3: changes for missing keys."""
    if batch_id == 2:
        add_same_key_twice(state, batch_id, rng, batch_changes)
    elif batch_id == 3:
        add_missing_key_changes(state, batch_id, rng, batch_changes)


def write_batch(spark, table_name, batch_id, changes):
    """Write one batch of one table as JSON lines to the landing volume."""
    data_columns = []
    for column in changes[0].keys():
        if column not in CDC_COLUMNS:
            data_columns.append(column)

    # Explicit schema: data columns stay string like bronze, batch_id is int
    fields = []
    for column in data_columns:
        fields.append(StructField(column, StringType(), True))
    fields.append(StructField("op_type", StringType(), False))
    fields.append(StructField("change_ts", StringType(), False))
    fields.append(StructField("batch_id", IntegerType(), False))
    schema = StructType(fields)

    rows = []
    for change in changes:
        row = []
        for field in fields:
            row.append(change[field.name])
        rows.append(row)

    df_batch = spark.createDataFrame(rows, schema)
    batch_path = f"{LANDING_PATH}/{table_name}/batch_{batch_id:04d}"
    df_batch.coalesce(1).write.mode("overwrite").json(batch_path)
    return batch_path


def run(spark, num_batches, seed):
    """Generate CDC batches from bronze and write them to the landing volume."""
    state = load_current_state(spark)
    rng = random.Random(seed)

    for batch_id in range(1, num_batches + 1):
        print(f"=== Batch {batch_id} ===")
        batch_changes = {
            "client": generate_client_changes(state, batch_id, rng),
            "account": generate_account_changes(state, batch_id, rng),
            "card": generate_card_changes(state, batch_id, rng),
            "loan": generate_loan_changes(state, batch_id, rng),
        }
        add_edge_cases(state, batch_id, rng, batch_changes)

        for table_name, changes in batch_changes.items():
            apply_to_state(state, table_name, changes)
            batch_path = write_batch(spark, table_name, batch_id, changes)
            print(f"{table_name}: {len(changes)} rows -> {batch_path}")


