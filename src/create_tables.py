import re

catalog = "dev"
volume_schema = "config"
volume = "configuration"

base_path = f"/Volumes/{catalog}/{volume_schema}/{volume}"

VALID_LAYERS = ["bronze", "silver", "gold"]


def sanitize(name):
    return re.sub(r"[^a-zA-Z0-9_]", "_", name.lower())


def build_schema(csv_path):
    df = spark.read.option("header", True).csv(csv_path)
    rows = df.collect()

    columns = []
    for r in rows:
        col_name = r["column_name"]
        data_type = r["data_type"]
        comment = r["comment"] if r["comment"] else ""

        columns.append(f"`{col_name}` {data_type} COMMENT '{comment}'")

    return ",\n  ".join(columns)


def get_target_schema(layer):
    if layer not in VALID_LAYERS:
        raise ValueError(f"Invalid layer: {layer}")
    return layer


def build_table_name(file_name, layer):
    base_name = sanitize(file_name.replace(".csv", ""))

    if layer in ["bronze", "silver"]:
        return f"{base_name}_{layer}"
    return base_name


def create_table(layer, table_name, schema_sql):
    target_schema = get_target_schema(layer)
    full_table = f"{catalog}.{target_schema}.{table_name}"

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {full_table} (
      {schema_sql}
    )
    USING DELTA
    """

    spark.sql(create_sql)


folders = dbutils.fs.ls(base_path)

for folder in folders:
    if not folder.isDir():
        continue

    layer = folder.name.rstrip("/").lower()

    if layer not in VALID_LAYERS:
        print(f"Skipping non-layer folder: {layer}")
        continue

    files = dbutils.fs.ls(folder.path)

    for file in files:
        if not file.name.endswith(".csv"):
            continue

        table_name = build_table_name(file.name, layer)
        schema_sql = build_schema(file.path)

        create_table(layer, table_name, schema_sql)
