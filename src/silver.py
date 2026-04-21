from pyspark.sql import functions as F
from pyspark.sql.window import Window

catalog = "dev"
bronze_schema = "bronze"
silver_schema = "silver"


def normalize_string_columns(df):
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, F.trim(F.col(c)))
            df = df.withColumn(
                c,
                F.when(F.col(c).isin("", " ", "N/A", "NULL", "null"), None).otherwise(
                    F.col(c)
                ),
            )
    return df


def to_array(col):
    return F.when(F.col(col).isNull(), F.array()).otherwise(
        F.split(F.regexp_replace(F.col(col), r"\[|\]| ", ""), ",")
    )


def deduplicate_by_key(df, key_cols):
    key_expr = F.concat_ws("_", *[F.col(c).cast("string") for c in key_cols])
    window = Window.partitionBy(key_expr).orderBy(F.monotonically_increasing_id())

    return (
        df.withColumn("_row", F.row_number().over(window))
        .filter(F.col("_row") == 1)
        .drop("_row")
    )


# ---------------- EMISSIONS ----------------
emissions = spark.table(f"{catalog}.{bronze_schema}.fueleconomy_emissions_bronze")
emissions = normalize_string_columns(emissions)
emissions = deduplicate_by_key(emissions, ["efid", "id", "salesArea"])

emissions.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog}.{silver_schema}.emissions_silver"
)


# ---------------- VEHICLES ----------------
vehicles = spark.table(f"{catalog}.{bronze_schema}.fueleconomy_vehicles_bronze")
vehicles = normalize_string_columns(vehicles)
vehicles = deduplicate_by_key(vehicles, ["id", "make", "model", "year"])

vehicles = vehicles.withColumn(
    "cylinders", F.when(F.col("cylinders") == 0, None).otherwise(F.col("cylinders"))
)

vehicles.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog}.{silver_schema}.vehicles_silver"
)


# ---------------- SAFERCAR ----------------
safercar = spark.table(f"{catalog}.{bronze_schema}.nhtsa_safercar_bronze")
safercar = normalize_string_columns(safercar)

safercar = deduplicate_by_key(safercar, ["MAKE", "MODEL", "MODEL_YR", "FRNT_TEST_NO"])

array_cols = ["SEAT_LOC", "HEAD_SAB_LOC", "TORSO_SAB_LOC"]

for c in array_cols:
    if c in safercar.columns:
        safercar = safercar.withColumn(c, to_array(c))

safercar.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog}.{silver_schema}.safercar_silver"
)


# ---------------- HEAVY DUTY ENGINES ----------------
engines = spark.table(f"{catalog}.{bronze_schema}.heavy_duty_engines_bronze")
engines = normalize_string_columns(engines)

engines = deduplicate_by_key(engines, ["Engine ID"])

engines = engines.withColumn("vehicle_ids", to_array("vehicle_ids"))

engines.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog}.{silver_schema}.heavy_duty_engines_silver"
)
