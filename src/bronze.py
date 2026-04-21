from pyspark.sql import functions as F

catalog = "dev"
schema = "bronze"
api_key = dbutils.secrets.get(scope="nlr", key="transport_api_key")


def load_csv(url):
    return spark.read.option("header", True).option("inferSchema", False).csv(url)


def write(df, table):
    full_table = f"{catalog}.{schema}.{table}"

    df = df.withColumn("ingestion_time", F.current_timestamp())

    df.write.format("delta").mode("overwrite").saveAsTable(full_table)


# HEAVY DUTY ENGINES
hd_url = f"https://developer.nlr.gov/api/vehicles/v1/heavy_duty_engines.csv?api_key={api_key}"
df_hd = load_csv(hd_url)
write(df_hd, "heavy_duty_engines_bronze")


# FUEL ECONOMY VEHICLES
df_fuel = load_csv("https://fueleconomy.gov/feg/epadata/vehicles.csv")
write(df_fuel, "fueleconomy_vehicles_bronze")


# EMISSIONS
df_emissions = load_csv("https://fueleconomy.gov/feg/epadata/emissions.csv")
write(df_emissions, "fueleconomy_emissions_bronze")


# SAFERCAR
df_safercar = load_csv(
    "https://static.nhtsa.gov/nhtsa/downloads/Safercar/Safercar_data.csv"
)
write(df_safercar, "nhtsa_safercar_bronze")
