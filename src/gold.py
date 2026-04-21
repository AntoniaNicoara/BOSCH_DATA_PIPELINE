from pyspark.sql import functions as F
from pyspark.sql.window import Window

catalog = "dev"
schema = "gold"
volume_path = "/Volumes/dev/config/configuration/scripts"


def run_sql_file(file_path):
    sql_text = spark.read.text(file_path).collect()
    sql_query = "\n".join([row.value for row in sql_text])
    return spark.sql(sql_query)


def table_name_from_file(file_name):
    return file_name.replace(".sql", "")


files = dbutils.fs.ls(volume_path)

for f in files:
    if not f.name.endswith(".sql"):
        continue

    df = run_sql_file(f.path)

    table_name = table_name_from_file(f.name)
    full_table = f"{catalog}.{schema}.{table_name}"

    df.write.format("delta").mode("overwrite").saveAsTable(full_table)
