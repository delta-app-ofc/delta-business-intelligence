import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


BUCKET = "s3://delta-app-oficial-datalake"


def read_delta(path):
    return (
        spark.read
        .format("delta")
        .load(path)
    )


def keep_latest(df, keys, *order_columns):
    """
    Remove duplicidades de forma determinística.
    Mantém o registro mais recente para cada chave.
    """

    window = (
        Window
        .partitionBy(*keys)
        .orderBy(*order_columns)
    )

    return (
        df
        .withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )


def save_and_show(df, path, table_name):
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(path)
    )

    print(f"\n===== {table_name} =====")
    print(f"Path: {path}")
    print(f"Rows: {df.count()}")

    df.printSchema()
    df.show(20, truncate=False)

BRONZE_PATH = f"{BUCKET}/bronze/app/user_preferences/"
SILVER_PATH = f"{BUCKET}/silver/app/user_preferences/"


df = read_delta(BRONZE_PATH)

silver = (
    df
    .select(
        F.col("_id").cast("string").alias("preference_id"),

        F.col("user_id").cast("int"),
        F.col("daily_liters_target").cast("int"),

        F.col("notifications_enabled").cast("boolean"),
        F.col("dark_mode_enabled").cast("boolean"),

        F.col("quiet_hours.start_hour")
            .cast("string")
            .alias("quiet_start_hour"),

        F.col("quiet_hours.end_hour")
            .cast("string")
            .alias("quiet_end_hour"),

        F.col("_ingested_at").cast("timestamp"),
        F.col("_source_database"),
        F.col("_source_collection")
    )
)

# Deve existir somente uma preferência atual por usuário.
silver = keep_latest(
    silver,
    ["user_id"],
    F.col("_ingested_at").desc()
)

silver = silver.withColumn(
    "_silver_processed_at",
    F.current_timestamp()
)

save_and_show(
    silver,
    SILVER_PATH,
    "silver.app.user_preferences"
)

job.commit()
