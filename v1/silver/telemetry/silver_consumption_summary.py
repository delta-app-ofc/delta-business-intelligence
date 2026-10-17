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

BRONZE_PATH = f"{BUCKET}/bronze/telemetry/consumption_summary/"
SILVER_PATH = f"{BUCKET}/silver/telemetry/consumption_summary/"


df = read_delta(BRONZE_PATH)

silver = (
    df
    .select(
        F.col("_id")
            .cast("string")
            .alias("consumption_id"),

        F.col("device_id")
            .cast("string"),

        F.col("user_id")
            .cast("int"),

        F.col("window_started_at")
            .cast("timestamp"),

        F.col("window_finished_at")
            .cast("timestamp"),

        F.col("consumption_liters")
            .cast("double"),

        F.col("lpm_average")
            .cast("double"),

        F.col("anomaly_detected")
            .cast("boolean"),

        F.col("_ingested_at")
            .cast("timestamp")
    )
)


silver = keep_latest(
    silver,
    [
        "device_id",
        "window_started_at",
        "window_finished_at"
    ],
    F.col("_ingested_at").desc()
)

silver = silver.withColumn(
    "_silver_processed_at",
    F.current_timestamp()
)


save_and_show(
    silver,
    SILVER_PATH,
    "silver.telemetry.consumption_summary"
)

job.commit()
