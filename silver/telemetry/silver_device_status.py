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
# Prefixo padrão do Mongo; não depende de função auxiliar.
MONGO_PREFIX = (
    getResolvedOptions(sys.argv, ["MONGO_PREFIX"])["MONGO_PREFIX"]
    if "--MONGO_PREFIX" in sys.argv else "mongo-database"
).strip().strip("/")
if MONGO_PREFIX not in ("", "mongo-database"):
    raise ValueError("MONGO_PREFIX inválido")
BUCKET += "/" + MONGO_PREFIX if MONGO_PREFIX else ""
spark.conf.set("spark.sql.session.timeZone", "UTC")


def read_delta(path):
    return (
        spark.read
        .format("delta")
        .load(path)
    )


def keep_latest(df, keys, *order_columns):
    """
    Remove duplicidades conforme a ordenação informada.
    Mantém o primeiro registro para cada chave.
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

BRONZE_PATH = f"{BUCKET}/bronze/telemetry/device_status/"
SILVER_PATH = f"{BUCKET}/silver/telemetry/device_status/"


df = read_delta(BRONZE_PATH)

# O array é normalizado sem depender de todos os campos terem sido inferidos.
EVENT_TYPE = "array<struct<started_at:timestamp,ended_at:timestamp,unavailable_seconds:bigint,unavailability_reason:string>>"
def optional(name, kind):
    return (F.col(name) if name in df.columns else F.lit(None)).cast(kind).alias(name)

events = (F.from_json(F.to_json(F.col("unavailabilities")), EVENT_TYPE)
          if "unavailabilities" in df.columns else F.lit(None).cast(EVENT_TYPE))
silver = df.select(
    F.col("_id").cast("string").alias("device_status_id"),
    F.col("device_id").cast("string"),
    optional("last_ping_at", "timestamp"), optional("wifi_signal_rssi", "string"),
    optional("firmware_version", "string"), optional("connectivity_status", "string"),
    optional("battery_level", "int"), optional("availability_started_at", "timestamp"),
    optional("availability_updated_at", "timestamp"), optional("available_seconds", "bigint"),
    events.alias("unavailabilities"), F.col("_ingested_at").cast("timestamp")
)
# Um estado offline pode ter ping antigo. A captura mais recente vence.
silver = keep_latest(silver, ["device_id"], F.col("_ingested_at").desc(),
                     F.col("availability_updated_at").desc_nulls_last(), F.col("last_ping_at").desc_nulls_last())

silver = silver.withColumn(
    "_silver_processed_at",
    F.current_timestamp()
)


save_and_show(
    silver,
    SILVER_PATH,
    "silver.telemetry.device_status"
)

job.commit()
