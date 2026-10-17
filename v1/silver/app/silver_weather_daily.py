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

BRONZE_PATH = f"{BUCKET}/bronze/app/weather_daily/"

DAILY_PATH = f"{BUCKET}/silver/app/weather_daily/"
HOURLY_PATH = f"{BUCKET}/silver/app/weather_hourly/"


df = read_delta(BRONZE_PATH)


# ============================================================
# Achata localização
# ============================================================

base = (
    df
    .select(
        F.col("_id").cast("string").alias("weather_daily_id"),

        F.col("location.city")
            .cast("string")
            .alias("city"),

        F.col("location.state")
            .cast("string")
            .alias("state"),

        F.col("location.country")
            .cast("string")
            .alias("country"),

        F.col("weather_date")
            .cast("date")
            .alias("weather_date"),

        F.col("samples")
            .cast("int"),

        F.col("created_at")
            .cast("timestamp"),

        F.col("updated_at")
            .cast("timestamp"),

        F.col("hours"),

        F.col("_ingested_at")
            .cast("timestamp")
    )
)


# Pode existir somente uma versão por localização/data.
base = keep_latest(
    base,
    ["city", "state", "country", "weather_date"],
    F.col("updated_at").desc_nulls_last(),
    F.col("_ingested_at").desc()
)


# ============================================================
# WEATHER DAILY
# ============================================================

daily = (
    base
    .drop("hours")
    .withColumn(
        "_silver_processed_at",
        F.current_timestamp()
    )
)


# ============================================================
# WEATHER HOURLY
# ============================================================

hourly = (
    base
    .select(
        "weather_daily_id",
        "city",
        "state",
        "country",
        "weather_date",

        F.explode_outer("hours").alias("hour_data"),

        "_ingested_at"
    )
    .filter(F.col("hour_data").isNotNull())
    .select(
        "weather_daily_id",
        "city",
        "state",
        "country",
        "weather_date",

        F.col("hour_data.hour")
            .cast("int")
            .alias("hour"),

        F.col("hour_data.observed_at")
            .cast("timestamp")
            .alias("observed_at"),

        F.col("hour_data.temperature_c")
            .cast("double")
            .alias("temperature_c"),

        F.col("hour_data.feels_like_c")
            .cast("double")
            .alias("feels_like_c"),

        F.col("hour_data.humidity_percent")
            .cast("double")
            .alias("humidity_percent"),

        F.col("hour_data.rain_mm")
            .cast("double")
            .alias("rain_mm"),

        F.col("hour_data.wind_speed_mps")
            .cast("double")
            .alias("wind_speed_mps"),

        F.col("hour_data.condition")
            .cast("string")
            .alias("condition"),

        "_ingested_at"
    )
)

hourly = keep_latest(
    hourly,
    [
        "city",
        "state",
        "country",
        "weather_date",
        "hour"
    ],
    F.col("observed_at").desc_nulls_last(),
    F.col("_ingested_at").desc()
)

hourly = hourly.withColumn(
    "_silver_processed_at",
    F.current_timestamp()
)


save_and_show(
    daily,
    DAILY_PATH,
    "silver.app.weather_daily"
)

save_and_show(
    hourly,
    HOURLY_PATH,
    "silver.app.weather_hourly"
)

job.commit()
