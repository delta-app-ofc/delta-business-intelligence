import logging
import sys

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable


args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.session.timeZone", "UTC")

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

S3_SOURCE = "s3://delta-app-oficial-datalake/mongo-database/bronze/telemetry/"
S3_DESTINATION = "s3://delta-app-oficial-datalake/mongo-database/silver/telemetry/"

logger = logging.getLogger("telemetry_silver")
failed_tables = []
completed_tables = []


def read_bronze(table_path: str, schema: StructType):
    df = spark.read.format("delta").load(table_path)

    # O schema cria os campos ausentes como null, inclusive dentro dos arrays.
    # Tipos incompatíveis interrompem a tabela, sem ocultar erros de conversão.
    return (
        df.select(
            F.col("_id").cast(StringType()).alias("_id"),
            F.col("_ingested_at").cast(TimestampType()).alias("_ingested_at"),
            F.from_json(
                F.to_json(F.struct("*")),
                schema,
                {"mode": "FAILFAST"},
            ).alias("document"),
        )
        .select("_id", "_ingested_at", "document.*")
    )


def keep_latest(df, keys, *order_columns):
    window = Window.partitionBy(*keys).orderBy(*order_columns)

    return (
        df.withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )


# =================================
# consumption_summary
# =================================

CONSUMPTION_SCHEMA = StructType([
    StructField("device_id", StringType()),
    StructField("user_id", IntegerType()),
    StructField("window_started_at", TimestampType()),
    StructField("window_finished_at", TimestampType()),
    StructField("consumption_liters", DoubleType()),
    StructField("lpm_average", DoubleType()),
    StructField("anomaly_detected", BooleanType()),
])

table_name = "consumption_summary"
source_path = f"{S3_SOURCE}{table_name}/"
table_path = f"{S3_DESTINATION}{table_name}/"
stage = "leitura da Bronze"

try:
    print(f"\n[INÍCIO] silver.telemetry.{table_name}", flush=True)
    df = read_bronze(source_path, CONSUMPTION_SCHEMA)

    stage = "transformação e deduplicação"
    silver = df.select(
        F.col("_id").alias("consumption_id"),
        "device_id",
        "user_id",
        "window_started_at",
        "window_finished_at",
        "consumption_liters",
        "lpm_average",
        "anomaly_detected",
        "_ingested_at",
    )

    silver = keep_latest(
        silver,
        ["device_id", "window_started_at", "window_finished_at"],
        F.col("_ingested_at").desc(),
    ).withColumn("_silver_processed_at", F.current_timestamp())

    stage = "gravação Delta (overwrite)"
    (
        silver.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(table_path)
    )

    stage = "leitura das métricas Delta"
    spark.catalog.refreshByPath(table_path)
    metrics = (
        DeltaTable.forPath(spark, table_path)
        .history(1)
        .select("operationMetrics")
        .collect()[0]["operationMetrics"]
    )
    written_count = int(metrics.get("numOutputRows", 0))

    completed_tables.append(table_name)
    print(
        f"[SUCESSO] silver.telemetry.{table_name}: "
        f"{written_count} registros gravados. Destino: {table_path}",
        flush=True,
    )

except Exception:
    failed_tables.append(table_name)
    logger.exception(
        "[ERRO] Falha em silver.telemetry.%s na etapa '%s'. Origem: %s. "
        "Destino: %s. O processamento das demais tabelas continuará.",
        table_name, stage, source_path, table_path,
    )


# =================================
# device_status
# =================================

UNAVAILABILITY_SCHEMA = StructType([
    StructField("started_at", TimestampType()),
    StructField("ended_at", TimestampType()),
    StructField("unavailable_seconds", LongType()),
    StructField("unavailability_reason", StringType()),
])

DEVICE_STATUS_SCHEMA = StructType([
    StructField("device_id", StringType()),
    StructField("last_ping_at", TimestampType()),
    StructField("wifi_signal_rssi", StringType()),
    StructField("firmware_version", StringType()),
    StructField("connectivity_status", StringType()),
    StructField("battery_level", IntegerType()),
    StructField("availability_started_at", TimestampType()),
    StructField("availability_updated_at", TimestampType()),
    StructField("available_seconds", LongType()),
    StructField("unavailabilities", ArrayType(UNAVAILABILITY_SCHEMA)),
])

table_name = "device_status"
source_path = f"{S3_SOURCE}{table_name}/"
table_path = f"{S3_DESTINATION}{table_name}/"
stage = "leitura da Bronze"

try:
    print(f"\n[INÍCIO] silver.telemetry.{table_name}", flush=True)
    df = read_bronze(source_path, DEVICE_STATUS_SCHEMA)

    stage = "transformação e deduplicação"
    silver = df.select(
        F.col("_id").alias("device_status_id"),
        "device_id",
        "last_ping_at",
        "wifi_signal_rssi",
        "firmware_version",
        "connectivity_status",
        "battery_level",
        "availability_started_at",
        "availability_updated_at",
        "available_seconds",
        "unavailabilities",
        "_ingested_at",
    )

    # Um dispositivo offline pode ter ping antigo: a ingestão mais recente vence.
    silver = keep_latest(
        silver,
        ["device_id"],
        F.col("_ingested_at").desc(),
        F.col("availability_updated_at").desc_nulls_last(),
        F.col("last_ping_at").desc_nulls_last(),
    ).withColumn("_silver_processed_at", F.current_timestamp())

    stage = "gravação Delta (overwrite)"
    (
        silver.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(table_path)
    )

    stage = "leitura das métricas Delta"
    spark.catalog.refreshByPath(table_path)
    metrics = (
        DeltaTable.forPath(spark, table_path)
        .history(1)
        .select("operationMetrics")
        .collect()[0]["operationMetrics"]
    )
    written_count = int(metrics.get("numOutputRows", 0))

    completed_tables.append(table_name)
    print(
        f"[SUCESSO] silver.telemetry.{table_name}: "
        f"{written_count} registros gravados. Destino: {table_path}",
        flush=True,
    )

except Exception:
    failed_tables.append(table_name)
    logger.exception(
        "[ERRO] Falha em silver.telemetry.%s na etapa '%s'. Origem: %s. "
        "Destino: %s. O processamento das demais tabelas continuará.",
        table_name, stage, source_path, table_path,
    )


print(
    f"\n[RESUMO] Gravadas: {len(completed_tables)}; falhas: {len(failed_tables)}.",
    flush=True,
)

if failed_tables:
    raise RuntimeError(
        "Silver telemetry falhou após processar todas as tabelas. "
        "Tabelas com erro: " + ", ".join(failed_tables)
    )

job.commit()
