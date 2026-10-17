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
    DateType,
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

S3_SOURCE = "s3://delta-app-oficial-datalake/mongo-database/bronze/app/"
S3_DESTINATION = "s3://delta-app-oficial-datalake/mongo-database/silver/app/"

logger = logging.getLogger("app_silver")
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
# alerts_history
# =================================

ALERT_SCHEMA = StructType([
    StructField("device_id", StringType()),
    StructField("user_id", IntegerType()),
    StructField("alert_type", StringType()),
    StructField("severity", StringType()),
    StructField("triggered_at", TimestampType()),
    StructField("resolved_at", TimestampType()),
])

table_name = "alerts_history"
source_path = f"{S3_SOURCE}{table_name}/"
table_path = f"{S3_DESTINATION}{table_name}/"
stage = "leitura da Bronze"

try:
    print(f"\n[INÍCIO] silver.app.{table_name}", flush=True)
    df = read_bronze(source_path, ALERT_SCHEMA)

    stage = "transformação e deduplicação"
    silver = df.select(
        F.col("_id").alias("alert_id"),
        "device_id",
        "user_id",
        "alert_type",
        "severity",
        "triggered_at",
        "resolved_at",
        "_ingested_at",
    )

    silver = keep_latest(
        silver,
        ["alert_id"],
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
        f"[SUCESSO] silver.app.{table_name}: "
        f"{written_count} registros gravados. Destino: {table_path}",
        flush=True,
    )

except Exception:
    failed_tables.append(table_name)
    logger.exception(
        "[ERRO] Falha em silver.app.%s na etapa '%s'. Origem: %s. "
        "Destino: %s. O processamento das demais tabelas continuará.",
        table_name, stage, source_path, table_path,
    )


# =================================
# chat_feedback
# =================================

FEEDBACK_SCHEMA = StructType([
    StructField("session_id", StringType()),
    StructField("is_satisfied", BooleanType()),
    StructField("user_comment", StringType()),
    StructField("created_at", TimestampType()),
])

table_name = "chat_feedback"
source_path = f"{S3_SOURCE}{table_name}/"
table_path = f"{S3_DESTINATION}{table_name}/"
stage = "leitura da Bronze"

try:
    print(f"\n[INÍCIO] silver.app.{table_name}", flush=True)
    df = read_bronze(source_path, FEEDBACK_SCHEMA)

    stage = "transformação e deduplicação"
    silver = df.select(
        F.col("_id").alias("feedback_id"),
        "session_id",
        "is_satisfied",
        "user_comment",
        "created_at",
        "_ingested_at",
    )

    silver = keep_latest(
        silver,
        ["feedback_id"],
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
        f"[SUCESSO] silver.app.{table_name}: "
        f"{written_count} registros gravados. Destino: {table_path}",
        flush=True,
    )

except Exception:
    failed_tables.append(table_name)
    logger.exception(
        "[ERRO] Falha em silver.app.%s na etapa '%s'. Origem: %s. "
        "Destino: %s. O processamento das demais tabelas continuará.",
        table_name, stage, source_path, table_path,
    )


# =================================
# chat_sessions
# =================================

MESSAGE_SCHEMA = StructType([
    StructField("role", StringType()),
    StructField("content_type", StringType()),
    StructField("content", StringType()),
    StructField("sent_at", TimestampType()),
    StructField("api_status_code", IntegerType()),
    StructField("agent_name", StringType()),
    StructField("response_time_ms", LongType()),
])

SESSION_SCHEMA = StructType([
    StructField("user_id", IntegerType()),
    StructField("started_at", TimestampType()),
    StructField("last_activity_at", TimestampType()),
    StructField("is_open", BooleanType()),
    StructField("is_active", BooleanType()),
    StructField("is_deleted", BooleanType()),
    StructField("messages", ArrayType(MESSAGE_SCHEMA)),
])

table_name = "chat_sessions"
source_path = f"{S3_SOURCE}{table_name}/"
table_path = f"{S3_DESTINATION}{table_name}/"
stage = "leitura da Bronze"

try:
    print(f"\n[INÍCIO] silver.app.{table_name}", flush=True)
    df = read_bronze(source_path, SESSION_SCHEMA)

    stage = "transformação e deduplicação"
    df = keep_latest(df, ["_id"], F.col("_ingested_at").desc())

    silver = df.select(
        F.col("_id").alias("session_id"),
        "user_id",
        "started_at",
        "last_activity_at",
        "is_open",
        "is_active",
        "is_deleted",
        "_ingested_at",
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
        f"[SUCESSO] silver.app.{table_name}: "
        f"{written_count} registros gravados. Destino: {table_path}",
        flush=True,
    )

except Exception:
    failed_tables.append(table_name)
    logger.exception(
        "[ERRO] Falha em silver.app.%s na etapa '%s'. Origem: %s. "
        "Destino: %s. O processamento das demais tabelas continuará.",
        table_name, stage, source_path, table_path,
    )


# =================================
# chat_messages
# =================================

table_name = "chat_messages"
source_path = f"{S3_SOURCE}chat_sessions/"
table_path = f"{S3_DESTINATION}{table_name}/"
stage = "leitura da Bronze"

try:
    print(f"\n[INÍCIO] silver.app.{table_name}", flush=True)
    df = read_bronze(source_path, SESSION_SCHEMA)

    stage = "deduplicação das sessões e expansão das mensagens"
    df = keep_latest(df, ["_id"], F.col("_ingested_at").desc())

    # A posição original identifica a mensagem dentro da sessão.
    # Arrays vazios ou nulos não geram mensagens artificiais.
    silver = (
        df.select(
            F.col("_id").alias("session_id"),
            "_ingested_at",
            F.posexplode("messages").alias("message_index", "message"),
        )
        .select("session_id", "message_index", "message.*", "_ingested_at")
        .withColumn("_silver_processed_at", F.current_timestamp())
    )

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
        f"[SUCESSO] silver.app.{table_name}: "
        f"{written_count} registros gravados. Destino: {table_path}",
        flush=True,
    )

except Exception:
    failed_tables.append(table_name)
    logger.exception(
        "[ERRO] Falha em silver.app.%s na etapa '%s'. Origem: %s. "
        "Destino: %s. O processamento das demais tabelas continuará.",
        table_name, stage, source_path, table_path,
    )


# =================================
# user_preferences
# =================================

PREFERENCE_SCHEMA = StructType([
    StructField("user_id", IntegerType()),
    StructField("daily_liters_target", IntegerType()),
    StructField("notifications_enabled", BooleanType()),
    StructField("dark_mode_enabled", BooleanType()),
    StructField("quiet_hours", StructType([
        StructField("start_hour", StringType()),
        StructField("end_hour", StringType()),
    ])),
])

table_name = "user_preferences"
source_path = f"{S3_SOURCE}{table_name}/"
table_path = f"{S3_DESTINATION}{table_name}/"
stage = "leitura da Bronze"

try:
    print(f"\n[INÍCIO] silver.app.{table_name}", flush=True)
    df = read_bronze(source_path, PREFERENCE_SCHEMA)

    stage = "transformação e deduplicação"
    silver = df.select(
        F.col("_id").alias("preference_id"),
        "user_id",
        "daily_liters_target",
        "notifications_enabled",
        "dark_mode_enabled",
        F.col("quiet_hours.start_hour").alias("quiet_start_hour"),
        F.col("quiet_hours.end_hour").alias("quiet_end_hour"),
        "_ingested_at",
        # A Bronze v2 não grava estes metadados; a origem é conhecida pelo job.
        F.lit("db_delta_app").alias("_source_database"),
        F.lit(table_name).alias("_source_collection"),
    )

    silver = keep_latest(
        silver,
        ["user_id"],
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
        f"[SUCESSO] silver.app.{table_name}: "
        f"{written_count} registros gravados. Destino: {table_path}",
        flush=True,
    )

except Exception:
    failed_tables.append(table_name)
    logger.exception(
        "[ERRO] Falha em silver.app.%s na etapa '%s'. Origem: %s. "
        "Destino: %s. O processamento das demais tabelas continuará.",
        table_name, stage, source_path, table_path,
    )


# =================================
# weather_daily
# =================================

WEATHER_HOUR_SCHEMA = StructType([
    StructField("hour", IntegerType()),
    StructField("observed_at", TimestampType()),
    StructField("temperature_c", DoubleType()),
    StructField("feels_like_c", DoubleType()),
    StructField("humidity_percent", DoubleType()),
    StructField("rain_mm", DoubleType()),
    StructField("wind_speed_mps", DoubleType()),
    StructField("condition", StringType()),
])

WEATHER_SCHEMA = StructType([
    StructField("location", StructType([
        StructField("city", StringType()),
        StructField("state", StringType()),
        StructField("country", StringType()),
    ])),
    # StringType aceita a data serializada tanto de DateType quanto de TimestampType.
    StructField("weather_date", StringType()),
    StructField("samples", IntegerType()),
    StructField("created_at", TimestampType()),
    StructField("updated_at", TimestampType()),
    StructField("hours", ArrayType(WEATHER_HOUR_SCHEMA)),
])

table_name = "weather_daily"
source_path = f"{S3_SOURCE}{table_name}/"
table_path = f"{S3_DESTINATION}{table_name}/"
stage = "leitura da Bronze"

try:
    print(f"\n[INÍCIO] silver.app.{table_name}", flush=True)
    df = read_bronze(source_path, WEATHER_SCHEMA)

    stage = "transformação e deduplicação"
    silver = df.select(
        F.col("_id").alias("weather_daily_id"),
        F.col("location.city").alias("city"),
        F.col("location.state").alias("state"),
        F.col("location.country").alias("country"),
        F.col("weather_date").cast(DateType()).alias("weather_date"),
        "samples",
        "created_at",
        "updated_at",
        "_ingested_at",
    )

    silver = keep_latest(
        silver,
        ["city", "state", "country", "weather_date"],
        F.col("updated_at").desc_nulls_last(),
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
        f"[SUCESSO] silver.app.{table_name}: "
        f"{written_count} registros gravados. Destino: {table_path}",
        flush=True,
    )

except Exception:
    failed_tables.append(table_name)
    logger.exception(
        "[ERRO] Falha em silver.app.%s na etapa '%s'. Origem: %s. "
        "Destino: %s. O processamento das demais tabelas continuará.",
        table_name, stage, source_path, table_path,
    )


# =================================
# weather_hourly
# =================================

table_name = "weather_hourly"
source_path = f"{S3_SOURCE}weather_daily/"
table_path = f"{S3_DESTINATION}{table_name}/"
stage = "leitura da Bronze"

try:
    print(f"\n[INÍCIO] silver.app.{table_name}", flush=True)
    df = read_bronze(source_path, WEATHER_SCHEMA)

    stage = "deduplicação dos dias e expansão das horas"
    base = df.select(
        F.col("_id").alias("weather_daily_id"),
        F.col("location.city").alias("city"),
        F.col("location.state").alias("state"),
        F.col("location.country").alias("country"),
        F.col("weather_date").cast(DateType()).alias("weather_date"),
        "updated_at",
        "hours",
        "_ingested_at",
    )

    base = keep_latest(
        base,
        ["city", "state", "country", "weather_date"],
        F.col("updated_at").desc_nulls_last(),
        F.col("_ingested_at").desc(),
    )

    silver = (
        base.select(
            "weather_daily_id",
            "city",
            "state",
            "country",
            "weather_date",
            F.explode("hours").alias("hour_data"),
            "_ingested_at",
        )
        .filter(F.col("hour_data").isNotNull())
        .select(
            "weather_daily_id",
            "city",
            "state",
            "country",
            "weather_date",
            "hour_data.*",
            "_ingested_at",
        )
    )

    silver = keep_latest(
        silver,
        ["city", "state", "country", "weather_date", "hour"],
        F.col("observed_at").desc_nulls_last(),
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
        f"[SUCESSO] silver.app.{table_name}: "
        f"{written_count} registros gravados. Destino: {table_path}",
        flush=True,
    )

except Exception:
    failed_tables.append(table_name)
    logger.exception(
        "[ERRO] Falha em silver.app.%s na etapa '%s'. Origem: %s. "
        "Destino: %s. O processamento das demais tabelas continuará.",
        table_name, stage, source_path, table_path,
    )


print(
    f"\n[RESUMO] Gravadas: {len(completed_tables)}; falhas: {len(failed_tables)}.",
    flush=True,
)

# Cada saída é tentada antes de propagar a falha do job.
if failed_tables:
    raise RuntimeError(
        "Silver app falhou após processar todas as tabelas. "
        "Tabelas com erro: " + ", ".join(failed_tables)
    )

job.commit()
