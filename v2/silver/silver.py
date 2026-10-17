"""Glue Spark v2: Silver app + telemetry e exportação do lote para o Databricks.

Execute depois do SQL v2 no mesmo workflow. Um único contexto Spark e um commit.
"""

import json
import logging
import sys
from datetime import datetime, timezone
from uuid import uuid4

import boto3
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


def read_bronze(spark, table_path: str, schema: StructType):
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


def process_app(spark):
    S3_SOURCE = "s3://delta-app-oficial-datalake/mongo-database/bronze/app/"
    S3_DESTINATION = "s3://delta-app-oficial-datalake/mongo-database/silver/app/"

    logger = logging.getLogger("app_silver")
    failed_tables = []
    completed_tables = []

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
        df = read_bronze(spark, source_path, ALERT_SCHEMA)

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
        df = read_bronze(spark, source_path, FEEDBACK_SCHEMA)

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
        df = read_bronze(spark, source_path, SESSION_SCHEMA)

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
        df = read_bronze(spark, source_path, SESSION_SCHEMA)

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
        df = read_bronze(spark, source_path, PREFERENCE_SCHEMA)

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
        df = read_bronze(spark, source_path, WEATHER_SCHEMA)

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
        df = read_bronze(spark, source_path, WEATHER_SCHEMA)

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


def process_telemetry(spark):
    S3_SOURCE = "s3://delta-app-oficial-datalake/mongo-database/bronze/telemetry/"
    S3_DESTINATION = "s3://delta-app-oficial-datalake/mongo-database/silver/telemetry/"

    logger = logging.getLogger("telemetry_silver")
    failed_tables = []
    completed_tables = []

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
        df = read_bronze(spark, source_path, CONSUMPTION_SCHEMA)

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
        df = read_bronze(spark, source_path, DEVICE_STATUS_SCHEMA)

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


def export_batch(spark, args):
    MONGO_PREFIX = "mongo-database"
    SQL_PREFIX = "sql-database"

    BUCKET = "delta-app-oficial-datalake"
    BATCH_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:12]
    EXPORT_PREFIX = f"exports/databricks/{BATCH_ID}/"

    TABLES = [
        ("telemetry", "silver_telemetry", "consumption_summary"),
        ("telemetry", "silver_telemetry", "device_status"),
        ("app", "silver_app", "weather_daily"),
        ("app", "silver_app", "weather_hourly"),
        ("app", "silver_app", "chat_sessions"),
        ("app", "silver_app", "chat_messages"),
        ("app", "silver_app", "chat_feedback"),
        ("app", "silver_app", "alerts_history"),
        ("app", "silver_app", "user_preferences"),
    ]

    SQL_TABLES = [
        ("public", "tb_device", "tb_device"),
        ("silver", "bi_property_context", "bi_property_context"),
        ("silver", "bi_tariff_context", "bi_tariff_context"),
        ("public", "tb_user_property", "tb_user_property"),
    ]

    s3 = boto3.client("s3")
    glue = boto3.client("glue")
    workflow_context = {
        "Name": args["WORKFLOW_NAME"],
        "RunId": args["WORKFLOW_RUN_ID"],
    }


    def set_run_properties(values):
        properties = glue.get_workflow_run_properties(**workflow_context)["RunProperties"]
        properties.update(values)
        glue.put_workflow_run_properties(**workflow_context, RunProperties=properties)


    # Invalida um eventual lote de uma tentativa anterior desta mesma execução.
    set_run_properties(
        {
            "delta_export_status": "EXPORTING",
            "delta_manifest_uri": "",
            "delta_batch_id": "",
            "delta_databricks_status": "NOT_STARTED",
            "delta_databricks_run_id": "",
        }
    )

    properties = glue.get_workflow_run_properties(**workflow_context)["RunProperties"]
    if (
        properties.get("delta_sql_status") != "READY"
        or properties.get("delta_sql_contract_version") != "2"
        or properties.get("delta_sql_workflow_run_id") != args["WORKFLOW_RUN_ID"]
    ):
        raise ValueError("O SQL v2 precisa terminar com sucesso nesta mesma execução do workflow.")

    mongo_root = f"s3://{BUCKET}/{MONGO_PREFIX}/"
    sources = [(f"{mongo_root}silver/{area}/{name}/", schema, name) for area, schema, name in TABLES]
    sources.extend(
        (f"s3://{BUCKET}/{SQL_PREFIX}/datamart/{schema}/{name}/", "silver_sql", target)
        for schema, name, target in SQL_TABLES
    )

    manifest = {
        "manifest_version": 4,
        "batch_id": BATCH_ID,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "workflow_name": args["WORKFLOW_NAME"],
        "workflow_run_id": args["WORKFLOW_RUN_ID"],
        "mongo_prefix": MONGO_PREFIX,
        "sql_included": True,
        "tables": [],
    }


    def read_snapshot(path):
        version = DeltaTable.forPath(spark, path).history(1).select("version").first()["version"]

        frame = spark.read.format("delta").option("versionAsOf", version).load(path)

        return frame, version


    for source_path, target_schema, table_name in sources:
        destination_prefix = f"{EXPORT_PREFIX}{target_schema}/{table_name}/"
        destination_path = f"s3://{BUCKET}/{destination_prefix}"

        df, source_version = read_snapshot(source_path)
        row_count = df.count()

        # O caminho do lote é novo. Não sobrescreve Silver nem lotes anteriores.
        # Não há coalesce(1): arquivos menores são mais fáceis de transferir.
        (
            df.write.format("parquet")
            .mode("errorifexists")
            .option("maxRecordsPerFile", 50000)
            .save(destination_path)
        )

        files = []
        paginator = s3.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=BUCKET, Prefix=destination_prefix):
            for item in page.get("Contents", []):
                relative_name = item["Key"][len(destination_prefix) :]
                if relative_name.endswith(".parquet") and "/" not in relative_name:
                    files.append(
                        {
                            "s3_key": item["Key"],
                            "name": relative_name,
                            "size": item["Size"],
                        }
                    )

        if row_count > 0 and not files:
            raise ValueError(f"Exportação sem arquivos: {table_name}")

        manifest["tables"].append(
            {
                "schema_name": target_schema,
                "table_name": table_name,
                "source_path": source_path,
                "source_delta_version": source_version,
                "row_count": row_count,
                "spark_schema": df.schema.jsonValue(),
                "files": sorted(files, key=lambda item: item["name"]),
            }
        )

        print(f"Exportado: {target_schema}.{table_name}; linhas: {row_count}; versão: {source_version}")

    # O manifest só existe depois que todas as exportações terminam.
    manifest_key = f"{EXPORT_PREFIX}manifest.json"
    s3.put_object(
        Bucket=BUCKET,
        Key=manifest_key,
        Body=json.dumps(manifest, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )

    print(f"BATCH_ID: {BATCH_ID}")
    set_run_properties(
        {
            "delta_export_status": "READY",
            "delta_manifest_uri": f"s3://{BUCKET}/{manifest_key}",
            "delta_batch_id": BATCH_ID,
        }
    )

    print(f"MANIFEST_URI: s3://{BUCKET}/{manifest_key}")


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "WORKFLOW_NAME", "WORKFLOW_RUN_ID"])
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    # Uma reexecução nunca pode reaproveitar o manifest READY de uma tentativa anterior.
    glue = boto3.client("glue")
    context = {"Name": args["WORKFLOW_NAME"], "RunId": args["WORKFLOW_RUN_ID"]}
    properties = glue.get_workflow_run_properties(**context)["RunProperties"]
    properties.update({
        "delta_export_status": "NOT_STARTED",
        "delta_manifest_uri": "",
        "delta_batch_id": "",
        "delta_databricks_status": "NOT_STARTED",
        "delta_databricks_run_id": "",
    })
    glue.put_workflow_run_properties(**context, RunProperties=properties)
    if (
        properties.get("delta_sql_status") != "READY"
        or properties.get("delta_sql_contract_version") != "2"
        or properties.get("delta_sql_workflow_run_id") != args["WORKFLOW_RUN_ID"]
    ):
        raise ValueError("Execute o SQL v2 com sucesso neste mesmo workflow antes da Silver.")

    process_app(spark)
    process_telemetry(spark)
    export_batch(spark, args)
    job.commit()


if __name__ == "__main__":
    main()
