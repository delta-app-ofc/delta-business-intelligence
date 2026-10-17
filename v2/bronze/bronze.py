"""Glue Spark v2: ingestão sequencial de app e telemetry em um único job."""

import logging
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from delta.tables import DeltaTable
from pyspark.context import SparkContext
from pyspark.sql.functions import col, current_timestamp


S3_ROOT = "s3://delta-app-oficial-datalake/mongo-database/bronze/"
SOURCES = (
    (
        "app", "delta-app-atlas-connection", "db_delta_app",
        ("alerts_history", "chat_feedback", "chat_sessions", "user_preferences", "weather_daily"),
    ),
    (
        "telemetry", "delta-telemetry-atlas-connection", "db_delta_telemetry",
        ("consumption_summary", "device_status"),
    ),
)
logger = logging.getLogger("bronze_v2")


def execute_merge(spark, table_path, frame):
    spark.catalog.refreshByPath(table_path)
    (
        DeltaTable.forPath(spark, table_path)
        .alias("target")
        .merge(frame.alias("source"), "target._id = source._id")
        .withSchemaEvolution()
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete()
        .execute()
    )


def ingest_sources(glue_context, spark):
    completed_tables = []
    empty_tables = []
    failed_tables = []
    for area, connection, database, collections in SOURCES:
        for table_name in collections:
            source = f"{database}.{table_name}"
            table_path = f"{S3_ROOT}{area}/{table_name}/"
            stage = "leitura do MongoDB"
            try:
                print(f"\n[INÍCIO] {source}", flush=True)
                frame = glue_context.create_dynamic_frame.from_options(
                    connection_type="mongodb",
                    connection_options={
                        "connectionName": connection,
                        "database": database,
                        "collection": table_name,
                        "partitioner": (
                            "com.mongodb.spark.sql.connector.read.partitioner."
                            "SinglePartitionPartitioner"
                        ),
                    },
                ).toDF()
                if frame.count() == 0:
                    # Preserva a regra anterior: coleção vazia não apaga a Bronze.
                    empty_tables.append(source)
                    print(f"[SEM DADOS] {source}: nenhuma gravação.", flush=True)
                    continue

                stage = "preparação dos dados"
                frame = frame.withColumn("_id", col("_id").cast("string"))
                if table_name == "chat_feedback":
                    frame = frame.withColumn("session_id", col("session_id").cast("string"))
                frame = frame.withColumn("_ingested_at", current_timestamp())

                stage = "gravação Delta"
                if DeltaTable.isDeltaTable(spark, table_path):
                    try:
                        execute_merge(spark, table_path, frame)
                    except Exception as error:
                        if "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS" not in str(error):
                            raise
                        logger.warning("Schema alterado em %s; atualizando cache.", source)
                        spark.catalog.clearCache()
                        execute_merge(spark, table_path, frame)
                else:
                    (
                        frame.write.format("delta")
                        .mode("overwrite")
                        .option("mergeSchema", "true")
                        .save(table_path)
                    )

                stage = "leitura das métricas Delta"
                spark.catalog.refreshByPath(table_path)
                metrics = (
                    DeltaTable.forPath(spark, table_path).history(1)
                    .select("operationMetrics").collect()[0]["operationMetrics"]
                )
                inserted = int(metrics.get("numTargetRowsInserted", metrics.get("numOutputRows", 0)))
                completed_tables.append(source)
                print(f"[SUCESSO] {source}: {inserted} inseridos. Destino: {table_path}", flush=True)
            except Exception:
                failed_tables.append(source)
                logger.exception("[ERRO] %s; etapa=%s; destino=%s", source, stage, table_path)

    print(
        f"\n[RESUMO] Gravadas: {len(completed_tables)}; "
        f"vazias: {len(empty_tables)}; falhas: {len(failed_tables)}.",
        flush=True,
    )
    if failed_tables:
        raise RuntimeError("Bronze v2 falhou. Tabelas com erro: " + ", ".join(failed_tables))


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    ingest_sources(glue_context, spark)
    job.commit()


if __name__ == "__main__":
    main()
