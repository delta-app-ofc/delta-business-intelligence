import logging
import sys

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, current_timestamp
from delta.tables import DeltaTable


args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

spark.conf.set(
    "spark.databricks.delta.schema.autoMerge.enabled",
    "true"
)

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

S3_DESTINATION = "s3://delta-app-oficial-datalake/mongo-database/bronze/telemetry/"

GLUE_CONNECTION = "delta-telemetry-atlas-connection"

MONGO_DATABASE = "db_delta_telemetry"

logger = logging.getLogger("telemetry_bronze")
failed_tables = []
completed_tables = []
empty_tables = []


def execute_merge(table_path: str, df):
    spark.catalog.refreshByPath(table_path)

    target = DeltaTable.forPath(
        spark,
        table_path
    )

    (
        target.alias("target")
        .merge(
            df.alias("source"),
            "target._id = source._id"
        )
        .withSchemaEvolution()
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete()
        .execute()
    )


# =================================
# consumption_summary 
# =================================

table_name = "consumption_summary"
table_path = f"{S3_DESTINATION}{table_name}/"
stage = "leitura do MongoDB"

try:
    print(f"\n[INÍCIO] {MONGO_DATABASE}.{table_name}", flush=True)

    mongo_frame = glueContext.create_dynamic_frame.from_options(
        connection_type="mongodb",
        connection_options={
            "connectionName": GLUE_CONNECTION,
            "database": MONGO_DATABASE,
            "collection": table_name,
            "partitioner": (
                "com.mongodb.spark.sql.connector.read.partitioner."
                "SinglePartitionPartitioner"
            )
        }
    )

    df = mongo_frame.toDF()
    source_count = df.count()

    if source_count != 0:
        stage = "preparação dos dados"
        df = (
            df
            .withColumn("_id", col("_id").cast("string"))
            .withColumn("_ingested_at", current_timestamp())
        )

        stage = "verificação do destino Delta"
        if DeltaTable.isDeltaTable(spark, table_path):
            stage = "gravação Delta (MERGE)"
            try:
                execute_merge(table_path, df)

            except Exception as error:
                if "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS" not in str(error):
                    raise

                logger.warning(
                    "Schema alterado em %s.%s; atualizando cache antes da segunda tentativa.",
                    MONGO_DATABASE, table_name
                )
                spark.catalog.clearCache()
                spark.catalog.refreshByPath(table_path)

                execute_merge(table_path, df)

            stage = "leitura das métricas Delta"
            spark.catalog.refreshByPath(table_path)

            metrics = (
                DeltaTable
                .forPath(spark, table_path)
                .history(1)
                .select("operationMetrics")
                .collect()[0]["operationMetrics"]
            )

            inserted_count = int(metrics.get("numTargetRowsInserted", 0))

        else:
            stage = "gravação Delta (criação)"
            (
                df.write
                .format("delta")
                .mode("overwrite")
                .option("mergeSchema", "true")
                .save(table_path)
            )

            stage = "leitura das métricas Delta"
            metrics = (
                DeltaTable.forPath(spark, table_path)
                .history(1)
                .select("operationMetrics")
                .collect()[0]["operationMetrics"]
            )

            inserted_count = int(metrics.get("numOutputRows", 0))

        spark.catalog.refreshByPath(table_path)
        completed_tables.append(table_name)
        print(
            f"[SUCESSO] {MONGO_DATABASE}.{table_name}: "
            f"{inserted_count} registros inseridos nesta execução. Destino: {table_path}",
            flush=True
        )

    else:
        empty_tables.append(table_name)
        print(
            f"[SEM DADOS] {MONGO_DATABASE}.{table_name}: "
            "collection vazia. Nenhum dado será gravado.",
            flush=True
        )

except Exception:
    failed_tables.append(table_name)
    logger.exception(
        "[ERRO] Falha em %s.%s na etapa '%s'. Destino: %s. "
        "O processamento das demais tabelas continuará.",
        MONGO_DATABASE, table_name, stage, table_path
    )


# =================================
# device_status 
# =================================

table_name = "device_status"
table_path = f"{S3_DESTINATION}{table_name}/"
stage = "leitura do MongoDB"

try:
    print(f"\n[INÍCIO] {MONGO_DATABASE}.{table_name}", flush=True)

    mongo_frame = glueContext.create_dynamic_frame.from_options(
        connection_type="mongodb",
        connection_options={
            "connectionName": GLUE_CONNECTION,
            "database": MONGO_DATABASE,
            "collection": table_name,
            "partitioner": (
                "com.mongodb.spark.sql.connector.read.partitioner."
                "SinglePartitionPartitioner"
            )
        }
    )

    df = mongo_frame.toDF()
    source_count = df.count()

    if source_count != 0:
        stage = "preparação dos dados"
        df = (
            df
            .withColumn("_id", col("_id").cast("string"))
            .withColumn("_ingested_at", current_timestamp())
        )

        stage = "verificação do destino Delta"
        if DeltaTable.isDeltaTable(spark, table_path):
            stage = "gravação Delta (MERGE)"
            try:
                execute_merge(table_path, df)

            except Exception as error:
                if "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS" not in str(error):
                    raise

                logger.warning(
                    "Schema alterado em %s.%s; atualizando cache antes da segunda tentativa.",
                    MONGO_DATABASE, table_name
                )
                spark.catalog.clearCache()
                spark.catalog.refreshByPath(table_path)

                execute_merge(table_path, df)

            stage = "leitura das métricas Delta"
            spark.catalog.refreshByPath(table_path)

            metrics = (
                DeltaTable
                .forPath(spark, table_path)
                .history(1)
                .select("operationMetrics")
                .collect()[0]["operationMetrics"]
            )

            inserted_count = int(metrics.get("numTargetRowsInserted", 0))

        else:
            stage = "gravação Delta (criação)"
            (
                df.write
                .format("delta")
                .mode("overwrite")
                .option("mergeSchema", "true")
                .save(table_path)
            )

            stage = "leitura das métricas Delta"
            metrics = (
                DeltaTable.forPath(spark, table_path)
                .history(1)
                .select("operationMetrics")
                .collect()[0]["operationMetrics"]
            )

            inserted_count = int(metrics.get("numOutputRows", 0))

        spark.catalog.refreshByPath(table_path)
        completed_tables.append(table_name)
        print(
            f"[SUCESSO] {MONGO_DATABASE}.{table_name}: "
            f"{inserted_count} registros inseridos nesta execução. Destino: {table_path}",
            flush=True
        )

    else:
        empty_tables.append(table_name)
        print(
            f"[SEM DADOS] {MONGO_DATABASE}.{table_name}: "
            "collection vazia. Nenhum dado será gravado.",
            flush=True
        )

except Exception:
    failed_tables.append(table_name)
    logger.exception(
        "[ERRO] Falha em %s.%s na etapa '%s'. Destino: %s. "
        "O processamento das demais tabelas continuará.",
        MONGO_DATABASE, table_name, stage, table_path
    )


print(
    f"\n[RESUMO] Gravadas: {len(completed_tables)}; "
    f"vazias: {len(empty_tables)}; falhas: {len(failed_tables)}.",
    flush=True
)

# A falha é propagada somente depois de tentar todas as coleções.
if failed_tables:
    raise RuntimeError(
        "Bronze telemetry falhou após processar todas as coleções. "
        "Tabelas com erro: " + ", ".join(failed_tables)
    )

job.commit()
