"""Glue Spark v2: exporta nove Silvers Mongo e quatro snapshots SQL do mesmo workflow."""

import json
import sys
from datetime import datetime, timezone
from uuid import uuid4

import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from delta.tables import DeltaTable

args = getResolvedOptions(sys.argv, ["JOB_NAME", "WORKFLOW_NAME", "WORKFLOW_RUN_ID"])


# Layout fixo da v2; o lote completo contém nove tabelas Mongo e quatro SQL.
MONGO_PREFIX = "mongo-database"
SQL_PREFIX = "sql-database"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.session.timeZone", "UTC")

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

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

job.commit()
