"""Glue Spark: quatro snapshots SQL necessários aos 13 KPIs da BI v2.

As consultas leem somente public; não executam DDL/ETL no PostgreSQL.
Execute antes da Silver unificada, no mesmo workflow, para liberar a exportação.
SQL_CHECK_ONLY=true permite conferir as quatro fontes sem escrever snapshots.
"""

import json
import logging
import sys
from urllib.parse import parse_qs, urlparse

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from delta.tables import DeltaTable
from pyspark.context import SparkContext
from pyspark.sql import functions as F


parameters = ["JOB_NAME"] + [
    name
    for name in ("SQL_CONNECTION_NAME", "SQL_CHECK_ONLY", "WORKFLOW_NAME", "WORKFLOW_RUN_ID")
    if "--" + name in sys.argv
]
args = getResolvedOptions(sys.argv, parameters)
GLUE_CONNECTION = args.get("SQL_CONNECTION_NAME", "delta-sql-connection")
CHECK_ONLY = args.get("SQL_CHECK_ONLY", "false")
if CHECK_ONLY not in ("true", "false"):
    raise ValueError("SQL_CHECK_ONLY deve ser true ou false.")

workflow_name = args.get("WORKFLOW_NAME", "")
workflow_run_id = args.get("WORKFLOW_RUN_ID", "")
if bool(workflow_name) != bool(workflow_run_id):
    raise ValueError("Informe WORKFLOW_NAME e WORKFLOW_RUN_ID juntos.")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.session.timeZone", "UTC")
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

S3_DESTINATION = "s3://delta-app-oficial-datalake/sql-database/datamart/"
logger = logging.getLogger("sql_v2")
completed_tables = []
failed_tables = []

# Usuário/senha vêm da autenticação da conexão Glue.
configuration = glueContext.extract_jdbc_conf(GLUE_CONNECTION)
jdbc_url = configuration.get("fullUrl", "")
if not jdbc_url.startswith("jdbc:postgresql://"):
    raise ValueError("A conexão precisa de fullUrl JDBC PostgreSQL incluindo o banco.")
parsed_url = urlparse(jdbc_url[len("jdbc:") :])
if (
    not parsed_url.hostname
    or parsed_url.path in ("", "/")
    or parsed_url.username
    or parsed_url.password
    or any(key.lower() in ("user", "password") for key in parse_qs(parsed_url.query))
):
    raise ValueError(
        "Informe somente host/banco na URL; credenciais ficam na autenticação da conexão Glue."
    )
if not configuration.get("user") or not configuration.get("password"):
    raise ValueError("A conexão Glue não retornou usuário e senha.")


def read_sql(query):
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", query)
        .option("user", configuration["user"])
        .option("password", configuration["password"])
        .option("driver", "org.postgresql.Driver")
        .option("sslmode", "require")
        .option("connectTimeout", "60")
        .option("socketTimeout", "300")
        .option("fetchsize", "1000")
        .load()
    )


glue = boto3.client("glue") if workflow_name else None


def set_run_properties(values):
    if glue is not None:
        context = {"Name": workflow_name, "RunId": workflow_run_id}
        properties = glue.get_workflow_run_properties(**context)["RunProperties"]
        properties.update(values)
        glue.put_workflow_run_properties(**context, RunProperties=properties)


if CHECK_ONLY == "false":
    # Invalida o resultado de uma tentativa anterior antes de escrever qualquer tabela.
    set_run_properties(
        {
            "delta_sql_status": "LOADING",
            "delta_sql_workflow_run_id": "",
            "delta_sql_contract_version": "",
            "delta_export_status": "NOT_STARTED",
            "delta_manifest_uri": "",
            "delta_batch_id": "",
        }
    )

context = read_sql("(SELECT current_database() AS database_name) AS source_context").first()
source_database = context["database_name"]
print(f"Conexão SQL: {GLUE_CONNECTION}; banco: {source_database}", flush=True)


# =================================
# tb_device
# =================================

# "silver" nos contextos é o namespace do snapshot, não um schema exigido na origem.
table_name = "tb_device"
table_path = f"{S3_DESTINATION}public/{table_name}/"
query = """(SELECT d.device_id, d.property_id, d.installation_date
        FROM public.tb_device d
        JOIN public.tb_property p ON p.id = d.property_id
        JOIN public.tb_address a ON a.id = p.address_id
        WHERE upper(trim(a.state)) IN ('SP', 'SÃO PAULO', 'SAO PAULO')) AS tb_device"""
stage = "leitura JDBC"

try:
    print(f"\n[INÍCIO] SQL.{table_name}", flush=True)
    df = read_sql(query)

    if CHECK_ONLY == "true":
        stage = "validação de acesso à origem"
        df.limit(1).count()
        print(f"[VALIDADO] SQL.{table_name}: acesso confirmado, sem gravação.", flush=True)
    else:
        stage = "preparação do snapshot"
        snapshot = (
            df.withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_database", F.lit(source_database))
            .withColumn("_source_schema", F.lit("public"))
            .withColumn("_source_table", F.lit(table_name))
            .withColumn("_source_workflow_run_id", F.lit(workflow_run_id))
        )

        stage = "gravação Delta (overwrite)"
        (
            snapshot.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option(
                "userMetadata",
                json.dumps(
                    {
                        "workflow_run_id": workflow_run_id,
                        "sql_contract_version": 2,
                        "snapshot_name": table_name,
                    }
                ),
            )
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
        print(
            f"[SUCESSO] SQL.{table_name}: {written_count} registros gravados. "
            f"Destino: {table_path}",
            flush=True,
        )

    completed_tables.append(table_name)

except Exception:
    failed_tables.append(table_name)
    logger.exception(
        "[ERRO] Falha em SQL.%s na etapa '%s'. Destino: %s. "
        "O processamento das demais tabelas continuará.",
        table_name,
        stage,
        table_path,
    )


# =================================
# bi_property_context
# =================================

# "silver" nos contextos é o namespace do snapshot, não um schema exigido na origem.
table_name = "bi_property_context"
table_path = f"{S3_DESTINATION}silver/{table_name}/"
query = """(SELECT p.id AS property_id, a.region_id, r.name AS region_name,
            a.city, a.state, pc.group_name AS classification_group, a.latitude, a.longitude
        FROM public.tb_property p
        JOIN public.tb_address a ON a.id = p.address_id
        JOIN public.tb_region r ON r.id = a.region_id
        JOIN public.tb_property_classification pc ON pc.id = p.classification_id
        WHERE upper(trim(a.state)) IN ('SP', 'SÃO PAULO', 'SAO PAULO')) AS bi_property_context"""
stage = "leitura JDBC"

try:
    print(f"\n[INÍCIO] SQL.{table_name}", flush=True)
    df = read_sql(query)

    if CHECK_ONLY == "true":
        stage = "validação de acesso à origem"
        df.limit(1).count()
        print(f"[VALIDADO] SQL.{table_name}: acesso confirmado, sem gravação.", flush=True)
    else:
        stage = "preparação do snapshot"
        snapshot = (
            df.withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_database", F.lit(source_database))
            .withColumn("_source_schema", F.lit("public"))
            .withColumn("_source_table", F.lit(table_name))
            .withColumn("_source_workflow_run_id", F.lit(workflow_run_id))
        )

        stage = "gravação Delta (overwrite)"
        (
            snapshot.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option(
                "userMetadata",
                json.dumps(
                    {
                        "workflow_run_id": workflow_run_id,
                        "sql_contract_version": 2,
                        "snapshot_name": table_name,
                    }
                ),
            )
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
        print(
            f"[SUCESSO] SQL.{table_name}: {written_count} registros gravados. "
            f"Destino: {table_path}",
            flush=True,
        )

    completed_tables.append(table_name)

except Exception:
    failed_tables.append(table_name)
    logger.exception(
        "[ERRO] Falha em SQL.%s na etapa '%s'. Destino: %s. "
        "O processamento das demais tabelas continuará.",
        table_name,
        stage,
        table_path,
    )


# =================================
# bi_tariff_context
# =================================

# "silver" nos contextos é o namespace do snapshot, não um schema exigido na origem.
table_name = "bi_tariff_context"
table_path = f"{S3_DESTINATION}silver/{table_name}/"
query = """(SELECT rr.region_id, r.name AS region_name, rr.classification_id,
            pc.group_name AS classification_group, rr.initial_validity, rr.final_validity, rr.m3_value
        FROM public.tb_region_rate rr
        JOIN public.tb_region r ON r.id = rr.region_id
        JOIN public.tb_property_classification pc ON pc.id = rr.classification_id
        WHERE EXISTS (
            SELECT 1 FROM public.tb_address a
            WHERE a.region_id = rr.region_id
              AND upper(trim(a.state)) IN ('SP', 'SÃO PAULO', 'SAO PAULO')
        )) AS bi_tariff_context"""
stage = "leitura JDBC"

try:
    print(f"\n[INÍCIO] SQL.{table_name}", flush=True)
    df = read_sql(query)

    if CHECK_ONLY == "true":
        stage = "validação de acesso à origem"
        df.limit(1).count()
        print(f"[VALIDADO] SQL.{table_name}: acesso confirmado, sem gravação.", flush=True)
    else:
        stage = "preparação do snapshot"
        snapshot = (
            df.withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_database", F.lit(source_database))
            .withColumn("_source_schema", F.lit("public"))
            .withColumn("_source_table", F.lit(table_name))
            .withColumn("_source_workflow_run_id", F.lit(workflow_run_id))
        )

        stage = "gravação Delta (overwrite)"
        (
            snapshot.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option(
                "userMetadata",
                json.dumps(
                    {
                        "workflow_run_id": workflow_run_id,
                        "sql_contract_version": 2,
                        "snapshot_name": table_name,
                    }
                ),
            )
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
        print(
            f"[SUCESSO] SQL.{table_name}: {written_count} registros gravados. "
            f"Destino: {table_path}",
            flush=True,
        )

    completed_tables.append(table_name)

except Exception:
    failed_tables.append(table_name)
    logger.exception(
        "[ERRO] Falha em SQL.%s na etapa '%s'. Destino: %s. "
        "O processamento das demais tabelas continuará.",
        table_name,
        stage,
        table_path,
    )


# =================================
# tb_user_property
# =================================

# "silver" nos contextos é o namespace do snapshot, não um schema exigido na origem.
table_name = "tb_user_property"
table_path = f"{S3_DESTINATION}public/{table_name}/"
query = """(SELECT up.user_id, up.property_id, up.association_date
        FROM public.tb_user_property up
        JOIN public.tb_property p ON p.id = up.property_id
        JOIN public.tb_address a ON a.id = p.address_id
        WHERE upper(trim(a.state)) IN ('SP', 'SÃO PAULO', 'SAO PAULO')) AS tb_user_property"""
stage = "leitura JDBC"

try:
    print(f"\n[INÍCIO] SQL.{table_name}", flush=True)
    df = read_sql(query)

    if CHECK_ONLY == "true":
        stage = "validação de acesso à origem"
        df.limit(1).count()
        print(f"[VALIDADO] SQL.{table_name}: acesso confirmado, sem gravação.", flush=True)
    else:
        stage = "preparação do snapshot"
        snapshot = (
            df.withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_database", F.lit(source_database))
            .withColumn("_source_schema", F.lit("public"))
            .withColumn("_source_table", F.lit(table_name))
            .withColumn("_source_workflow_run_id", F.lit(workflow_run_id))
        )

        stage = "gravação Delta (overwrite)"
        (
            snapshot.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option(
                "userMetadata",
                json.dumps(
                    {
                        "workflow_run_id": workflow_run_id,
                        "sql_contract_version": 2,
                        "snapshot_name": table_name,
                    }
                ),
            )
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
        print(
            f"[SUCESSO] SQL.{table_name}: {written_count} registros gravados. "
            f"Destino: {table_path}",
            flush=True,
        )

    completed_tables.append(table_name)

except Exception:
    failed_tables.append(table_name)
    logger.exception(
        "[ERRO] Falha em SQL.%s na etapa '%s'. Destino: %s. "
        "O processamento das demais tabelas continuará.",
        table_name,
        stage,
        table_path,
    )


print(f"\n[RESUMO] Sucessos: {len(completed_tables)}; falhas: {len(failed_tables)}.", flush=True)
if failed_tables:
    if CHECK_ONLY == "false":
        set_run_properties({"delta_sql_status": "FAILED", "delta_sql_workflow_run_id": ""})
    raise RuntimeError(
        "SQL v2 falhou após tentar todas as tabelas. Tabelas com erro: " + ", ".join(failed_tables)
    )

if CHECK_ONLY == "false":
    set_run_properties(
        {
            "delta_sql_status": "READY",
            "delta_sql_workflow_run_id": workflow_run_id,
            "delta_sql_contract_version": "2",
        }
    )
    if not workflow_name:
        print(
            "Carga avulsa concluída. Para transferir, execute o SQL e a Silver no mesmo workflow.",
            flush=True,
        )

job.commit()
