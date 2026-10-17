"""Um job Spark: lê 16 snapshots SQL dos KPIs e grava snapshots Delta no S3."""
import sys
from urllib.parse import parse_qs, urlparse

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import StorageLevel
from pyspark.context import SparkContext
from pyspark.sql import functions as F


def option(name, default):
    return getResolvedOptions(sys.argv, [name])[name] if "--" + name in sys.argv else default


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
GLUE_CONNECTION = option("SQL_CONNECTION_NAME", "delta-sql-connection")
CHECK_ONLY = option("SQL_CHECK_ONLY", "true")
if CHECK_ONLY not in ("true", "false"):
    raise ValueError("SQL_CHECK_ONLY deve ser true ou false.")

workflow_name = option("WORKFLOW_NAME", "")
workflow_run_id = option("WORKFLOW_RUN_ID", "")
if CHECK_ONLY == "false" and not (workflow_name and workflow_run_id):
    raise ValueError("Execute a carga pelo workflow; SQL_CHECK_ONLY=true permite teste isolado.")

BUCKET = "s3://delta-app-oficial-datalake"
SQL_PREFIX = "sql-database"
DESTINATION_ROOT = f"{BUCKET}/{SQL_PREFIX}/datamart"
TABLES = [
    ("silver", "dm_person"),
    ("silver", "dm_property"),
    ("silver", "ft_consumption_reading"),
    ("silver", "ft_consumption_daily"),
    ("silver", "ft_water_bill"),
    ("gold", "dm_person"),
    ("gold", "dm_property"),
    ("gold", "dm_date"),
    ("gold", "ft_consumption_daily"),
    ("gold", "ft_water_bill_monthly"),
    ("gold", "ft_investment_scenario"),
    ("public", "tb_device"),
    ("public", "tb_region_rate"),
    ("silver", "bi_property_context"),
    ("silver", "bi_tariff_context"),
    ("public", "tb_user_property"),
]


# Consultas derivadas: não criam tabelas/views no PostgreSQL. O schema "silver"
# abaixo é somente o namespace do snapshot exportado para S3/Databricks.
SQL_QUERIES = {
    "bi_property_context": """(SELECT p.id AS property_id, a.region_id, r.name AS region_name,
        a.city, a.state, pc.group_name AS classification_group, a.latitude, a.longitude
        FROM public.tb_property p JOIN public.tb_address a ON a.id=p.address_id
        JOIN public.tb_region r ON r.id=a.region_id
        JOIN public.tb_property_classification pc ON pc.id=p.classification_id) AS bi_property_context""",
    "bi_tariff_context": """(SELECT rr.region_id, r.name AS region_name, rr.classification_id,
        pc.group_name AS classification_group, rr.initial_validity, rr.final_validity, rr.m3_value
        FROM public.tb_region_rate rr JOIN public.tb_region r ON r.id=rr.region_id
        JOIN public.tb_property_classification pc ON pc.id=rr.classification_id
        WHERE EXISTS (SELECT 1 FROM public.tb_address a WHERE a.region_id=rr.region_id
          AND upper(trim(a.state)) IN ('SP','SÃO PAULO','SAO PAULO'))) AS bi_tariff_context""",
}

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.session.timeZone", "UTC")
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# As credenciais vêm da conexão Glue, não do código ou dos parâmetros do job.
configuration = glueContext.extract_jdbc_conf(GLUE_CONNECTION)
jdbc_url = configuration.get("fullUrl", "")
if not jdbc_url.startswith("jdbc:postgresql://"):
    raise ValueError("A conexão precisa de fullUrl JDBC PostgreSQL incluindo o nome do banco.")
parsed_url = urlparse(jdbc_url[len("jdbc:"):])
if (not parsed_url.hostname or parsed_url.path in ("", "/")
        or parsed_url.username or parsed_url.password):
    raise ValueError("Confira host e banco na URL JDBC; informe usuário/senha nos campos de autenticação.")
if any(key.lower() in ("user", "password") for key in parse_qs(parsed_url.query)):
    raise ValueError("A URL JDBC deve conter host/banco; credenciais ficam na autenticação da conexão.")
if not configuration.get("user") or not configuration.get("password"):
    raise ValueError("A conexão Glue não retornou usuário e senha. Confira autenticação e acesso ao segredo.")


def read_sql(dbtable):
    return (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", dbtable)
        .option("user", configuration["user"])
        .option("password", configuration["password"])
        .option("driver", "org.postgresql.Driver")
        .option("sslmode", "require")
        .option("connectTimeout", "60")
        .option("socketTimeout", "300")
        .option("fetchsize", "1000")
        .load()
    )


context = read_sql("(SELECT current_database() AS database_name, current_user AS user_name) AS source_context").first()
source_database = context["database_name"]
print(f"Conexão SQL: {GLUE_CONNECTION}; banco: {source_database}; usuário: {context['user_name']}")

# Confira as 16 fontes antes de escrever. Fontes vazias são válidas e ficam explícitas.
for source_schema, table_name in TABLES:
    table = SQL_QUERIES.get(table_name, f'"{source_schema}"."{table_name}"')
    sample = read_sql(f"(SELECT * FROM {table} LIMIT 1) AS source_check")
    sample_count = sample.count()
    print(f"Fonte acessível: {source_schema}.{table_name}; amostra: {sample_count} linha(s)")
    sample.printSchema()

if CHECK_ONLY == "true":
    print("SQL_CHECK_ONLY: conexão, SSL e leitura das 16 fontes SQL validados. Nenhum snapshot gravado.")
else:
    glue = boto3.client("glue")
    workflow_context = {"Name": workflow_name, "RunId": workflow_run_id}


    def set_run_properties(values):
        properties = glue.get_workflow_run_properties(**workflow_context)["RunProperties"]
        properties.update(values)
        glue.put_workflow_run_properties(**workflow_context, RunProperties=properties)


    set_run_properties({"delta_sql_status": "LOADING", "delta_sql_workflow_run_id": ""})

    for source_schema, table_name in TABLES:
        table = SQL_QUERIES.get(table_name, f'"{source_schema}"."{table_name}"')
        destination = f"{DESTINATION_ROOT}/{source_schema}/{table_name}/"
        frame = read_sql(table).persist(StorageLevel.MEMORY_AND_DISK)
        try:
            count = frame.count()
            # Metadados técnicos; não calcula KPIs, não deduplica nem altera campos de negócio.
            snapshot = (
                frame
                .withColumn("_ingested_at", F.current_timestamp())
                .withColumn("_source_database", F.lit(source_database))
                .withColumn("_source_schema", F.lit(source_schema))
                .withColumn("_source_table", F.lit(table_name))
                .withColumn("_source_workflow_run_id", F.lit(workflow_run_id))
            )
            (
                snapshot.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(destination)
            )
            print(f"Snapshot SQL: {source_schema}.{table_name}; linhas: {count}; destino: {destination}")
        finally:
            frame.unpersist()

    set_run_properties({"delta_sql_status": "READY", "delta_sql_workflow_run_id": workflow_run_id})
    print("Carga SQL concluída: 16 snapshots. Exportador pode consumir os snapshots desta execução.")

# Termine normalmente: SystemExit(0) também pode ser registrado como falha pelo launcher Glue.
job.commit()
