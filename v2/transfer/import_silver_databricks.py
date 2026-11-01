# Databricks notebook source
import json
import re
from pathlib import Path
from pyspark.sql.types import StructType

# COMMAND ----------
# Troque os valores padrão se seu catálogo/volume tiver outro nome.
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("volume_path", "/Volumes/workspace/delta/delta_silver")
dbutils.widgets.text("batch_id", "")

catalog = dbutils.widgets.get("catalog")
volume_path = dbutils.widgets.get("volume_path").strip().rstrip("/")
batch_id = dbutils.widgets.get("batch_id")

if not re.fullmatch(r"[A-Za-z0-9_-]+", catalog):
    raise ValueError("Catálogo inválido.")
if not re.fullmatch(r"/Volumes/[A-Za-z0-9_-]+/[A-Za-z0-9_-]+/[A-Za-z0-9_-]+", volume_path):
    raise ValueError(f"Caminho de volume inválido: {volume_path!r}.")
if not re.fullmatch(r"[A-Za-z0-9_-]+", batch_id):
    raise ValueError("Preencha batch_id com o lote enviado.")

spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------
batch_path = Path(volume_path) / batch_id
manifest = json.loads((batch_path / "manifest.json").read_text(encoding="utf-8"))

expected = {
    ("silver_telemetry", "consumption_summary"),
    ("silver_telemetry", "device_status"),
    ("silver_app", "weather_daily"),
    ("silver_app", "weather_hourly"),
    ("silver_app", "chat_sessions"),
    ("silver_app", "chat_messages"),
    ("silver_app", "chat_feedback"),
    ("silver_app", "alerts_history"),
    ("silver_app", "user_preferences"),
}
actual = [(item["schema_name"], item["table_name"]) for item in manifest["tables"]]
version = manifest.get("manifest_version")
sql_included = manifest.get("sql_included", False)
mongo_prefix = manifest.get("mongo_prefix", "")
if (
    version != 4
    or sql_included is not True
    or mongo_prefix != "mongo-database"
    or manifest.get("batch_id") != batch_id
):
    raise ValueError("Manifest não corresponde ao lote v2 informado (versão 4; 9 Mongo + 4 SQL).")
expected |= {
    ("silver_sql", name)
    for name in (
        "tb_device",
        "bi_property_context",
        "bi_tariff_context",
        "tb_user_property",
    )
}
if len(actual) != len(expected) or set(actual) != expected:
    raise ValueError(f"Manifest não contém exatamente as {len(expected)} tabelas esperadas.")

# COMMAND ----------
# Confira todas as fontes antes de escrever a primeira tabela.
frames = []
for table in manifest["tables"]:
    schema_name = table["schema_name"]
    table_name = table["table_name"]
    schema = StructType.fromJson(table["spark_schema"])
    paths = []
    for file in table["files"]:
        name = file["name"]
        if Path(name).name != name or not name.endswith(".parquet"):
            raise ValueError("Arquivo inválido no manifest.")
        path = batch_path / schema_name / table_name / name
        if path.stat().st_size != file["size"]:
            raise ValueError(f"Arquivo ausente/incompleto: {table_name}")
        paths.append(str(path))

    if paths:
        df = spark.read.schema(schema).parquet(*paths)
    else:
        df = spark.createDataFrame([], schema)

    if df.count() != table["row_count"]:
        raise ValueError(f"Contagem divergente: {schema_name}.{table_name}")

    target = f"`{catalog}`.`{schema_name}`.`{table_name}`"
    if spark.catalog.tableExists(target):
        metadata = spark.sql(f"DESCRIBE TABLE EXTENDED {target}").collect()
        table_type = next((row["data_type"] for row in metadata if row["col_name"] == "Type"), "")
        if table_type.upper() != "MANAGED":
            raise ValueError(f"Destino não é tabela gerenciada: {target}")

    frames.append((target, df))

# COMMAND ----------
for schema_name in sorted({table["schema_name"] for table in manifest["tables"]}):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema_name}`")

for target, df in frames:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option(
            "userMetadata",
            json.dumps(
                {
                    "source_batch_id": batch_id,
                    "manifest_version": 4,
                    "source_exported_at": manifest["created_at"],
                    "workflow_run_id": manifest["workflow_run_id"],
                }
            ),
        )
        .saveAsTable(target)
    )
    print(f"Silver gerenciada atualizada: {target}")

print(f"Importação concluída: {batch_id}. Gold pode consumir as {len(manifest['tables'])} tabelas.")
