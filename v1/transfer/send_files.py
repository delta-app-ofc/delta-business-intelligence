"""Glue Python Shell 3.9: S3 -> Files API -> Job Databricks, aguardando sucesso."""
import argparse
import hashlib
import io
import json
import re
import tempfile
import time
from pathlib import Path
from urllib.parse import quote, urlparse
from uuid import uuid4

BUCKET = "delta-app-oficial-datalake"
MAX_FILE_BYTES = 5 * 1024 ** 3
EXPECTED_TABLES = {
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
SQL_TABLES = {
    "silver_dm_person": ("silver", "dm_person"),
    "silver_dm_property": ("silver", "dm_property"),
    "silver_ft_consumption_reading": ("silver", "ft_consumption_reading"),
    "silver_ft_consumption_daily": ("silver", "ft_consumption_daily"),
    "silver_ft_water_bill": ("silver", "ft_water_bill"),
    "gold_dm_person": ("gold", "dm_person"),
    "gold_dm_property": ("gold", "dm_property"),
    "gold_dm_date": ("gold", "dm_date"),
    "gold_ft_consumption_daily": ("gold", "ft_consumption_daily"),
    "gold_ft_water_bill_monthly": ("gold", "ft_water_bill_monthly"),
    "gold_ft_investment_scenario": ("gold", "ft_investment_scenario"),
    "tb_device": ("public", "tb_device"),
    "tb_region_rate": ("public", "tb_region_rate"),
}

BI_SQL_TABLES = {
    "bi_property_context": ("silver", "bi_property_context"),
    "bi_tariff_context": ("silver", "bi_tariff_context"),
    "tb_user_property": ("public", "tb_user_property"),
}



def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--DATABRICKS_SECRET_ID", required=True)
    parser.add_argument("--DATABRICKS_JOB_ID", required=True, type=int)
    parser.add_argument("--TARGET_CATALOG", default="workspace")
    parser.add_argument("--VOLUME_PATH", default="/Volumes/workspace/delta/delta_silver")
    parser.add_argument("--CHECK_ONLY", choices=("true", "false"), default="false")
    parser.add_argument("--WORKFLOW_NAME")
    parser.add_argument("--WORKFLOW_RUN_ID")
    parser.add_argument("--WAIT_TIMEOUT_SECONDS", type=int, default=3600)
    parser.add_argument("--POLL_SECONDS", type=int, default=30)
    # Glue também injeta JOB_NAME, JOB_RUN_ID e parâmetros de infraestrutura.
    args, _ = parser.parse_known_args(argv)
    args.VOLUME_PATH = args.VOLUME_PATH.strip().rstrip("/")
    if not re.fullmatch(r"[A-Za-z0-9_-]+", args.TARGET_CATALOG):
        parser.error("TARGET_CATALOG inválido.")
    if not re.fullmatch(r"/Volumes/[A-Za-z0-9_-]+/[A-Za-z0-9_-]+/[A-Za-z0-9_-]+", args.VOLUME_PATH):
        parser.error(f"VOLUME_PATH inválido: {args.VOLUME_PATH!r}. Informe /Volumes/catalog/schema/volume.")
    if args.DATABRICKS_JOB_ID <= 0 or args.WAIT_TIMEOUT_SECONDS <= 0 or not 5 <= args.POLL_SECONDS <= 60:
        parser.error("JOB_ID/WAIT_TIMEOUT devem ser positivos; POLL_SECONDS entre 5 e 60.")
    if args.CHECK_ONLY == "false" and not (args.WORKFLOW_NAME and args.WORKFLOW_RUN_ID):
        parser.error("Execute pelo workflow Glue; CHECK_ONLY=true permite o teste isolado.")
    return args


def read_credentials(secrets, secret_id):
    try:
        value = secrets.get_secret_value(SecretId=secret_id)
    except Exception as error:
        code = getattr(error, "response", {}).get("Error", {}).get("Code", "erro de acesso")
        raise RuntimeError(
            f"Secrets Manager: {code}. A role deste job não conseguiu ler o segredo. Confira região, Secret ID, "
            "secretsmanager:GetSecretValue e, se usar chave própria, kms:Decrypt."
        ) from None
    try:
        credentials = json.loads(value["SecretString"])
        host = credentials["host"].rstrip("/")
        token = credentials["token"].strip()
        parsed = urlparse(host)
        if (parsed.scheme != "https" or not parsed.hostname or parsed.path or parsed.query
                or parsed.fragment or parsed.username or parsed.password):
            raise ValueError()
        if not isinstance(token, str) or not token.strip() or "\n" in token or "\r" in token:
            raise ValueError()
    except (KeyError, TypeError, ValueError, AttributeError):
        raise ValueError("O segredo deve conter JSON com host HTTPS base e token válido.") from None
    return host, token


class DatabricksAPI:
    def __init__(self, host, session, network_errors=(ConnectionError, TimeoutError), sleep=time.sleep):
        self.host = host
        self.session = session
        self.network_errors = network_errors
        self.sleep = sleep

    def request(self, method, path, expected=(200,), **kwargs):
        stream = kwargs.get("data")
        start = stream.tell() if hasattr(stream, "seek") else None
        timeout = kwargs.pop("timeout", (30, 120))
        for attempt in range(4):
            if start is not None:
                stream.seek(start)
            try:
                response = self.session.request(
                    method, self.host + path, timeout=timeout,
                    allow_redirects=False, **kwargs,
                )
            except self.network_errors:
                if attempt == 3:
                    raise RuntimeError("Falha de rede na API Databricks após quatro tentativas.") from None
                self.sleep(2 ** attempt)
                continue
            if response.status_code in expected:
                return response
            status = response.status_code
            retry_after = response.headers.get("Retry-After", "")
            response.close()
            if status in (429, 500, 502, 503, 504) and attempt < 3:
                try:
                    delay = min(30, max(1, float(retry_after)))
                except ValueError:
                    delay = 2 ** attempt
                self.sleep(delay)
                continue
            hint = {401: "token inválido/expirado", 403: "permissão insuficiente",
                    404: "job, volume ou arquivo não encontrado"}.get(status, "confira o recurso solicitado")
            # Não imprime payload, headers ou token em erros da API.
            raise RuntimeError(f"API Databricks: HTTP {status}; {hint}.")

    def json_request(self, method, path, **kwargs):
        response = self.request(method, path, **kwargs)
        try:
            return response.json()
        finally:
            response.close()

    def mkdir(self, path):
        self.request("PUT", "/api/2.0/fs/directories" + quote(path, safe="/"),
                     expected=(200, 204)).close()

    def upload(self, path, stream):
        self.request("PUT", "/api/2.0/fs/files" + quote(path, safe="/"),
                     expected=(204,), params={"overwrite": "true"},
                     headers={"Content-Type": "application/octet-stream"},
                     data=stream, timeout=(30, 1800)).close()


def check_job(api, job_id):
    job = api.json_request("GET", "/api/2.2/jobs/get", params={"job_id": job_id})
    settings = job["settings"]
    names = {item["name"] for item in settings.get("parameters", [])}
    if not {"catalog", "volume_path", "batch_id"}.issubset(names):
        raise ValueError("Cadastre catalog, volume_path e batch_id em Job parameters no Databricks.")
    if settings.get("max_concurrent_runs", 1) != 1:
        raise ValueError("Configure Max concurrent runs = 1 no Job Databricks.")
    task = next((item for item in settings.get("tasks", []) if item.get("task_key") == "import_silver"), None)
    if not task or "notebook_task" not in task or task.get("depends_on"):
        raise ValueError("O Job deve ter a tarefa inicial de notebook import_silver, sem dependências.")


def check_access(api, job_id, volume_path):
    check_job(api, job_id)
    probe_dir = f"{volume_path}/_delta_probe_{uuid4().hex}"
    probe_path = f"{probe_dir}/check.txt"
    content = b"Delta Files API access check\n"
    api.mkdir(probe_dir)
    uploaded = False
    try:
        api.upload(probe_path, io.BytesIO(content))
        uploaded = True
        response = api.request("GET", "/api/2.0/fs/files" + quote(probe_path, safe="/"))
        try:
            if response.content != content:
                raise ValueError("Conteúdo do teste diverge do enviado.")
        finally:
            response.close()
    finally:
        if uploaded:
            api.request("DELETE", "/api/2.0/fs/files" + quote(probe_path, safe="/"), expected=(204,)).close()
        api.request("DELETE", "/api/2.0/fs/directories" + quote(probe_dir, safe="/"), expected=(204,)).close()
    print("CHECK_ONLY: segredo lido; job encontrado; volume escrito, lido e teste removido.")
    print("A primeira execução completa verificará CAN MANAGE RUN, compute e permissões do notebook.")


def workflow_properties(glue, args, values=None):
    context = {"Name": args.WORKFLOW_NAME, "RunId": args.WORKFLOW_RUN_ID}
    properties = glue.get_workflow_run_properties(**context)["RunProperties"]
    if values is not None:
        properties.update(values)
        glue.put_workflow_run_properties(**context, RunProperties=properties)
    return properties


def read_manifest(s3, properties, args):
    if properties.get("delta_export_status") != "READY":
        raise ValueError("Não há exportação READY nesta execução do workflow.")
    batch = properties.get("delta_batch_id", "")
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,96}", batch):
        raise ValueError("batch_id inválido nas propriedades do workflow.")
    prefix = f"exports/databricks/{batch}/"
    uri = f"s3://{BUCKET}/{prefix}manifest.json"
    if properties.get("delta_manifest_uri") != uri:
        raise ValueError("Manifest deve pertencer ao bucket e lote esperados.")
    body = s3.get_object(Bucket=BUCKET, Key=prefix + "manifest.json")["Body"]
    try:
        raw = body.read()
    finally:
        body.close()
    manifest = json.loads(raw)
    version = manifest.get("manifest_version")
    mongo_prefix = manifest.get("mongo_prefix", "")
    sql_included = manifest.get("sql_included", False)
    if (version not in (1, 2, 3) or manifest.get("batch_id") != batch
            or manifest.get("workflow_name") != args.WORKFLOW_NAME
            or manifest.get("workflow_run_id") != args.WORKFLOW_RUN_ID):
        raise ValueError("Manifest não pertence a esta execução do workflow.")
    if (mongo_prefix not in ("", "mongo-database") or type(sql_included) is not bool
            or (version == 1 and (mongo_prefix or sql_included))):
        raise ValueError("Configuração de fontes inválida no manifest.")
    sql_tables = {**SQL_TABLES, **(BI_SQL_TABLES if version == 3 else {})}
    expected = EXPECTED_TABLES | ({("silver_sql", name) for name in sql_tables} if sql_included else set())
    tables = manifest["tables"]
    identities = [(table["schema_name"], table["table_name"]) for table in tables]
    if len(identities) != len(expected) or set(identities) != expected:
        raise ValueError(f"São necessárias exatamente {len(expected)} tabelas esperadas neste lote.")
    for table in tables:
        schema, name = table["schema_name"], table["table_name"]
        if schema == "silver_sql":
            source_schema, source_table = sql_tables[name]
            source_path = f"s3://{BUCKET}/sql-database/datamart/{source_schema}/{source_table}/"
        else:
            area = "telemetry" if schema == "silver_telemetry" else "app"
            root = f"s3://{BUCKET}/" + (mongo_prefix + "/" if mongo_prefix else "")
            source_path = f"{root}silver/{area}/{name}/"
        if table["source_path"] != source_path:
            raise ValueError("Origem SQL/Mongo inesperada no manifest.")
        if type(table["row_count"]) is not int or table["row_count"] < 0:
            raise ValueError("row_count inválido.")
        if table["spark_schema"].get("type") != "struct" or not table["spark_schema"].get("fields"):
            raise ValueError("Schema Spark ausente ou inválido.")
        files = table["files"]
        if table["row_count"] > 0 and not files:
            raise ValueError("Tabela com registros sem arquivos.")
        names = set()
        for file in files:
            filename = file["name"]
            if not re.fullmatch(r"[A-Za-z0-9_.-]+\.parquet", filename) or filename in names:
                raise ValueError("Nome de arquivo inválido ou duplicado.")
            names.add(filename)
            if file["s3_key"] != f"{prefix}{schema}/{name}/{filename}":
                raise ValueError("Arquivo fora do lote/tabela esperados.")
            if type(file["size"]) is not int or not 0 <= file["size"] <= MAX_FILE_BYTES:
                raise ValueError("Arquivo acima de 5 GiB ou tamanho inválido. Reduza maxRecordsPerFile.")
    return manifest, raw


def send_batch(s3, api, manifest, raw, volume_path):
    batch_path = f"{volume_path}/{manifest['batch_id']}"
    api.mkdir(batch_path)
    with tempfile.TemporaryDirectory(prefix="delta-silver-upload-") as temp_dir:
        for table in manifest["tables"]:
            remote_dir = f"{batch_path}/{table['schema_name']}/{table['table_name']}"
            api.mkdir(remote_dir)
            for file in table["files"]:
                local = Path(temp_dir) / file["name"]
                s3.download_file(BUCKET, file["s3_key"], str(local))
                if local.stat().st_size != file["size"]:
                    raise ValueError("Arquivo baixado diverge do tamanho no manifest.")
                with local.open("rb") as stream:
                    api.upload(f"{remote_dir}/{file['name']}", stream)
                local.unlink()
            print(f"Enviado: {table['schema_name']}.{table['table_name']}")
        # O notebook só recebe o lote depois da publicação deste manifest.
        api.upload(batch_path + "/manifest.json", io.BytesIO(raw))
    return batch_path


def trigger_import(api, args, batch_id):
    identity = f"{args.WORKFLOW_NAME}:{args.WORKFLOW_RUN_ID}:{batch_id}:{args.DATABRICKS_JOB_ID}"
    result = api.json_request("POST", "/api/2.2/jobs/run-now", json={
        "job_id": args.DATABRICKS_JOB_ID,
        "idempotency_token": hashlib.sha256(identity.encode("utf-8")).hexdigest(),
        "job_parameters": {"catalog": args.TARGET_CATALOG,
                           "volume_path": args.VOLUME_PATH, "batch_id": batch_id},
    })
    run_id = result["run_id"]
    if type(run_id) is not int or run_id <= 0:
        raise ValueError("A API não retornou um run_id válido.")
    return run_id


def wait_for_import(api, run_id, timeout, poll, clock=time.monotonic, sleep=time.sleep):
    deadline = clock() + timeout
    previous = None
    while clock() < deadline:
        run = api.json_request("GET", "/api/2.2/jobs/runs/get", params={"run_id": run_id})
        state = run.get("state", {})
        lifecycle, result = state.get("life_cycle_state"), state.get("result_state")
        if (lifecycle, result) != previous:
            print(f"Databricks run_id={run_id}: {lifecycle}; resultado={result}")
            previous = (lifecycle, result)
        if lifecycle in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            if lifecycle == "TERMINATED" and result == "SUCCESS":
                return
            raise RuntimeError(f"Importação Databricks falhou: run_id={run_id}; {lifecycle}/{result}.")
        sleep(min(poll, max(0, deadline - clock())))
    raise TimeoutError(
        f"Tempo de espera esgotado para run_id={run_id}. A execução Databricks pode continuar; "
        "confira esse run antes de iniciar uma nova carga."
    )


def transfer_and_import(s3, glue, api, args):
    properties = workflow_properties(glue, args)
    manifest, raw = read_manifest(s3, properties, args)
    check_job(api, args.DATABRICKS_JOB_ID)
    workflow_properties(glue, args, {"delta_databricks_status": "UPLOADING"})
    try:
        send_batch(s3, api, manifest, raw, args.VOLUME_PATH)
        run_id = trigger_import(api, args, manifest["batch_id"])
        workflow_properties(glue, args, {"delta_databricks_status": "RUNNING",
                                        "delta_databricks_run_id": str(run_id)})
        print(f"Databricks Job ID: {args.DATABRICKS_JOB_ID}; Run ID: {run_id}")
        wait_for_import(api, run_id, args.WAIT_TIMEOUT_SECONDS, args.POLL_SECONDS)
        workflow_properties(glue, args, {"delta_databricks_status": "SUCCESS"})
    except Exception:
        # Conserva o erro original caso até a atualização das propriedades falhe.
        try:
            workflow_properties(glue, args, {"delta_databricks_status": "FAILED"})
        except Exception:
            print("Não foi possível registrar FAILED nas propriedades do workflow.")
        raise
    print(f"Carga completa: {manifest['batch_id']}. {len(manifest['tables'])} tabelas importadas.")


def main(argv=None):
    args = parse_args(argv)
    import boto3
    import requests

    host, token = read_credentials(boto3.client("secretsmanager"), args.DATABRICKS_SECRET_ID)
    with requests.Session() as session:
        session.headers["Authorization"] = "Bearer " + token
        api = DatabricksAPI(host, session, (requests.ConnectionError, requests.Timeout))
        if args.CHECK_ONLY == "true":
            check_access(api, args.DATABRICKS_JOB_ID, args.VOLUME_PATH)
        else:
            transfer_and_import(boto3.client("s3"), boto3.client("glue"), api, args)


if __name__ == "__main__":
    main()
