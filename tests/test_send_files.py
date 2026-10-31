"""Valida o contrato de transferência offline, sem AWS ou Databricks reais."""

import copy
import io
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "v1" / "transfer"))

import send_files


class Response:
    def __init__(self, status=200, payload=None, headers=None, content=b"", json_error=None):
        self.status_code = status
        self.payload = payload
        self.headers = headers or {}
        self.content = content
        self.json_error = json_error
        self.closed = False

    def json(self):
        if self.json_error:
            raise self.json_error
        return self.payload

    def close(self):
        self.closed = True


class Session:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []
        self.request_bodies = []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        data = kwargs.get("data")
        if data is not None and hasattr(data, "read"):
            self.request_bodies.append(data.read())
        result = self.responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


class Secrets:
    def __init__(self, value=None, error=None):
        self.value = value
        self.error = error
        self.requested = []

    def get_secret_value(self, **kwargs):
        self.requested.append(kwargs)
        if self.error:
            raise self.error
        return {"SecretString": self.value}


class MemoryBody:
    def __init__(self, raw):
        self.raw = raw
        self.closed = False

    def read(self):
        return self.raw

    def close(self):
        self.closed = True


def valid_args(**overrides):
    values = dict(
        DATABRICKS_SECRET_ID="test/secret",
        DATABRICKS_JOB_ID=42,
        TARGET_CATALOG="workspace",
        VOLUME_PATH="/Volumes/workspace/delta/delta_silver",
        CHECK_ONLY="true",
        WORKFLOW_NAME="delta-test",
        WORKFLOW_RUN_ID="run-123",
        WAIT_TIMEOUT_SECONDS=30,
        POLL_SECONDS=5,
    )
    values.update(overrides)
    return SimpleNamespace(**values)


def valid_manifest(version=1, sql_included=False, mongo_prefix=""):
    batch = "batch_20261031"
    schema = {"type": "struct", "fields": [{"name": "id", "type": "string"}]}
    tables = []
    all_tables = sorted(send_files.EXPECTED_TABLES)
    if sql_included:
        sql_tables = dict(send_files.SQL_TABLES)
        if version == 3:
            sql_tables.update(send_files.BI_SQL_TABLES)
        all_tables.extend(("silver_sql", name) for name in sorted(sql_tables))

    for schema_name, table_name in all_tables:
        if schema_name == "silver_sql":
            source_schema, source_table = sql_tables[table_name]
            source_path = (
                f"s3://{send_files.BUCKET}/sql-database/datamart/"
                f"{source_schema}/{source_table}/"
            )
        else:
            area = "telemetry" if schema_name == "silver_telemetry" else "app"
            root = f"s3://{send_files.BUCKET}/"
            if mongo_prefix:
                root += mongo_prefix + "/"
            source_path = f"{root}silver/{area}/{table_name}/"
        tables.append(
            {
                "schema_name": schema_name,
                "table_name": table_name,
                "source_path": source_path,
                "source_delta_version": 3,
                "row_count": 0,
                "spark_schema": copy.deepcopy(schema),
                "files": [],
            }
        )

    return {
        "manifest_version": version,
        "batch_id": batch,
        "workflow_name": "delta-test",
        "workflow_run_id": "run-123",
        "mongo_prefix": mongo_prefix,
        "sql_included": sql_included,
        "tables": tables,
    }


def s3_for_manifest(manifest):
    raw = json.dumps(manifest).encode("utf-8")
    body = MemoryBody(raw)

    class S3:
        def __init__(self):
            self.calls = []

        def get_object(self, **kwargs):
            self.calls.append(kwargs)
            return {"Body": body}

    return S3(), body, raw


def properties_for(manifest):
    batch = manifest["batch_id"]
    prefix = f"exports/databricks/{batch}/"
    return {
        "delta_export_status": "READY",
        "delta_batch_id": batch,
        "delta_manifest_uri": f"s3://{send_files.BUCKET}/{prefix}manifest.json",
    }


def test_parse_args_normalizes_volume_and_accepts_glue_parameters():
    args = send_files.parse_args(
        [
            "--DATABRICKS_SECRET_ID=test/secret",
            "--DATABRICKS_JOB_ID=42",
            "--VOLUME_PATH=/Volumes/workspace/delta/silver/",
            "--CHECK_ONLY=true",
            "--JOB_NAME=glue-injected",
        ]
    )

    assert args.DATABRICKS_JOB_ID == 42
    assert args.VOLUME_PATH == "/Volumes/workspace/delta/silver"
    assert args.POLL_SECONDS == 30


@pytest.mark.parametrize(
    "overrides",
    [
        {"--DATABRICKS_JOB_ID": "0"},
        {"--TARGET_CATALOG": "workspace;DROP"},
        {"--VOLUME_PATH": "/Volumes/workspace/delta/silver/extra"},
        {"--POLL_SECONDS": "4"},
        {"--WAIT_TIMEOUT_SECONDS": "0"},
    ],
)
def test_parse_args_rejects_unsafe_or_out_of_range_configuration(overrides):
    arguments = [
        "--DATABRICKS_SECRET_ID=test/secret",
        "--DATABRICKS_JOB_ID=42",
        "--CHECK_ONLY=true",
    ]
    arguments.extend(f"{key}={value}" for key, value in overrides.items())

    with pytest.raises(SystemExit):
        send_files.parse_args(arguments)


def test_parse_args_requires_workflow_context_for_real_import():
    with pytest.raises(SystemExit):
        send_files.parse_args(
            ["--DATABRICKS_SECRET_ID=s", "--DATABRICKS_JOB_ID=1", "--CHECK_ONLY=false"]
        )


def test_read_credentials_accepts_https_base_and_strips_secret_whitespace():
    secrets = Secrets(json.dumps({"host": "https://workspace.example/", "token": " token-value \n"}))

    host, token = send_files.read_credentials(secrets, "test/secret")

    assert (host, token) == ("https://workspace.example", "token-value")
    assert secrets.requested == [{"SecretId": "test/secret"}]


@pytest.mark.parametrize(
    "secret",
    [
        "not-json",
        json.dumps({"host": "http://workspace.example", "token": "token"}),
        json.dumps({"host": "https://workspace.example/path", "token": "token"}),
        json.dumps({"host": "https://user@workspace.example", "token": "token"}),
        json.dumps({"host": "https://workspace.example?debug=1", "token": "token"}),
        json.dumps({"host": "https://workspace.example", "token": "token\nforged"}),
    ],
)
def test_read_credentials_rejects_invalid_secrets_without_echoing_values(secret):
    with pytest.raises(ValueError, match="segredo deve conter JSON") as error:
        send_files.read_credentials(Secrets(secret), "test/secret")

    assert "forged" not in str(error.value)
    assert "workspace.example" not in str(error.value)


def test_read_credentials_sanitizes_secrets_manager_error():
    error = RuntimeError("sensitive token")
    error.response = {"Error": {"Code": "AccessDeniedException"}}

    with pytest.raises(RuntimeError, match="AccessDeniedException") as result:
        send_files.read_credentials(Secrets(error=error), "test/secret")

    assert "sensitive token" not in str(result.value)


def test_databricks_api_retries_transient_status_and_rewinds_upload_stream():
    first = Response(status=503, headers={"Retry-After": "0.5"})
    second = Response(status=204)
    session = Session([first, second])
    sleeps = []
    stream = io.BytesIO(b"payload")
    stream.seek(2)
    api = send_files.DatabricksAPI("https://workspace.example", session, sleep=sleeps.append)

    result = api.request("PUT", "/upload", expected=(204,), data=stream)

    assert result is second
    assert first.closed is True
    assert session.request_bodies == [b"yload", b"yload"]
    assert sleeps == [1]
    assert session.calls[0][2]["timeout"] == (30, 120)
    assert session.calls[0][2]["allow_redirects"] is False


def test_databricks_api_retries_network_error_then_returns_response():
    response = Response(status=200)
    session = Session([TimeoutError(), response])
    sleeps = []
    api = send_files.DatabricksAPI(
        "https://workspace.example", session, network_errors=(TimeoutError,), sleep=sleeps.append
    )

    assert api.request("GET", "/status") is response
    assert sleeps == [1]
    assert len(session.calls) == 2


def test_databricks_api_stops_after_four_network_failures():
    session = Session([TimeoutError(), TimeoutError(), TimeoutError(), TimeoutError()])
    sleeps = []
    api = send_files.DatabricksAPI(
        "https://workspace.example", session, network_errors=(TimeoutError,), sleep=sleeps.append
    )

    with pytest.raises(RuntimeError, match="quatro tentativas"):
        api.request("GET", "/status")

    assert len(session.calls) == 4
    assert sleeps == [1, 2, 4]


def test_databricks_api_error_does_not_expose_response_body():
    response = Response(status=403, headers={}, content=b"private token and SQL")
    api = send_files.DatabricksAPI("https://workspace.example", Session([response]))

    with pytest.raises(RuntimeError, match="HTTP 403") as error:
        api.request("GET", "/private")

    assert response.closed is True
    assert "private token" not in str(error.value)


def test_json_request_closes_response_when_json_parsing_fails():
    response = Response(json_error=ValueError("bad JSON"))
    api = send_files.DatabricksAPI("https://workspace.example", Session([response]))

    with pytest.raises(ValueError, match="bad JSON"):
        api.json_request("GET", "/manifest")

    assert response.closed is True


def test_check_job_accepts_only_serial_import_job_contract():
    api = SimpleNamespace(
        json_request=lambda method, path, **kwargs: {
            "settings": {
                "parameters": [{"name": name} for name in ("catalog", "volume_path", "batch_id")],
                "max_concurrent_runs": 1,
                "tasks": [{"task_key": "import_silver", "notebook_task": {"notebook_path": "/import"}}],
            }
        }
    )

    assert send_files.check_job(api, 42) is None


@pytest.mark.parametrize(
    "settings, message",
    [
        ({"parameters": [], "tasks": [{"task_key": "import_silver", "notebook_task": {}}]}, "Job parameters"),
        ({"parameters": [{"name": "catalog"}, {"name": "volume_path"}, {"name": "batch_id"}], "max_concurrent_runs": 2, "tasks": [{"task_key": "import_silver", "notebook_task": {}}]}, "Max concurrent"),
        ({"parameters": [{"name": "catalog"}, {"name": "volume_path"}, {"name": "batch_id"}], "tasks": [{"task_key": "import_silver", "notebook_task": {}, "depends_on": [{"task_key": "other"}]}]}, "tarefa inicial"),
    ],
)
def test_check_job_rejects_configuration_that_can_break_import(settings, message):
    api = SimpleNamespace(json_request=lambda *args, **kwargs: {"settings": settings})

    with pytest.raises(ValueError, match=message):
        send_files.check_job(api, 42)


def test_read_manifest_validates_legacy_batch_and_closes_s3_body():
    manifest = valid_manifest()
    s3, body, raw = s3_for_manifest(manifest)

    actual, actual_raw = send_files.read_manifest(
        s3, properties_for(manifest), valid_args()
    )

    assert actual == manifest
    assert actual_raw == raw
    assert body.closed is True
    assert s3.calls[0]["Bucket"] == send_files.BUCKET
    assert s3.calls[0]["Key"] == f"exports/databricks/{manifest['batch_id']}/manifest.json"


def test_read_manifest_maps_v3_sql_and_mongo_sources_from_manifest():
    manifest = valid_manifest(version=3, sql_included=True, mongo_prefix="mongo-database")
    s3, _, _ = s3_for_manifest(manifest)

    result, _ = send_files.read_manifest(s3, properties_for(manifest), valid_args())

    assert len(result["tables"]) == len(send_files.EXPECTED_TABLES) + len(send_files.SQL_TABLES) + len(send_files.BI_SQL_TABLES)


def test_read_manifest_rejects_cross_run_manifest_and_missing_data_files():
    manifest = valid_manifest()
    manifest["workflow_run_id"] = "another-run"
    s3, _, _ = s3_for_manifest(manifest)

    with pytest.raises(ValueError, match="não pertence"):
        send_files.read_manifest(s3, properties_for(manifest), valid_args())

    manifest = valid_manifest()
    manifest["tables"][0]["row_count"] = 1
    s3, _, _ = s3_for_manifest(manifest)
    with pytest.raises(ValueError, match="sem arquivos"):
        send_files.read_manifest(s3, properties_for(manifest), valid_args())


def test_send_batch_verifies_download_size_and_publishes_manifest_last(monkeypatch):
    temporary_directory = send_files.tempfile.TemporaryDirectory
    monkeypatch.setattr(
        send_files.tempfile,
        "TemporaryDirectory",
        lambda prefix: temporary_directory(prefix=prefix, dir=ROOT),
    )
    payload = b"synthetic parquet bytes"
    raw_manifest = b'{"batch_id":"batch_1"}'
    manifest = {
        "batch_id": "batch_1",
        "tables": [
            {
                "schema_name": "silver_app",
                "table_name": "device_status",
                "files": [
                    {"name": "part-0001.parquet", "s3_key": "exports/batch_1/part-0001.parquet", "size": len(payload)}
                ],
            }
        ],
    }

    class S3:
        def download_file(self, bucket, key, destination):
            assert bucket == send_files.BUCKET
            assert key.endswith("part-0001.parquet")
            Path(destination).write_bytes(payload)

    class API:
        def __init__(self):
            self.directories = []
            self.uploads = []

        def mkdir(self, path):
            self.directories.append(path)

        def upload(self, path, stream):
            self.uploads.append((path, stream.read()))

    api = API()
    volume = "/Volumes/workspace/delta/delta_silver"

    batch_path = send_files.send_batch(S3(), api, manifest, raw_manifest, volume)

    file_path = f"{batch_path}/silver_app/device_status/part-0001.parquet"
    assert api.uploads == [(file_path, payload), (f"{batch_path}/manifest.json", raw_manifest)]
    assert api.uploads[-1][0].endswith("/manifest.json")
    assert api.directories == [batch_path, f"{batch_path}/silver_app/device_status"]

    class TruncatedS3:
        def download_file(self, bucket, key, destination):
            Path(destination).write_bytes(b"short")

    truncated_api = API()
    with pytest.raises(ValueError, match="diverge do tamanho"):
        send_files.send_batch(TruncatedS3(), truncated_api, manifest, raw_manifest, volume)
    assert truncated_api.uploads == []


def test_trigger_import_sets_idempotency_token_and_rejects_invalid_run_id():
    calls = []
    api = SimpleNamespace(json_request=lambda *args, **kwargs: calls.append((args, kwargs)) or {"run_id": 456})

    assert send_files.trigger_import(api, valid_args(), "batch-1") == 456
    payload = calls[0][1]["json"]["job_parameters"]
    assert payload == {
        "catalog": "workspace",
        "volume_path": "/Volumes/workspace/delta/delta_silver",
        "batch_id": "batch-1",
    }
    token = calls[0][1]["json"]["idempotency_token"]
    assert len(token) == 64

    api.json_request = lambda *args, **kwargs: {"run_id": "not-an-integer"}
    with pytest.raises(ValueError, match="run_id válido"):
        send_files.trigger_import(api, valid_args(), "batch-1")


def test_wait_for_import_returns_only_after_success():
    states = iter(
        [
            {"state": {"life_cycle_state": "RUNNING"}},
            {"state": {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS"}},
        ]
    )
    api = SimpleNamespace(json_request=lambda *args, **kwargs: next(states))
    now = [0]

    result = send_files.wait_for_import(
        api, 456, timeout=20, poll=5, clock=lambda: now[0], sleep=lambda seconds: now.__setitem__(0, now[0] + seconds)
    )

    assert result is None
    assert now[0] == 5


def test_wait_for_import_reports_terminal_failure_and_timeout():
    failed = SimpleNamespace(
        json_request=lambda *args, **kwargs: {
            "state": {"life_cycle_state": "TERMINATED", "result_state": "FAILED"}
        }
    )
    with pytest.raises(RuntimeError, match="TERMINATED/FAILED"):
        send_files.wait_for_import(failed, 456, timeout=10, poll=5, clock=lambda: 0, sleep=lambda _: None)

    running = SimpleNamespace(json_request=lambda *args, **kwargs: {"state": {"life_cycle_state": "RUNNING"}})
    now = [0]
    with pytest.raises(TimeoutError, match="run_id=456"):
        send_files.wait_for_import(
            running, 456, timeout=6, poll=5, clock=lambda: now[0], sleep=lambda seconds: now.__setitem__(0, now[0] + seconds)
        )


def test_workflow_properties_merge_state_without_dropping_existing_values():
    class Glue:
        def __init__(self):
            self.values = {"existing": "keep"}
            self.writes = []

        def get_workflow_run_properties(self, **context):
            return {"RunProperties": dict(self.values)}

        def put_workflow_run_properties(self, **kwargs):
            self.writes.append(kwargs)
            self.values = kwargs["RunProperties"]

    glue = Glue()
    args = valid_args()

    values = send_files.workflow_properties(glue, args, {"delta_databricks_status": "RUNNING"})

    assert values == {"existing": "keep", "delta_databricks_status": "RUNNING"}
    assert glue.writes[0]["Name"] == args.WORKFLOW_NAME
    assert glue.writes[0]["RunId"] == args.WORKFLOW_RUN_ID
