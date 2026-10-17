"""Executa as transformações reais com fixtures JVM; sem AWS/Atlas/Databricks."""
import ctypes
import importlib.util
import json
import os
import re
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1] / "gold"
DEPS = Path(os.environ.get("DELTA_SPARK_DEPS", str(ROOT.parents[2] / "output/databricks/gold/test-deps")))
sys.path.insert(0, str(DEPS))
from pyspark.sql import SparkSession, functions as F
from datetime import date, datetime

def jvm_frame(session, rows, schema):
    """Fixtures via SQL VALUES: não dependem do worker Python do Spark no Windows."""
    columns=[tuple(part.strip().split(" ",1)) for part in schema.split(",")]
    def literal(value, kind):
        if value is None:
            return f"CAST(NULL AS {kind})"
        if isinstance(value,bool):
            return "true" if value else "false"
        if isinstance(value,datetime):
            return "TIMESTAMP '"+value.strftime("%Y-%m-%d %H:%M:%S.%f")+"'"
        if isinstance(value,date):
            return "DATE '"+value.isoformat()+"'"
        if isinstance(value,str):
            return "'"+value.replace("\\","\\\\").replace("'","\\'")+"'"
        return f"CAST({value} AS {kind})"
    if not rows:
        return session.sql("SELECT "+",".join(f"CAST(NULL AS {kind}) `{name}`" for name,kind in columns)+" WHERE false")
    values=",".join("("+",".join(literal(v,columns[i][1]) for i,v in enumerate(row))+")" for row in rows)
    names=",".join(f"`{name}`" for name,kind in columns)
    return session.sql(f"SELECT * FROM VALUES {values} AS fixture({names})")

import ast
from types import SimpleNamespace
from pyspark.sql import Window
tree = ast.parse((ROOT / "gold_bi.py").read_text(encoding="utf-8"))
functions = [node for node in tree.body if isinstance(node, ast.FunctionDef)]
bi = SimpleNamespace(F=F, Window=Window, TZ="America/Sao_Paulo")
exec(compile(ast.Module(body=functions, type_ignores=[]), str(ROOT / "gold_bi.py"), "exec"), bi.__dict__)


def short(path):
    buf = ctypes.create_unicode_buffer(32768)
    ctypes.windll.kernel32.GetShortPathNameW(str(path), buf, len(buf))
    return buf.value or str(path)


class BIV2Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        temp = Path(os.environ.get("DELTA_TEST_TMP", str(ROOT.parents[2] / "tmp/gold-governance/spark"))).resolve()
        temp.mkdir(parents=True, exist_ok=True)
        os.chdir(temp)
        os.environ.setdefault("JAVA_HOME", str(Path.home() / ".jdks/ms-21.0.8"))
        os.environ["SPARK_HOME"] = short(DEPS / "pyspark")
        os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
        local_tmp = short(temp).replace("\\", "/")
        os.environ["JAVA_TOOL_OPTIONS"] = f"-Djava.io.tmpdir={local_tmp} -Djdk.net.unixdomain.tmpdir={local_tmp}"
        print({key:os.environ[key] for key in ("SPARK_HOME","JAVA_HOME","JAVA_TOOL_OPTIONS")},flush=True)
        cls.spark = (SparkSession.builder.master("local[1]").appName("delta-bi-v2")
                     .config("spark.ui.enabled", "false").config("spark.ui.showConsoleProgress", "false")
                     .config("spark.sql.shuffle.partitions", "1").config("spark.driver.host", "127.0.0.1")
                     .config("spark.sql.session.timeZone", "UTC").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.cut = "2026-10-17T03:00:00Z"

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def frame(self, rows, schema):
        return jvm_frame(self.spark, rows, schema)

    def props(self):
        return bi.properties_sp(self.frame([
            (1, 1, "GRANDE_SP", "São Paulo", "SP", "RESIDENCIAL", -23.55, -46.63),
            (2, 2, "LINS", "São Paulo", "SP", "COMERCIAL", -23.54, -46.62),
            (3, 3, "OUTRA", "Rio", "RJ", "RESIDENCIAL", -22.9, -43.2),
        ], "property_id INT, region_id INT, region_name STRING, city STRING, state STRING, classification_group STRING, latitude DOUBLE, longitude DOUBLE"))

    def devices(self, two=False):
        rows = [("a", 1, "2026-10-01")]
        if two:
            rows.append(("b", 1, "2026-10-01"))
        return self.frame(rows, "device_id STRING, property_id INT, installation_date STRING").withColumn("installation_date", F.to_date("installation_date"))

    def windows(self, rows):
        return (self.frame(rows, "device_id STRING, user_id INT, window_started_at STRING, window_finished_at STRING, consumption_liters DOUBLE")
                .withColumn("window_started_at", F.to_timestamp("window_started_at"))
                .withColumn("window_finished_at", F.to_timestamp("window_finished_at")))

    def bridge(self):
        return self.frame([(10, 1, "2026-10-01")], "user_id INT, property_id INT, association_date STRING").withColumn("association_date", F.to_date("association_date"))

    def keys(self):
        return self.frame([(10, "uuid-10")], "user_id INT, user_key STRING")

    def test_split_conserves_liters_and_detects_missing_device(self):
        windows = self.windows([("a", 10, "2026-10-14 11:30:00", "2026-10-14 12:30:00", 6.0)])
        parts = bi.consumption_parts(windows, self.devices(True), self.props(), self.cut)
        hourly = bi.consumption_hourly(parts, self.devices(True)).orderBy("hour_utc").collect()
        self.assertEqual(len(hourly), 2)
        self.assertEqual([r.consumption_liters for r in hourly], [3.0, 3.0])
        self.assertEqual([r.covered_seconds for r in hourly], [1800.0, 1800.0])
        self.assertEqual([r.expected_seconds for r in hourly], [7200.0, 7200.0])
        self.assertEqual(hourly[0].hour, 8)
        self.assertEqual(hourly[0].latitude, -23.55)

    def test_overlap_is_rejected(self):
        windows = self.windows([("a", 10, "2026-10-14 11:00:00", "2026-10-14 12:00:00", 6.0),
                                ("a", 10, "2026-10-14 11:30:00", "2026-10-14 12:30:00", 6.0)])
        with self.assertRaisesRegex(ValueError, "sobrepostas"):
            bi.consumption_parts(windows, self.devices(), self.props(), self.cut)

    def test_weather_is_not_duplicated_by_region(self):
        weather = self.frame([("São Paulo", "SP", "BR", "2026-10-14 12:00:00", 25.0, 29.0)],
            "city STRING, state STRING, country STRING, observed_at STRING, temperature_c DOUBLE, feels_like_c DOUBLE").withColumn("observed_at", F.to_timestamp("observed_at"))
        result = bi.weather_hourly(weather, self.props(), self.cut).collect()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].region_ids, [1, 2])
        self.assertEqual(result[0].temperature_c, 25.0)

    def test_latency_uses_backend_field_and_keeps_error_without_latency(self):
        sessions = self.frame([("s", 10, False)], "session_id STRING, user_id INT, is_deleted BOOLEAN")
        messages = self.frame([
            ("s", 1, "bot", "text", "2026-10-14 12:00:00", 200, "leak", 1800),
            ("s", 2, "bot", "text", "2026-10-14 12:01:00", 503, "forecast", None),
            ("s", 3, "bot", "text", "2026-10-14 12:02:00", 200, "judge", 100),
        ], "session_id STRING, message_index INT, role STRING, content_type STRING, sent_at STRING, api_status_code INT, agent_name STRING, response_time_ms BIGINT").withColumn("sent_at", F.to_timestamp("sent_at"))
        users = bi.user_context(self.bridge(), self.props(), self.cut)
        result = bi.agent_calls(messages, sessions, self.cut).orderBy("message_index").collect()
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0].response_time_ms, 1800.0)
        self.assertEqual(result[1].api_status_code, 503)
        self.assertIsNone(result[1].response_time_ms)
        self.assertIsNone(result[2].response_time_ms)

    def test_targets_exclude_incomplete_days(self):
        windows = self.windows([("a", 10, "2026-10-14 03:00:00", "2026-10-15 03:00:00", 240.0),
                                ("a", 10, "2026-10-15 03:00:00", "2026-10-15 15:00:00", 120.0)])
        parts = bi.consumption_parts(windows, self.devices(), self.props(), self.cut)
        pref = self.frame([(10, 300)], "user_id INT, daily_liters_target INT")
        users = bi.user_context(self.bridge(), self.props(), self.cut)
        result = bi.target_compliance_daily(parts, pref, self.devices(), self.bridge(), self.keys()).orderBy("local_date").collect()
        self.assertEqual([r.eligible_days for r in result], [1])
        self.assertEqual([r.days_within_target for r in result], [1])

    def status(self, available):
        rows = self.frame([("a", "2026-10-14 03:00:00", "2026-10-16 03:00:00", available)],
            "device_id STRING, availability_started_at STRING, availability_updated_at STRING, available_seconds BIGINT")
        events = [{"started_at": "2026-10-15T02:50:00Z", "ended_at": "2026-10-15T03:10:00Z", "unavailable_seconds": 1200}]
        return (rows.withColumn("availability_started_at", F.to_timestamp("availability_started_at"))
                .withColumn("availability_updated_at", F.to_timestamp("availability_updated_at"))
                .withColumn("unavailabilities", F.from_json(F.lit(json.dumps(events)),
                    "array<struct<started_at:timestamp,ended_at:timestamp,unavailable_seconds:bigint>>")))

    def test_availability_splits_midnight_and_reconciles_counter(self):
        result = bi.device_availability_daily(self.status(171600), self.devices(), self.props(), self.cut).orderBy("local_date").collect()
        self.assertEqual(len(result), 2)
        self.assertEqual([r.unavailable_seconds for r in result], [600.0, 600.0])
        self.assertEqual([r.available_seconds for r in result], [85800.0, 85800.0])
        with self.assertRaisesRegex(ValueError, "não cobre"):
            bi.device_availability_daily(self.status(100), self.devices(), self.props(), self.cut)

    def test_overlapping_tariff_is_rejected(self):
        rates = self.frame([(1,"GRANDE_SP",1,"RESIDENCIAL","2026-01-01","2026-12-31",8.0),
                            (1,"GRANDE_SP",1,"RESIDENCIAL","2026-10-01",None,9.0)],
            "region_id INT, region_name STRING, classification_id INT, classification_group STRING, initial_validity STRING, final_validity STRING, m3_value DOUBLE")
        rates = rates.withColumn("initial_validity", F.to_date("initial_validity")).withColumn("final_validity", F.to_date("final_validity"))
        with self.assertRaisesRegex(ValueError, "sobrepostas"):
            bi.tariff_validity(rates)

    def test_alerts_count_opening_ids_not_duration(self):
        alerts = self.frame([("x","a","vazamento_continuo","2026-10-14 12:00:00"),
                             ("y","a","vazamento_continuo","2026-10-14 13:00:00"),
                             ("z","a","fluxo_atipico","2026-10-14 14:00:00")],
            "alert_id STRING, device_id STRING, alert_type STRING, triggered_at STRING").withColumn("triggered_at",F.to_timestamp("triggered_at"))
        rows = bi.alert_count_daily(alerts,self.devices(),self.props(),self.cut).collect()
        self.assertEqual({r.alert_type:r.alert_count for r in rows},{"vazamento_continuo":2,"fluxo_atipico":1})

    def test_dashboard_sql_resolves_all_thirteen_datasets(self):
        schemas = {
            "tb_consumption_hourly": "property_id INT, hour_utc TIMESTAMP, local_date DATE, hour INT, region_id INT, region_name STRING, city STRING, state STRING, classification_group STRING, latitude DOUBLE, longitude DOUBLE, consumption_liters DOUBLE, covered_seconds DOUBLE, expected_seconds DOUBLE",
            "tb_weather_hourly": "city STRING, state STRING, hour_utc TIMESTAMP, local_date DATE, temperature_c DOUBLE, feels_like_c DOUBLE, region_ids ARRAY<INT>",
            "tb_alert_count_daily": "local_date DATE, device_id STRING, property_id INT, region_id INT, region_name STRING, alert_type STRING, alert_count BIGINT",
            "tb_agent_call": "session_id STRING, message_index INT, occurred_at TIMESTAMP, agent_name STRING, response_time_ms DOUBLE, api_status_code INT",
            "tb_tariff_validity": "region_id INT, region_name STRING, classification_id INT, classification_group STRING, initial_validity DATE, final_validity DATE, tariff_per_m3 DOUBLE",
            "tb_target_compliance_daily": "user_key STRING, local_date DATE, consumption_liters DOUBLE, daily_liters_target INT, days_within_target INT, eligible_days INT",
            "tb_device_availability_daily": "device_id STRING, property_id INT, region_id INT, region_name STRING, local_date DATE, expected_seconds DOUBLE, unavailable_seconds DOUBLE, available_seconds DOUBLE",
        }
        for name,schema in schemas.items():
            columns=[part.strip().split(" ",1) for part in schema.split(",")]
            self.spark.sql("SELECT "+",".join(f"CAST(NULL AS {kind}) AS {column}" for column,kind in columns)+" WHERE false").createOrReplaceTempView(name)
        source = re.sub(r"--[^\n]*", "", (ROOT/"CONSULTAS.sql").read_text(encoding="utf-8").replace("workspace.gold_bi.", ""))
        values={"start_date":"'2026-10-01'","end_date":"'2026-11-01'","region_id":"'ALL'","reference_date":"'2026-10-16'","temperature_band_c":"5"}
        source = re.sub(r":([a-z_]+)",lambda match:values[match[1]],source)
        selects=0
        for statement in source.split(";"):
            statement=statement.strip()
            if not statement or statement.startswith("USE "):
                continue
            self.spark.sql(statement).schema
            if not statement.startswith("CREATE "):
                selects+=1
        self.assertEqual(selects,14)  # 13 indicadores e opções reais de região.


    def test_router_and_judge_variants_are_excluded_from_latency(self):
        sessions = self.frame([("s",10,False)], "session_id STRING, user_id INT, is_deleted BOOLEAN")
        messages = self.frame([
            ("s",1,"bot","text","2026-10-14 12:00:00",200,"Judge_Agent",100),
            ("s",2,"bot","text","2026-10-14 12:01:00",503,"router-agent",200),
            ("s",3,"bot","text","2026-10-14 12:02:00",200,"LeakAgent",1800),
        ], "session_id STRING, message_index INT, role STRING, content_type STRING, sent_at STRING, api_status_code INT, agent_name STRING, response_time_ms BIGINT").withColumn("sent_at", F.to_timestamp("sent_at"))
        rows = bi.agent_calls(messages, sessions, self.cut).orderBy("message_index").collect()
        self.assertIsNone(rows[0].response_time_ms)
        self.assertIsNone(rows[1].response_time_ms)
        self.assertEqual(rows[1].api_status_code, 503)
        self.assertEqual(rows[2].response_time_ms, 1800)

    def test_silver_schema_handles_absent_fields_with_real_spark(self):
        from pyspark.sql.types import StructType, StructField, ArrayType, StringType, TimestampType, IntegerType, LongType
        from types import SimpleNamespace
        path = ROOT.parent / "silver/app_silver.py"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        func = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "read_bronze")
        schema = StructType([
            StructField("user_id", IntegerType()),
            StructField("messages", ArrayType(StructType([
                StructField("agent_name", StringType()), StructField("sent_at", TimestampType()),
                StructField("response_time_ms", LongType()),
            ]))),
        ])
        raw = self.spark.sql("SELECT 's' AS _id, TIMESTAMP '2026-10-14 12:00:00' AS _ingested_at, array(named_struct('agent_name','LeakAgent','sent_at',TIMESTAMP '2026-10-14 12:00:00')) AS messages")
        reader = SimpleNamespace(format=lambda _: SimpleNamespace(load=lambda _: raw))
        namespace = {"F":F, "StructType":StructType, "StringType":StringType, "TimestampType":TimestampType, "spark":SimpleNamespace(read=reader)}
        exec(compile(ast.Module(body=[func], type_ignores=[]), str(path), "exec"), namespace)
        normalized = namespace["read_bronze"]("fixture", schema)
        row = normalized.first()
        self.assertIsNone(row.user_id)
        self.assertEqual(row.messages[0].agent_name, "LeakAgent")
        self.assertIsNone(row.messages[0].response_time_ms)
        self.assertEqual(normalized.select(F.col("messages")[0]["sent_at"].cast("string").alias("utc_time")).first().utc_time, "2026-10-14 12:00:00")
        raw = raw.drop("messages")
        row = namespace["read_bronze"]("fixture", schema).first()
        self.assertIsNone(row.messages)

    def test_city_variants_have_one_join_key(self):
        props = self.props()
        weather = self.frame([
            ("  Sao   Paulo ", "São Paulo", "Brasil", "2026-10-14 12:00:00", 25.0, 29.0),
            ("São Paulo", "SP", "BR", "2026-10-14 12:00:00", 25.0, 29.0),
        ], "city STRING, state STRING, country STRING, observed_at STRING, temperature_c DOUBLE, feels_like_c DOUBLE").withColumn("observed_at", F.to_timestamp("observed_at"))
        rows = bi.weather_hourly(weather, props, self.cut).collect()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].city, "SAO PAULO")
        self.assertEqual(rows[0].region_ids, [1, 2])
        self.assertEqual(props.select('city').distinct().first().city, rows[0].city)

    def test_conflicting_weather_does_not_become_an_arbitrary_average(self):
        weather = self.frame([
            ("Sao Paulo", "SP", "BR", "2026-10-14 12:00:00", 25.0, 29.0),
            ("São Paulo", "SP", "BR", "2026-10-14 12:00:00", 26.0, 29.0),
        ], "city STRING, state STRING, country STRING, observed_at STRING, temperature_c DOUBLE, feels_like_c DOUBLE").withColumn("observed_at", F.to_timestamp("observed_at"))
        with self.assertRaisesRegex(ValueError, "duplicada"):
            bi.weather_hourly(weather, self.props(), self.cut)

    def test_null_weather_is_rejected_instead_of_silently_discarded(self):
        weather = self.frame([("São Paulo", "SP", "BR", "2026-10-14 12:00:00", 25.0, None)],
            "city STRING, state STRING, country STRING, observed_at STRING, temperature_c DOUBLE, feels_like_c DOUBLE").withColumn("observed_at", F.to_timestamp("observed_at"))
        with self.assertRaisesRegex(ValueError, "obrigatório"):
            bi.weather_hourly(weather, self.props(), self.cut)

    def test_region_dictionary_reconciles_tariff_and_property_names(self):
        props = self.props()
        rates = self.frame([(1, "  Grande SP "), (2, "LINS"), (4, "OUTRA REGIAO")], "region_id INT, region_name STRING")
        names = {r.region_id: r.region_name for r in bi.region_dictionary(props, rates).collect()}
        self.assertEqual(names[1], "GRANDE_SP")
        self.assertEqual(len(names), 3)  # Mais regiões de tarifa é legítimo; não inventa consumo nelas.
        with self.assertRaisesRegex(ValueError, "nomes diferentes"):
            bi.region_dictionary(props, rates.withColumn("region_name", F.when(F.col("region_id") == 1, F.lit("LITORAL")).otherwise(F.col("region_name"))))

    def test_missing_map_coordinates_block_publication(self):
        props = self.props().withColumn("latitude", F.lit(None).cast('double'))
        with self.assertRaisesRegex(ValueError, "coordenadas"):
            bi.properties_sp(props)

    def test_agent_fact_has_no_inferred_geography_or_user_identifier(self):
        sessions = self.frame([("s", 10, False)], "session_id STRING, user_id INT, is_deleted BOOLEAN")
        messages = self.frame([("s", 1, "bot", "text", "2026-10-14 12:00:00", 200, "Leak_Agent", 1800)],
            "session_id STRING, message_index INT, role STRING, content_type STRING, sent_at STRING, api_status_code INT, agent_name STRING, response_time_ms BIGINT").withColumn("sent_at", F.to_timestamp("sent_at"))
        result = bi.agent_calls(messages, sessions, self.cut)
        self.assertEqual(set(result.columns), {'session_id', 'message_index', 'occurred_at', 'agent_name', 'response_time_ms', 'api_status_code'})
        self.assertEqual(result.first().agent_name, 'leak')
        self.assertEqual(bi.validate_bi('tb_agent_call', result)['rows'], 1)
        with self.assertRaisesRegex(ValueError, "fora do contrato"):
            bi.validate_bi('tb_agent_call', result.withColumn('region_id', F.lit(1)))
        with self.assertRaisesRegex(ValueError, "obrigatório"):
            bi.agent_calls(messages.withColumn('agent_name', F.lit(None).cast('string')), sessions, self.cut)

    def test_three_weather_hours_are_reported_as_one_day_without_filling_history(self):
        weather = self.frame([
            ("São Paulo", "SP", "BR", "2026-09-27 00:00:00", 18.2, 18.0),
            ("São Paulo", "SP", "BR", "2026-09-27 01:00:00", 17.9, 17.6),
            ("São Paulo", "SP", "BR", "2026-09-27 02:00:00", 17.5, 17.3),
        ], "city STRING, state STRING, country STRING, observed_at STRING, temperature_c DOUBLE, feels_like_c DOUBLE").withColumn("observed_at", F.to_timestamp("observed_at"))
        result = bi.weather_hourly(weather, self.props(), self.cut)
        quality = bi.validate_bi('tb_weather_hourly', result)
        self.assertEqual(quality['rows'], 3)
        self.assertEqual(quality['days'], 1)
        self.assertEqual(quality['min_date'], '2026-09-26')  # UTC -> São Paulo; não trocar pela data do bucket.
        self.assertIn('warning', quality)

    def test_invalid_consumption_is_not_filtered_into_a_successful_empty_table(self):
        with self.assertRaisesRegex(ValueError, "volume/período"):
            bi.consumption_parts(self.windows([('a', 10, '2026-10-14 12:00:00', '2026-10-14 13:00:00', -1.0)]), self.devices(), self.props(), self.cut)


if __name__ == "__main__":
    unittest.main(verbosity=2)
