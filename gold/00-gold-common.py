# Databricks notebook source
# MAGIC %md
# MAGIC # Apoio compartilhado dos oito pipelines Gold
# COMMAND ----------
from datetime import date, datetime, timezone
import re
from decimal import Decimal
from pyspark.sql import functions as F
from pyspark.sql import Window

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("batch_id", "")
dbutils.widgets.text("cutoff_at", "")  # Opcional: data UTC; vazio = início desta execução.
CATALOG = dbutils.widgets.get("catalog").strip()
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", CATALOG):
    raise ValueError("catalog deve ser somente workspace ou outro nome de catálogo.")

PARAMETERS = {
    "coverage_threshold": 95,
    "fresh_ping_minutes": 15,
    "low_battery_pct": 20,
    "hot_temperature_c": 30,
    "peer_min_count": 10,
    "correlation_min_pairs": 30,
}
spark.conf.set("spark.sql.session.timeZone", "UTC")
started = datetime.now(timezone.utc)
cutoff_text = dbutils.widgets.get("cutoff_at").strip()
CUTOFF = datetime.fromisoformat(cutoff_text.replace("Z", "+00:00")) if cutoff_text else started
if CUTOFF.tzinfo is None:
    CUTOFF = CUTOFF.replace(tzinfo=timezone.utc)  # O campo cutoff_at é declarado UTC.
CUTOFF = CUTOFF.astimezone(timezone.utc)
ASOF = CUTOFF.strftime("%Y-%m-%d %H:%M:%S.%f")
GENERATED_AT = started.strftime("%Y-%m-%d %H:%M:%S.%f")
try:
    BATCH = dbutils.widgets.get("batch_id").strip() or started.strftime("manual_%Y%m%d_%H%M%S")
except Exception:
    BATCH = started.strftime("manual_%Y%m%d_%H%M%S")
if not (
    0 < PARAMETERS["coverage_threshold"] <= 100
    and PARAMETERS["fresh_ping_minutes"] > 0
    and 0 <= PARAMETERS["low_battery_pct"] <= 100
    and PARAMETERS["peer_min_count"] >= 2
    and PARAMETERS["correlation_min_pairs"] >= 3
):
    raise ValueError("Confira os limites dos parâmetros de cálculo.")

# Todos os dispositivos cadastrados são considerados automaticamente pelo contexto SQL.
# Não existe lista de autorização por gestor no pipeline. O backend aplica esse recorte.
# installation_date é o início cadastral; o SQL atual não guarda histórico de vínculos.

# Opcional: (property_id, início, fim ou None, orçamento MENSAL em litros).
MONTHLY_GOALS = [
    (156, "2026-10-12", None, 30000.0),
]
# Opcional: (scenario_id SQL, property_id, mês-base completo, anualização constante).
SCENARIO_SCOPE = [
    # (1, 123, "2026-09-01", False),
]

# COMMAND ----------
frames = {}


def target(schema, name):
    return f"`{CATALOG}`.`{schema}`.`{name}`"


def view(name, frame):
    frames[name] = frame
    return frame


def assert_no_rows(frame, message):
    if frame.limit(1).count():
        raise ValueError(message)


def unique(frame, keys, message):
    duplicates = frame.groupBy(*keys).count().filter(F.col("count") > 1)
    assert_no_rows(duplicates, message)


# Persistência apenas das bases; resumos abaixo permanecem em frames.
def with_property_context(frame):
    attributes = ["property_id", "classification_group", "property_type", "city", "state",
                  "built_area_m2", "area_band", "country", "time_zone", "region_id", "classification_id"]
    missing = [name for name in attributes if name not in frame.columns]
    if not missing:
        return frame
    return frame.join(frames["p_tb_property"].select("property_id", *missing), "property_id", "left")


def base_for_storage(name, frame):
    if name == "tb_consumption_window":
        # Uma janela por sensor: serve a qualquer recorte de hora/dia/mês.
        # recent_window depende de T e será calculado no dashboard.
        return with_property_context(frame.drop("recent_window"))

    if name == "tb_device_snapshot":
        # Limites de ping/bateria/sinal não ficam congelados em colunas de KPI.
        base = with_property_context(frame.drop(
            "low_battery", "weak_wifi", "ping_age_minutes", "freshness_status"))
        monitoring = frames["c_tb_meter_scope"].groupBy("device_id").agg(
            F.collect_list(F.struct("property_id", "valid_from", "valid_to", "included", "scope_version"))
                .alias("monitoring_periods"))
        goals = frames["c_tb_goal_config"].groupBy("property_id").agg(
            F.collect_list(F.struct("valid_from", "valid_to", "monthly_target_liters", "goal_version"))
                .alias("monthly_goal_periods"))
        return (base.join(monitoring, "device_id", "left")
            .join(goals, "property_id", "left")
            .withColumn("monitoring_periods", F.coalesce(F.col("monitoring_periods"), F.array()))
            .withColumn("monthly_goal_periods", F.coalesce(F.col("monthly_goal_periods"), F.array())))

    if name == "tb_alert_case":
        # Durações/backlog e volume associado são medidas do período escolhido.
        return frame.select("alert_id", "device_id", "property_id", "organization_id",
            "alert_type", "severity", "triggered_at", "resolved_at", "triggered_date",
            "resolved_date", "quality_status")

    if name == "tb_investment_scenario":
        base = frame.select("scenario_id", "scenario_version", "reference_month", "organization_id",
            "name", "investment_value", "reduction_pct", "annual_savings_reported", "payback_months_reported")
        scope = (frames["c_tb_scenario_scope"].alias("c")
            .join(frames["p_tb_property"].alias("p"), F.col("c.property_id") == F.col("p.property_id"))
            .select("c.scenario_id", "c.scenario_version", "c.reference_month", "p.organization_id",
                    "c.property_id", "c.annualization_constant")
            .groupBy("scenario_id", "scenario_version", "reference_month", "organization_id")
            .agg(F.collect_list(F.struct("property_id", "annualization_constant")).alias("property_scope")))
        condition = (F.col("base.scenario_id").eqNullSafe(F.col("scope.scenario_id"))
            & F.col("base.scenario_version").eqNullSafe(F.col("scope.scenario_version"))
            & F.col("base.reference_month").eqNullSafe(F.col("scope.reference_month"))
            & F.col("base.organization_id").eqNullSafe(F.col("scope.organization_id")))
        return (base.alias("base").join(scope.alias("scope"), condition, "left")
            .select("base.*", F.coalesce(F.col("scope.property_scope"), F.array()).alias("property_scope")))

    if name == "tb_chat_session":
        # O evento de texto fica dentro da sessão, sem content/user_comment.
        # Arrays preservam eventos individuais para filtrar por sent_at e calcular quantis.
        messages = frames["v_messages"].alias("m")
        pairs = frames["b_tb_chat_response_pair"].alias("pair")
        bot = frames["v_messages"].select("session_id", "message_index", "sent_at").alias("bot")
        events = (messages.join(pairs,
            (F.col("m.session_id") == F.col("pair.session_id"))
            & (F.col("m.message_index") == F.col("pair.user_message_index")), "left")
            .join(bot, (F.col("pair.session_id") == F.col("bot.session_id"))
                & (F.col("pair.bot_message_index") == F.col("bot.message_index")), "left")
            .groupBy(F.col("m.session_id").alias("session_id"))
            .agg(F.collect_list(F.struct(
                F.col("m.message_index").alias("message_index"),
                F.col("m.role").alias("role"), F.col("m.sent_at").alias("sent_at"),
                F.col("m.api_status_code").alias("api_status_code"),
                F.col("pair.bot_message_index").alias("bot_message_index"),
                F.col("bot.sent_at").alias("response_at"),
                F.col("pair.delay_seconds").alias("delay_seconds"))).alias("message_events")))
        return (frame.select("session_id", "user_key", "started_at", "last_activity_at",
            "session_start_date", "is_open", "is_active", "feedback_id", "is_satisfied", "feedback_at")
            .join(events, "session_id", "left")
            .withColumn("message_events", F.coalesce(F.col("message_events"), F.array())))

    if name == "tb_bill_monthly":
        return frame.drop("previous_month_rate", "rate_variation_pct")
    return frame


def save_table(schema, name, frame, keys):
    stamps = [value.agg(F.max('_ingested_at').alias('t'))
              for key, value in frames.items() if key in SOURCES]
    watermark = None
    for stamp in stamps:
        watermark = stamp if watermark is None else watermark.union(stamp)
    SOURCE_WATERMARK = watermark.agg(F.max('t').alias('t')).first()['t'] if watermark is not None else None
    unique(frame, keys, f"Chave duplicada em {schema}.{name}: {keys}")
    # Os demais cálculos do notebook usam o resultado completo em memória.
    # A tabela persistida recebe somente fatos/atributos e configurações necessárias.
    frames[("p_" if schema == "gold_platform" else "b_") + name] = frame
    enriched = (base_for_storage(name, frame)
        .withColumn("generated_at", F.lit(GENERATED_AT).cast("timestamp"))
        .withColumn("cutoff_at", F.lit(ASOF).cast("timestamp"))
        .withColumn("source_watermark", F.lit(SOURCE_WATERMARK).cast("timestamp"))
        .withColumn("source_batch_id", F.lit(BATCH)))
    (enriched.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").saveAsTable(target(schema, name)))
    print(f"Base criada/atualizada: {CATALOG}.{schema}.{name}")


def ensure_user_keys():
    # Merge Delta com expressões Column: mantém o mesmo UUID nas próximas cargas.
    from delta.tables import DeltaTable

    ref = target("gold_bi", "tb_user_key")
    if not spark.catalog.tableExists(ref):
        (
            spark.createDataFrame([], "user_id INT, user_key STRING")
            .write.format("delta")
            .saveAsTable(ref)
        )
    users = (
        frames["s_sessions"]
        .select("user_id")
        .union(frames["s_preferences"].select("user_id"))
        .union(frames["s_person"].select("user_id"))
        .filter(F.col("user_id").isNotNull())
        .distinct()
    )
    (
        DeltaTable.forName(spark, ref)
        .alias("t")
        .merge(users.alias("s"), F.col("t.user_id") == F.col("s.user_id"))
        .whenNotMatchedInsert(
            values={"user_id": F.col("s.user_id"), "user_key": F.call_function("uuid")}
        )
        .execute()
    )
    frames["v_user_key"] = spark.table(ref)

SOURCES = {
    "s_consumption": ("silver_telemetry", "consumption_summary"),
    "s_status": ("silver_telemetry", "device_status"),
    "s_weather_daily": ("silver_app", "weather_daily"),
    "s_weather_hourly": ("silver_app", "weather_hourly"),
    "s_sessions": ("silver_app", "chat_sessions"),
    "s_messages": ("silver_app", "chat_messages"),
    "s_feedback": ("silver_app", "chat_feedback"),
    "s_alerts": ("silver_app", "alerts_history"),
    "s_preferences": ("silver_app", "user_preferences"),
    "s_property": ("silver_sql", "silver_dm_property"),
    "s_sql_daily": ("silver_sql", "silver_ft_consumption_daily"),
    "s_sql_reading": ("silver_sql", "silver_ft_consumption_reading"),
    "s_sql_gold_daily": ("silver_sql", "gold_ft_consumption_daily"),
    "s_sql_gold_property": ("silver_sql", "gold_dm_property"),
    "s_date": ("silver_sql", "gold_dm_date"),
    "s_person": ("silver_sql", "gold_dm_person"),
    "s_bill": ("silver_sql", "gold_ft_water_bill_monthly"),
    "s_scenario": ("silver_sql", "gold_ft_investment_scenario"),
    "s_device": ("silver_sql", "tb_device"),
    "s_rate": ("silver_sql", "tb_region_rate"),
}

class SilverFrames(dict):
    """Load only the Silver sources requested by the current pipeline."""
    def __missing__(self, alias):
        if alias not in SOURCES:
            raise KeyError(f'Intermediário não preparado: {alias}')
        schema, name = SOURCES[alias]
        ref = target(schema, name)
        if not spark.catalog.tableExists(ref):
            raise ValueError(f'Fonte Silver ainda não importada: {ref}')
        frame = spark.table(ref)
        self[alias] = frame
        return frame

frames = SilverFrames()
for schema in ('gold_platform', 'gold_bi'):
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{schema}`')


def initialize_property_context():
    """Prepare registered devices and business parameters, without a manager allowlist."""
    CONFIGS = {
        "tb_property_config": "property_id INT, country STRING, time_zone STRING",
        "tb_meter_scope": "device_id STRING, property_id INT, valid_from DATE, valid_to DATE, included BOOLEAN, scope_version STRING",
        "tb_goal_config": "property_id INT, valid_from DATE, valid_to DATE, monthly_target_liters DOUBLE, goal_version STRING",
        "tb_baseline_config": "property_id INT, valid_from DATE, valid_to DATE, baseline_version STRING, method STRING",
        "tb_scenario_scope": "scenario_id INT, property_id INT, reference_month DATE, scenario_version STRING, annualization_constant BOOLEAN",
    }


    def parsed_date(value):
        return date.fromisoformat(value) if isinstance(value, str) else value


    # São DataFrames em memória, não tabelas de configuração em outro pipeline.
    # Vazia para manter o default BR/SP identificado como REPORT_DEFAULT_BR_SP.
    view("c_tb_property_config", spark.createDataFrame([], CONFIGS["tb_property_config"]))
    # Todos os vínculos SQL, inclusive de dispositivos atualmente inativos.
    # Um vínculo sem data/contexto fica desconhecido; sua telemetria não é descartada.
    scope = (frames["s_device"].alias("d")
        .join(frames["s_property"].select("property_id").alias("p"), "property_id", "inner")
        .filter(F.col("d.device_id").isNotNull() & F.col("d.installation_date").isNotNull())
        .select(F.col("d.device_id").alias("device_id"),
                F.col("d.property_id").alias("property_id"),
                F.col("d.installation_date").cast("date").alias("valid_from"),
                F.lit(None).cast("date").alias("valid_to"),
                F.lit(True).alias("included"),
                F.lit("SQL_CURRENT_MAPPING").alias("scope_version")))
    view("c_tb_meter_scope", scope)
    goals = [
        (property_id, parsed_date(start), parsed_date(end), float(liters), "NOTEBOOK_GOAL_V1")
        for property_id, start, end, liters in MONTHLY_GOALS
    ]
    view("c_tb_goal_config", spark.createDataFrame(goals, CONFIGS["tb_goal_config"]))
    c_tb_baseline_config_1 = frames["c_tb_meter_scope"]
    c_tb_baseline_config_1 = c_tb_baseline_config_1.groupBy(F.col("property_id").alias("_group_0")).agg(
        F.min(F.col("valid_from")).alias("_agg_0")
    )
    c_tb_baseline_config_1 = c_tb_baseline_config_1.select(
        F.col("_group_0").alias("property_id"),
        F.col("_agg_0").alias("valid_from"),
        F.lit(None).cast("DATE").alias("valid_to"),
        F.lit("PREVIOUS_3_COMPLETE_MONTHS_V1").alias("baseline_version"),
        F.lit("PREVIOUS_3_COMPLETE_MONTHS").alias("method"),
    )
    view("c_tb_baseline_config", c_tb_baseline_config_1)
    scenarios = [
        (scenario_id, property_id, parsed_date(month), "NOTEBOOK_SCENARIO_V1", annualize)
        for scenario_id, property_id, month, annualize in SCENARIO_SCOPE
    ]
    view("c_tb_scenario_scope", spark.createDataFrame(scenarios, CONFIGS["tb_scenario_scope"]))
    cut = F.lit(ASOF).cast("timestamp")
    # O consumidor deve respeitar a relação física entre hidrômetros e submedidores.
    validation_2_1 = frames["s_property"]
    validation_2_1 = validation_2_1.select("*")
    unique(validation_2_1, ["property_id"], "property_id duplicado no SQL.")
    validation_3_1 = frames["s_device"]
    validation_3_1 = validation_3_1.select("*")
    unique(validation_3_1, ["device_id"], "device_id aponta para mais de uma propriedade.")
    validation_4_1 = frames["c_tb_property_config"]
    validation_4_1 = validation_4_1.select("*")
    unique(validation_4_1, ["property_id"], "Configuração de fuso duplicada.")
    # Ausência de dimensão não elimina dispositivos da Gold; contexto fica NULL.
    validation_6_1 = frames["c_tb_property_config"]
    validation_6_1 = validation_6_1.filter((F.col("country").isNull() | F.col("time_zone").isNull()))
    validation_6_1 = validation_6_1.select("*")
    assert_no_rows(validation_6_1, "País/fuso inválidos.")
    validation_7_1 = frames["c_tb_property_config"]
    validation_7_1 = validation_7_1.filter(
        (~F.col("time_zone").isin(F.lit("America/Sao_Paulo"), F.lit("UTC")))
    )
    validation_7_1 = validation_7_1.select("*")
    assert_no_rows(
        validation_7_1,
        "Esta versão atende America/Sao_Paulo e UTC; valide outros fusos antes de ampliar o recorte.",
    )
    validation_8_1 = frames["c_tb_meter_scope"]
    validation_8_1 = validation_8_1.filter(
        ((F.col("property_id").isNull() | F.col("scope_version").isNull()) | F.col("included").isNull())
    )
    validation_8_1 = validation_8_1.select("*")
    assert_no_rows(validation_8_1, "Escopo sem propriedade, versão ou inclusão explícita.")
    for table in ("tb_meter_scope", "tb_goal_config", "tb_baseline_config"):
        entity = "device_id" if table == "tb_meter_scope" else "property_id"
        validation_96_1 = frames["c_" + table]
        validation_96_1 = validation_96_1.filter(
            (
                (F.col(entity).isNull() | F.col("valid_from").isNull())
                | (F.col("valid_to") < F.col("valid_from"))
            )
        )
        validation_96_1 = validation_96_1.select("*")
        assert_no_rows(validation_96_1, f"Vigência inválida: {table}")
        validation_97_subquery_2 = frames["c_" + table]
        validation_97_subquery_2 = validation_97_subquery_2.select(
            "*",
            F.max(F.coalesce(F.col("valid_to"), F.lit("9999-12-31").cast("DATE")))
            .over(
                Window.partitionBy(F.col(entity))
                .orderBy(F.col("valid_from").asc_nulls_first())
                .rowsBetween(Window.unboundedPreceding, -1)
            )
            .alias("previous_end"),
        )
        validation_97_1 = validation_97_subquery_2
        validation_97_1 = validation_97_1.filter((F.col("previous_end") >= F.col("valid_from")))
        validation_97_1 = validation_97_1.select("*")
        assert_no_rows(validation_97_1, f"Vigências sobrepostas: {table}")
    validation_9_1 = frames["c_tb_goal_config"]
    validation_9_1 = validation_9_1.filter(
        (
            ((F.col("monthly_target_liters") <= F.lit(0)) | F.col("monthly_target_liters").isNull())
            | F.col("goal_version").isNull()
        )
    )
    validation_9_1 = validation_9_1.select("*")
    assert_no_rows(validation_9_1, "Meta/versão inválida.")
    validation_10_1 = frames["c_tb_baseline_config"]
    validation_10_1 = validation_10_1.filter(
        (
            ((F.col("method") != F.lit("PREVIOUS_3_COMPLETE_MONTHS")) | F.col("method").isNull())
            | F.col("baseline_version").isNull()
        )
    )
    validation_10_1 = validation_10_1.select("*")
    assert_no_rows(validation_10_1, "Baseline exige método PREVIOUS_3_COMPLETE_MONTHS e versão.")
    tb_property_1 = frames["s_property"].alias("p")
    tb_property_1 = tb_property_1.join(
        frames["c_tb_property_config"].alias("c"),
        (F.col("p.property_id") == F.col("c.property_id")),
        "left",
    )
    tb_property_1 = tb_property_1.select(
        "p.*",
        F.upper(F.trim(F.coalesce(F.col("c.country"), F.lit("BR")))).alias("country"),
        F.coalesce(F.col("c.time_zone"), F.lit("America/Sao_Paulo")).alias("time_zone"),
        F.when(
            ((F.col("built_area_m2") > F.lit(0)) & (F.col("built_area_m2") < F.lit(100))),
            F.lit("0_100"),
        )
        .when(
            ((F.col("built_area_m2") >= F.lit(100)) & (F.col("built_area_m2") < F.lit(300))),
            F.lit("100_300"),
        )
        .when(
            ((F.col("built_area_m2") >= F.lit(300)) & (F.col("built_area_m2") < F.lit(1000))),
            F.lit("300_1000"),
        )
        .when((F.col("built_area_m2") >= F.lit(1000)), F.lit("1000_PLUS"))
        .otherwise(F.lit("UNKNOWN"))
        .alias("area_band"),
        F.when(F.col("c.property_id").isNull(), F.lit("REPORT_DEFAULT_BR_SP"))
        .otherwise(F.lit("CONFIGURED"))
        .alias("location_status"),
    )
    view("p_tb_property", tb_property_1)
    p_tb_device_property_1 = frames["s_device"].alias("d")
    p_tb_device_property_1 = p_tb_device_property_1.join(
        frames["p_tb_property"].alias("p"), (F.col("d.property_id") == F.col("p.property_id")), "inner"
    )
    p_tb_device_property_1 = p_tb_device_property_1.select(
        F.col("d.device_id").alias("device_id"),
        F.col("d.property_id").alias("property_id"),
        F.col("p.organization_id").alias("organization_id"),
        F.col("d.is_active").alias("is_active"),
        F.col("d.installation_date").alias("installation_date"),
        F.lit(ASOF).cast("TIMESTAMP").alias("captured_at"),
        F.lit(ASOF).cast("TIMESTAMP").alias("valid_from"),
        F.lit(None).cast("TIMESTAMP").alias("valid_to"),
        F.lit("CURRENT_SQL_SNAPSHOT").alias("mapping_status"),
    )
    view("p_tb_device_property", p_tb_device_property_1)
    v_scope_1 = frames["c_tb_meter_scope"].alias("c")
    v_scope_1 = v_scope_1.join(
        frames["p_tb_property"].alias("p"), (F.col("c.property_id") == F.col("p.property_id")), "inner"
    )
    v_scope_1 = v_scope_1.filter((F.col("c.included") == F.lit(True)))
    v_scope_1 = v_scope_1.select(
        "c.*",
        F.col("p.organization_id").alias("organization_id"),
        F.col("p.time_zone").alias("time_zone"),
        F.col("p.region_id").alias("region_id"),
        F.col("p.classification_id").alias("classification_id"),
        F.to_utc_timestamp(F.col("c.valid_from").cast("TIMESTAMP"), F.col("p.time_zone")).alias(
            "scope_start_utc"
        ),
        F.to_utc_timestamp(
            F.date_add(
                F.coalesce(F.col("c.valid_to"), F.lit("9999-12-30").cast("DATE")), F.lit(1)
            ).cast("TIMESTAMP"),
            F.col("p.time_zone"),
        ).alias("scope_end_utc"),
    )
    view("v_scope", v_scope_1)
    validation_18_1 = frames["v_scope"].alias("c")
    validation_18_1 = validation_18_1.join(
        frames["s_device"].alias("d"),
        (
            (F.col("c.device_id") == F.col("d.device_id"))
            & (F.col("c.property_id") == F.col("d.property_id"))
        ),
        "left_anti",
    )
    validation_18_1 = validation_18_1.select("c.*")
    assert_no_rows(
        validation_18_1,
        "Escopo diverge do vínculo SQL atual. Confirme o vínculo antes de usar o histórico.",
    )
