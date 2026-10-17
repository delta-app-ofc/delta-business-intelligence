# Databricks notebook source
"""BI v2: sete tabelas finais para os 13 KPIs; somente medidas e cruzamentos.

Importe este arquivo como notebook Databricks e execute após o importador v2.
batch_id é o lote transferido; todas as saídas usam um único cutoff_at UTC.
Os blocos são independentes: cada erro é registrado e o notebook falha ao final.
"""

from datetime import datetime, timezone
import json
import logging
import re

from delta.tables import DeltaTable
from pyspark.sql import functions as F, Window
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


TZ = "America/Sao_Paulo"


def require_columns(frame, names, label):
    missing = sorted(set(names) - set(frame.columns))
    if missing:
        raise ValueError(f"{label}: campos ausentes {missing}. Atualize a Silver/importação.")


def reject_rows(frame, message):
    if frame.limit(1).count():
        raise ValueError(message)


def require_unique(frame, keys, label):
    reject_rows(
        frame.groupBy(*keys).count().filter("count > 1"), f"{label}: chave duplicada {keys}"
    )


def finite(column):
    value = F.col(column).cast("double")
    return value.isNotNull() & ~F.isnan(value) & (F.abs(value) < F.lit(float("inf")))


def seconds(end, start):
    return (F.unix_micros(end) - F.unix_micros(start)) / F.lit(1000000.0)


def local_date(column):
    return F.to_date(F.from_utc_timestamp(column, TZ))


def canonical_text(column):
    # Mesmo valor para grafias com/sem acento, espaços repetidos e Unicode decomposto.
    text = F.upper(F.trim(F.regexp_replace(F.col(column), r"\s+", " ")))
    text = F.translate(text, "ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑ", "AAAAAEEEEIIIIOOOOOUUUUCN")
    return F.regexp_replace(text, r"\p{M}", "")


def canonical_region(column):
    return F.regexp_replace(canonical_text(column), r"[ _-]+", "_")


def require_values(frame, columns, label):
    require_columns(frame, columns, label)
    condition = F.lit(False)
    for column in columns:
        condition = condition | F.col(column).isNull() | (F.length(F.trim(F.col(column).cast("string"))) == 0)
    reject_rows(frame.filter(condition), f"{label}: valor obrigatório ausente/vazio em {columns}")


def region_dictionary(*frames):
    rows = None
    for frame in frames:
        values = frame.select("region_id", canonical_region("region_name").alias("region_name"))
        require_values(values, ["region_id", "region_name"], "Regiões SQL")
        rows = values if rows is None else rows.unionByName(values)
    rows = rows.distinct()
    require_unique(rows, ["region_id"], "Regiões SQL: um ID não pode ter nomes diferentes")
    require_unique(rows, ["region_name"], "Regiões SQL: um nome não pode identificar IDs diferentes")
    return rows


def properties_sp(frame, rates=None):
    require_columns(
        frame,
        [
            "property_id",
            "region_id",
            "region_name",
            "city",
            "state",
            "classification_group",
            "latitude",
            "longitude",
        ],
        "bi_property_context",
    )
    require_values(frame, ["property_id", "state"], "Contexto cadastral SQL")
    result = (
        frame.withColumn("city", canonical_text("city"))
        .withColumn("state", canonical_text("state"))
        .filter(F.col("state").isin("SP", "SAO PAULO"))
        .withColumn("state", F.lit("SP"))
        .withColumn("region_name", canonical_region("region_name"))
        .withColumn("classification_group", canonical_text("classification_group"))
        .select(
            "property_id",
            "region_id",
            "region_name",
            "city",
            "state",
            "classification_group",
            "latitude",
            "longitude",
        )
    )
    require_values(result, ["property_id", "region_id", "region_name", "city", "state", "classification_group"], "Imóveis SP")
    require_unique(result, ["property_id"], "Contexto SP")
    region_dictionary(result, *([rates] if rates is not None else []))
    # Endereço incompleto impede publicação: não esconder imóveis do mapa nem inventar pontos.
    valid_coordinates = (
        finite("latitude") & finite("longitude")
        & F.col("latitude").between(-90, 90) & F.col("longitude").between(-180, 180)
        & ~((F.col("latitude") == 0) & (F.col("longitude") == 0))
    )
    reject_rows(result.filter(~F.coalesce(valid_coordinates, F.lit(False))), "Imóveis SP: coordenadas ausentes/inválidas; corrija tb_address na origem.")
    return result.withColumn("latitude", F.col("latitude").cast("double")).withColumn("longitude", F.col("longitude").cast("double"))


def device_context(devices, properties):
    require_unique(devices, ["device_id"], "Cadastro de dispositivos")
    return devices.select("device_id", "property_id", "installation_date").join(
        properties, "property_id", "inner"
    )


def user_context(bridge, properties, cutoff):
    # Uma linha por usuário. Vários imóveis não multiplicam mensagens ou metas.
    rows = (
        bridge.filter(F.col("association_date") <= F.to_date(F.lit(cutoff)))
        .select("user_id", "property_id")
        .distinct()
        .join(properties, "property_id")
    )
    return rows.groupBy("user_id").agg(
        F.when(F.countDistinct("region_id") == 1, F.first("region_id")).alias("region_id"),
        F.when(F.countDistinct("region_id") == 1, F.first("region_name")).alias("region_name"),
    )


def consumption_parts(windows, devices, properties, cutoff):
    require_columns(
        windows,
        ["device_id", "user_id", "window_started_at", "window_finished_at", "consumption_liters"],
        "consumption_summary",
    )
    require_unique(
        windows, ["device_id", "window_started_at", "window_finished_at"], "Janelas de consumo"
    )
    context = device_context(devices, properties)
    rows = (
        windows.select(
            "device_id", "user_id", "window_started_at", "window_finished_at", "consumption_liters"
        )
        .join(context, "device_id")
    )
    require_values(rows, ["device_id", "window_started_at", "window_finished_at", "consumption_liters", "installation_date"], "Janelas de consumo SP")
    reject_rows(rows.filter(~finite("consumption_liters") | (F.col("consumption_liters") < 0)
                           | (F.col("window_finished_at") <= F.col("window_started_at"))
                           | (local_date(F.col("window_started_at")) < F.col("installation_date"))), "Janelas de consumo SP: volume/período/instalação inválidos.")
    rows = rows.filter(F.col("window_finished_at") <= F.lit(cutoff).cast("timestamp"))
    order = (
        Window.partitionBy("device_id")
        .orderBy("window_started_at", "window_finished_at")
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    rows = rows.withColumn("_previous_end", F.max("window_finished_at").over(order))
    reject_rows(
        rows.filter(F.col("_previous_end") > F.col("window_started_at")),
        "Janelas sobrepostas por dispositivo; corrija a Silver antes de somar litros.",
    )
    rows = rows.withColumn(
        "hour_utc",
        F.explode(
            F.sequence(
                F.date_trunc("hour", "window_started_at"),
                F.date_trunc(
                    "hour", F.col("window_finished_at") - F.expr("INTERVAL 1 MICROSECOND")
                ),
                F.expr("INTERVAL 1 HOUR"),
            )
        ),
    )
    rows = rows.withColumn("_start", F.greatest("window_started_at", "hour_utc"))
    rows = rows.withColumn(
        "_end", F.least("window_finished_at", F.col("hour_utc") + F.expr("INTERVAL 1 HOUR"))
    )
    rows = rows.withColumn("covered_seconds", seconds(F.col("_end"), F.col("_start")))
    return (
        rows.withColumn(
            "liters",
            F.col("consumption_liters")
            * F.col("covered_seconds")
            / seconds(F.col("window_finished_at"), F.col("window_started_at")),
        )
        .withColumn("local_date", local_date(F.col("hour_utc")))
        .withColumn("hour", F.hour(F.from_utc_timestamp("hour_utc", TZ)))
    )


def consumption_hourly(parts, devices):
    totals = parts.groupBy("property_id", "hour_utc", "local_date", "hour").agg(
        F.sum("liters").alias("consumption_liters"),
        F.sum("covered_seconds").alias("covered_seconds"),
    )
    expected = (
        totals.select("property_id", "hour_utc", "local_date")
        .alias("h")
        .join(
            devices.select("property_id", "device_id", "installation_date").alias("d"),
            "property_id",
        )
        .filter(F.col("d.installation_date") <= F.col("h.local_date"))
        .groupBy("property_id", "hour_utc")
        .agg((F.countDistinct("device_id") * F.lit(3600.0)).alias("expected_seconds"))
    )
    props = parts.select(
        "property_id",
        "region_id",
        "region_name",
        "city",
        "state",
        "classification_group",
        "latitude",
        "longitude",
    ).distinct()
    return totals.join(expected, ["property_id", "hour_utc"]).join(props, "property_id")


def weather_hourly(weather, properties, cutoff):
    require_columns(
        weather,
        ["city", "state", "country", "observed_at", "temperature_c", "feels_like_c"],
        "weather_hourly",
    )
    require_values(weather, ["city", "state", "country"], "Localização climática")
    regions = properties.groupBy("city", "state").agg(
        F.sort_array(F.collect_set("region_id")).alias("region_ids")
    )
    rows = (
        weather.withColumn("city", canonical_text("city"))
        .withColumn("state", canonical_text("state"))
        .filter(F.col("state").isin("SP", "SAO PAULO"))
        .filter(canonical_text("country").isin("BR", "BRASIL", "BRAZIL"))
        .withColumn("state", F.lit("SP"))
    )
    require_values(rows, ["city", "observed_at", "temperature_c", "feels_like_c"], "Clima SP")
    reject_rows(rows.filter(~(finite("temperature_c") & finite("feels_like_c"))), "Clima SP: temperatura/sensação não finita.")
    rows = (rows.filter(F.col("observed_at") <= F.lit(cutoff).cast("timestamp"))
            .withColumn("hour_utc", F.date_trunc("hour", "observed_at"))
            .select("city", "state", "hour_utc", "temperature_c", "feels_like_c").distinct())
    # Grafias equivalentes com medidas idênticas são a mesma observação.
    # Medidas conflitantes na mesma hora precisam ser corrigidas na origem.
    require_unique(rows, ["city", "state", "hour_utc"], "Hora climática")
    return rows.join(regions, ["city", "state"], "left").select(
        "city",
        "state",
        F.coalesce("region_ids", F.array().cast("array<int>")).alias("region_ids"),
        "hour_utc",
        local_date(F.col("hour_utc")).alias("local_date"),
        F.col("temperature_c").cast("double"),
        F.col("feels_like_c").cast("double"),
    )


def alert_count_daily(alerts, devices, properties, cutoff):
    require_unique(alerts, ["alert_id"], "Alertas")
    rows = (
        alerts.filter(
            F.col("triggered_at").isNotNull()
            & (F.col("triggered_at") <= F.lit(cutoff).cast("timestamp"))
        )
        .join(
            device_context(devices, properties).select(
                "device_id", "property_id", "region_id", "region_name"
            ),
            "device_id",
        )
        .withColumn("local_date", local_date(F.col("triggered_at")))
    )
    return rows.groupBy(
        "local_date", "device_id", "property_id", "region_id", "region_name", "alert_type"
    ).agg(F.countDistinct("alert_id").alias("alert_count"))


def agent_calls(messages, sessions, cutoff):
    require_columns(
        messages,
        [
            "session_id",
            "message_index",
            "role",
            "content_type",
            "sent_at",
            "api_status_code",
            "agent_name",
            "response_time_ms",
        ],
        "chat_messages",
    )
    require_unique(messages, ["session_id", "message_index"], "Mensagens")
    require_unique(sessions, ["session_id"], "Sessões")
    bot_messages = messages.filter((F.col("role") == "bot") & (F.col("content_type") == "text"))
    require_values(bot_messages, ["session_id", "message_index", "sent_at"], "Identificação das chamadas IA")
    reject_rows(bot_messages.join(sessions.select("session_id"), "session_id", "left_anti"), "Chamadas IA: sessão inexistente na Silver.")
    context = sessions.filter(~F.coalesce(F.col("is_deleted"), F.lit(False))).select("session_id")
    rows = (
        messages.filter(
            (F.col("role") == "bot")
            & (F.col("content_type") == "text")
        )
        .join(context, "session_id")
        .withColumn("_agent", F.lower(F.trim("agent_name")))
    )
    require_values(rows, ["sent_at", "agent_name", "api_status_code"], "Chamadas IA: instrumentação do backend")
    rows = rows.filter(F.col("sent_at") <= F.lit(cutoff).cast("timestamp"))
    reject_rows(rows.filter(~F.col("api_status_code").between(100, 599)), "Chamadas IA: código HTTP inválido.")
    agent_kind = F.regexp_replace(F.col("_agent"), r"[\s_-]+", "")
    specialist = (
        F.col("_agent").isNotNull()
        & (F.length("_agent") > 0)
        & ~agent_kind.isin(
            "judge",
            "juiz",
            "router",
            "roteador",
            "judgeagent",
            "routeragent",
            "juizagent",
            "roteadoragent",
        )
    )
    # Falhas podem não ter duração; chamadas de sucesso de especialistas precisam dela.
    valid_duration = finite("response_time_ms") & (F.col("response_time_ms") >= 0)
    reject_rows(rows.filter(specialist & (F.col("api_status_code") < 400) & ~F.coalesce(valid_duration, F.lit(False))), "Chamadas IA: especialista com sucesso sem duração válida do backend.")
    reject_rows(rows.filter(F.col("response_time_ms").isNotNull() & ~F.coalesce(valid_duration, F.lit(False))), "Chamadas IA: duração inválida.")
    return rows.select(
        "session_id",
        "message_index",
        F.col("sent_at").alias("occurred_at"),
        F.when(agent_kind.isin("judge", "judgeagent", "juiz", "juizagent"), F.lit("judge"))
        .when(agent_kind.isin("router", "routeragent", "roteador", "roteadoragent"), F.lit("router"))
        .otherwise(F.regexp_replace(agent_kind, "agent$", "")).alias("agent_name"),
        F.when(
            specialist & finite("response_time_ms") & (F.col("response_time_ms") >= 0),
            F.col("response_time_ms").cast("double"),
        ).alias("response_time_ms"),
        "api_status_code",
    )


def tariff_validity(rates):
    require_columns(
        rates,
        [
            "region_id",
            "region_name",
            "classification_id",
            "classification_group",
            "initial_validity",
            "final_validity",
            "m3_value",
        ],
        "bi_tariff_context",
    )
    rows = (rates.withColumn("classification_group", canonical_text("classification_group"))
            .withColumn("region_name", canonical_region("region_name"))
            .filter(F.col("classification_group").isin("RESIDENCIAL", "COMERCIAL")))
    require_values(rows, ["region_id", "region_name", "classification_id", "initial_validity", "m3_value"], "Tarifas")
    region_dictionary(rows)
    reject_rows(rows.filter(~(finite("m3_value") & (F.col("m3_value") > 0))), "Tarifas: preço inválido.")
    require_unique(rows, ["region_id", "classification_id", "initial_validity"], "Tarifas")
    order = (
        Window.partitionBy("region_id", "classification_id")
        .orderBy("initial_validity")
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    rows = rows.withColumn(
        "_previous_end",
        F.max(F.coalesce("final_validity", F.lit("9999-12-31").cast("date"))).over(order),
    )
    reject_rows(
        rows.filter(
            (F.col("final_validity") < F.col("initial_validity"))
            | (F.col("_previous_end") >= F.col("initial_validity"))
        ),
        "Tarifas com vigências inválidas/sobrepostas.",
    )
    return rows.select(
        "region_id",
        "region_name",
        "classification_id",
        "classification_group",
        "initial_validity",
        "final_validity",
        F.col("m3_value").cast("double").alias("tariff_per_m3"),
    )


def target_compliance_daily(parts, preferences, devices, bridge, keys):
    require_unique(preferences, ["user_id"], "Metas atuais")
    daily = (
        parts.filter(F.col("user_id").isNotNull())
        .groupBy("user_id", "local_date")
        .agg(
            F.sum("liters").alias("consumption_liters"),
            F.sum("covered_seconds").alias("covered_seconds"),
        )
    )
    expected = (
        daily.select("user_id", "local_date")
        .join(bridge.select("user_id", "property_id", "association_date"), "user_id")
        .filter(F.col("association_date") <= F.col("local_date"))
        .join(devices.select("property_id", "device_id", "installation_date"), "property_id")
        .filter(F.col("installation_date") <= F.col("local_date"))
        .groupBy("user_id", "local_date")
        .agg((F.countDistinct("device_id") * F.lit(86400.0)).alias("expected_seconds"))
    )
    rows = (
        daily.join(expected, ["user_id", "local_date"], "left")
        .join(preferences.select("user_id", "daily_liters_target"), "user_id", "left")
        .join(keys, "user_id")
    )
    eligible = (
        (F.col("expected_seconds") > 0)
        & (F.abs(F.col("covered_seconds") - F.col("expected_seconds")) < 0.001)
        & finite("daily_liters_target")
        & (F.col("daily_liters_target") > 0)
    )
    # Dias incompletos/sem meta não são fatos de cumprimento nem zeros medidos.
    excluded = rows.filter(~F.coalesce(eligible, F.lit(False))).count()
    print(f"[QUALIDADE] Metas: {excluded} usuário-dias excluídos por cobertura/meta insuficiente.", flush=True)
    return rows.filter(eligible).select(
        "user_key",
        "local_date",
        "consumption_liters",
        "daily_liters_target",
        (F.col("consumption_liters") <= F.col("daily_liters_target")).cast("int").alias("days_within_target"),
        F.lit(1).alias("eligible_days"),
    )


def device_availability_daily(status, devices, properties, cutoff):
    require_columns(
        status,
        [
            "device_id",
            "availability_started_at",
            "availability_updated_at",
            "available_seconds",
            "unavailabilities",
        ],
        "device_status",
    )
    require_unique(status, ["device_id"], "Status")
    rows = status.join(
        device_context(devices, properties).select(
            "device_id", "property_id", "region_id", "region_name"
        ),
        "device_id",
    )
    require_values(rows, ["availability_started_at", "availability_updated_at", "available_seconds", "unavailabilities"], "Disponibilidade SP: histórico obrigatório")
    reject_rows(
        rows.filter(
            (F.col("availability_updated_at") < F.col("availability_started_at"))
            | (F.col("availability_updated_at") > F.lit(cutoff).cast("timestamp"))
            | (F.col("available_seconds") < 0)
        ),
        "Período/contador de disponibilidade inválido ou posterior ao corte.",
    )
    rows = rows.filter(F.col("availability_updated_at") > F.col("availability_started_at"))
    events = (
        rows.select(
            "device_id",
            "availability_started_at",
            "availability_updated_at",
            F.explode("unavailabilities").alias("event"),
        )
        .select(
            "device_id",
            "availability_started_at",
            "availability_updated_at",
            F.col("event.started_at").alias("started_at"),
            F.col("event.ended_at").alias("ended_at"),
            F.col("event.unavailable_seconds").alias("reported_seconds"),
        )
        .withColumn("end_at", F.coalesce("ended_at", "availability_updated_at"))
    )
    order = (
        Window.partitionBy("device_id")
        .orderBy("started_at", "end_at")
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    events = events.withColumn("_previous_end", F.max("end_at").over(order))
    reject_rows(
        events.filter(
            F.col("started_at").isNull()
            | F.col("reported_seconds").isNull()
            | (F.col("reported_seconds") < 0)
            | (F.col("started_at") < F.col("availability_started_at"))
            | (F.col("end_at") > F.col("availability_updated_at"))
            | (F.col("end_at") < F.col("started_at"))
            | (F.col("_previous_end") > F.col("started_at"))
            | (F.abs(F.col("reported_seconds") - seconds(F.col("end_at"), F.col("started_at"))) > 1)
        ),
        "Indisponibilidades inválidas, sobrepostas ou com duração incoerente.",
    )
    reject_rows(
        events.filter(F.col("ended_at").isNull()).groupBy("device_id").count().filter("count > 1"),
        "Mais de uma indisponibilidade aberta.",
    )
    sums = events.groupBy("device_id").agg(F.sum("reported_seconds").alias("_offline"))
    check = rows.join(sums, "device_id", "left")
    reject_rows(
        check.filter(
            F.abs(
                F.col("available_seconds")
                + F.coalesce("_offline", F.lit(0))
                - seconds(F.col("availability_updated_at"), F.col("availability_started_at"))
            )
            > 1
        ),
        "Disponibilidade não cobre todo o período; não preencher lacunas como online.",
    )
    days = (
        rows.withColumn(
            "local_date",
            F.explode(
                F.sequence(
                    local_date(F.col("availability_started_at")),
                    local_date(F.col("availability_updated_at") - F.expr("INTERVAL 1 MICROSECOND")),
                )
            ),
        )
        .withColumn("_day_start", F.to_utc_timestamp(F.col("local_date").cast("timestamp"), TZ))
        .withColumn(
            "_day_end", F.to_utc_timestamp(F.date_add("local_date", 1).cast("timestamp"), TZ)
        )
        .withColumn("_start", F.greatest("availability_started_at", "_day_start"))
        .withColumn("_end", F.least("availability_updated_at", "_day_end"))
    )
    overlap = (
        days.select("device_id", "local_date", "_start", "_end")
        .join(events.select("device_id", "started_at", "end_at"), "device_id", "left")
        .withColumn(
            "_seconds",
            F.when(
                F.col("started_at").isNotNull(),
                F.greatest(
                    F.lit(0.0),
                    seconds(F.least("_end", "end_at"), F.greatest("_start", "started_at")),
                ),
            ).otherwise(0.0),
        )
        .groupBy("device_id", "local_date")
        .agg(F.sum("_seconds").alias("unavailable_seconds"))
    )
    result = days.join(overlap, ["device_id", "local_date"]).withColumn(
        "expected_seconds", seconds(F.col("_end"), F.col("_start"))
    )
    return result.select(
        "device_id",
        "property_id",
        "region_id",
        "region_name",
        "local_date",
        "expected_seconds",
        "unavailable_seconds",
        (F.col("expected_seconds") - F.col("unavailable_seconds")).alias("available_seconds"),
    )


def validate_bi(name, frame):
    """Contrato estrito de publicação. Somente ausências com significado são permitidas."""
    contracts = {
        "tb_weather_hourly": ("city state region_ids hour_utc local_date temperature_c feels_like_c", "city state hour_utc"),
        "tb_consumption_hourly": ("property_id hour_utc local_date hour consumption_liters covered_seconds expected_seconds region_id region_name city state classification_group latitude longitude", "property_id hour_utc"),
        "tb_alert_count_daily": ("local_date device_id property_id region_id region_name alert_type alert_count", "local_date device_id alert_type"),
        "tb_agent_call": ("session_id message_index occurred_at agent_name response_time_ms api_status_code", "session_id message_index"),
        "tb_tariff_validity": ("region_id region_name classification_id classification_group initial_validity final_validity tariff_per_m3", "region_id classification_id initial_validity"),
        "tb_target_compliance_daily": ("user_key local_date consumption_liters daily_liters_target days_within_target eligible_days", "user_key local_date"),
        "tb_device_availability_daily": ("device_id property_id region_id region_name local_date expected_seconds unavailable_seconds available_seconds", "device_id local_date"),
    }
    columns, keys = (part.split() for part in contracts[name])
    if set(frame.columns) != set(columns):
        raise ValueError(f"{name}: colunas fora do contrato: {sorted(set(frame.columns) ^ set(columns))}")
    nullable = {"final_validity", "response_time_ms"}
    require_values(frame, [column for column in columns if column not in nullable], name)
    require_unique(frame, keys, name)
    nonnegative = set(columns) & {"consumption_liters", "covered_seconds", "expected_seconds", "unavailable_seconds", "available_seconds", "alert_count", "tariff_per_m3", "daily_liters_target"}
    for column in nonnegative:
        reject_rows(frame.filter(~finite(column) | (F.col(column) < 0)), f"{name}: medida inválida em {column}")
    if "region_id" in columns:
        region_dictionary(frame)
    if name == "tb_consumption_hourly":
        reject_rows(frame.filter((F.col("expected_seconds") <= 0) | (F.col("covered_seconds") > F.col("expected_seconds") + 0.001)), "Consumo: cobertura maior que o esperado/inválida.")
    if name == "tb_device_availability_daily":
        reject_rows(frame.filter((F.col("expected_seconds") <= 0) | (F.abs(F.col("available_seconds") + F.col("unavailable_seconds") - F.col("expected_seconds")) > 0.001)), "Disponibilidade: denominador/contadores inválidos.")
    if name == "tb_target_compliance_daily":
        reject_rows(frame.filter(~F.col("days_within_target").isin(0, 1) | (F.col("eligible_days") != 1)), "Metas: somente dias elegíveis podem ser publicados.")
    metrics = [F.count("*").alias("rows")]
    if "local_date" in columns:
        metrics += [F.min("local_date").alias("min_date"), F.max("local_date").alias("max_date"), F.countDistinct("local_date").alias("days")]
    if name == "tb_weather_hourly":
        metrics += [F.count_if(F.size("region_ids") == 0).alias("hours_without_property_region")]
    if name == "tb_agent_call":
        metrics += [F.count_if(F.col("response_time_ms").isNull()).alias("calls_without_specialist_duration")]
    summary = frame.agg(*metrics).first().asDict()
    summary = {key: value.isoformat() if hasattr(value, "isoformat") else value for key, value in summary.items()}
    summary["state"] = "EMPTY" if summary["rows"] == 0 else "VALID"
    if name == "tb_weather_hourly" and summary["days"] < 2:
        summary["warning"] = "Menos de dois dias de clima: não há série diária suficiente para tendência. Verifique hours[] na origem."
    print(f"[QUALIDADE] {name}: {summary}", flush=True)
    return summary


# COMMAND ----------
# Parâmetros e acesso às Silvers gerenciadas importadas pelo transfer v2.
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("batch_id", "")
dbutils.widgets.text("cutoff_at", "")
dbutils.widgets.text(
    "metrics_context", "Demonstração: latência e disponibilidade contêm dados mockados na origem"
)

BI_CATALOG = dbutils.widgets.get("catalog").strip()
BI_BATCH = dbutils.widgets.get("batch_id").strip()
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", BI_CATALOG):
    raise ValueError("Catálogo inválido.")
if not re.fullmatch(r"[A-Za-z0-9_-]{1,96}", BI_BATCH):
    raise ValueError("Informe em batch_id o lote importado pelo transfer v2.")

started_at = datetime.now(timezone.utc)
cutoff_text = dbutils.widgets.get("cutoff_at").strip() or started_at.isoformat()
cutoff = datetime.fromisoformat(cutoff_text.replace("Z", "+00:00"))
if cutoff.tzinfo is None:
    cutoff = cutoff.replace(tzinfo=timezone.utc)
BI_CUTOFF = cutoff.astimezone(timezone.utc).isoformat()
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{BI_CATALOG}`.`gold_bi`")

logger = logging.getLogger("gold_bi_v2")
completed_tables = []
failed_tables = []
prepared_tables = {}
quality_metrics = {}

BI_SOURCES = {
    "consumption": ("silver_telemetry", "consumption_summary"),
    "status": ("silver_telemetry", "device_status"),
    "weather": ("silver_app", "weather_hourly"),
    "sessions": ("silver_app", "chat_sessions"),
    "messages": ("silver_app", "chat_messages"),
    "alerts": ("silver_app", "alerts_history"),
    "preferences": ("silver_app", "user_preferences"),
    "devices": ("silver_sql", "tb_device"),
    "properties": ("silver_sql", "bi_property_context"),
    "rates": ("silver_sql", "bi_tariff_context"),
    "bridge": ("silver_sql", "tb_user_property"),
}
BI_KEYS = {
    "tb_weather_hourly": ["city", "state", "hour_utc"],
    "tb_consumption_hourly": ["property_id", "hour_utc"],
    "tb_alert_count_daily": ["local_date", "device_id", "alert_type"],
    "tb_agent_call": ["session_id", "message_index"],
    "tb_tariff_validity": ["region_id", "classification_id", "initial_validity"],
    "tb_target_compliance_daily": ["user_key", "local_date"],
    "tb_device_availability_daily": ["device_id", "local_date"],
}
_bi_frames = {}
_source_versions = {}


def bi_target(schema, name):
    return f"`{BI_CATALOG}`.`{schema}`.`{name}`"


def bi_read(name):
    if name not in _bi_frames:
        schema, table = BI_SOURCES[name]
        source = bi_target(schema, table)
        # OPTIMIZE/compactação pode vir depois da importação sem userMetadata.
        # O último commit identificado pelo importador fixa o snapshot do lote.
        commit = (
            DeltaTable.forName(spark, source)
            .history()
            .filter(F.get_json_object("userMetadata", "$.source_batch_id").isNotNull())
            .orderBy(F.col("version").desc())
            .select("version", "userMetadata")
            .first()
        )
        if commit is None:
            raise ValueError(f"{source}: nenhum lote identificado pelo transfer v2.")
        metadata = json.loads(commit["userMetadata"] or "{}")
        if metadata.get("source_batch_id") != BI_BATCH or metadata.get("manifest_version") != 4:
            raise ValueError(
                f"{source}: a Silver não pertence ao lote v2 {BI_BATCH}. Execute o transfer v2 completo."
            )
        version = int(commit["version"])
        # Fixar a versão evita misturar uma atualização posterior da mesma tabela.
        _bi_frames[name] = spark.read.format("delta").option("versionAsOf", version).table(source)
        _source_versions[source] = version
    return _bi_frames[name]


def bi_properties():
    return properties_sp(bi_read("properties"), bi_read("rates"))


def bi_parts():
    return consumption_parts(bi_read("consumption"), bi_read("devices"), bi_properties(), BI_CUTOFF)


def bi_users():
    return user_context(bi_read("bridge"), bi_properties(), BI_CUTOFF)


def bi_user_keys(users):
    # Reutiliza o vínculo privado de UUIDs; não cria schema de publicação.
    ref = bi_target("gold_bi", "tb_user_key")
    if not spark.catalog.tableExists(ref):
        schema = StructType(
            [
                StructField("user_id", IntegerType()),
                StructField("user_key", StringType()),
            ]
        )
        spark.createDataFrame([], schema).write.format("delta").saveAsTable(ref)

    users = users.select("user_id").filter(F.col("user_id").isNotNull()).distinct()
    (
        DeltaTable.forName(spark, ref)
        .alias("target")
        .merge(users.alias("source"), "target.user_id = source.user_id")
        .whenNotMatchedInsert(
            values={"user_id": F.col("source.user_id"), "user_key": F.expr("uuid()")}
        )
        .execute()
    )
    keys = spark.table(ref)
    require_unique(keys, ["user_id"], "Vínculo técnico de usuários")
    require_unique(keys, ["user_key"], "UUID de usuários")
    return keys


def save_bi(name, frame):
    quality = quality_metrics.get(name) or validate_bi(name, frame)
    # As tabelas finais recebem somente as colunas selecionadas nas transformações.
    # Identificação do lote e contexto ficam no histórico nativo Delta.
    metadata = json.dumps(
        {
            "source_batch_id": BI_BATCH,
            "cutoff_at": BI_CUTOFF,
            "generated_at": started_at.isoformat(),
            "source_versions": dict(_source_versions),
            "data_context": dbutils.widgets.get("metrics_context"),
            "quality_contract_version": 3,
            "quality": quality,
        },
        ensure_ascii=False,
    )
    (
        frame.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("userMetadata", metadata)
        .saveAsTable(bi_target("gold_bi", name))
    )
    print(f"[SUCESSO] gold_bi.{name}; lote={BI_BATCH}; corte={BI_CUTOFF}", flush=True)


def prepare_bi(name, frame):
    quality_metrics[name] = validate_bi(name, frame)
    prepared_tables[name] = frame


# COMMAND ----------
# =================================
# tb_weather_hourly — BI01 e BI06
# =================================

table_name = "tb_weather_hourly"
stage = "leitura e cálculo"

try:
    print(f"\n[INÍCIO] gold_bi.{table_name}", flush=True)
    gold = weather_hourly(bi_read("weather"), bi_properties(), BI_CUTOFF)

    stage = "validação e gravação Delta"
    prepare_bi(table_name, gold)
    completed_tables.append(table_name)

except Exception:
    failed_tables.append(table_name)
    logger.exception(
        "[ERRO] Falha em gold_bi.%s na etapa '%s'. O processamento das demais tabelas continuará.",
        table_name,
        stage,
    )


# COMMAND ----------
# =================================
# tb_consumption_hourly — BI02, BI06, BI07 e BI08
# =================================

table_name = "tb_consumption_hourly"
stage = "leitura e cálculo"

try:
    print(f"\n[INÍCIO] gold_bi.{table_name}", flush=True)
    gold = consumption_hourly(bi_parts(), bi_read("devices"))

    stage = "validação e gravação Delta"
    prepare_bi(table_name, gold)
    completed_tables.append(table_name)

except Exception:
    failed_tables.append(table_name)
    logger.exception(
        "[ERRO] Falha em gold_bi.%s na etapa '%s'. O processamento das demais tabelas continuará.",
        table_name,
        stage,
    )


# COMMAND ----------
# =================================
# tb_alert_count_daily — BI03 e BI04
# =================================

table_name = "tb_alert_count_daily"
stage = "leitura e cálculo"

try:
    print(f"\n[INÍCIO] gold_bi.{table_name}", flush=True)
    gold = alert_count_daily(bi_read("alerts"), bi_read("devices"), bi_properties(), BI_CUTOFF)

    stage = "validação e gravação Delta"
    prepare_bi(table_name, gold)
    completed_tables.append(table_name)

except Exception:
    failed_tables.append(table_name)
    logger.exception(
        "[ERRO] Falha em gold_bi.%s na etapa '%s'. O processamento das demais tabelas continuará.",
        table_name,
        stage,
    )


# COMMAND ----------
# =================================
# tb_agent_call — BI05 e BI09; sem avaliação do juiz
# =================================

table_name = "tb_agent_call"
stage = "leitura e cálculo"

try:
    print(f"\n[INÍCIO] gold_bi.{table_name}", flush=True)
    gold = agent_calls(
        bi_read("messages"),
        bi_read("sessions"),
        BI_CUTOFF,
    )

    stage = "validação e gravação Delta"
    prepare_bi(table_name, gold)
    completed_tables.append(table_name)

except Exception:
    failed_tables.append(table_name)
    logger.exception(
        "[ERRO] Falha em gold_bi.%s na etapa '%s'. O processamento das demais tabelas continuará.",
        table_name,
        stage,
    )


# COMMAND ----------
# =================================
# tb_tariff_validity — BI10 e BI11
# =================================

table_name = "tb_tariff_validity"
stage = "leitura e cálculo"

try:
    print(f"\n[INÍCIO] gold_bi.{table_name}", flush=True)
    bi_properties()  # Confere também o dicionário SQL compartilhado por tarifas/imóveis.
    gold = tariff_validity(bi_read("rates"))

    stage = "validação e gravação Delta"
    prepare_bi(table_name, gold)
    completed_tables.append(table_name)

except Exception:
    failed_tables.append(table_name)
    logger.exception(
        "[ERRO] Falha em gold_bi.%s na etapa '%s'. O processamento das demais tabelas continuará.",
        table_name,
        stage,
    )


# COMMAND ----------
# =================================
# tb_target_compliance_daily — BI12: percentual de dias dentro da meta diária atual
# =================================

table_name = "tb_target_compliance_daily"
stage = "leitura e cálculo"

try:
    print(f"\n[INÍCIO] gold_bi.{table_name}", flush=True)
    gold = target_compliance_daily(
        bi_parts(),
        bi_read("preferences"),
        bi_read("devices"),
        bi_read("bridge"),
        bi_user_keys(bi_read("consumption")),
    )

    stage = "validação e gravação Delta"
    prepare_bi(table_name, gold)
    completed_tables.append(table_name)

except Exception:
    failed_tables.append(table_name)
    logger.exception(
        "[ERRO] Falha em gold_bi.%s na etapa '%s'. O processamento das demais tabelas continuará.",
        table_name,
        stage,
    )


# COMMAND ----------
# =================================
# tb_device_availability_daily — BI13
# =================================

table_name = "tb_device_availability_daily"
stage = "leitura e cálculo"

try:
    print(f"\n[INÍCIO] gold_bi.{table_name}", flush=True)
    gold = device_availability_daily(
        bi_read("status"), bi_read("devices"), bi_properties(), BI_CUTOFF
    )

    stage = "validação e gravação Delta"
    prepare_bi(table_name, gold)
    completed_tables.append(table_name)

except Exception:
    failed_tables.append(table_name)
    logger.exception(
        "[ERRO] Falha em gold_bi.%s na etapa '%s'. O processamento das demais tabelas continuará.",
        table_name,
        stage,
    )


# COMMAND ----------
print(f"\n[RESUMO] Preparadas: {len(completed_tables)}; falhas: {len(failed_tables)}.", flush=True)
if failed_tables:
    raise RuntimeError(
        "Gold BI v2 falhou após tentar todas as tabelas. Tabelas com erro: "
        + ", ".join(failed_tables)
    )

for table_name, gold in prepared_tables.items():
    save_bi(table_name, gold)

print(f"Gold BI v2 concluída: 13 KPIs, sete tabelas; lote={BI_BATCH}.", flush=True)
