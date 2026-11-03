# Databricks notebook source
"""BI v3: oito tabelas finais para BI01-BI18; somente medidas e cruzamentos.

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
    text = F.upper(F.trim(F.regexp_replace(F.col(column), r"[\s\p{Z}]+", " ")))
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
    """Contexto SP pronto para BI; registros inválidos são descartados, não fatais.

    A limpeza primária ocorre no caminho Silver SQL original, sem caminhos
    adicionais. Esta barreira protege contra cargas antigas não tratadas.
    """
    require_columns(
        frame,
        [
            "property_id", "region_id", "region_name", "city", "state",
            "classification_group", "latitude", "longitude",
        ],
        "bi_property_context",
    )
    result = (
        frame.withColumn("city", canonical_text("city"))
        .withColumn("state", canonical_text("state"))
        .filter(F.col("state").isin("SP", "SAO PAULO"))
        .withColumn("state", F.lit("SP"))
        .withColumn("region_name", canonical_region("region_name"))
        .withColumn("classification_group", canonical_text("classification_group"))
        .withColumn(
            "latitude",
            F.expr("try_cast(replace(trim(cast(latitude AS string)), ',', '.') AS double)"),
        )
        .withColumn(
            "longitude",
            F.expr("try_cast(replace(trim(cast(longitude AS string)), ',', '.') AS double)"),
        )
        .select(
            "property_id", "region_id", "region_name", "city", "state",
            "classification_group", "latitude", "longitude",
        )
    )

    required_ok = F.lit(True)
    for name in ["property_id", "region_id", "region_name", "city", "state", "classification_group"]:
        required_ok = required_ok & F.col(name).isNotNull() & (
            F.length(F.trim(F.col(name).cast("string"))) > 0
        )
    valid_coordinates = (
        finite("latitude") & finite("longitude")
        & F.col("latitude").between(-90, 90)
        & F.col("longitude").between(-180, 180)
        & ~((F.col("latitude") == 0) & (F.col("longitude") == 0))
    )
    quality_ok = F.coalesce(
        required_ok
        & F.col("classification_group").isin("RESIDENCIAL", "COMERCIAL")
        & valid_coordinates,
        F.lit(False),
    )
    rejected = result.filter(~quality_ok)
    rejected_count = rejected.count()
    if rejected_count:
        print(
            f"[DESCARTE QUALIDADE] Imóveis SP: {rejected_count} registros "
            "inválidos ignorados na Gold (a Silver deve filtrá-los).",
            flush=True,
        )
    result = result.filter(quality_ok).distinct()
    # Duplicidades conflitantes continuam sendo erros estruturais; não criar
    # associação arbitrária entre dispositivo, imóvel e região.
    require_unique(result, ["property_id"], "Contexto SP")
    region_dictionary(result, *([rates] if rates is not None else []))
    return result


def device_context(devices, properties):
    require_values(devices, ["device_id", "property_id", "installation_date"], "Cadastro de dispositivos")
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
    require_values(windows, ["device_id", "user_id", "window_started_at", "window_finished_at", "consumption_liters"], "Janelas de consumo")
    reject_rows(windows.select("device_id").join(devices.select("device_id"), "device_id", "left_anti"), "Consumo: dispositivo sem cadastro SQL.")
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
    require_values(alerts, ["alert_id", "device_id", "alert_type", "triggered_at"], "Alertas")
    require_unique(alerts, ["alert_id"], "Alertas")
    reject_rows(alerts.select("device_id").join(devices.select("device_id"), "device_id", "left_anti"), "Alertas: dispositivo sem cadastro SQL.")
    alerts = alerts.withColumn("alert_type", F.lower(F.regexp_replace(canonical_text("alert_type"), "[ -]+", "_")))
    reject_rows(alerts.filter(~F.col("alert_type").isin("vazamento_continuo", "fluxo_atipico", "dispositivo_offline", "leitura_impossivel")), "Alertas: tipo fora do contrato Mongo.")
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


def ai_requests(observations, cutoff):
    require_columns(
        observations,
        ["request_id", "created_at", "mode", "outcome", "resolved", "total_ms", "agents_ms"],
        "observability_requests",
    )
    rows = observations.filter(F.col("mode").isin("chat", "judge"))
    require_values(rows, ["request_id", "created_at", "outcome", "resolved", "total_ms"], "Requisicoes IA")
    rows = rows.filter(F.col("created_at") <= F.lit(cutoff).cast("timestamp"))
    require_unique(rows, ["request_id"], "Requisicoes IA")
    reject_rows(rows.filter(~F.col("outcome").isin("success", "partial", "rejected", "error")), "Requisicoes IA: outcome desconhecido.")
    reject_rows(rows.filter(~finite("total_ms") | (F.col("total_ms") < 0)), "Requisicoes IA: duracao invalida.")
    return rows.select(
        "request_id", F.col("created_at").alias("occurred_at"),
        "outcome", "resolved", "total_ms",
    )


def agent_calls(observations, cutoff):
    requests = ai_requests(observations, cutoff)
    rows = (
        observations.join(requests.select("request_id", "occurred_at"), "request_id")
        .select("request_id", "occurred_at", F.explode("agents_ms").alias("agent_name", "response_time_ms"))
        .withColumn("agent_name", F.lower(F.trim("agent_name")))
    )
    require_values(rows, ["request_id", "occurred_at", "agent_name", "response_time_ms"], "Participacoes IA")
    reject_rows(rows.filter(~finite("response_time_ms") | (F.col("response_time_ms") < 0)), "Participacoes IA: duracao invalida.")
    require_unique(rows, ["request_id", "agent_name"], "Participacoes IA")
    # Uma linha por agente/requisicao. Nao representa cada chamada de modelo.
    # Mapa vazio/ausente nao inventa um agente e nao elimina o fato da requisicao.
    return rows.select("request_id", "occurred_at", "agent_name", "response_time_ms")


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
    require_values(rates, ["region_id", "region_name", "classification_id", "classification_group", "initial_validity", "m3_value"], "Tarifas SQL")
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
    require_values(preferences, ["user_id"], "Metas atuais")
    require_unique(preferences, ["user_id"], "Metas atuais")
    # Meta ausente é desconhecida; meta preenchida inválida é erro na origem.
    reject_rows(preferences.filter(F.col("daily_liters_target").isNotNull()
                                  & (~finite("daily_liters_target") | (F.col("daily_liters_target") <= 0))), "Metas atuais: valor preenchido inválido; corrija user_preferences.")
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
    require_values(status, ["device_id"], "Status")
    reject_rows(status.select("device_id").join(devices.select("device_id"), "device_id", "left_anti"), "Status: dispositivo sem cadastro SQL.")
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
            | ~finite("available_seconds")
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
            | ~finite("reported_seconds")
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


GOLD_DESCRIPTIONS = {
    "tb_weather_hourly": {
        "table": "Clima horário por cidade no estado de São Paulo, com temperaturas observada e aparente, para análises temporais e associação com consumo. Uma linha por cidade, estado e instante UTC.",
        "columns": {
            "city": "Cidade em grafia canônica, sem acentos e em maiúsculas.",
            "state": "Sigla da unidade federativa; nesta Gold, SP.",
            "region_ids": "IDs SQL das regiões associadas aos imóveis da cidade. Lista vazia significa que não há imóvel vinculado no contexto do lote.",
            "hour_utc": "Instante de referência da observação climática em UTC, truncado para a hora.",
            "local_date": "Data do instante hour_utc convertida para o fuso America/Sao_Paulo.",
            "temperature_c": "Temperatura do ar observada na hora, em graus Celsius.",
            "feels_like_c": "Sensação térmica observada na hora, em graus Celsius.",
        },
    },
    "tb_consumption_hourly": {
        "table": "Consumo e cobertura de telemetria por imóvel e hora local, enriquecidos com clima, endereço e classe. Uma linha por imóvel e hora completa elegível; consumo em litros.",
        "columns": {
            "property_id": "Identificador SQL do imóvel, usado como unidade espacial do fato.",
            "hour_utc": "Início da hora de consumo em UTC.",
            "local_date": "Data local da hora no fuso America/Sao_Paulo.",
            "hour": "Hora do dia local, de 0 a 23.",
            "consumption_liters": "Consumo observado e alocado ao intervalo, em litros.",
            "covered_seconds": "Segundos da hora cobertos por janelas de consumo válidas.",
            "expected_seconds": "Segundos esperados de cobertura para a hora conforme a instalação e o período elegível do dispositivo.",
            "region_id": "Identificador SQL da região cadastral do imóvel.",
            "region_name": "Nome canônico da região cadastral, em maiúsculas e com palavras separadas por sublinhado.",
            "city": "Cidade do endereço em grafia canônica, sem acentos e em maiúsculas.",
            "state": "Sigla da unidade federativa do endereço; nesta Gold, SP.",
            "classification_group": "Grupo tarifário do imóvel, por exemplo RESIDENCIAL ou COMERCIAL.",
            "latitude": "Latitude cadastrada para o endereço do imóvel, em graus decimais.",
            "longitude": "Longitude cadastrada para o endereço do imóvel, em graus decimais.",
        },
    },
    "tb_alert_count_daily": {
        "table": "Contagem diária de alertas registrados por dispositivo, tipo e imóvel. Uma linha por data local, dispositivo e tipo de alerta.",
        "columns": {
            "local_date": "Data local de abertura do alerta no fuso America/Sao_Paulo.",
            "device_id": "Identificador SQL do dispositivo associado ao alerta.",
            "property_id": "Identificador SQL do imóvel associado ao dispositivo.",
            "region_id": "Identificador SQL da região cadastral do imóvel.",
            "region_name": "Nome canônico da região cadastral, em maiúsculas e com palavras separadas por sublinhado.",
            "alert_type": "Tipo de alerta normalizado em minúsculas e com palavras separadas por sublinhado.",
            "alert_count": "Quantidade de alertas distintos registrados na data para o dispositivo e tipo.",
        },
    },
    "tb_agent_call": {
        "table": "Participacoes de agentes por requisicao dos modos chat e judge. Uma linha por request_id e agente; duracao acumulada em agents_ms. Nao informa sucesso/falha individual do agente.",
        "columns": {
            "request_id": "Identificador da requisicao; vinculo com tb_ai_request.",
            "occurred_at": "Timestamp UTC created_at da requisicao.",
            "agent_name": "Chave do agente no mapa agents_ms.",
            "response_time_ms": "Tempo acumulado do agente nesta requisicao, em milissegundos."
        }
    },
    "tb_ai_request": {
        "table": "Requisicoes dos modos chat e judge registradas em observability_requests. Uma linha por request_id; outcome descreve a requisicao inteira.",
        "columns": {
            "request_id": "Identificador unico da requisicao.",
            "occurred_at": "Timestamp UTC created_at da origem.",
            "outcome": "Resultado global: success, partial, rejected ou error.",
            "resolved": "Indicador de resolucao informado pela origem.",
            "total_ms": "Duracao total da requisicao em milissegundos."
        }
    },
    "tb_tariff_validity": {
        "table": "Tarifas de compra de água por região e classe, preservando a vigência cadastral. Uma linha por região, classificação e início de vigência.",
        "columns": {
            "region_id": "Identificador SQL da região tarifária.",
            "region_name": "Nome canônico da região, em maiúsculas e com palavras separadas por sublinhado.",
            "classification_id": "Identificador SQL da classificação tarifária.",
            "classification_group": "Grupo de consumo da classificação, como RESIDENCIAL ou COMERCIAL.",
            "initial_validity": "Data de início da vigência da tarifa.",
            "final_validity": "Data final da vigência. Nula representa vigência sem data final cadastrada.",
            "tariff_per_m3": "Valor cadastrado da tarifa de compra por metro cúbico, na moeda definida pela origem SQL.",
        },
    },
    "tb_target_compliance_daily": {
        "table": "Cumprimento da meta diária individual por usuário e data elegível. Publica somente dias completos com meta positiva e não atribui região ao usuário.",
        "columns": {
            "user_key": "Chave pseudonimizada estável do usuário, sem expor o identificador cadastral.",
            "local_date": "Data local do consumo no fuso America/Sao_Paulo.",
            "consumption_liters": "Consumo total do usuário no dia elegível, em litros.",
            "daily_liters_target": "Meta diária de consumo atualmente cadastrada para o usuário, em litros.",
            "days_within_target": "Indicador binário: 1 quando o consumo diário atende à meta; 0 quando não atende.",
            "eligible_days": "Quantidade de dias elegíveis representados nesta linha; sempre 1.",
        },
    },
    "tb_device_availability_daily": {
        "table": "Disponibilidade observada por dispositivo e dia local. O denominador cobre somente o período elegível monitorado; períodos desconhecidos não são presumidos como online.",
        "columns": {
            "device_id": "Identificador SQL do dispositivo monitorado.",
            "property_id": "Identificador SQL do imóvel associado ao dispositivo.",
            "region_id": "Identificador SQL da região cadastral do imóvel.",
            "region_name": "Nome canônico da região cadastral, em maiúsculas e com palavras separadas por sublinhado.",
            "local_date": "Data local do período monitorado no fuso America/Sao_Paulo.",
            "expected_seconds": "Segundos do período elegível monitorado, usados como denominador da disponibilidade.",
            "unavailable_seconds": "Segundos do período elegível em que o dispositivo foi observado indisponível.",
            "available_seconds": "Segundos do período elegível em que o dispositivo foi observado disponível; soma com unavailable_seconds igual a expected_seconds.",
        },
    },
}


def validate_bi(name, frame):
    """Contrato estrito de publicação. Somente ausências com significado são permitidas."""
    contracts = {
        "tb_weather_hourly": ("city state region_ids hour_utc local_date temperature_c feels_like_c", "city state hour_utc"),
        "tb_consumption_hourly": ("property_id hour_utc local_date hour consumption_liters covered_seconds expected_seconds region_id region_name city state classification_group latitude longitude", "property_id hour_utc"),
        "tb_alert_count_daily": ("local_date device_id property_id region_id region_name alert_type alert_count", "local_date device_id alert_type"),
        "tb_agent_call": ("request_id occurred_at agent_name response_time_ms", "request_id agent_name"),
        "tb_ai_request": ("request_id occurred_at outcome resolved total_ms", "request_id"),
        "tb_tariff_validity": ("region_id region_name classification_id classification_group initial_validity final_validity tariff_per_m3", "region_id classification_id initial_validity"),
        "tb_target_compliance_daily": ("user_key local_date consumption_liters daily_liters_target days_within_target eligible_days", "user_key local_date"),
        "tb_device_availability_daily": ("device_id property_id region_id region_name local_date expected_seconds unavailable_seconds available_seconds", "device_id local_date"),
    }
    columns, keys = (part.split() for part in contracts[name])
    documented_columns = set(GOLD_DESCRIPTIONS[name]["columns"])
    if documented_columns != set(columns):
        raise ValueError(
            f"{name}: descrições do catálogo não correspondem ao contrato: "
            f"{sorted(set(columns) ^ documented_columns)}"
        )
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
    if "city" in columns:
        reject_rows(frame.filter(F.col("city") != canonical_text("city")), f"{name}: cidade fora da grafia canônica.")
        reject_rows(frame.filter(F.col("state") != "SP"), f"{name}: estado fora do recorte SP.")
    if name in ("tb_agent_call", "tb_ai_request"):
        measure = "response_time_ms" if name == "tb_agent_call" else "total_ms"
        require_values(frame, [measure], name)
        reject_rows(frame.filter(~finite(measure) | (F.col(measure) < 0)), f"{name}: duracao invalida.")
    if name == "tb_ai_request":
        reject_rows(frame.filter(~F.col("outcome").isin("success", "partial", "rejected", "error")), "Requisicoes IA: outcome desconhecido.")
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
# Parâmetros e acesso às Silvers gerenciadas importadas pelo transfer v3.
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("batch_id", "")
dbutils.widgets.text("cutoff_at", "")
dbutils.widgets.text(
    "metrics_context", "IA: observability_requests; demais fontes conforme o lote importado"
)

BI_CATALOG = dbutils.widgets.get("catalog").strip()
BI_BATCH = dbutils.widgets.get("batch_id").strip()
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", BI_CATALOG):
    raise ValueError("Catálogo inválido.")
if not re.fullmatch(r"[A-Za-z0-9_-]{1,96}", BI_BATCH):
    raise ValueError("Informe em batch_id o lote importado pelo transfer v3.")

started_at = datetime.now(timezone.utc)
cutoff_text = dbutils.widgets.get("cutoff_at").strip() or started_at.isoformat()
cutoff = datetime.fromisoformat(cutoff_text.replace("Z", "+00:00"))
if cutoff.tzinfo is None:
    cutoff = cutoff.replace(tzinfo=timezone.utc)
BI_CUTOFF = cutoff.astimezone(timezone.utc).isoformat()
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{BI_CATALOG}`.`gold_bi`")

logger = logging.getLogger("gold_bi_v3")
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
    "observations": ("silver_app", "observability_requests"),
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
    "tb_agent_call": ["request_id", "agent_name"],
    "tb_ai_request": ["request_id"],
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
            raise ValueError(f"{source}: nenhum lote identificado pelo transfer v3.")
        metadata = json.loads(commit["userMetadata"] or "{}")
        if metadata.get("source_batch_id") != BI_BATCH or metadata.get("manifest_version") != 5:
            raise ValueError(
                f"{source}: a Silver não pertence ao lote v3 {BI_BATCH}. Execute o transfer v3 completo."
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
            "quality_contract_version": 4,
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
    table = bi_target("gold_bi", name)
    table_comment = GOLD_DESCRIPTIONS[name]["table"].replace("'", "''")
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('comment' = '{table_comment}')")
    for column, description in GOLD_DESCRIPTIONS[name]["columns"].items():
        column_comment = description.replace("'", "''")
        spark.sql(
            f"ALTER TABLE {table} ALTER COLUMN `{column}` COMMENT '{column_comment}'"
        )
    print(f"[SUCESSO] gold_bi.{name}; lote={BI_BATCH}; corte={BI_CUTOFF}", flush=True)


def prepare_bi(name, frame):
    quality_metrics[name] = validate_bi(name, frame)
    prepared_tables[name] = frame


# COMMAND ----------
# Compatibilidade relacional: descarta apenas registros sem vinculos SQL.
def _id_sql(frame):
    """Converte user_id de STRING para LONG so no cruzamento SQL da Gold."""
    return frame.withColumn(
        "user_id",
        F.expr("try_cast(trim(cast(user_id AS string)) AS bigint)"),
    )


def _somente_relacionados(frame, referencia, chaves, descricao):
    ref = referencia.select(*chaves).distinct()
    orfaos = frame.join(ref, chaves, "left_anti")
    total = orfaos.count()
    if total:
        print(f"[DESCARTE RELACIONAL] {descricao}: {total} registros ignorados.", flush=True)
        orfaos.select(*chaves).distinct().show(20, truncate=False)
    return frame.join(ref, chaves, "left_semi")


def _dispositivos_sql(devices, properties):
    return device_context(devices, properties).select("device_id", "property_id")


def _pares_dispositivo_usuario(devices, properties):
    contexto = _dispositivos_sql(devices, properties)
    vinculos = (
        bi_read("bridge")
        .select("property_id", F.col("user_id").cast("bigint").alias("user_id"))
        .distinct()
    )
    return (
        contexto.join(vinculos, "property_id", "inner")
        .select("device_id", "user_id")
        .distinct()
    )


_consumption_parts_original = consumption_parts


def consumption_parts(windows, devices, properties, cutoff):
    windows = _somente_relacionados(
        windows, _dispositivos_sql(devices, properties), ["device_id"],
        "Consumo sem dispositivo SQL",
    )
    windows = _somente_relacionados(
        _id_sql(windows), _pares_dispositivo_usuario(devices, properties),
        ["device_id", "user_id"], "Consumo sem vinculo usuario/dispositivo SQL",
    )
    # Descartar janelas invalidas antes das validacoes do calculo original.
    # Nao corrige volumes/datas nem presume consumo zero.
    checked = windows.join(
        devices.select(
            "device_id", F.col("installation_date").alias("_installation_date_sql")
        ),
        "device_id", "inner",
    )
    valid_window = F.coalesce(
        finite("consumption_liters")
        & (F.col("consumption_liters") >= 0)
        & (F.col("window_finished_at") > F.col("window_started_at"))
        & (local_date(F.col("window_started_at")) >= F.col("_installation_date_sql")),
        F.lit(False),
    )
    invalid = checked.filter(~valid_window)
    invalid_count = invalid.count()
    if invalid_count:
        print(
            f"[DESCARTE QUALIDADE] Consumo: {invalid_count} janelas com "
            "volume/periodo/instalacao invalidos ignoradas.", flush=True,
        )
        invalid.select(
            "device_id", "user_id", "consumption_liters", "window_started_at",
            "window_finished_at", "_installation_date_sql",
        ).show(20, truncate=False)
    windows = checked.filter(valid_window).drop("_installation_date_sql")
    return _consumption_parts_original(windows, devices, properties, cutoff)


_alert_count_daily_original = alert_count_daily


def alert_count_daily(alerts, devices, properties, cutoff):
    alerts = _somente_relacionados(
        alerts, _dispositivos_sql(devices, properties), ["device_id"],
        "Alerta sem dispositivo SQL",
    )
    return _alert_count_daily_original(alerts, devices, properties, cutoff)


_device_availability_daily_original = device_availability_daily


def device_availability_daily(status, devices, properties, cutoff):
    status = _somente_relacionados(
        status, _dispositivos_sql(devices, properties), ["device_id"],
        "Status sem dispositivo SQL",
    )
    return _device_availability_daily_original(status, devices, properties, cutoff)


_bi_user_keys_original = bi_user_keys


def bi_user_keys(users):
    # Chave técnica só para usuários com vínculo SQL comprovado.
    # Preserva a regra da Silver: user_id é texto até este JOIN.
    related = _pares_dispositivo_usuario(bi_read("devices"), bi_properties())
    users = _somente_relacionados(
        _id_sql(users), related, ["device_id", "user_id"],
        "Usuário sem relação com dispositivo/imóvel SQL",
    )
    return _bi_user_keys_original(users.select("user_id").distinct())


_target_compliance_daily_original = target_compliance_daily


def target_compliance_daily(parts, preferences, devices, bridge, keys):
    users_sql = bridge.select(F.col("user_id").cast("bigint").alias("user_id")).distinct()
    preferences = _somente_relacionados(
        _id_sql(preferences), users_sql, ["user_id"],
        "Preferência sem usuário SQL",
    )
    return _target_compliance_daily_original(parts, preferences, devices, bridge, keys)


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
# tb_ai_request: contagem global sem multiplicacao por agente.
table_name = "tb_ai_request"
stage = "leitura e calculo"
try:
    print(f"\n[INÍCIO] gold_bi.{table_name}", flush=True)
    gold = ai_requests(bi_read("observations"), BI_CUTOFF)
    prepare_bi(table_name, gold)
    completed_tables.append(table_name)
except Exception:
    failed_tables.append(table_name)
    logger.exception("[ERRO] gold_bi.%s; etapa=%s", table_name, stage)


# COMMAND ----------
# =================================
# tb_agent_call — BI05 e BI09; sem avaliação do juiz
# =================================

table_name = "tb_agent_call"
stage = "leitura e cálculo"

try:
    print(f"\n[INÍCIO] gold_bi.{table_name}", flush=True)
    gold = agent_calls(bi_read("observations"), BI_CUTOFF)

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
        "Gold BI v3 falhou após tentar todas as tabelas. Tabelas com erro: "
        + ", ".join(failed_tables)
    )

for table_name, gold in prepared_tables.items():
    save_bi(table_name, gold)

print(f"Gold BI v3 concluída: oito tabelas para BI01-BI18; lote={BI_BATCH}.", flush=True)
