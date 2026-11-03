-- BI01-BI18: SQL da versão salva inspecionada em 18/10/2026.
-- Datas inclusivas. Espaçamento normalizado; hashes do catálogo referem o SQL original.

-- BI01 — Temperatura e sensação térmica
-- Dataset: ds_bi01_weather
SELECT
  local_date,
  city,
  ROUND(AVG(temperature_c), 1) AS avg_temperature_c,
  ROUND(AVG(feels_like_c), 1) AS avg_feels_like_c
FROM
  tb_weather_hourly
WHERE
  (
    :date_range.min IS NULL
    OR local_date >= :date_range.min
  )
  AND (
    :date_range.max IS NULL
    OR local_date <= :date_range.max
  )
  AND (
    :city IS NULL
    OR array_contains(:city, city)
  )
GROUP BY
  local_date,
  city
ORDER BY
  local_date;

-- BI02 — Clima × consumo por imóvel/hora
-- Dataset: ds_bi06_climate_consumption
SELECT
  c.property_id,
  c.hour_utc,
  c.local_date,
  c.consumption_liters,
  c.city,
  w.temperature_c,
  w.feels_like_c
FROM
  tb_consumption_hourly c
    JOIN tb_weather_hourly w
      ON c.city = w.city
      AND c.hour_utc = w.hour_utc
WHERE
  (
    :date_range.min IS NULL
    OR c.local_date >= :date_range.min
  )
  AND (
    :date_range.max IS NULL
    OR c.local_date <= :date_range.max
  )
  AND (
    :region IS NULL
    OR array_contains(:region, c.region_name)
  )
  AND (
    :city IS NULL
    OR array_contains(:city, c.city)
  );

-- BI03 — Consumo por imóvel no período
-- Dataset: ds_bi02_consumption
SELECT
    property_id,
    region_id,
    region_name,
    CAST(latitude AS DOUBLE) AS latitude,
    CAST(longitude AS DOUBLE) AS longitude,
    SUM(consumption_liters) AS consumption_liters,
    COUNT(DISTINCT hour_utc) AS hours_with_data,
    100.0 * SUM(covered_seconds)
        / NULLIF(SUM(expected_seconds), 0) AS recorded_hours_coverage_pct
FROM workspace.gold_bi.tb_consumption_hourly
WHERE
    (
        :date_range.min IS NULL
        OR local_date >= :date_range.min
    )
    AND (
        :date_range.max IS NULL
        OR local_date <= :date_range.max
    )
    AND (
        :region IS NULL
        OR array_contains(:region, region_name)
    )
    AND latitude IS NOT NULL
    AND longitude IS NOT NULL
GROUP BY
    property_id,
    region_id,
    region_name,
    latitude,
    longitude;

-- BI04 — Consumo médio por hora do dia
-- Dataset: ds_bi08_consumption_region
SELECT
  HOUR(hour_utc) AS hour_of_day,
  ROUND(AVG(consumption_liters), 1) AS avg_consumption_liters
FROM
  tb_consumption_hourly
WHERE
  (
    :date_range.min IS NULL
    OR local_date >= :date_range.min
  )
  AND (
    :date_range.max IS NULL
    OR local_date <= :date_range.max
  )
  AND (
    :region IS NULL
    OR array_contains(:region, region_name)
  )
GROUP BY
  HOUR(hour_utc)
ORDER BY
  hour_of_day;

-- BI05 — Alertas por tipo
-- Dataset: ds_bi03_alerts_type
SELECT
  alert_type,
  SUM(alert_count) AS total_alerts
FROM
  tb_alert_count_daily
WHERE
  (
    :date_range.min IS NULL
    OR local_date >= :date_range.min
  )
  AND (
    :date_range.max IS NULL
    OR local_date <= :date_range.max
  )
  AND (
    :region IS NULL
    OR array_contains(:region, region_name)
  )
GROUP BY
  alert_type
ORDER BY
  total_alerts DESC;

-- BI06 — Tendência de alertas
-- Dataset: ds_bi04_alerts_trend
SELECT
  local_date,
  alert_type,
  SUM(alert_count) AS daily_alerts
FROM
  tb_alert_count_daily
WHERE
  (
    :date_range.min IS NULL
    OR local_date >= :date_range.min
  )
  AND (
    :date_range.max IS NULL
    OR local_date <= :date_range.max
  )
  AND (
    :region IS NULL
    OR array_contains(:region, region_name)
  )
GROUP BY
  local_date,
  alert_type
ORDER BY
  local_date;

-- BI07 — Disponibilidade de dispositivos
-- Dataset: ds_bi13_availability
SELECT
  local_date,
  region_name,
  ROUND(SUM(available_seconds) * 100.0 / SUM(expected_seconds), 1) AS availability_pct
FROM
  tb_device_availability_daily
WHERE
  (
    :date_range.min IS NULL
    OR local_date >= :date_range.min
  )
  AND (
    :date_range.max IS NULL
    OR local_date <= :date_range.max
  )
  AND (
    :region IS NULL
    OR array_contains(:region, region_name)
  )
GROUP BY
  local_date,
  region_name
ORDER BY
  local_date;

-- BI08 — Mediana de disponibilidade
-- Dataset: 1bed6bb1
WITH per_device AS (
  SELECT
    device_id, SUM(available_seconds)
      / NULLIF(SUM(expected_seconds), 0) AS availability_pct
  FROM
    gold_bi.tb_device_availability_daily
  WHERE
    (
      :date_range.min IS NULL
      OR local_date >= :date_range.min
    )
    AND (
      :date_range.max IS NULL
      OR local_date <= :date_range.max
    )
  GROUP BY
    device_id
  HAVING
    SUM(expected_seconds) > 0
)
SELECT
  PERCENTILE_CONT(0.5) WITHIN GROUP (
    ORDER BY availability_pct
  ) AS median_device_availability_pct
FROM
  per_device;

-- BI09 — Consumo por grupo tarifário
-- Dataset: ds_bi07_consumption_group
SELECT
  classification_group,
  ROUND(SUM(consumption_liters), 1) AS total_liters
FROM
  tb_consumption_hourly
WHERE
  (
    :date_range.min IS NULL
    OR local_date >= :date_range.min
  )
  AND (
    :date_range.max IS NULL
    OR local_date <= :date_range.max
  )
  AND (
    :region IS NULL
    OR array_contains(:region, region_name)
  )
GROUP BY
  classification_group
ORDER BY
  total_liters DESC;

-- BI10 — Tarifas vigentes por região
-- Dataset: ds_bi11_tariff_current
SELECT
  region_name,
  classification_group,
  tariff_per_m3
FROM
  tb_tariff_validity
WHERE
  (
    final_validity IS NULL
    OR final_validity >= CURRENT_DATE()
  )
  AND (
    :region IS NULL
    OR array_contains(:region, region_name)
  )
ORDER BY
  region_name,
  classification_group;

-- BI11 — Consumo médio e meta diária
-- Dataset: 998e5adf
SELECT
    local_date,
    AVG(consumption_liters) AS avg_consumption_liters,
    AVG(daily_liters_target) AS avg_daily_target_liters
FROM workspace.gold_bi.tb_target_compliance_daily
WHERE (
    :date_range.min IS NULL
    OR local_date >= :date_range.min
)
AND (
    :date_range.max IS NULL
    OR local_date <= :date_range.max
)
GROUP BY local_date
ORDER BY local_date;

-- BI12 — Cumprimento da meta diária
-- Dataset: ds_bi12_compliance
SELECT
  ROUND(SUM(days_within_target) / SUM(eligible_days), 1) AS compliance_pct,
  SUM(eligible_days) AS total_days,
  SUM(days_within_target) AS days_in_target
FROM
  tb_target_compliance_daily
WHERE
  (
    :date_range.min IS NULL
    OR local_date >= :date_range.min
  )
  AND (
    :date_range.max IS NULL
    OR local_date <= :date_range.max
  );

-- BI13 — Total de litros excedentes à meta
-- Dataset: 7cd74f95
SELECT
    SUM(
        GREATEST(consumption_liters - daily_liters_target, 0.0)
    ) AS excess_consumption_liters
FROM workspace.gold_bi.tb_target_compliance_daily
WHERE (
    :date_range.min IS NULL
    OR local_date >= :date_range.min
)
AND (
    :date_range.max IS NULL
    OR local_date <= :date_range.max
);

-- BI14: resultados das requisicoes (4 categorias; atualizar cores/legenda).
SELECT outcome AS call_status, COUNT(*) AS call_count
FROM workspace.gold_bi.tb_ai_request r
WHERE (:date_range.min IS NULL OR TO_DATE(FROM_UTC_TIMESTAMP(r.occurred_at, 'America/Sao_Paulo')) >= :date_range.min)
  AND (:date_range.max IS NULL OR TO_DATE(FROM_UTC_TIMESTAMP(r.occurred_at, 'America/Sao_Paulo')) <= :date_range.max)
GROUP BY outcome;

-- BI15: participacoes de especialistas acima de 5 segundos; fracao 0-1.
SELECT COUNT_IF(response_time_ms > 5000) / NULLIF(COUNT(*), 0) AS slow_response_pct,
       COUNT(*) AS measured_responses
FROM workspace.gold_bi.tb_agent_call a
WHERE (:date_range.min IS NULL OR TO_DATE(FROM_UTC_TIMESTAMP(a.occurred_at, 'America/Sao_Paulo')) >= :date_range.min)
  AND (:date_range.max IS NULL OR TO_DATE(FROM_UTC_TIMESTAMP(a.occurred_at, 'America/Sao_Paulo')) <= :date_range.max);

-- BI16: distribuicao do tempo acumulado por agente/requisicao, em ms.
SELECT agent_name, response_time_ms
FROM workspace.gold_bi.tb_agent_call a
WHERE (:date_range.min IS NULL OR TO_DATE(FROM_UTC_TIMESTAMP(a.occurred_at, 'America/Sao_Paulo')) >= :date_range.min)
  AND (:date_range.max IS NULL OR TO_DATE(FROM_UTC_TIMESTAMP(a.occurred_at, 'America/Sao_Paulo')) <= :date_range.max);

-- BI17: requisicoes com erro por agente participante, nao falha do agente.
-- partial/rejected permanecem no denominador e nao sao convertidos em error.
SELECT a.agent_name,
       100.0 * COUNT_IF(r.outcome = 'error') / NULLIF(COUNT(*), 0) AS failure_rate_pct,
       COUNT(*) AS recorded_responses
FROM workspace.gold_bi.tb_agent_call a
JOIN workspace.gold_bi.tb_ai_request r ON r.request_id = a.request_id
WHERE (:date_range.min IS NULL OR TO_DATE(FROM_UTC_TIMESTAMP(a.occurred_at, 'America/Sao_Paulo')) >= :date_range.min)
  AND (:date_range.max IS NULL OR TO_DATE(FROM_UTC_TIMESTAMP(a.occurred_at, 'America/Sao_Paulo')) <= :date_range.max)
GROUP BY a.agent_name
ORDER BY failure_rate_pct DESC;

-- BI18: P90 da duracao acumulada do agente por requisicao; segundos.
SELECT TO_DATE(FROM_UTC_TIMESTAMP(a.occurred_at, 'America/Sao_Paulo')) AS local_date,
       a.agent_name,
       PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY a.response_time_ms) / 1000.0 AS p90_response_seconds,
       COUNT(*) AS measured_responses
FROM workspace.gold_bi.tb_agent_call a
WHERE (:date_range.min IS NULL OR TO_DATE(FROM_UTC_TIMESTAMP(a.occurred_at, 'America/Sao_Paulo')) >= :date_range.min)
  AND (:date_range.max IS NULL OR TO_DATE(FROM_UTC_TIMESTAMP(a.occurred_at, 'America/Sao_Paulo')) <= :date_range.max)
GROUP BY TO_DATE(FROM_UTC_TIMESTAMP(a.occurred_at, 'America/Sao_Paulo')), a.agent_name
ORDER BY local_date, agent_name;
