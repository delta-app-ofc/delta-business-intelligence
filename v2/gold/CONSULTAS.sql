-- BI v2: cada SELECT é um dataset separado do dashboard.
-- start_date inclusiva; end_date exclusiva. region_id = 'ALL' ou um ID cadastrado.
-- reference_date é a data de vigência das tarifas; temperature_band_c > 0.
-- Os nomes completos usam workspace; substitua se o seu catálogo for outro.

-- Cada dataset usa somente CTEs próprias e tabelas gold_bi.
-- IA (BI05/BI09) e meta do usuário (BI12) usam somente período: sem região inferida.

-- BI01: duas linhas, sobre as mesmas observações pareadas.
SELECT local_date, avg(temperature_c) AS temperature_c, avg(feels_like_c) AS feels_like_c
FROM workspace.gold_bi.tb_weather_hourly
WHERE local_date >= cast(:start_date AS DATE) AND local_date < cast(:end_date AS DATE)
  AND (:region_id='ALL' OR array_contains(region_ids,try_cast(:region_id AS INT)))
GROUP BY local_date ORDER BY local_date;

-- BI02: mapa. Coordenadas são do imóvel, nunca do ponto de consulta do clima.
-- O consumo é o volume observado; exibir a cobertura no tooltip.
SELECT property_id, region_id, region_name, latitude, longitude,
       sum(consumption_liters) AS consumption_liters,
       count(DISTINCT hour_utc) AS hours_with_data,
       100.0*sum(covered_seconds)/nullif(sum(expected_seconds),0) AS recorded_hours_coverage_pct
FROM workspace.gold_bi.tb_consumption_hourly
WHERE local_date >= cast(:start_date AS DATE) AND local_date < cast(:end_date AS DATE)
  AND (:region_id='ALL' OR region_id=try_cast(:region_id AS INT))
  AND latitude IS NOT NULL AND longitude IS NOT NULL
GROUP BY property_id,region_id,region_name,latitude,longitude;

-- BI03: alertas registrados por dispositivo (data da abertura).
SELECT device_id, sum(alert_count) AS alert_count FROM workspace.gold_bi.tb_alert_count_daily
WHERE local_date >= cast(:start_date AS DATE) AND local_date < cast(:end_date AS DATE)
  AND (:region_id='ALL' OR region_id=try_cast(:region_id AS INT))
GROUP BY device_id ORDER BY alert_count DESC;

-- BI04: mesmos alertas, agrupados pelo tipo real do banco.
SELECT alert_type, sum(alert_count) AS alert_count FROM workspace.gold_bi.tb_alert_count_daily
WHERE local_date >= cast(:start_date AS DATE) AND local_date < cast(:end_date AS DATE)
  AND (:region_id='ALL' OR region_id=try_cast(:region_id AS INT))
GROUP BY alert_type ORDER BY alert_count DESC;

-- BI05: somente falhas técnicas registradas; nenhuma avaliação do juiz nesta versão.
SELECT to_date(from_utc_timestamp(occurred_at,'America/Sao_Paulo')) AS local_date,
       count_if(api_status_code BETWEEN 400 AND 599) AS error_count,
       count(*) AS recorded_responses
FROM workspace.gold_bi.tb_agent_call
WHERE to_date(from_utc_timestamp(occurred_at,'America/Sao_Paulo')) >= cast(:start_date AS DATE)
  AND to_date(from_utc_timestamp(occurred_at,'America/Sao_Paulo')) < cast(:end_date AS DATE)
GROUP BY ALL ORDER BY local_date;

-- BI06: scatter. Uma observação por imóvel/hora completa, não por leitura.
WITH bi_complete_hour AS (
  SELECT * FROM workspace.gold_bi.tb_consumption_hourly
  WHERE expected_seconds > 0 AND abs(covered_seconds-expected_seconds) < 0.001
), paired AS (
  SELECT c.property_id,c.hour_utc,c.consumption_liters,w.temperature_c
  FROM bi_complete_hour c JOIN workspace.gold_bi.tb_weather_hourly w
    ON c.city=w.city AND c.state=w.state AND c.hour_utc=w.hour_utc
  WHERE c.local_date >= cast(:start_date AS DATE) AND c.local_date < cast(:end_date AS DATE)
    AND (:region_id='ALL' OR c.region_id=try_cast(:region_id AS INT))
), bands AS (
  SELECT *, floor(temperature_c/nullif(try_cast(:temperature_band_c AS DOUBLE),0))
                    *try_cast(:temperature_band_c AS DOUBLE) AS band_start_c
  FROM paired WHERE try_cast(:temperature_band_c AS DOUBLE)>0
)
SELECT band_start_c,band_start_c+try_cast(:temperature_band_c AS DOUBLE)/2 AS band_center_c,
       avg(consumption_liters) AS mean_liters_per_property_hour,count(*) AS property_hours
FROM bands GROUP BY band_start_c ORDER BY band_start_c;

-- BI07: linhas residencial/comercial, média por imóvel/dia completo.
WITH bi_complete_hour AS (
  SELECT * FROM workspace.gold_bi.tb_consumption_hourly
  WHERE expected_seconds > 0 AND abs(covered_seconds-expected_seconds) < 0.001
),
bi_complete_day AS (
  SELECT property_id, classification_group, region_id, region_name, local_date,
         sum(consumption_liters) AS consumption_liters
  FROM bi_complete_hour
  GROUP BY property_id, classification_group, region_id, region_name, local_date
  HAVING count(*) = timestampdiff(HOUR,
    to_utc_timestamp(cast(local_date AS TIMESTAMP),'America/Sao_Paulo'),
    to_utc_timestamp(cast(date_add(local_date,1) AS TIMESTAMP),'America/Sao_Paulo'))
)
SELECT local_date,classification_group,avg(consumption_liters) AS mean_liters_per_property_day,
       count(DISTINCT property_id) AS properties
FROM bi_complete_day
WHERE local_date >= cast(:start_date AS DATE) AND local_date < cast(:end_date AS DATE)
  AND (:region_id='ALL' OR region_id=try_cast(:region_id AS INT))
  AND classification_group IN ('RESIDENCIAL','COMERCIAL')
GROUP BY local_date,classification_group ORDER BY local_date,classification_group;

-- BI08: colunas 0..23; média por imóvel/hora completa nos dias selecionados.
WITH bi_complete_hour AS (
  SELECT * FROM workspace.gold_bi.tb_consumption_hourly
  WHERE expected_seconds > 0 AND abs(covered_seconds-expected_seconds) < 0.001
)
SELECT hour,avg(consumption_liters) AS mean_liters_per_property_hour,count(*) AS property_hours
FROM bi_complete_hour
WHERE local_date >= cast(:start_date AS DATE) AND local_date < cast(:end_date AS DATE)
  AND (:region_id='ALL' OR region_id=try_cast(:region_id AS INT))
GROUP BY hour ORDER BY hour;

-- BI09: o boxplot recebe amostras individuais. Não usar uma média/p90 como boxplot.
SELECT agent_name,response_time_ms/1000.0 AS response_seconds
FROM workspace.gold_bi.tb_agent_call
WHERE to_date(from_utc_timestamp(occurred_at,'America/Sao_Paulo')) >= cast(:start_date AS DATE)
  AND to_date(from_utc_timestamp(occurred_at,'America/Sao_Paulo')) < cast(:end_date AS DATE)
  AND agent_name IS NOT NULL AND response_time_ms IS NOT NULL;
-- Na medida do visual, média=avg(response_seconds), p90=percentile_approx(response_seconds,0.9).
-- Juiz/roteador já têm duração NULL na tabela. Nunca usar atraso usuário→bot.

-- BI10: card residencial, R$/m³. Uma tarifa por região/classificação vigente.
SELECT avg(tariff_per_m3) AS residential_tariff_per_m3,count(*) AS tariffs
FROM workspace.gold_bi.tb_tariff_validity
WHERE classification_group='RESIDENCIAL'
  AND initial_validity <= cast(:reference_date AS DATE)
  AND (final_validity IS NULL OR final_validity >= cast(:reference_date AS DATE))
  AND (:region_id='ALL' OR region_id=try_cast(:region_id AS INT));

-- BI11: card comercial, R$/m³. Não repetir a tarifa pelo número de imóveis.
SELECT avg(tariff_per_m3) AS commercial_tariff_per_m3,count(*) AS tariffs
FROM workspace.gold_bi.tb_tariff_validity
WHERE classification_group='COMERCIAL'
  AND initial_validity <= cast(:reference_date AS DATE)
  AND (final_validity IS NULL OR final_validity >= cast(:reference_date AS DATE))
  AND (:region_id='ALL' OR region_id=try_cast(:region_id AS INT));

-- BI12: média dos percentuais individuais de dias dentro da meta diária ATUAL.
-- É uma comparação com a meta atual, sem afirmar qual era a meta no passado.
WITH per_user AS (
  SELECT user_key,100.0*sum(days_within_target)/nullif(sum(eligible_days),0) AS compliance_pct
  FROM workspace.gold_bi.tb_target_compliance_daily
  WHERE local_date >= cast(:start_date AS DATE) AND local_date < cast(:end_date AS DATE)
  GROUP BY user_key HAVING sum(eligible_days)>0
)
SELECT avg(compliance_pct) AS mean_daily_target_compliance_pct,count(*) AS users FROM per_user;

-- BI13: primeiro calcula o percentual no período de cada device; depois tira a média.
WITH per_device AS (
  SELECT device_id,100.0*sum(available_seconds)/nullif(sum(expected_seconds),0) AS availability_pct
  FROM workspace.gold_bi.tb_device_availability_daily
  WHERE local_date >= cast(:start_date AS DATE) AND local_date < cast(:end_date AS DATE)
    AND (:region_id='ALL' OR region_id=try_cast(:region_id AS INT))
  GROUP BY device_id HAVING sum(expected_seconds)>0
)
SELECT avg(availability_pct) AS mean_device_availability_pct,count(*) AS devices FROM per_device;

-- Opções do filtro: regiões presentes nas medidas, sem depender de existir tarifa.
SELECT DISTINCT region_id, region_name FROM (
  SELECT region_id, region_name FROM workspace.gold_bi.tb_tariff_validity
  UNION ALL SELECT region_id, region_name FROM workspace.gold_bi.tb_consumption_hourly
  UNION ALL SELECT region_id, region_name FROM workspace.gold_bi.tb_alert_count_daily
  UNION ALL SELECT region_id, region_name FROM workspace.gold_bi.tb_device_availability_daily
) WHERE region_id IS NOT NULL ORDER BY region_name;
