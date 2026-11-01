-- Somente leitura. Execute cada SELECT separadamente no catálogo workspace.
-- As Silver atuais podem pertencer a lote posterior ao da Gold: confira o histórico Delta.
-- Nenhuma consulta geocodifica, cria mocks ou altera dados.

-- 1. O seed local tem um dia, samples=3 e três elementos em hours[].
-- Aqui samples é o declarado no bucket; a segunda consulta mede as horas efetivamente importadas.
SELECT count(*) AS daily_buckets, sum(samples) AS declared_samples,
       min(weather_date) AS first_bucket, max(weather_date) AS last_bucket
FROM workspace.silver_app.weather_daily;

SELECT city,state,country,count(*) AS imported_hours,
       min(observed_at) AS first_observation,max(observed_at) AS last_observation,
       count_if(observed_at IS NULL) AS missing_time,
       count_if(temperature_c IS NULL OR feels_like_c IS NULL) AS missing_temperature_pair
FROM workspace.silver_app.weather_hourly GROUP BY city,state,country;

SELECT city,state,count(*) AS gold_hours,count(DISTINCT local_date) AS local_days,
       min(hour_utc) AS first_hour,max(hour_utc) AS last_hour,
       count_if(size(region_ids)=0) AS hours_without_property_region
FROM workspace.gold_bi.tb_weather_hourly GROUP BY city,state;

-- 2. Grafias que precisam convergir na mesma chave de cidade.
WITH city_spellings AS (
  SELECT 'SQL' AS source,city,state FROM workspace.silver_sql.bi_property_context
  UNION ALL
  SELECT 'MONGO',city,state FROM workspace.silver_app.weather_hourly
)
SELECT upper(regexp_replace(translate(trim(city),
         'ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑáàâãäéèêëíìîïóòôõöúùûüçñ',
         'AAAAAEEEEIIIIOOOOOUUUUCNaaaaaeeeeiiiiooooouuuucn'),'\\s+',' ')) AS canonical_city,
       collect_set(city) AS raw_spellings,collect_set(source) AS sources,count(*) AS rows
FROM city_spellings GROUP BY ALL;

-- 3. Região cadastral e cidade são dimensões diferentes. Tarifas podem cobrir regiões sem consumo.
-- classification_id diferencia categorias dentro do mesmo grupo residencial/comercial.
SELECT region_id,region_name,count(DISTINCT property_id) AS properties,
       collect_set(city) AS cities,
       count_if(latitude IS NULL OR longitude IS NULL
                OR NOT latitude BETWEEN -90 AND 90 OR NOT longitude BETWEEN -180 AND 180
                OR (latitude=0 AND longitude=0)) AS invalid_map_coordinates
FROM workspace.silver_sql.bi_property_context GROUP BY region_id,region_name;

SELECT region_id,region_name,classification_id,classification_group,
       initial_validity,final_validity,m3_value
FROM workspace.silver_sql.bi_tariff_context
ORDER BY region_id,classification_id,initial_validity;

-- ID com nomes realmente conflitantes, depois de normalizar grafias.
WITH regions AS (
  SELECT region_id,region_name FROM workspace.silver_sql.bi_property_context
  UNION ALL SELECT region_id,region_name FROM workspace.silver_sql.bi_tariff_context
), normalized AS (
  SELECT region_id,regexp_replace(upper(translate(trim(region_name),
    'ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑáàâãäéèêëíìîïóòôõöúùûüçñ',
    'AAAAAEEEEIIIIOOOOOUUUUCNaaaaaeeeeiiiiooooouuuucn')),'[ _-]+','_') AS name
  FROM regions
)
SELECT region_id,collect_set(name) AS names FROM normalized GROUP BY region_id
HAVING count(DISTINCT name)>1 OR count_if(name IS NULL OR name='')>0;

-- 4. Instrumentação na origem: não usar endereço do usuário para localizar chamadas.
SELECT agent_name,api_status_code,count(*) AS calls,
       count_if(sent_at IS NULL) AS missing_time,
       count_if(response_time_ms IS NULL) AS missing_duration,
       count_if(response_time_ms < 0) AS invalid_duration
FROM workspace.silver_app.chat_messages WHERE role='bot' AND content_type='text'
GROUP BY agent_name,api_status_code ORDER BY agent_name,api_status_code;

SELECT agent_name,count(*) AS calls,count(response_time_ms) AS latency_samples,
       avg(response_time_ms)/1000 AS mean_seconds,
       percentile_approx(response_time_ms,0.9)/1000 AS p90_seconds
FROM workspace.gold_bi.tb_agent_call GROUP BY agent_name ORDER BY agent_name;

-- 5. Cobertura necessária aos gráficos (janelas incompletas não viram consumo zero).
SELECT classification_group,count(*) AS property_hours,
       count_if(expected_seconds>0 AND abs(covered_seconds-expected_seconds)<0.001) AS complete_hours,
       min(local_date) AS first_day,max(local_date) AS last_day
FROM workspace.gold_bi.tb_consumption_hourly GROUP BY classification_group;

SELECT count(*) AS complete_consumption_hours,
       count_if(w.hour_utc IS NOT NULL) AS paired_weather_hours
FROM workspace.gold_bi.tb_consumption_hourly c
LEFT JOIN workspace.gold_bi.tb_weather_hourly w
  ON c.city=w.city AND c.state=w.state AND c.hour_utc=w.hour_utc
WHERE c.expected_seconds>0 AND abs(c.covered_seconds-c.expected_seconds)<0.001;

-- 6. Conferir lote, corte e quality_contract_version=3 nas sete tabelas.
DESCRIBE HISTORY workspace.gold_bi.tb_weather_hourly LIMIT 1;
DESCRIBE HISTORY workspace.gold_bi.tb_consumption_hourly LIMIT 1;
DESCRIBE HISTORY workspace.gold_bi.tb_alert_count_daily LIMIT 1;
DESCRIBE HISTORY workspace.gold_bi.tb_agent_call LIMIT 1;
DESCRIBE HISTORY workspace.gold_bi.tb_tariff_validity LIMIT 1;
DESCRIBE HISTORY workspace.gold_bi.tb_target_compliance_daily LIMIT 1;
DESCRIBE HISTORY workspace.gold_bi.tb_device_availability_daily LIMIT 1;
