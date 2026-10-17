-- Exemplos de medidas sobre as 8 bases; não criam tabelas.
-- O notebook continua PySpark. Estas consultas são exemplos para o consumidor SQL.

-- Consumo com granulação parametrizável: troque 'HOUR' por 'DAY' ou 'MONTH'.
-- O filtro usa sobreposição de intervalos [start_at,end_at), repartindo litros por duração.
WITH parameters AS (
  SELECT TIMESTAMP '2026-10-01 03:00:00' start_at,
         TIMESTAMP '2026-11-01 03:00:00' end_at, 'DAY' grain
), clipped AS (
  SELECT w.*,p.grain,greatest(w.window_started_at,p.start_at) clipped_start,
         least(w.window_finished_at,p.end_at) clipped_end
  FROM workspace.gold_platform.tb_consumption_window w CROSS JOIN parameters p
  WHERE w.scope_eligible AND w.organization_id=1
    AND w.window_started_at<p.end_at AND w.window_finished_at>p.start_at
), hours AS (
  SELECT *,explode(sequence(date_trunc('hour',clipped_start),
      date_trunc('hour',clipped_end-INTERVAL 1 MICROSECOND),INTERVAL 1 HOUR)) hour_start
  FROM clipped
), parts AS (
  SELECT *,consumption_liters*
      (unix_micros(least(clipped_end,hour_start+INTERVAL 1 HOUR))-
       unix_micros(greatest(clipped_start,hour_start)))/
      (unix_micros(window_finished_at)-unix_micros(window_started_at)) part_liters
  FROM hours
)
SELECT property_id,date_trunc(grain,from_utc_timestamp(hour_start,time_zone)) local_period,
       sum(part_liters) measured_liters,sum(part_liters)/1000 measured_m3
FROM parts GROUP BY property_id,date_trunc(grain,from_utc_timestamp(hour_start,time_zone));

-- Snapshot: limite de ping alterável, sem tabela de saúde agregada.
SELECT organization_id,count(CASE WHEN device_active THEN 1 END) eligible_devices,
       count(CASE WHEN device_active AND connectivity_status='online'
          AND last_ping_at<=snapshot_at AND last_ping_at>=snapshot_at-INTERVAL 15 MINUTES
          THEN 1 END) online_recent_devices,
       avg(CASE WHEN device_active THEN battery_level END) battery_mean
FROM workspace.gold_platform.tb_device_snapshot GROUP BY organization_id;

-- Configurações operacionais necessárias para cobertura e metas, sem outras tabelas.
SELECT device_id,property_id,explode(monitoring_periods) monitoring_period
FROM workspace.gold_platform.tb_device_snapshot;
SELECT DISTINCT property_id,goal.valid_from,goal.valid_to,goal.monthly_target_liters,goal.goal_version
FROM workspace.gold_platform.tb_device_snapshot
LATERAL VIEW explode(monthly_goal_periods) goals AS goal;

-- Alertas: backlog em um instante selecionado.
SELECT alert_type,severity,count(*) open_alerts
FROM workspace.gold_platform.tb_alert_case
WHERE quality_status='VALID' AND triggered_at<=TIMESTAMP '2026-10-12 15:00:00'
  AND (resolved_at IS NULL OR resolved_at>TIMESTAMP '2026-10-12 15:00:00')
GROUP BY alert_type,severity;

-- Clima: a mesma base atende visão horária e diária/mensal.
SELECT location_key,date_trunc('DAY',from_utc_timestamp(observed_at,time_zone)) local_period,
       avg(temperature_c) temperature_mean,avg(feels_like_c) feels_like_mean,
       avg(feels_like_c-temperature_c) thermal_gap_c,
       count(*) paired_hours,count(CASE WHEN feels_like_c>=30 THEN 1 END) hot_hours
FROM workspace.gold_bi.tb_weather_hourly
GROUP BY location_key,date_trunc('DAY',from_utc_timestamp(observed_at,time_zone));

-- Chat: atividade por evento, e não pelo início da sessão.
WITH events AS (
  SELECT user_key,session_id,event.* FROM workspace.gold_bi.tb_chat_session
  LATERAL VIEW explode(message_events) messages AS event
)
SELECT to_date(from_utc_timestamp(sent_at,'America/Sao_Paulo')) local_date,
       count(DISTINCT user_key) active_chat_users
FROM events WHERE role='user' AND sent_at IS NOT NULL
GROUP BY to_date(from_utc_timestamp(sent_at,'America/Sao_Paulo'));

-- Chat: percentis sobre os pares individuais após o filtro do período.
WITH events AS (
  SELECT event.* FROM workspace.gold_bi.tb_chat_session
  LATERAL VIEW explode(message_events) messages AS event
)
SELECT count(*) response_pairs,percentile_approx(delay_seconds,0.5) median_seconds,
       percentile_approx(delay_seconds,0.9) p90_seconds
FROM events WHERE role='user' AND delay_seconds IS NOT NULL
  AND sent_at>=TIMESTAMP '2026-10-01 03:00:00'
  AND sent_at<TIMESTAMP '2026-11-01 03:00:00'
  AND response_at<TIMESTAMP '2026-11-01 03:00:00';

-- Preferências: filtros alteram os denominadores; flags desconhecidas são excluídas.
SELECT 100*try_divide(count(CASE WHEN notifications_enabled THEN 1 END),count(notifications_enabled)) notification_opt_in_pct,
       percentile_approx(daily_liters_target,0.5) daily_target_median_liters
FROM workspace.gold_bi.tb_preferences_snapshot;

-- Conta e cenário são fatos/entradas; comparações e resultados são medidas.
SELECT user_key,reference_month,charged_rate_per_m3,bill_total_value
FROM workspace.gold_bi.tb_bill_monthly;
SELECT scenario_id,scenario_version,property_scope,investment_value,reduction_pct,
       annual_savings_reported,
       CASE WHEN investment_value>0 AND annual_savings_reported>0
         THEN investment_value/(annual_savings_reported/12) END reference_payback_months
FROM workspace.gold_platform.tb_investment_scenario;
