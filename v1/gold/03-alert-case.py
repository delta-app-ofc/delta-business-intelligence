# Databricks notebook source
# MAGIC %md
# MAGIC # tb_alert_case
# MAGIC Atualiza somente esta base Gold.
# COMMAND ----------
# MAGIC %run ./00-gold-common
# COMMAND ----------
initialize_property_context()
validation_41_1 = frames["s_alerts"]
validation_41_1 = validation_41_1.select("*")
unique(validation_41_1, ["alert_id"], "alert_id duplicado.")
v_alert_1 = frames["s_alerts"].alias("a")
v_alert_1 = v_alert_1.join(
    frames["s_device"].alias("d"), (F.col("a.device_id") == F.col("d.device_id")), "left"
)
v_alert_1 = v_alert_1.join(
    frames["p_tb_property"].alias("p"), (F.col("d.property_id") == F.col("p.property_id")), "left"
)
v_alert_1 = v_alert_1.select(
    "a.*",
    F.col("d.property_id").alias("property_id"),
    F.col("p.organization_id").alias("organization_id"),
    F.col("p.time_zone").alias("time_zone"),
    F.to_date(
        F.from_utc_timestamp(F.col("triggered_at").cast("TIMESTAMP"), F.col("p.time_zone"))
    ).alias("triggered_date"),
    F.to_date(
        F.from_utc_timestamp(F.col("resolved_at").cast("TIMESTAMP"), F.col("p.time_zone"))
    ).alias("resolved_date"),
    F.least(
        F.coalesce(F.col("resolved_at"), F.lit(ASOF).cast("TIMESTAMP")),
        F.lit(ASOF).cast("TIMESTAMP"),
    ).alias("event_end"),
    F.when(
        (
            (F.col("alert_id").isNull() | F.col("triggered_at").isNull())
            | (F.col("triggered_at") > F.lit(ASOF).cast("TIMESTAMP"))
        ),
        F.lit("INVALID_START"),
    )
    .when(
        (
            (F.col("resolved_at") < F.col("triggered_at"))
            | (F.col("resolved_at") > F.lit(ASOF).cast("TIMESTAMP"))
        ),
        F.lit("INVALID_RESOLUTION"),
    )
    .when(F.col("d.device_id").isNull(), F.lit("UNMAPPED_DEVICE"))
    .otherwise(F.lit("VALID"))
    .alias("quality_status"),
)
view("v_alert", v_alert_1)

tb_alert_case = frames['v_alert'].withColumn('severity', F.coalesce(F.col('severity'), F.lit('unknown')))
save_table('gold_platform', 'tb_alert_case', tb_alert_case, ['alert_id'])
