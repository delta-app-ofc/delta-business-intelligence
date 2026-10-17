# Databricks notebook source
# MAGIC %md
# MAGIC # tb_consumption_window
# MAGIC Atualiza somente esta base Gold.
# COMMAND ----------
# MAGIC %run ./00-gold-common
# COMMAND ----------
initialize_property_context()
validation_19_1 = frames["s_consumption"]
validation_19_1 = validation_19_1.select("*")
unique(
    validation_19_1,
    ["device_id", "window_started_at", "window_finished_at"],
    "Chave natural duplicada na Silver de consumo.",
)
v_window_validity_ordered_1 = frames["s_consumption"]
v_window_validity_ordered_1 = v_window_validity_ordered_1.select(
    "*",
    F.max(F.col("window_finished_at"))
    .over(
        Window.partitionBy(F.col("device_id"))
        .orderBy(
            F.col("window_started_at").asc_nulls_first(),
            F.col("window_finished_at").asc_nulls_first(),
        )
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    .alias("previous_end"),
    F.lead(F.col("window_started_at"))
    .over(
        Window.partitionBy(F.col("device_id")).orderBy(
            F.col("window_started_at").asc_nulls_first(),
            F.col("window_finished_at").asc_nulls_first(),
        )
    )
    .alias("next_start"),
)
v_window_validity_2 = v_window_validity_ordered_1
v_window_validity_2 = v_window_validity_2.select(
    "*",
    F.when(
        (
            (F.col("device_id").isNull() | F.col("window_started_at").isNull())
            | F.col("window_finished_at").isNull()
        ),
        F.lit("NULL_KEY"),
    )
    .when((F.col("window_finished_at") <= F.col("window_started_at")), F.lit("INVALID_DURATION"))
    .when(
        (
            (
                ~(
                    (
                        (
                            (~F.col("consumption_liters").isNull())
                            & (~F.isnan(F.col("consumption_liters").cast("DOUBLE")))
                        )
                        & (F.abs(F.col("consumption_liters")) < F.lit(1e300))
                    )
                )
            )
            | (F.col("consumption_liters") < F.lit(0))
        ),
        F.lit("INVALID_LITERS"),
    )
    .when(
        (F.col("window_finished_at") > F.lit(ASOF).cast("TIMESTAMP")), F.lit("FUTURE_OR_UNFINISHED")
    )
    .when(
        (
            (F.col("previous_end") > F.col("window_started_at"))
            | (F.col("next_start") < F.col("window_finished_at"))
        ),
        F.lit("OVERLAP"),
    )
    .otherwise(F.lit(None))
    .alias("reject_reason"),
)
view("v_window_validity", v_window_validity_2)
p_tb_consumption_rejected_1 = frames["v_window_validity"]
p_tb_consumption_rejected_1 = p_tb_consumption_rejected_1.filter((~F.col("reject_reason").isNull()))
p_tb_consumption_rejected_1 = p_tb_consumption_rejected_1.select(
    F.col("consumption_id").alias("consumption_id"),
    F.col("device_id").alias("device_id"),
    F.col("window_started_at").alias("window_started_at"),
    F.col("window_finished_at").alias("window_finished_at"),
    F.col("reject_reason").alias("reject_reason"),
)
view("p_tb_consumption_rejected", p_tb_consumption_rejected_1)
tb_consumption_window_1 = frames["v_window_validity"].alias("w")
tb_consumption_window_1 = tb_consumption_window_1.join(
    frames["s_device"].alias("d"), (F.col("w.device_id") == F.col("d.device_id")), "left"
)
tb_consumption_window_1 = tb_consumption_window_1.join(
    frames["p_tb_property"].alias("p"), (F.col("d.property_id") == F.col("p.property_id")), "left"
)
tb_consumption_window_1 = tb_consumption_window_1.join(
    frames["v_scope"].alias("c"),
    (
        (
            (
                (F.col("w.device_id") == F.col("c.device_id"))
                & (F.col("d.property_id") == F.col("c.property_id"))
            )
            & (F.col("w.window_started_at") >= F.col("c.scope_start_utc"))
        )
        & (F.col("w.window_finished_at") <= F.col("c.scope_end_utc"))
    ),
    "left",
)
tb_consumption_window_1 = tb_consumption_window_1.filter(F.col("w.reject_reason").isNull())
tb_consumption_window_1 = tb_consumption_window_1.select(
    F.col("w.consumption_id").alias("consumption_id"),
    F.col("w.device_id").alias("device_id"),
    F.col("d.property_id").alias("property_id"),
    F.col("p.organization_id").alias("organization_id"),
    F.col("w.window_started_at").alias("window_started_at"),
    F.col("w.window_finished_at").alias("window_finished_at"),
    F.col("w.consumption_liters").cast("DOUBLE").alias("consumption_liters"),
    (
        (
            (
                F.unix_micros(F.col("w.window_finished_at"))
                - F.unix_micros(F.col("w.window_started_at"))
            )
        )
        / F.lit(Decimal("60000000.0"))
    ).alias("duration_minutes"),
    F.col("w.anomaly_detected").alias("anomaly_detected"),
    F.col("w.lpm_average").alias("lpm_average"),
    F.col("c.scope_version").alias("scope_version"),
    (~F.col("c.scope_version").isNull()).alias("scope_eligible"),
    F.col("p.time_zone").alias("time_zone"),
    F.col("p.region_id").alias("region_id"),
    F.col("p.classification_id").alias("classification_id"),
    (
        (
            F.col("w.window_finished_at")
            >= (F.lit(ASOF).cast("TIMESTAMP") - F.make_dt_interval(secs=F.lit(Decimal("7200.0"))))
        )
        & (F.col("w.window_finished_at") < F.lit(ASOF).cast("TIMESTAMP"))
    ).alias("recent_window"),
    F.when(F.col("d.device_id").isNull(), F.lit("UNMAPPED_DEVICE"))
    .when(F.col("p.property_id").isNull(), F.lit("UNMAPPED_PROPERTY"))
    .when(F.col("c.scope_version").isNull(), F.lit("OUTSIDE_REGISTERED_MONITORING_PERIOD"))
    .when(F.col("p.organization_id").isNull(), F.lit("NO_ORGANIZATION"))
    .otherwise(F.lit("SQL_CURRENT_MAPPING"))
    .alias("quality_status"),
)
save_table(
    "gold_platform",
    "tb_consumption_window",
    tb_consumption_window_1,
    ["device_id", "window_started_at", "window_finished_at"],
)
