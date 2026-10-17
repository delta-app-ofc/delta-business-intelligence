# Databricks notebook source
# MAGIC %md
# MAGIC # tb_device_snapshot
# MAGIC Atualiza somente esta base Gold.
# COMMAND ----------
# MAGIC %run ./00-gold-common
# COMMAND ----------
initialize_property_context()
# O Mongo atual guarda motivos somente dentro dos intervalos.
if "unavailability_reason" not in frames["s_status"].columns:
    frames["s_status"] = frames["s_status"].withColumn("unavailability_reason",
        F.try_element_at(F.sort_array(F.filter(F.col("unavailabilities"), lambda e: e.ended_at.isNull()), asc=False), F.lit(1)).unavailability_reason)
fresh = PARAMETERS["fresh_ping_minutes"]
battery = PARAMETERS["low_battery_pct"]
validation_38_1 = frames["s_status"]
validation_38_1 = validation_38_1.select("*")
unique(validation_38_1, ["device_id"], "Status duplicado por dispositivo.")
tb_device_snapshot_1 = frames["s_device"].alias("d")
tb_device_snapshot_1 = tb_device_snapshot_1.join(
    frames["s_status"].alias("s"), F.col("d.device_id") == F.col("s.device_id"), "full_outer"
)
tb_device_snapshot_1 = tb_device_snapshot_1.join(
    frames["p_tb_property"].alias("p"), F.col("d.property_id") == F.col("p.property_id"), "left"
)

tb_device_snapshot_1 = tb_device_snapshot_1.select(
    F.coalesce(F.col("d.device_id"), F.col("s.device_id")).alias("device_id"),
    F.col("d.property_id").alias("property_id"),
    F.col("p.organization_id").alias("organization_id"),
    F.lit(ASOF).cast("TIMESTAMP").alias("snapshot_at"),
    F.col("s._ingested_at").alias("status_captured_at"),
    F.col("d.is_active").alias("device_active"),
    F.col("s.last_ping_at").alias("last_ping_at"),
    F.col("s.connectivity_status").alias("connectivity_status"),
    F.col("s.wifi_signal_rssi").alias("wifi_signal_rssi"),
    F.col("s.firmware_version").alias("firmware_version"),
    F.col("s.unavailability_reason").alias("unavailability_reason"),
    F.when(F.col("d.device_id").isNull(), F.lit("UNMAPPED_DEVICE"))
    .when(F.col("p.property_id").isNull(), F.lit("UNMAPPED_PROPERTY"))
    .otherwise(F.lit("SQL_CURRENT_MAPPING"))
    .alias("mapping_status"),
    F.when(F.col("s.battery_level").between(F.lit(0), F.lit(100)), F.col("s.battery_level"))
    .otherwise(F.lit(None))
    .alias("battery_level"),
    F.when(
        F.col("s.battery_level").between(F.lit(0), F.lit(100)),
        (F.col("s.battery_level") < F.lit(battery)),
    )
    .otherwise(F.lit(None))
    .alias("low_battery"),
    F.when(
        F.col("s.wifi_signal_rssi").isin(
            F.lit("excellent"), F.lit("good"), F.lit("fair"), F.lit("weak"), F.lit("critical")
        ),
        F.col("s.wifi_signal_rssi").isin(F.lit("weak"), F.lit("critical")),
    )
    .otherwise(F.lit(None))
    .alias("weak_wifi"),
    (
        ((F.unix_micros(F.lit(ASOF).cast("TIMESTAMP")) - F.unix_micros(F.col("s.last_ping_at"))))
        / F.lit(Decimal("60000000.0"))
    ).alias("ping_age_minutes"),
    F.when(
        (
            (F.col("s.last_ping_at").isNull() | F.col("s.connectivity_status").isNull())
            | (F.col("s.last_ping_at") > F.lit(ASOF).cast("TIMESTAMP"))
        ),
        F.lit("UNKNOWN"),
    )
    .when(
        (
            (F.col("s.connectivity_status") == F.lit("online"))
            & (
                (
                    (
                        (
                            F.unix_micros(F.lit(ASOF).cast("TIMESTAMP"))
                            - F.unix_micros(F.col("s.last_ping_at"))
                        )
                    )
                    / F.lit(Decimal("60000000.0"))
                )
                <= F.lit(fresh)
            )
        ),
        F.lit("ONLINE_FRESH"),
    )
    .when((F.col("s.connectivity_status") == F.lit("online")), F.lit("STALE_ONLINE"))
    .otherwise(F.lit("OFFLINE_OR_OTHER"))
    .alias("freshness_status"),
)
save_table(
    "gold_platform", "tb_device_snapshot", tb_device_snapshot_1, ["device_id", "snapshot_at"]
)
