# Databricks notebook source
# MAGIC %md
# MAGIC # tb_preferences_snapshot
# MAGIC Atualiza somente esta base Gold.
# COMMAND ----------
# MAGIC %run ./00-gold-common
# COMMAND ----------
ensure_user_keys()
validation_86_1 = frames["s_preferences"]
validation_86_1 = validation_86_1.select("*")
unique(validation_86_1, ["user_id"], "Preferência duplicada por usuário.")
tb_preferences_snapshot_1 = frames["s_preferences"].alias("p")
tb_preferences_snapshot_1 = tb_preferences_snapshot_1.join(
    frames["v_user_key"].alias("u"), (F.col("p.user_id") == F.col("u.user_id")), "inner"
)
tb_preferences_snapshot_1 = tb_preferences_snapshot_1.select(
    F.col("u.user_key").alias("user_key"),
    F.lit(ASOF).cast("TIMESTAMP").alias("snapshot_at"),
    F.col("p._ingested_at").alias("preference_captured_at"),
    F.when((F.col("p.daily_liters_target") > F.lit(0)), F.col("p.daily_liters_target"))
    .otherwise(F.lit(None))
    .alias("daily_liters_target"),
    F.col("p.notifications_enabled").alias("notifications_enabled"),
    F.col("p.dark_mode_enabled").alias("dark_mode_enabled"),
)
save_table(
    "gold_bi", "tb_preferences_snapshot", tb_preferences_snapshot_1, ["user_key", "snapshot_at"]
)
