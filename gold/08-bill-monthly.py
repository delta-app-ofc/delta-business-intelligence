# Databricks notebook source
# MAGIC %md
# MAGIC # tb_bill_monthly
# MAGIC Atualiza somente esta base Gold.
# COMMAND ----------
# MAGIC %run ./00-gold-common
# COMMAND ----------
ensure_user_keys()
# m3_value é tarifa cobrada por m³, confirmada pela DBA. Nunca deduza volume total_value/m3_value.
validation_89_1 = frames["s_person"]
validation_89_1 = validation_89_1.select("*")
unique(validation_89_1, ["person_key"], "person_key duplicado.")
validation_90_1 = frames["s_bill"].alias("f")
validation_90_1 = validation_90_1.join(
    frames["s_person"].alias("p"), (F.col("f.person_key") == F.col("p.person_key")), "left_anti"
)
validation_90_1 = validation_90_1.select("f.*")
assert_no_rows(validation_90_1, "Conta sem pessoa SQL.")
validation_91_1 = frames["s_bill"].alias("f")
validation_91_1 = validation_91_1.join(
    frames["s_date"].alias("d"), (F.col("f.date_key") == F.col("d.date_key")), "left_anti"
)
validation_91_1 = validation_91_1.select("f.*")
assert_no_rows(validation_91_1, "Conta sem data SQL.")
v_bill_1 = frames["s_bill"].alias("f")
v_bill_1 = v_bill_1.join(
    frames["s_person"].alias("p"), (F.col("f.person_key") == F.col("p.person_key")), "inner"
)
v_bill_1 = v_bill_1.join(
    frames["v_user_key"].alias("u"), (F.col("p.user_id") == F.col("u.user_id")), "inner"
)
v_bill_1 = v_bill_1.join(
    frames["s_date"].alias("d"), (F.col("f.date_key") == F.col("d.date_key")), "inner"
)
v_bill_1 = v_bill_1.filter(
    (
        F.col("d.full_date")
        <= F.to_date(
            F.from_utc_timestamp(F.lit(ASOF).cast("TIMESTAMP"), F.lit("America/Sao_Paulo"))
        )
    )
)
v_bill_1 = v_bill_1.select(
    F.col("u.user_key").alias("user_key"),
    F.trunc(F.col("d.full_date"), "month").alias("reference_month"),
    F.col("f.date_key").alias("date_key"),
    F.col("f.fact_key").alias("fact_key"),
    F.col("f.m3_value").cast("DOUBLE").alias("charged_rate_per_m3"),
    F.col("f.total_value").cast("DOUBLE").alias("bill_total_value"),
)
view("v_bill", v_bill_1)
validation_93_1 = frames["v_bill"]
validation_93_1 = validation_93_1.select("*")
unique(
    validation_93_1,
    ["user_key", "reference_month"],
    "Mais de uma conta por pessoa/mês: defina o grão antes de agregar tarifas.",
)
tb_bill_monthly = frames['v_bill'].withColumn('value_semantics', F.lit('CHARGED_PRICE_PER_M3_NO_BILLED_VOLUME'))
save_table('gold_bi', 'tb_bill_monthly', tb_bill_monthly, ['user_key', 'reference_month'])
