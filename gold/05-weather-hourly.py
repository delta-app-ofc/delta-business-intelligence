# Databricks notebook source
# MAGIC %md
# MAGIC # tb_weather_hourly
# MAGIC Atualiza somente esta base Gold.
# COMMAND ----------
# MAGIC %run ./00-gold-common
# COMMAND ----------
initialize_property_context()
cut = F.lit(ASOF).cast("timestamp")
hot = PARAMETERS["hot_temperature_c"]
min_peers = int(PARAMETERS["peer_min_count"])
# A cidade do SQL determina o fuso; o default BR/SP é o recorte explícito do relatório.
b_tb_location_locations_left_1 = frames["p_tb_property"]
b_tb_location_locations_left_1 = b_tb_location_locations_left_1.select(
    F.upper(F.trim(F.col("city"))).alias("city"),
    F.upper(F.trim(F.col("state"))).alias("state"),
    F.upper(F.trim(F.col("country"))).alias("country"),
    F.col("time_zone").alias("time_zone"),
)
b_tb_location_locations_left_1 = b_tb_location_locations_left_1.distinct()
b_tb_location_locations_right_2 = frames["s_weather_daily"]
b_tb_location_locations_right_2 = b_tb_location_locations_right_2.select(
    F.upper(F.trim(F.col("city"))),
    F.upper(F.trim(F.col("state"))),
    F.when(
        F.upper(F.trim(F.col("country"))).isin(F.lit("BR"), F.lit("BRASIL"), F.lit("BRAZIL")),
        F.lit("BR"),
    ).otherwise(F.upper(F.trim(F.col("country")))),
    F.when(
        F.upper(F.trim(F.col("country"))).isin(F.lit("BR"), F.lit("BRASIL"), F.lit("BRAZIL")),
        F.lit("America/Sao_Paulo"),
    ).otherwise(F.lit(None)),
)
b_tb_location_locations_right_2 = b_tb_location_locations_right_2.distinct()
b_tb_location_locations_3 = b_tb_location_locations_left_1.union(
    b_tb_location_locations_right_2
).distinct()
b_tb_location_4 = b_tb_location_locations_3
b_tb_location_4 = b_tb_location_4.select(
    F.sha2(
        F.concat_ws("|", F.col("city"), F.col("state"), F.col("country"), F.col("time_zone")), 256
    ).alias("location_key"),
    "*",
)
view("b_tb_location", b_tb_location_4)
validation_58_1 = frames["b_tb_location"]
validation_58_1 = validation_58_1.groupBy(
    F.col("city").alias("_group_0"),
    F.col("state").alias("_group_1"),
    F.col("country").alias("_group_2"),
).agg(F.count(F.lit(1)).alias("_agg_0"))
validation_58_1 = validation_58_1.filter((F.col("_agg_0") > F.lit(1)))
validation_58_1 = validation_58_1.select(
    F.col("_group_0").alias("city"),
    F.col("_group_1").alias("state"),
    F.col("_group_2").alias("country"),
)
assert_no_rows(validation_58_1, "Localização tem fusos conflitantes. Ajuste tb_property_config.")
v_weather_normalized_1 = frames["s_weather_hourly"]
v_weather_normalized_1 = v_weather_normalized_1.select(
    "*",
    F.upper(F.trim(F.col("city"))).alias("city_key"),
    F.upper(F.trim(F.col("state"))).alias("state_key"),
    F.when(
        F.upper(F.trim(F.col("country"))).isin(F.lit("BR"), F.lit("BRASIL"), F.lit("BRAZIL")),
        F.lit("BR"),
    )
    .otherwise(F.upper(F.trim(F.col("country"))))
    .alias("country_key"),
)
v_weather_2 = v_weather_normalized_1.alias("w")
v_weather_2 = v_weather_2.join(
    frames["b_tb_location"].alias("l"),
    (
        ((F.col("l.city") == F.col("w.city_key")) & (F.col("l.state") == F.col("w.state_key")))
        & (F.col("l.country") == F.col("w.country_key"))
    ),
    "left",
)
v_weather_2 = v_weather_2.select(
    "w.*",
    F.col("l.location_key").alias("location_key"),
    F.col("l.time_zone").alias("time_zone"),
    F.date_trunc("hour", F.col("observed_at")).alias("observed_hour_utc"),
    F.when(
        (F.col("l.location_key").isNull() | F.col("l.time_zone").isNull()),
        F.lit("LOCATION_OR_TIMEZONE_UNKNOWN"),
    )
    .when(
        (
            (
                (F.col("weather_date").isNull() | F.col("hour").isNull())
                | F.col("observed_at").isNull()
            )
            | (~F.col("hour").between(F.lit(0), F.lit(23)))
        ),
        F.lit("INVALID_TIME_KEY"),
    )
    .when(
        (
            (
                F.to_date(
                    F.from_utc_timestamp(
                        F.col("observed_at").cast("TIMESTAMP"), F.col("l.time_zone")
                    )
                )
                != F.col("weather_date")
            )
            | (
                F.hour(
                    F.from_utc_timestamp(
                        F.col("observed_at").cast("TIMESTAMP"), F.col("l.time_zone")
                    )
                )
                != F.col("w.hour")
            )
        ),
        F.lit("LOCAL_DATE_HOUR_MISMATCH"),
    )
    .when((F.col("observed_at") > F.lit(ASOF).cast("TIMESTAMP")), F.lit("FUTURE_OBSERVATION"))
    .when(
        (
            (
                ~(
                    (
                        (
                            (~F.col("temperature_c").isNull())
                            & (~F.isnan(F.col("temperature_c").cast("DOUBLE")))
                        )
                        & (F.abs(F.col("temperature_c")) < F.lit(1e300))
                    )
                )
            )
            | (
                ~(
                    (
                        (
                            (~F.col("feels_like_c").isNull())
                            & (~F.isnan(F.col("feels_like_c").cast("DOUBLE")))
                        )
                        & (F.abs(F.col("feels_like_c")) < F.lit(1e300))
                    )
                )
            )
        ),
        F.lit("INVALID_TEMPERATURE_PAIR"),
    )
    .when(
        (
            (
                F.col("humidity_percent").isNull()
                | (~F.col("humidity_percent").between(F.lit(0), F.lit(100)))
            )
            | F.isnan(F.col("humidity_percent").cast("DOUBLE"))
        ),
        F.lit("INVALID_HUMIDITY"),
    )
    .when(
        (
            (
                (
                    (
                        ~(
                            (
                                (
                                    (~F.col("rain_mm").isNull())
                                    & (~F.isnan(F.col("rain_mm").cast("DOUBLE")))
                                )
                                & (F.abs(F.col("rain_mm")) < F.lit(1e300))
                            )
                        )
                    )
                    | (F.col("rain_mm") < F.lit(0))
                )
                | (
                    ~(
                        (
                            (
                                (~F.col("wind_speed_mps").isNull())
                                & (~F.isnan(F.col("wind_speed_mps").cast("DOUBLE")))
                            )
                            & (F.abs(F.col("wind_speed_mps")) < F.lit(1e300))
                        )
                    )
                )
            )
            | (F.col("wind_speed_mps") < F.lit(0))
        ),
        F.lit("INVALID_RAIN_OR_WIND"),
    )
    .otherwise(F.lit(None))
    .alias("reject_reason"),
)
view("v_weather", v_weather_2)
validation_60_1 = frames["v_weather"]
validation_60_1 = validation_60_1.filter(F.col("reject_reason").isNull())
validation_60_1 = validation_60_1.select("*")
unique(
    validation_60_1,
    ["location_key", "observed_hour_utc"],
    "Hora climática duplicada após normalização.",
)
b_tb_weather_rejected_1 = frames["v_weather"]
b_tb_weather_rejected_1 = b_tb_weather_rejected_1.filter((~F.col("reject_reason").isNull()))
b_tb_weather_rejected_1 = b_tb_weather_rejected_1.select(
    F.col("weather_daily_id").alias("weather_daily_id"),
    F.col("city").alias("city"),
    F.col("state").alias("state"),
    F.col("country").alias("country"),
    F.col("weather_date").alias("weather_date"),
    F.col("hour").alias("hour"),
    F.col("observed_at").alias("observed_at"),
    F.col("reject_reason").alias("reject_reason"),
)
view("b_tb_weather_rejected", b_tb_weather_rejected_1)
tb_weather_hourly_1 = frames["v_weather"]
tb_weather_hourly_1 = tb_weather_hourly_1.filter(F.col("reject_reason").isNull())
tb_weather_hourly_1 = tb_weather_hourly_1.select(
    F.col("location_key").alias("location_key"),
    F.col("city_key").alias("city"),
    F.col("state_key").alias("state"),
    F.col("country_key").alias("country"),
    F.col("time_zone").alias("time_zone"),
    F.col("weather_date").alias("local_date"),
    F.col("hour").alias("hour"),
    F.col("observed_at").alias("observed_at"),
    F.col("observed_hour_utc").alias("observed_hour_utc"),
    F.col("temperature_c").alias("temperature_c"),
    F.col("feels_like_c").alias("feels_like_c"),
    (F.col("feels_like_c") - F.col("temperature_c")).alias("thermal_gap_c"),
    F.col("humidity_percent").alias("humidity_percent"),
    F.col("rain_mm").alias("rain_mm"),
    F.col("wind_speed_mps").alias("wind_speed_mps"),
    F.col("condition").alias("condition"),
    F.lit("PAIRED_VALID_OBSERVATION").alias("quality_status"),
)
save_table(
    "gold_bi", "tb_weather_hourly", tb_weather_hourly_1, ["location_key", "observed_hour_utc"]
)
