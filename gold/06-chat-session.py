# Databricks notebook source
# MAGIC %md
# MAGIC # tb_chat_session
# MAGIC Atualiza somente esta base Gold.
# COMMAND ----------
# MAGIC %run ./00-gold-common
# COMMAND ----------
cut = F.lit(ASOF).cast("timestamp")
# Vínculo privado: UUID persistido, em vez de hash previsível de IDs pequenos.
ensure_user_keys()
validation_73_1 = frames["s_sessions"]
validation_73_1 = validation_73_1.select("*")
unique(validation_73_1, ["session_id"], "session_id duplicado.")
validation_74_1 = frames["s_messages"]
validation_74_1 = validation_74_1.select("*")
unique(validation_74_1, ["session_id", "message_index"], "Mensagem duplicada.")
validation_75_1 = frames["s_feedback"]
validation_75_1 = validation_75_1.select("*")
unique(validation_75_1, ["feedback_id"], "feedback_id duplicado.")
v_sessions_1 = frames["s_sessions"].alias("s")
v_sessions_1 = v_sessions_1.join(
    frames["v_user_key"].alias("u"), (F.col("s.user_id") == F.col("u.user_id")), "inner"
)
v_sessions_1 = v_sessions_1.filter(
    (
        (
            ((F.col("s.is_deleted") == F.lit(False)) & (~F.col("s.session_id").isNull()))
            & (~F.col("s.started_at").isNull())
        )
        & (F.col("s.started_at") <= F.lit(ASOF).cast("TIMESTAMP"))
    )
)
v_sessions_1 = v_sessions_1.select(
    "s.*",
    F.col("u.user_key").alias("user_key"),
    F.to_date(
        F.from_utc_timestamp(F.col("started_at").cast("TIMESTAMP"), F.lit("America/Sao_Paulo"))
    ).alias("session_start_date"),
)
view("v_sessions", v_sessions_1)
v_messages_1 = frames["s_messages"].alias("m")
v_messages_1 = v_messages_1.join(
    frames["v_sessions"].alias("s"), (F.col("m.session_id") == F.col("s.session_id")), "inner"
)
v_messages_1 = v_messages_1.filter(
    (
        ((F.col("m.content_type") == F.lit("text")) & (F.col("m.message_index") >= F.lit(0)))
        & ((F.col("m.sent_at").isNull() | (F.col("m.sent_at") <= F.lit(ASOF).cast("TIMESTAMP"))))
    )
)
v_messages_1 = v_messages_1.select(
    F.col("m.session_id").alias("session_id"),
    F.col("m.message_index").alias("message_index"),
    F.col("m.role").alias("role"),
    F.col("m.content_type").alias("content_type"),
    F.col("m.sent_at").alias("sent_at"),
    F.col("m.api_status_code").alias("api_status_code"),
    F.col("s.user_key").alias("user_key"),
    F.col("s.session_start_date").alias("session_start_date"),
    F.to_date(
        F.from_utc_timestamp(F.col("m.sent_at").cast("TIMESTAMP"), F.lit("America/Sao_Paulo"))
    ).alias("message_date"),
)
view("v_messages", v_messages_1)
v_feedback_subquery_2 = frames["s_feedback"]
v_feedback_subquery_2 = v_feedback_subquery_2.filter(
    (
        ((~F.col("is_satisfied").isNull()) & (~F.col("created_at").isNull()))
        & (F.col("created_at") <= F.lit(ASOF).cast("TIMESTAMP"))
    )
)
v_feedback_subquery_2 = v_feedback_subquery_2.select(
    "*",
    F.row_number()
    .over(
        Window.partitionBy(F.col("session_id")).orderBy(
            F.col("created_at").desc_nulls_last(),
            F.col("_ingested_at").desc_nulls_last(),
            F.col("feedback_id").desc_nulls_last(),
        )
    )
    .alias("position"),
)
v_feedback_1 = v_feedback_subquery_2
v_feedback_1 = v_feedback_1.filter((F.col("position") == F.lit(1)))
v_feedback_1 = v_feedback_1.select(
    *[F.col("" + name) for name in v_feedback_subquery_2.columns if name not in ["position"]]
)
view("v_feedback", v_feedback_1)
b_tb_feedback_orphan_1 = frames["s_feedback"].alias("f")
b_tb_feedback_orphan_1 = b_tb_feedback_orphan_1.join(
    frames["s_sessions"].alias("s"), (F.col("f.session_id") == F.col("s.session_id")), "left_anti"
)
b_tb_feedback_orphan_1 = b_tb_feedback_orphan_1.select(
    F.col("f.feedback_id").alias("feedback_id"),
    F.col("f.session_id").alias("session_id"),
    F.col("f.created_at").alias("created_at"),
    F.lit("SESSION_NOT_FOUND").alias("quality_status"),
)
view("b_tb_feedback_orphan", b_tb_feedback_orphan_1)
v_response_indices_1 = frames["v_messages"]
v_response_indices_1 = v_response_indices_1.select(
    "*",
    F.min(F.when((F.col("role") == F.lit("user")), F.col("message_index")).otherwise(F.lit(None)))
    .over(
        Window.partitionBy(F.col("session_id"))
        .orderBy(F.col("message_index").asc_nulls_first())
        .rowsBetween(1, Window.unboundedFollowing)
    )
    .alias("next_user_index"),
    F.min(F.when((F.col("role") == F.lit("bot")), F.col("message_index")).otherwise(F.lit(None)))
    .over(
        Window.partitionBy(F.col("session_id"))
        .orderBy(F.col("message_index").asc_nulls_first())
        .rowsBetween(1, Window.unboundedFollowing)
    )
    .alias("next_bot_index"),
)
view("v_response_indices", v_response_indices_1)
tb_chat_response_pair_1 = frames["v_response_indices"].alias("u")
tb_chat_response_pair_1 = tb_chat_response_pair_1.join(
    frames["v_messages"].alias("b"),
    (
        (F.col("b.session_id") == F.col("u.session_id"))
        & (F.col("b.message_index") == F.col("u.next_bot_index"))
    ),
    "inner",
)
tb_chat_response_pair_1 = tb_chat_response_pair_1.filter(
    (
        (
            (
                (
                    (F.col("u.role") == F.lit("user"))
                    & (
                        (
                            F.col("u.next_user_index").isNull()
                            | (F.col("u.next_bot_index") < F.col("u.next_user_index"))
                        )
                    )
                )
                & (~F.col("u.sent_at").isNull())
            )
            & (~F.col("b.sent_at").isNull())
        )
        & (F.col("b.sent_at") >= F.col("u.sent_at"))
    )
)
tb_chat_response_pair_1 = tb_chat_response_pair_1.select(
    F.col("u.session_id").alias("session_id"),
    F.col("u.message_index").alias("user_message_index"),
    F.col("b.message_index").alias("bot_message_index"),
    F.col("u.user_key").alias("user_key"),
    F.col("u.session_start_date").alias("session_start_date"),
    (
        ((F.unix_micros(F.col("b.sent_at")) - F.unix_micros(F.col("u.sent_at"))))
        / F.lit(Decimal("1000000.0"))
    ).alias("delay_seconds"),
)
view("b_tb_chat_response_pair", tb_chat_response_pair_1)
tb_chat_session = (frames['v_sessions'].alias('s')
    .join(frames['v_feedback'].alias('f'), F.col('s.session_id') == F.col('f.session_id'), 'left')
    .select('s.session_id', 's.user_key', 's.started_at', 's.last_activity_at',
            's.session_start_date', 's.is_open', 's.is_active',
            'f.feedback_id', 'f.is_satisfied', F.col('f.created_at').alias('feedback_at')))
save_table('gold_bi', 'tb_chat_session', tb_chat_session, ['session_id'])
