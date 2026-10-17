"""Glue Spark: preserva sessões e expande mensagens, incluindo agente e duração."""
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F, Window

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.session.timeZone", "UTC")
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
BUCKET = "s3://delta-app-oficial-datalake"
# Prefixo padrão do Mongo; não depende de função auxiliar.
MONGO_PREFIX = (
    getResolvedOptions(sys.argv, ["MONGO_PREFIX"])["MONGO_PREFIX"]
    if "--MONGO_PREFIX" in sys.argv else "mongo-database"
).strip().strip("/")
if MONGO_PREFIX not in ("", "mongo-database"):
    raise ValueError("MONGO_PREFIX inválido")
root = BUCKET + ("/" + MONGO_PREFIX if MONGO_PREFIX else "")
df = spark.read.format("delta").load(f"{root}/bronze/app/chat_sessions/")

def optional(name, kind):
    return (F.col(name) if name in df.columns else F.lit(None)).cast(kind).alias(name)

df = (
    df.withColumn("_position", F.row_number()
        .over(Window.partitionBy("_id").orderBy(F.col("_ingested_at").desc()))
    )
    .filter("_position = 1").drop("_position")
)

sessions = df.select(
    F.col("_id").cast("string").alias("session_id"),
    optional("user_id", "int"), 
    optional("started_at", "timestamp"), 
    optional("last_activity_at", "timestamp"),
    optional("is_open", "boolean"), 
    optional("is_active", "boolean"), 
    optional("is_deleted", "boolean"), 
    "_ingested_at"
)

message_type = "array<struct<role:string,content_type:string,content:string,sent_at:timestamp,api_status_code:int,agent_name:string,response_time_ms:bigint>>"

messages_array = (
    F.from_json(
        F.to_json("messages"), message_type
    ) if "messages" in df.columns else F.lit(None).cast(message_type)
)

messages = (
    df.select(
        F.col("_id").cast("string").alias("session_id"), 
        "_ingested_at",
        F.posexplode(messages_array).alias("message_index", "message")
    )
    .select(
        "session_id", 
        "message_index", 
        "message.*", 
        "_ingested_at"
    )
)
for name, frame in [("chat_sessions", sessions), ("chat_messages", messages)]:
    (frame.withColumn("_silver_processed_at", F.current_timestamp()).write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true").save(f"{root}/silver/app/{name}/"))
    print(f"Silver atualizada: {name}")
job.commit()
