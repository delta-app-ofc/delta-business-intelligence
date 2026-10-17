import sys

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, current_timestamp, lit
from delta.tables import DeltaTable


args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

spark.conf.set(
    "spark.databricks.delta.schema.autoMerge.enabled",
    "true"
)

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


GLUE_CONNECTION = "delta-telemetry-atlas-connection"

MONGO_DATABASE = "db_delta_telemetry"
MONGO_COLLECTION = "consumption_summary"

S3_DESTINATION = (
    "s3://delta-app-oficial-datalake/"
    "bronze/telemetry/consumption_summary/"
)


mongo_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="mongodb",
    connection_options={
        "connectionName": GLUE_CONNECTION,
        "database": MONGO_DATABASE,
        "collection": MONGO_COLLECTION,
        "partitioner": (
            "com.mongodb.spark.sql.connector.read.partitioner."
            "SinglePartitionPartitioner"
        )
    }
)

df = mongo_frame.toDF()

source_count = df.count()


if source_count == 0:
    print("Collection vazia. Nenhum dado será gravado.")

else:
    df = (
        df
        .withColumn("_id", col("_id").cast("string"))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_database", lit(MONGO_DATABASE))
        .withColumn("_source_collection", lit(MONGO_COLLECTION))
    )

    if DeltaTable.isDeltaTable(spark, S3_DESTINATION):

        def execute_merge():
            spark.catalog.refreshByPath(S3_DESTINATION)

            target = DeltaTable.forPath(
                spark,
                S3_DESTINATION
            )

            (
                target.alias("target")
                .merge(
                    df.alias("source"),
                    "target._id = source._id"
                )
                .withSchemaEvolution()
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

        try:
            execute_merge()

        except Exception as error:
            if "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS" not in str(error):
                raise

            spark.catalog.clearCache()
            spark.catalog.refreshByPath(S3_DESTINATION)

            execute_merge()

        spark.catalog.refreshByPath(S3_DESTINATION)

        metrics = (
            DeltaTable
            .forPath(spark, S3_DESTINATION)
            .history(1)
            .select("operationMetrics")
            .collect()[0]["operationMetrics"]
        )

        inserted_count = int(
            metrics.get("numTargetRowsInserted", 0)
        )

    else:
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .save(S3_DESTINATION)
        )

        inserted_count = source_count


    spark.catalog.refreshByPath(S3_DESTINATION)

    bronze_df = (
        spark.read
        .format("delta")
        .load(S3_DESTINATION)
    )

    print(
        f"\nRegistros inseridos nesta execução: "
        f"{inserted_count}"
    )

    print("\nAmostra da Bronze:")

    bronze_df.show(
        20,
        truncate=False
    )


job.commit()
