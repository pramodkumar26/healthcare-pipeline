from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, when, lit

spark = SparkSession.builder \
    .appName("healthcare-etl") \
    .getOrCreate()

INPUT_PATH = "gs://healthcare-pipeline-raw-data/raw/medicare_2022.csv"
OUTPUT_PATH = "gs://healthcare-pipeline-raw-data/processed/"

df = spark.read.csv(INPUT_PATH, header=True, inferSchema=True)

DROP_COLS = [
    "Rndrng_Prvdr_MI",
    "Rndrng_Prvdr_St2",
    "Rndrng_Prvdr_RUCA",
    "Rndrng_Prvdr_RUCA_Desc",
    "Rndrng_Prvdr_State_FIPS",
    "Rndrng_Prvdr_Cntry",
    "Rndrng_Prvdr_Zip5",
    "Rndrng_Prvdr_Mdcr_Prtcptg_Ind"
]

df = df.drop(*DROP_COLS)

df = df.withColumnRenamed("Rndrng_NPI", "provider_id") \
    .withColumnRenamed("Rndrng_Prvdr_Last_Org_Name", "provider_name") \
    .withColumnRenamed("Rndrng_Prvdr_First_Name", "first_name") \
    .withColumnRenamed("Rndrng_Prvdr_Crdntls", "credentials") \
    .withColumnRenamed("Rndrng_Prvdr_Ent_Cd", "entity_code") \
    .withColumnRenamed("Rndrng_Prvdr_St1", "address") \
    .withColumnRenamed("Rndrng_Prvdr_City", "city") \
    .withColumnRenamed("Rndrng_Prvdr_State_Abrvtn", "state") \
    .withColumnRenamed("Rndrng_Prvdr_Type", "provider_type") \
    .withColumnRenamed("HCPCS_Cd", "hcpcs_code") \
    .withColumnRenamed("HCPCS_Desc", "hcpcs_desc") \
    .withColumnRenamed("HCPCS_Drug_Ind", "drug_indicator") \
    .withColumnRenamed("Place_Of_Srvc", "place_of_service") \
    .withColumnRenamed("Tot_Benes", "total_beneficiaries") \
    .withColumnRenamed("Tot_Srvcs", "total_services") \
    .withColumnRenamed("Tot_Bene_Day_Srvcs", "total_bene_day_services") \
    .withColumnRenamed("Avg_Sbmtd_Chrg", "avg_submitted_charge") \
    .withColumnRenamed("Avg_Mdcr_Alowd_Amt", "avg_medicare_allowed") \
    .withColumnRenamed("Avg_Mdcr_Pymt_Amt", "avg_medicare_payment") \
    .withColumnRenamed("Avg_Mdcr_Stdzd_Amt", "avg_standardized_amount")

df = df.fillna({
    "first_name": "Unknown",
    "credentials": "Unknown"
})

df = df.withColumn(
    "total_submitted_charge",
    round(col("total_services") * col("avg_submitted_charge"), 2)
).withColumn(
    "total_medicare_payment",
    round(col("total_services") * col("avg_medicare_payment"), 2)
).withColumn(
    "payment_ratio",
    round(col("avg_medicare_payment") / when(col("avg_submitted_charge") == 0, 1)
    .otherwise(col("avg_submitted_charge")), 4)
)
df = df.withColumn("provider_type_col", col("provider_type")) \
       .withColumn("state_col", col("state"))

df = df.filter(
    (col("avg_submitted_charge") > 0) &
    (col("total_services") > 0) &
    (col("total_beneficiaries") >= 11)
)

print(f"Final row count: {df.count()}")
print(f"Final column count: {len(df.columns)}")
df.printSchema()

df.write.mode("overwrite") \
    .partitionBy("provider_type", "state") \
    .parquet(OUTPUT_PATH)

print("ETL complete. Data written to GCS.")

df.write.mode("overwrite") \
    .parquet("gs://healthcare-pipeline-raw-data/curated/medicare_final.parquet")

df.select(
    "provider_id", "provider_name", "first_name", "credentials",
    "entity_code", "address", "city", "state", "provider_type"
).dropDuplicates(["provider_id"]) \
 .write.mode("overwrite") \
 .parquet("gs://healthcare-pipeline-raw-data/curated/dim_provider.parquet")

df.select(
    "hcpcs_code", "hcpcs_desc", "drug_indicator", "place_of_service"
).dropDuplicates(["hcpcs_code"]) \
 .write.mode("overwrite") \
 .parquet("gs://healthcare-pipeline-raw-data/curated/dim_procedure.parquet")

df.select(
    "provider_id", "hcpcs_code", "total_beneficiaries", "total_services",
    "total_bene_day_services", "avg_submitted_charge", "avg_medicare_allowed",
    "avg_medicare_payment", "avg_standardized_amount", "total_submitted_charge",
    "total_medicare_payment", "payment_ratio"
).write.mode("overwrite") \
 .parquet("gs://healthcare-pipeline-raw-data/curated/fact_claims.parquet")


spark.stop()