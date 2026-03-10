from google.cloud import bigquery

client = bigquery.Client(project="healthcare-pipeline-489402")
dataset_id = "healthcare_analytics"

dim_provider_schema = [
    bigquery.SchemaField("provider_id", "INTEGER"),
    bigquery.SchemaField("provider_name", "STRING"),
    bigquery.SchemaField("first_name", "STRING"),
    bigquery.SchemaField("credentials", "STRING"),
    bigquery.SchemaField("entity_code", "STRING"),
    bigquery.SchemaField("address", "STRING"),
    bigquery.SchemaField("city", "STRING"),
    bigquery.SchemaField("state", "STRING"),
    bigquery.SchemaField("provider_type", "STRING"),
]

dim_procedure_schema = [
    bigquery.SchemaField("hcpcs_code", "STRING"),
    bigquery.SchemaField("hcpcs_desc", "STRING"),
    bigquery.SchemaField("drug_indicator", "STRING"),
    bigquery.SchemaField("place_of_service", "STRING"),
]

fact_claims_schema = [
    bigquery.SchemaField("provider_id", "INTEGER"),
    bigquery.SchemaField("hcpcs_code", "STRING"),
    bigquery.SchemaField("total_beneficiaries", "INTEGER"),
    bigquery.SchemaField("total_services", "FLOAT"),
    bigquery.SchemaField("total_bene_day_services", "INTEGER"),
    bigquery.SchemaField("avg_submitted_charge", "FLOAT"),
    bigquery.SchemaField("avg_medicare_allowed", "FLOAT"),
    bigquery.SchemaField("avg_medicare_payment", "FLOAT"),
    bigquery.SchemaField("avg_standardized_amount", "FLOAT"),
    bigquery.SchemaField("total_submitted_charge", "FLOAT"),
    bigquery.SchemaField("total_medicare_payment", "FLOAT"),
    bigquery.SchemaField("payment_ratio", "FLOAT"),
]

def create_table(table_name, schema):
    table_ref = client.dataset(dataset_id).table(table_name)
    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table, exists_ok=True)
    print(f"Table {table_name} created.")

create_table("dim_provider", dim_provider_schema)
create_table("dim_procedure", dim_procedure_schema)
create_table("fact_claims", fact_claims_schema)

def load_from_gcs(table_name, gcs_uri, schema):
    table_ref = client.dataset(dataset_id).table(table_name)
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    load_job.result()
    table = client.get_table(table_ref)
    print(f"Loaded {table.num_rows} rows into {table_name}.")

load_from_gcs(
    "dim_provider",
    "gs://healthcare-pipeline-raw-data/curated/medicare_final.parquet/*.parquet",
    dim_provider_schema
)

load_from_gcs(
    "dim_procedure",
    "gs://healthcare-pipeline-raw-data/curated/medicare_final.parquet/*.parquet",
    dim_procedure_schema
)

load_from_gcs(
    "fact_claims",
    "gs://healthcare-pipeline-raw-data/curated/medicare_final.parquet/*.parquet",
    fact_claims_schema
)

print("BigQuery load complete.")
