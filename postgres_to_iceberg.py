import json
import google.auth
from pyspark.sql import SparkSession
from google.cloud import secretmanager

# --- Configuration ---
SECRET_ID = "cloud-sql-dataproc-connection" 
GCS_BUCKET = "gs://bl-dataproc-resources"

# --- Tables to process ---
# A list of tuples: (source_postgres_table, target_iceberg_table)
TABLES_TO_CREATE = [
    ("prod.v_repo_contributors", "public_research.contributor_repo_commits_v2"),
    ("prod.latest_top_contributors", "public_research.contributors")
]

# --- Automatically discover the GCP Project ID ---
credentials, GCP_PROJECT_ID = google.auth.default()
print(f"INFO:     Successfully discovered project ID: {GCP_PROJECT_ID}")

def get_secret(project_id, secret_id, version_id="latest"):
    """Fetches a secret from Google Cloud Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

# function to create a single Iceberg table
def create_iceberg_table_from_postgres(spark, jdbc_url, db_user, db_password, source_table, target_table):
    """
    Reads data from a PostgreSQL table and writes it to a new or replacement Iceberg table.
    """
    print(f"--- Processing: {source_table} -> {target_table} ---")
    
    # --- Read from PostgreSQL ---
    print(f"INFO:     Reading data from PostgreSQL table '{source_table}'")
    try:
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", source_table) \
            .option("user", db_user) \
            .option("password", db_password) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        print(f"INFO:     Successfully loaded data from '{source_table}'")
    except Exception as e:
        print(f"ERROR:    Failed to read from '{source_table}'. Error: {e}")
        return # Skip to the next table if reading fails

    # --- Write the DataFrame as an Iceberg table ---
    print(f"INFO:     Writing Iceberg table '{target_table}'")
    try:
        # Ensure the schema exists before writing
        schema = target_table.split('.')[0]
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        
        df.writeTo(target_table).createOrReplace()
        print(f"INFO:     Successfully created or replaced Iceberg table '{target_table}'")
    except Exception as e:
        print(f"ERROR:    Failed to write Iceberg table '{target_table}'. Error: {e}")
    
    print("-" * (len(source_table) + len(target_table) + 20))


# --- Main script execution ---

# --- Fetch the secret using the discovered project ID ---
db_config_json = get_secret(GCP_PROJECT_ID, SECRET_ID)
db_config = json.loads(db_config_json)

# --- Setup Spark Session with Iceberg Catalog Configuration ---
spark = SparkSession.builder \
    .appName("Postgres to Iceberg Direct") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
    .config("spark.sql.catalog.spark_catalog.warehouse", f"{GCS_BUCKET}/warehouse") \
    .config("spark.sql.default.format-version", "2") \
    .getOrCreate()

print("INFO:     Default 'spark_catalog' is now configured for Iceberg.")

# --- Sanitize secret values and construct the JDBC URL ---
host = db_config['host'].strip()
port = db_config['port'].strip()
dbname = db_config['dbname'].strip()
user = db_config['user'].strip()
password = db_config['password'].strip()

jdbc_url = f"jdbc:postgresql://{host}:{port}/{dbname}"

# Loop through the list of tables and create each one
for source, target in TABLES_TO_CREATE:
    create_iceberg_table_from_postgres(spark, jdbc_url, user, password, source, target)

print("\nINFO:     All jobs completed.")
spark.stop()
