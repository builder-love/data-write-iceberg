import json
import google.auth
from pyspark.sql import SparkSession
from google.cloud import secretmanager

# --- Configuration ---
SECRET_ID = "cloud-sql-dataproc-connection" 
GCS_BUCKET = "gs://bl-dataproc-resources"
TABLE_NAME = "public_research.contributor_repo_commits_v2"

# --- Automatically discover the GCP Project ID ---
credentials, GCP_PROJECT_ID = google.auth.default()
print(f"INFO:     Successfully discovered project ID: {GCP_PROJECT_ID}")

def get_secret(project_id, secret_id, version_id="latest"):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

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

# No longer need 'USE catalog' because we overrode the default
print("INFO:     Default 'spark_catalog' is now configured for Iceberg.")

# Create the schema within the default (now Iceberg) catalog
spark.sql(f"CREATE SCHEMA IF NOT EXISTS public_research")
print("INFO:     Ensured schema 'public_research' exists.")


# --- Sanitize secret values and construct the JDBC URL ---
host = db_config['host'].strip()
port = db_config['port'].strip()
dbname = db_config['dbname'].strip()
user = db_config['user'].strip()
password = db_config['password'].strip()

jdbc_url = f"jdbc:postgresql://{host}:{port}/{dbname}"


# --- Read from PostgreSQL using the Private IP ---
print(f"INFO:     Reading data from PostgreSQL table 'prod.latest_project_repos_contributors'")
df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "prod.latest_project_repos_contributors") \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "org.postgresql.Driver") \
    .load()
print("INFO:     Successfully loaded data from PostgreSQL")

# --- Write the DataFrame as an Iceberg table ---
print(f"INFO:     Writing Iceberg table '{TABLE_NAME}'")
df.writeTo(TABLE_NAME).createOrReplace() # Using writeTo() is a more modern Iceberg practice

print(f"INFO:     Successfully created or replaced Iceberg table '{TABLE_NAME}'")
spark.stop()