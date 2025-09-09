import json
import google.auth
from pyspark.sql import SparkSession
from google.cloud import secretmanager

# --- Configuration ---
SECRET_ID = "cloud-sql-dataproc-connection" 
GCS_BUCKET = "gs://bl-dataproc-resources"
TABLE_NAME = "public_research.contributor_repo_commits"

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

# --- Setup Spark Session ---
spark = SparkSession.builder \
    .appName("Postgres to Iceberg Direct") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.my_iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_iceberg_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_iceberg_catalog.warehouse", f"{GCS_BUCKET}/warehouse") \
    .getOrCreate()

# --- Read from PostgreSQL using the Private IP ---
jdbc_url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/{db_config['dbname']}"

df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "prod.latest_project_repos_contributors") \
    .option("user", db_config['user']) \
    .option("password", db_config['password']) \
    .option("driver", "org.postgresql.Driver") \
    .load()

# --- Write the DataFrame as an Iceberg table ---
df.write.format("iceberg").mode("overwrite").save(TABLE_NAME)

print(f"Successfully created Iceberg table '{TABLE_NAME}'")
spark.stop()