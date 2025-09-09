## GCP Dataproc script to create iceberg files using apache spark
To run the script, you need to setup a GCP cloud sql postgres database with a private IP address

- Create a secret and a GCS bucket
- Download and save the required JDBC driver (org.postgresql:postgresql) to your GCS bucket
- Save the postgres_to_iceberg.py, init-actions.sh, and requirements.txt to the same GCS bucket
- Using glcloud cli, create the dataproc cluster:
```
gcloud dataproc clusters create your-cluster-name \
    --region=your-region \
    --initialization-actions=gs://bl-dataproc-resources/init-actions/init-action.sh \
    --metadata=PIP_PACKAGES="google-cloud-secret-manager google-auth"
```
- submit the dataproc job:
```
gcloud dataproc jobs submit pyspark gs://bl-dataproc-resources/resources/postgres_to_iceberg.py \
    --cluster=your-cluster-name \
    --region=your-region \
    --jars=gs://bl-dataproc-resources/jars/postgresql-42.7.3.jar
```