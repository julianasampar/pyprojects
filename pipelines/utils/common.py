## Importing libraries to execute querying on BigQuery

from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage
import pipelines.personal_env as penv

## Importing Credentials from Google Cloud

CREDENTIALS = service_account.Credentials.from_service_account_file(penv.bq_path)
BIGQUERY = bigquery.Client(credentials=CREDENTIALS)

## Importing Credentials from Google Cloud

STORAGE = storage.Client(credentials=CREDENTIALS)

# Acessing Bucket Path

bucket = STORAGE.get_bucket(penv.bucket_path)