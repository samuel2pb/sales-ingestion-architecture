import os
import re
from unicodedata import normalize
import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from google.cloud import bigquery
from google.cloud import storage

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# NOTE: The date is passed as an argument
PROJECT_ID = 'retail-engine'
CATALOG_NAME = 'catalog'
DATABASE_NAME = 'abinbev'
TABLE_NAME = 'stage_sales'
TMP_BUCKET = 'abinbev-tmp'
RAW_BUCKET = 'abinbev-raw'
ARTEFACTS_BUCKET = 'abinbev-artefacts'
JARS_PATH = '/mnt/c/users/samuc/downloads'
GCS_JAR = "gcs-connector-hadoop3-2.2.16-shaded.jar"
BIG_QUERY_JAR = 'spark-3.3-bigquery-0.32.0.jar'
SQL_PATH = "sql/"
CREDENTIALS = "/mnt/e/reps/GCP/retail-engine-6d2031bd8b3b.json"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS
execution_date = None
parser = argparse.ArgumentParser()
parser.add_argument(
    "--logical-date", help="Date of logical execution YYYYMMDD", required=True)


class Pipeline:
    def __init__(self):
        self.spark = SparkSession.builder.appName("Create stage") \
            .config("spark.jars",
                    f"{JARS_PATH}/{BIG_QUERY_JAR},{JARS_PATH}/{GCS_JAR}") \
            .config("spark.hadoop.fs.gs.impl",
                    "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
            .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                    CREDENTIALS) \
            .getOrCreate()

    @staticmethod
    def normalize_string(s):
        removed = normalize('NFKD', s).encode(
            'ASCII', 'ignore').decode('ASCII')
        removed = re.sub(r'[^\w\s]', ' ', removed)
        normalized_words = removed.split()
        normalized_string = '_'.join(normalized_words)
        return normalized_string.upper()

    @staticmethod
    def read_sql_file(file_name):
        client = storage.Client(project=PROJECT_ID)
        bucket = client.get_bucket(ARTEFACTS_BUCKET)
        blob_name = f"sql/{file_name}"
        blob = bucket.blob(blob_name)
        file_content = blob.download_as_bytes()
        query = file_content.decode('utf-8')
        return query

    @staticmethod
    def run_bq_query(**kwargs):
        project_id = kwargs['project_id']
        query = kwargs['query']
        try:
            client = bigquery.Client(project_id)
            job = client.query(query)
            job.result()
        except Exception as e:
            print(e)

    def stage(self, execution_date):
        logging.info("Starting the stage process...")
        FILE_PATH_CHANNEL = f"gs://{RAW_BUCKET}/sales/abi_bus_case1_beverage_channel_group_{execution_date}.csv"
        FILE_PATH_SALES = f"gs://{RAW_BUCKET}/sales/abi_bus_case1_beverage_sales_{execution_date}.csv"

        # NOTE: The files differ in encoding and delimiter
        channel = self.spark.read \
            .option("sep", ",") \
            .option("inferSchema", True) \
            .option("header", "true") \
            .option("encoding", "utf-8") \
            .option("multiline", "true") \
            .csv(FILE_PATH_CHANNEL)

        sales = self.spark.read \
            .option('sep', '\t') \
            .option("inferSchema", True) \
            .option('header', 'true') \
            .option('encoding', 'UTF-16') \
            .option('multiline', 'true') \
            .csv(FILE_PATH_SALES)

        stage = sales.join(channel, on="TRADE_CHNL_DESC", how='left')

        # NOTE: The columns have spaces and special characters
        for column in stage.columns:
            stage = stage.withColumnRenamed(
                column, self.normalize_string(column))

        stage = stage.withColumn("DATE", to_date(col("DATE"), "M/d/yyyy"))

        stage.write.format("bigquery") \
            .option("project", f"{PROJECT_ID}") \
            .option("temporaryGcsBucket", TMP_BUCKET) \
            .mode("overwrite") \
            .option("dataset", f"{DATABASE_NAME}") \
            .option("table", f"{TABLE_NAME}") \
            .save()
        logging.info("SUCCESS")
        logging.info(f"Saved to {DATABASE_NAME}.{TABLE_NAME}")

    def ssot(self):
        # NOTE: The table has no primary key
        # One has been added in order to demonstrate the use of MERGE
        # On production transactional tables should have a primary key
        logging.info("Running SSOT query")
        sql_file = "ssot_sales.sql"
        query = self.read_sql_file(sql_file)

        params = {'project_id': PROJECT_ID,
                  'query': query}
        self.run_bq_query(**params)
        logging.info("SUCCESS")

    def warehouse(self):
        logging.info("Running warehouse queries...")
        client = storage.Client(project=PROJECT_ID)
        bucket = client.get_bucket(ARTEFACTS_BUCKET)
        blobs = bucket.list_blobs(prefix=SQL_PATH)
        logging.info(f"ARTEFACTS on :  {ARTEFACTS_BUCKET}/{SQL_PATH}...")
        for blob in blobs:
            file_name = os.path.basename(blob.name)
            if file_name.startswith("dim") or file_name.startswith("ft"):
                file_content = blob.download_as_bytes()
                query = file_content.decode('utf-8')
                params = {
                    'project_id': PROJECT_ID,
                    'query': query
                }
                logging.info(f"Running {file_name} ")
                self.run_bq_query(**params)
        logging.info("SUCCESS")

    def workflow(self, execution_date):
        logging.info("Starting the workflow...")
        self.stage(execution_date)
        self.ssot()
        self.warehouse()
        logging.info("Running Pipeline was a Success...")
        logging.info("Shuting down Spark...")
        self.spark.stop()

    def main(self):
        args = parser.parse_args()
        args_dict = vars(args)
        execution_date = args_dict['logical_date']
        self.workflow(execution_date)


if __name__ == "__main__":
    pipeline = Pipeline()
    pipeline.main()
