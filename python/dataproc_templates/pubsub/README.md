Template for reading files from Cloud Pub/sub and writing them to a BigQuery table.

It uses the [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example) for writing to BigQuery.

## Arguments

* `pubsub.to.bq.input.subscription`: Cloud Storage location of the input files (format: `gs://bucket/...`)
* `pubsub.to.bq.write.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists)(Defaults to append)
* `pubsub.to.bq.output.dataset`: BigQuery output dataset name
* `pubsub.to.bq.output.table`: BigQuery output table name
* `pubsub.to.bq.bucket.name`: Temporary bucket for the Spark BigQuery connector

## Usage

```
$ python main.py --template PUBSUBTOBQ --help

usage: main.py --template PUBSUBTOBQ [-h] \
    --pubsub.to.bq.input.subscription PUBSUB.BIGQUERY.INPUT.SUBSCRIPTION \
    --pubsub.to.bq.output.dataset PUBSUB.BIGQUERY.OUTPUT.DATASET \
    --pubsub.to.bq.output.table PUBSUB.BIGQUERY.OUTPUT.TABLE \
    --pubsub.to.bq.bucket.name PUBSUB.BIGQUERY.BUCKET.NAME \
    [--pubsub.to.bq.write.mode {overwrite,append,ignore,errorifexists}]

optional arguments:
  -h, --help            show this help message and exit
  --pubsub.to.bq.input.subscription PUBSUB.BIGQUERY.INPUT.SUBSCRIPTION
                        Pubsub to BQ Input subscription name
  --pubsub.to.bq.output.dataset PUBSUB.BIGQUERY.OUTPUT.DATASET
                        BigQuery dataset for the output table
  --pubsub.to.bq.output.table PUBSUB.BIGQUERY.OUTPUT.TABLE
                        BigQuery output table name
  --pubsub.to.bq.bucket.name PUBSUB.BIGQUERY.BUCKET.NAME
                        Spark BigQuery connector temporary bucket
  --pubsub.to.bq.write.mode {overwrite,append,ignore,errorifexists}
                        Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)
```

## Required JAR files

This template requires the [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example) to be available in the Dataproc cluster.

## Example submission

```
export GCP_PROJECT=<project_id>
export REGION=<region>
export GCS_STAGING_LOCATION=<gcs-staging-bucket-name> 
export JARS="gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"

./bin/start.sh \
-- --template=PUBSUBTOBQ \
    --pubsub.to.bq.input.subscription="<pub/sub subscription name>" \
    --pubsub.to.bq.output.dataset="<dataset>" \
    --pubsub.to.bq.output.table="<table>" \
    --pubsub.to.bq.write.mode=<append|overwrite|ignore|errorifexists>\
    --pubsub.to.bq.bucket.name="<temp-bq-bucket-name>"
```