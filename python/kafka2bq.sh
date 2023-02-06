export GCP_PROJECT=yadavaja-sandbox
export REGION=us-west1 
export GCS_STAGING_LOCATION='gs://anshumanwins-test'
export JARS="gs://dataproc-templates-jrs/latest/python/kafka-clients-3.2.0.jar,gs://dataproc-templates-jrs/latest/python/spark-streaming_2.12-2.4.0.jar,gs://dataproc-templates-jrs/latest/python/spark-core_2.12-2.4.0.jar,gs://dataproc-templates-jrs/latest/python/spark-sql-kafka-0-10_2.12-3.2.1.jar,gs://dataproc-templates-jrs/latest/python/spark-token-provider-kafka-0-10_2.12-3.0.0.jar,gs://dataproc-templates-jrs/latest/python/commons-pool2-2.8.0.jar,gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"

./bin/start.sh \
-- --template=KAFKATOBQ \
    --kafka.to.bq.checkpoint.location="gs://anshumanwins-test/kafka2bq_temp/checkpoints/" \
    --kafka.bootstrap.servers="10.0.0.2:9093" \
    --kafka.bq.topic="test" \
    --kafka.to.bq.starting.offset="earliest" \
    --kafka.to.bq.dataset="anshumanwins_dataset" \
    --kafka.to.bq.table="kafka2bq_test" \
    --kafka.to.bq.temp.bucket.name="anshumanwins-test" \
    --kafka.to.bq.termination.timeout="10000"