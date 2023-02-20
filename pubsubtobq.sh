cd python
pip3 install grpcio
export CLOUDSDK_PYTHON_SITEPACKAGES=1
gcloud pubsub lite-topics publish arindamsarkar-test --location=us-west1 --message='{"Name": "TestMsg", "Age": 0}' 2> /dev/null || true

export GCP_PROJECT=yadavaja-sandbox
export REGION=us-west1
export SUBNET=projects/yadavaja-sandbox/regions/us-west1/subnetworks/test-subnet1
export GCS_STAGING_LOCATION=gs://arindamsarkar-test 
export JARS="gs://spark-lib/pubsublite/pubsublite-spark-sql-streaming-LATEST-with-dependencies.jar","gs://mugdhapattnaik-test/dependencies/bigtable-hbase-2.x-hadoop-2.3.0.jar","gs://mugdhapattnaik-test/dependencies/hbase-client-2.4.12.jar","gs://mugdhapattnaik-test/dependencies/hbase-shaded-mapreduce-2.4.12.jar","gs://mugdhapattnaik-test/dependencies/hbase-spark-1.0.0.jar"

bin/start.sh \
-- --template=PUBSUBTOBQ \
    --pubsub.to.bq.input.subscription="arindamsarkar-test-sub-lite" \
    --pubsub.to.bq.output.dataset="arindamsarkar_test" \
    --pubsub.to.bq.output.table="pubsubtobq" \
    --pubsub.to.bq.write.mode="overwrite" \
    --pubsub.to.bq.bucket.name="gs://arindamsarkar-test"