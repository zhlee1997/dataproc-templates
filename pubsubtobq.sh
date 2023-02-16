cd python
pip3 install grpcio
export CLOUDSDK_PYTHON_SITEPACKAGES=1
gcloud pubsub lite-topics publish arindamsarkar-test --location=us-west1 --message='{"Name": "TestMsg", "Age": 0}' 2> /dev/null || true

export GCP_PROJECT=yadavaja-sandbox
export REGION=us-west1
export SUBNET=projects/yadavaja-sandbox/regions/us-west1/subnetworks/test-subnet1
export GCS_STAGING_LOCATION=gs://arindamsarkar-test 
export JARS="gs://arindamsarkar-test/pubsublite-jars/pubsublite-spark-sql-streaming-1.0.0-with-dependencies.jar"

bin/start.sh \
-- --template=PUBSUBTOBQ \
    --pubsub.to.bq.input.subscription="arindamsarkar-test-sub-lite" \
    --pubsub.to.bq.output.dataset="arindamsarkar_test" \
    --pubsub.to.bq.output.table="pubsubtobq" \
    --pubsub.to.bq.write.mode="overwrite" \
    --pubsub.to.bq.bucket.name="gs://arindamsarkar-test"