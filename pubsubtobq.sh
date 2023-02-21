export GCP_PROJECT=yadavaja-sandbox
export REGION=us-west1
export SUBNET=projects/yadavaja-sandbox/regions/us-west1/subnetworks/test-subnet1
export GCS_STAGING_LOCATION="gs://arindamsarkar-test"
export JARS="gs://spark-lib/pubsublite/pubsublite-spark-sql-streaming-LATEST-with-dependencies.jar,gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"

cd python

./bin/start.sh \
-- --template=PUBSUBTOBQ \
--pubsub.to.bq.input.subscription=arindamsarkar-test-sub-lite \
--pubsub.to.bq.project.id=yadavaja-sandbox \
--pubsub.to.bq.output.dataset=arindamsarkar_test \
--pubsub.to.bq.output.table=pubsubtobq \
--pubsub.to.bq.write.mode=overwrite \
--pubsub.to.bq.bucket.name=arindamsarkar-test \
--pubsub.to.bq.checkpoint.location=gs://arindamsarkar-test/pubsubtogcs-checkpoint-location