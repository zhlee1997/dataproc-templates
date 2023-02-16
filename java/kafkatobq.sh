gsutil rm -r gs://anshumanwins-test/kafka2bq_temp/checkpoints/*

export GCP_PROJECT=yadavaja-sandbox
export REGION=us-west1 
export GCS_STAGING_LOCATION='gs://anshumanwins-test'
export SUBNET="projects/yadavaja-sandbox/regions/us-west1/subnetworks/test-subnet1"

bin/start.sh \
-- \
--template KAFKATOBQ \
--templateProperty project.id=$GCP_PROJECT \
--templateProperty kafka.bq.checkpoint.location='gs://anshumanwins-test/kafka2bq_temp/checkpoints/' \
--templateProperty kafka.bq.bootstrap.servers=10.0.0.2:9093 \
--templateProperty kafka.bq.topic=test \
--templateProperty kafka.bq.starting.offset=earliest \
--templateProperty kafka.bq.dataset=anshumanwins_dataset \
--templateProperty kafka.bq.table=kafka2bq_test \
--templateProperty kafka.bq.temp.gcs.bucket=anshumanwins-test \
--templateProperty kafka.message.format=bytes \
--templateProperty kafka.bq.await.termination.timeout=15000