/*
 * Copyright (C) 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataproc.templates.pubsublite;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;

import com.google.cloud.dataproc.templates.BaseTemplate;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubSubLiteToGCS implements BaseTemplate {
  private static final Logger LOGGER = LoggerFactory.getLogger(PubSubLiteToGCS.class);
  private String pubsublite_subscription;

  public PubSubLiteToGCS() {
    pubsublite_subscription = "";
  }

  @Override
  public void runTemplate() throws StreamingQueryException, TimeoutException {
    validateInput();

    // Initialize the Spark session
    SparkSession spark =
        SparkSession.builder().appName("Spark PubSubLiteToGCS Demo Job").getOrCreate();

    // Stream data from Kafka topic
    Dataset<Row> df =
        spark
            .readStream()
            .format("pubsublite")
            .option("pubsublite.subscription", pubsublite_subscription)
            .load();

    StreamingQuery query =
        df.writeStream()
            .format("console")
            .outputMode(OutputMode.Append())
            .trigger(Trigger.ProcessingTime(1, TimeUnit.SECONDS))
            .start();

    // Wait enough time to execute query
    query.awaitTermination(60 * 1000); // 60s
    query.stop();

    LOGGER.info("Job completed.");
    spark.stop();
  }

  public void validateInput() {}
}
