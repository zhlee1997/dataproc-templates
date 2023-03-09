package com.google.cloud.dataproc.templates.pubsublite;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;

import com.google.cloud.dataproc.templates.BaseTemplate;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubsubliteToGCS implements BaseTemplate {
    public static final Logger LOGGER = LoggerFactory.getLogger(PubsubliteToGCS.class);

@Override
public void runTemplate() {
    validateInput();
}

@Override
public void validateInput() {

}
}