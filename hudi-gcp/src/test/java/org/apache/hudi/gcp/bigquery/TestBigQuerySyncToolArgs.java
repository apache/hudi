/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.gcp.bigquery;

import com.beust.jcommander.JCommander;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_BIG_LAKE_CONNECTION_ID;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_DATASET_LOCATION;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_DATASET_NAME;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_PARTITION_FIELDS;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_PROJECT_ID;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_REQUIRE_PARTITION_FILTER;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_SOURCE_URI;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_SOURCE_URI_PREFIX;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_TABLE_NAME;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_USE_BQ_MANIFEST_FILE;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_USE_FILE_LISTING_FROM_METADATA;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestBigQuerySyncToolArgs {

  @Test
  public void testArgsParse() {
    BigQuerySyncConfig.BigQuerySyncConfigParams params = new BigQuerySyncConfig.BigQuerySyncConfigParams();
    JCommander cmd = JCommander.newBuilder().addObject(params).build();
    String[] args = {
        "--project-id", "hudi-bq",
        "--dataset-name", "foobar",
        "--dataset-location", "us-west1",
        "--table", "foobartable",
        "--source-uri", "gs://foobartable/year=*",
        "--source-uri-prefix", "gs://foobartable/",
        "--base-path", "gs://foobartable",
        "--partitioned-by", "year,month,day",
        "--big-lake-connection-id", "connection-id",
        "--use-bq-manifest-file",
        "--use-file-listing-from-metadata",
        "--require-partition-filter"
    };
    cmd.parse(args);

    final Properties props = params.toProps();
    assertEquals("hudi-bq", props.getProperty(BIGQUERY_SYNC_PROJECT_ID.key()));
    assertEquals("foobar", props.getProperty(BIGQUERY_SYNC_DATASET_NAME.key()));
    assertEquals("us-west1", props.getProperty(BIGQUERY_SYNC_DATASET_LOCATION.key()));
    assertEquals("foobartable", props.getProperty(BIGQUERY_SYNC_TABLE_NAME.key()));
    assertEquals("gs://foobartable/year=*", props.getProperty(BIGQUERY_SYNC_SOURCE_URI.key()));
    assertEquals("gs://foobartable/", props.getProperty(BIGQUERY_SYNC_SOURCE_URI_PREFIX.key()));
    assertEquals("year,month,day", props.getProperty(BIGQUERY_SYNC_PARTITION_FIELDS.key()));
    assertEquals("true", props.getProperty(BIGQUERY_SYNC_USE_BQ_MANIFEST_FILE.key()));
    assertEquals("true", props.getProperty(BIGQUERY_SYNC_USE_FILE_LISTING_FROM_METADATA.key()));
    assertEquals("true", props.getProperty(BIGQUERY_SYNC_REQUIRE_PARTITION_FILTER.key()));
    assertEquals("connection-id", props.getProperty(BIGQUERY_SYNC_BIG_LAKE_CONNECTION_ID.key()));
  }
}
