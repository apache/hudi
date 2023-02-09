/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.gcp.bigquery;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Properties;

import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_ASSUME_DATE_PARTITIONING;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_DATASET_LOCATION;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_DATASET_NAME;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_PARTITION_FIELDS;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_PROJECT_ID;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_SOURCE_URI;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_SOURCE_URI_PREFIX;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_SYNC_BASE_PATH;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_TABLE_NAME;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_USE_FILE_LISTING_FROM_METADATA;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestBigQuerySyncConfig {

  BigQuerySyncConfig syncConfig;

  @BeforeEach
  void setUp() {
    Properties props = new Properties();
    props.setProperty(BIGQUERY_SYNC_PROJECT_ID.key(), "fooproject");
    props.setProperty(BIGQUERY_SYNC_DATASET_NAME.key(), "foodataset");
    props.setProperty(BIGQUERY_SYNC_DATASET_LOCATION.key(), "US");
    props.setProperty(BIGQUERY_SYNC_TABLE_NAME.key(), "footable");
    props.setProperty(BIGQUERY_SYNC_SOURCE_URI.key(), "gs://test-bucket/dwh/table_name/dt=*");
    props.setProperty(BIGQUERY_SYNC_SOURCE_URI_PREFIX.key(), "gs://test-bucket/dwh/table_name/");
    props.setProperty(BIGQUERY_SYNC_SYNC_BASE_PATH.key(), "gs://test-bucket/dwh/table_name");
    props.setProperty(BIGQUERY_SYNC_PARTITION_FIELDS.key(), "a,b");
    props.setProperty(BIGQUERY_SYNC_USE_FILE_LISTING_FROM_METADATA.key(), "true");
    props.setProperty(BIGQUERY_SYNC_ASSUME_DATE_PARTITIONING.key(), "true");
    syncConfig = new BigQuerySyncConfig(props);
  }

  @Test
  public void testGetConfigs() {
    assertEquals("fooproject", syncConfig.getString(BIGQUERY_SYNC_PROJECT_ID));
    assertEquals("foodataset", syncConfig.getString(BIGQUERY_SYNC_DATASET_NAME));
    assertEquals("US", syncConfig.getString(BIGQUERY_SYNC_DATASET_LOCATION));
    assertEquals("footable", syncConfig.getString(BIGQUERY_SYNC_TABLE_NAME));
    assertEquals("gs://test-bucket/dwh/table_name/dt=*", syncConfig.getString(BIGQUERY_SYNC_SOURCE_URI));
    assertEquals("gs://test-bucket/dwh/table_name/", syncConfig.getString(BIGQUERY_SYNC_SOURCE_URI_PREFIX));
    assertEquals("gs://test-bucket/dwh/table_name", syncConfig.getString(BIGQUERY_SYNC_SYNC_BASE_PATH));
    assertEquals(Arrays.asList("a", "b"), syncConfig.getSplitStrings(BIGQUERY_SYNC_PARTITION_FIELDS));
    assertEquals(true, syncConfig.getBoolean(BIGQUERY_SYNC_USE_FILE_LISTING_FROM_METADATA));
    assertEquals(true, syncConfig.getBoolean(BIGQUERY_SYNC_ASSUME_DATE_PARTITIONING));
  }

}
