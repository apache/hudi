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

import org.apache.hudi.common.config.TypedProperties;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

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
    syncConfig = new BigQuerySyncConfig();
    syncConfig.projectId = "fooproject";
    syncConfig.datasetName = "foodataset";
    syncConfig.datasetLocation = "US";
    syncConfig.tableName = "footable";
    syncConfig.sourceUri = "gs://test-bucket/dwh/table_name/dt=*";
    syncConfig.sourceUriPrefix = "gs://test-bucket/dwh/table_name/";
    syncConfig.basePath = "gs://test-bucket/dwh/table_name";
    syncConfig.partitionFields = Arrays.asList("a", "b");
    syncConfig.useFileListingFromMetadata = true;
    syncConfig.assumeDatePartitioning = true;
    syncConfig.help = true;
  }

  @Test
  public void testCopy() {
    BigQuerySyncConfig copied = BigQuerySyncConfig.copy(syncConfig);
    assertEquals(copied.partitionFields, syncConfig.partitionFields);
    assertEquals(copied.basePath, syncConfig.basePath);
    assertEquals(copied.projectId, syncConfig.projectId);
    assertEquals(copied.datasetName, syncConfig.datasetName);
    assertEquals(copied.datasetLocation, syncConfig.datasetLocation);
    assertEquals(copied.tableName, syncConfig.tableName);
    assertEquals(copied.sourceUri, syncConfig.sourceUri);
    assertEquals(copied.sourceUriPrefix, syncConfig.sourceUriPrefix);
    assertEquals(copied.useFileListingFromMetadata, syncConfig.useFileListingFromMetadata);
    assertEquals(copied.assumeDatePartitioning, syncConfig.assumeDatePartitioning);
    assertEquals(copied.help, syncConfig.help);
  }

  @Test
  public void testToProps() {
    TypedProperties props = syncConfig.toProps();
    assertEquals("fooproject", props.getString(BIGQUERY_SYNC_PROJECT_ID));
    assertEquals("foodataset", props.getString(BIGQUERY_SYNC_DATASET_NAME));
    assertEquals("US", props.getString(BIGQUERY_SYNC_DATASET_LOCATION));
    assertEquals("footable", props.getString(BIGQUERY_SYNC_TABLE_NAME));
    assertEquals("gs://test-bucket/dwh/table_name/dt=*", props.getString(BIGQUERY_SYNC_SOURCE_URI));
    assertEquals("gs://test-bucket/dwh/table_name/", props.getString(BIGQUERY_SYNC_SOURCE_URI_PREFIX));
    assertEquals("gs://test-bucket/dwh/table_name", props.getString(BIGQUERY_SYNC_SYNC_BASE_PATH));
    assertEquals("a,b", props.getString(BIGQUERY_SYNC_PARTITION_FIELDS));
    assertEquals("true", props.getString(BIGQUERY_SYNC_USE_FILE_LISTING_FROM_METADATA));
    assertEquals("true", props.getString(BIGQUERY_SYNC_ASSUME_DATE_PARTITIONING));
  }

  @Test
  public void fromProps() {
    TypedProperties props = new TypedProperties();
    props.put(BIGQUERY_SYNC_PROJECT_ID, "fooproject");
    props.put(BIGQUERY_SYNC_DATASET_NAME, "foodataset");
    props.put(BIGQUERY_SYNC_DATASET_LOCATION, "US");
    props.put(BIGQUERY_SYNC_TABLE_NAME, "footable");
    props.put(BIGQUERY_SYNC_SOURCE_URI, "gs://test-bucket/dwh/table_name/dt=*");
    props.put(BIGQUERY_SYNC_SOURCE_URI_PREFIX, "gs://test-bucket/dwh/table_name/");
    props.put(BIGQUERY_SYNC_SYNC_BASE_PATH, "gs://test-bucket/dwh/table_name");
    props.put(BIGQUERY_SYNC_PARTITION_FIELDS, "a,b");
    props.put(BIGQUERY_SYNC_USE_FILE_LISTING_FROM_METADATA, true);
    props.put(BIGQUERY_SYNC_ASSUME_DATE_PARTITIONING, true);
    BigQuerySyncConfig cfg = BigQuerySyncConfig.fromProps(props);

    assertEquals(syncConfig.projectId, cfg.projectId);
    assertEquals(syncConfig.datasetName, cfg.datasetName);
    assertEquals(syncConfig.datasetLocation, cfg.datasetLocation);
    assertEquals(syncConfig.tableName, cfg.tableName);
    assertEquals(syncConfig.sourceUri, cfg.sourceUri);
    assertEquals(syncConfig.sourceUriPrefix, cfg.sourceUriPrefix);
    assertEquals(syncConfig.basePath, cfg.basePath);
    assertEquals(syncConfig.partitionFields, cfg.partitionFields);
    assertEquals(syncConfig.useFileListingFromMetadata, cfg.useFileListingFromMetadata);
    assertEquals(syncConfig.assumeDatePartitioning, cfg.assumeDatePartitioning);
  }
}
