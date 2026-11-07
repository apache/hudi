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

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Properties;

import static org.apache.hudi.common.config.HoodieMetadataConfig.DEFAULT_METADATA_ENABLE_FOR_READERS;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_ASSUME_DATE_PARTITIONING;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_BILLING_PROJECT_ID;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_DATASET_LOCATION;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_DATASET_NAME;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_PARTITION_FIELDS;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_PROJECT_ID;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_SOURCE_URI;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_SOURCE_URI_PREFIX;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_SYNC_BASE_PATH;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_TABLE_NAME;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_USE_BQ_MANIFEST_FILE;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_USE_FILE_LISTING_FROM_METADATA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestBigQuerySyncConfig {

  @Test
  public void testGetConfigs() {
    Properties props = new Properties();
    props.setProperty(BIGQUERY_SYNC_PROJECT_ID.key(), "fooproject");
    props.setProperty(BIGQUERY_SYNC_BILLING_PROJECT_ID.key(), "foobillingproject");
    props.setProperty(BIGQUERY_SYNC_DATASET_NAME.key(), "foodataset");
    props.setProperty(BIGQUERY_SYNC_DATASET_LOCATION.key(), "US");
    props.setProperty(BIGQUERY_SYNC_TABLE_NAME.key(), "footable");
    props.setProperty(BIGQUERY_SYNC_USE_BQ_MANIFEST_FILE.key(), "true");
    props.setProperty(BIGQUERY_SYNC_SOURCE_URI.key(), "gs://test-bucket/dwh/table_name/dt=*");
    props.setProperty(BIGQUERY_SYNC_SOURCE_URI_PREFIX.key(), "gs://test-bucket/dwh/table_name/");
    props.setProperty(BIGQUERY_SYNC_SYNC_BASE_PATH.key(), "gs://test-bucket/dwh/table_name");
    props.setProperty(BIGQUERY_SYNC_PARTITION_FIELDS.key(), "a,b");
    props.setProperty(BIGQUERY_SYNC_USE_FILE_LISTING_FROM_METADATA.key(), "true");
    props.setProperty(BIGQUERY_SYNC_ASSUME_DATE_PARTITIONING.key(), "true");
    BigQuerySyncConfig syncConfig = new BigQuerySyncConfig(props);
    assertEquals("fooproject", syncConfig.getString(BIGQUERY_SYNC_PROJECT_ID));
    assertEquals("foobillingproject", syncConfig.getString(BIGQUERY_SYNC_BILLING_PROJECT_ID));
    assertEquals("foodataset", syncConfig.getString(BIGQUERY_SYNC_DATASET_NAME));
    assertEquals("US", syncConfig.getString(BIGQUERY_SYNC_DATASET_LOCATION));
    assertEquals("footable", syncConfig.getString(BIGQUERY_SYNC_TABLE_NAME));
    assertEquals(true, syncConfig.getBoolean(BIGQUERY_SYNC_USE_BQ_MANIFEST_FILE));
    assertEquals("gs://test-bucket/dwh/table_name/dt=*", syncConfig.getString(BIGQUERY_SYNC_SOURCE_URI));
    assertEquals("gs://test-bucket/dwh/table_name/", syncConfig.getString(BIGQUERY_SYNC_SOURCE_URI_PREFIX));
    assertEquals("gs://test-bucket/dwh/table_name", syncConfig.getString(BIGQUERY_SYNC_SYNC_BASE_PATH));
    assertEquals(Arrays.asList("a", "b"), syncConfig.getSplitStrings(BIGQUERY_SYNC_PARTITION_FIELDS));
    assertEquals(true, syncConfig.getBoolean(BIGQUERY_SYNC_USE_FILE_LISTING_FROM_METADATA));
    assertEquals(true, syncConfig.getBoolean(BIGQUERY_SYNC_ASSUME_DATE_PARTITIONING));
  }

  @Test
  public void testInferDatasetAndTableNames() {
    Properties props1 = new Properties();
    props1.setProperty(HoodieTableConfig.DATABASE_NAME.key(), "db1");
    props1.setProperty(HoodieTableConfig.HOODIE_TABLE_NAME_KEY, "tbl1");
    BigQuerySyncConfig config1 = new BigQuerySyncConfig(props1);
    assertEquals("db1", config1.getString(BIGQUERY_SYNC_DATASET_NAME));
    assertEquals("tbl1", config1.getString(BIGQUERY_SYNC_TABLE_NAME));

    Properties props2 = new Properties();
    props2.setProperty(HoodieTableConfig.DATABASE_NAME.key(), "db2");
    props2.setProperty(HoodieTableConfig.HOODIE_WRITE_TABLE_NAME_KEY, "tbl2");
    BigQuerySyncConfig config2 = new BigQuerySyncConfig(props2);
    assertEquals("db2", config2.getString(BIGQUERY_SYNC_DATASET_NAME));
    assertEquals("tbl2", config2.getString(BIGQUERY_SYNC_TABLE_NAME));
  }

  @Test
  public void testInferPartitionFields() {
    Properties props0 = new Properties();
    BigQuerySyncConfig config0 = new BigQuerySyncConfig(props0);
    assertNull(config0.getString(BIGQUERY_SYNC_PARTITION_FIELDS),
        String.format("should get null due to absence of both %s and %s",
            HoodieTableConfig.PARTITION_FIELDS.key(), KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key()));

    Properties props1 = new Properties();
    props1.setProperty(HoodieTableConfig.PARTITION_FIELDS.key(), "foo,bar,baz");
    BigQuerySyncConfig config1 = new BigQuerySyncConfig(props1);
    assertEquals("foo,bar,baz", config1.getString(BIGQUERY_SYNC_PARTITION_FIELDS),
        String.format("should infer from %s", HoodieTableConfig.PARTITION_FIELDS.key()));

    Properties props2 = new Properties();
    props2.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "foo,bar");
    BigQuerySyncConfig config2 = new BigQuerySyncConfig(props2);
    assertEquals("foo,bar", config2.getString(BIGQUERY_SYNC_PARTITION_FIELDS),
        String.format("should infer from %s", KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key()));

    Properties props3 = new Properties();
    props3.setProperty(HoodieTableConfig.PARTITION_FIELDS.key(), "foo,bar,baz");
    props3.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "foo,bar");
    BigQuerySyncConfig config3 = new BigQuerySyncConfig(props3);
    assertEquals("foo,bar,baz", config3.getString(BIGQUERY_SYNC_PARTITION_FIELDS),
        String.format("should infer from %s, which has higher precedence.", HoodieTableConfig.PARTITION_FIELDS.key()));

  }

  @Test
  void testInferUseFileListingFromMetadata() {
    BigQuerySyncConfig config1 = new BigQuerySyncConfig(new Properties());
    assertEquals(DEFAULT_METADATA_ENABLE_FOR_READERS, config1.getBoolean(BIGQUERY_SYNC_USE_FILE_LISTING_FROM_METADATA));

    Properties props2 = new Properties();
    props2.setProperty(HoodieMetadataConfig.ENABLE.key(), "true");
    BigQuerySyncConfig config2 = new BigQuerySyncConfig(props2);
    assertEquals(true, config2.getBoolean(BIGQUERY_SYNC_USE_FILE_LISTING_FROM_METADATA));
  }

  @Test
  void testInferAssumeDatePartition() {
    BigQuerySyncConfig config1 = new BigQuerySyncConfig(new Properties());
    assertEquals(false, config1.getBoolean(BIGQUERY_SYNC_ASSUME_DATE_PARTITIONING));

    Properties props2 = new Properties();
    props2.setProperty(HoodieMetadataConfig.ASSUME_DATE_PARTITIONING.key(), "true");
    BigQuerySyncConfig config2 = new BigQuerySyncConfig(props2);
    assertEquals(true, config2.getBoolean(BIGQUERY_SYNC_ASSUME_DATE_PARTITIONING));
  }
}
