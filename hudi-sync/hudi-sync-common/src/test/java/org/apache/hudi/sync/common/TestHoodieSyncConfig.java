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

package org.apache.hudi.sync.common;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.apache.hudi.common.config.HoodieMetadataConfig.DEFAULT_METADATA_ENABLE_FOR_READERS;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_FILE_FORMAT;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DECODE_PARTITION;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_FIELDS;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_TABLE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_USE_FILE_LISTING_FROM_METADATA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHoodieSyncConfig {

  @Test
  void testInferDatabaseAndTableNames() {
    Properties props1 = new Properties();
    props1.setProperty(HoodieTableConfig.DATABASE_NAME.key(), "db1");
    props1.setProperty(HoodieTableConfig.HOODIE_TABLE_NAME_KEY, "tbl1");
    HoodieSyncConfig config1 = new HoodieSyncConfig(props1, new Configuration());
    assertEquals("db1", config1.getString(META_SYNC_DATABASE_NAME));
    assertEquals("tbl1", config1.getString(META_SYNC_TABLE_NAME));

    Properties props2 = new Properties();
    props2.setProperty(HoodieTableConfig.DATABASE_NAME.key(), "db2");
    props2.setProperty(HoodieTableConfig.HOODIE_WRITE_TABLE_NAME_KEY, "tbl2");
    HoodieSyncConfig config2 = new HoodieSyncConfig(props2, new Configuration());
    assertEquals("db2", config2.getString(META_SYNC_DATABASE_NAME));
    assertEquals("tbl2", config2.getString(META_SYNC_TABLE_NAME));

    HoodieSyncConfig config3 = new HoodieSyncConfig(new Properties(), new Configuration());
    assertEquals("default", config3.getString(META_SYNC_DATABASE_NAME));
    assertEquals("unknown", config3.getString(META_SYNC_TABLE_NAME));
  }

  @Test
  void testInferBaseFileFormat() {
    Properties props1 = new Properties();
    props1.setProperty(HoodieTableConfig.BASE_FILE_FORMAT.key(), "ORC");
    HoodieSyncConfig config1 = new HoodieSyncConfig(props1, new Configuration());
    assertEquals("ORC", config1.getStringOrDefault(META_SYNC_BASE_FILE_FORMAT));

    HoodieSyncConfig config2 = new HoodieSyncConfig(new Properties(), new Configuration());
    assertEquals("PARQUET", config2.getStringOrDefault(META_SYNC_BASE_FILE_FORMAT));
  }

  @Test
  void testInferPartitionFields() {
    Properties props0 = new Properties();
    HoodieSyncConfig config0 = new HoodieSyncConfig(props0, new Configuration());
    assertEquals("", config0.getStringOrDefault(META_SYNC_PARTITION_FIELDS),
        String.format("should get default value due to absence of both %s and %s",
            HoodieTableConfig.PARTITION_FIELDS.key(), KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key()));

    Properties props1 = new Properties();
    props1.setProperty(HoodieTableConfig.PARTITION_FIELDS.key(), "foo,bar,baz");
    HoodieSyncConfig config1 = new HoodieSyncConfig(props1, new Configuration());
    assertEquals("foo,bar,baz", config1.getStringOrDefault(META_SYNC_PARTITION_FIELDS),
        String.format("should infer from %s", HoodieTableConfig.PARTITION_FIELDS.key()));

    Properties props2 = new Properties();
    props2.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "foo,bar");
    HoodieSyncConfig config2 = new HoodieSyncConfig(props2, new Configuration());
    assertEquals("foo,bar", config2.getStringOrDefault(META_SYNC_PARTITION_FIELDS),
        String.format("should infer from %s", KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key()));

    Properties props3 = new Properties();
    props3.setProperty(HoodieTableConfig.PARTITION_FIELDS.key(), "foo,bar,baz");
    props3.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "foo,bar");
    HoodieSyncConfig config3 = new HoodieSyncConfig(props3, new Configuration());
    assertEquals("foo,bar,baz", config3.getStringOrDefault(META_SYNC_PARTITION_FIELDS),
        String.format("should infer from %s, which has higher precedence.", HoodieTableConfig.PARTITION_FIELDS.key()));

  }

  @Test
  void testInferPartitionExtractorClass() {
    Properties props0 = new Properties();
    HoodieSyncConfig config0 = new HoodieSyncConfig(props0, new Configuration());
    assertEquals("org.apache.hudi.hive.MultiPartKeysValueExtractor",
        config0.getStringOrDefault(META_SYNC_PARTITION_EXTRACTOR_CLASS),
        String.format("should get default value due to absence of both %s and %s",
            HoodieTableConfig.PARTITION_FIELDS.key(), KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key()));

    Properties props1 = new Properties();
    props1.setProperty(HoodieTableConfig.PARTITION_FIELDS.key(), "");
    HoodieSyncConfig config1 = new HoodieSyncConfig(props1, new Configuration());
    assertEquals("org.apache.hudi.hive.NonPartitionedExtractor",
        config1.getStringOrDefault(META_SYNC_PARTITION_EXTRACTOR_CLASS),
        String.format("should infer from %s", HoodieTableConfig.PARTITION_FIELDS.key()));

    Properties props2 = new Properties();
    props2.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "foo,bar");
    HoodieSyncConfig config2 = new HoodieSyncConfig(props2, new Configuration());
    assertEquals("org.apache.hudi.hive.MultiPartKeysValueExtractor",
        config2.getStringOrDefault(META_SYNC_PARTITION_EXTRACTOR_CLASS),
        String.format("should infer from %s", KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key()));

    Properties props3 = new Properties();
    props3.setProperty(HoodieTableConfig.PARTITION_FIELDS.key(), "");
    props3.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "foo,bar");
    HoodieSyncConfig config3 = new HoodieSyncConfig(props3, new Configuration());
    assertEquals("org.apache.hudi.hive.NonPartitionedExtractor",
        config3.getStringOrDefault(META_SYNC_PARTITION_EXTRACTOR_CLASS),
        String.format("should infer from %s, which has higher precedence.", HoodieTableConfig.PARTITION_FIELDS.key()));

    Properties props4 = new Properties();
    props4.setProperty(HoodieTableConfig.PARTITION_FIELDS.key(), "foo");
    props4.setProperty(HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE.key(), "true");
    HoodieSyncConfig config4 = new HoodieSyncConfig(props4, new Configuration());
    assertEquals("org.apache.hudi.hive.HiveStylePartitionValueExtractor",
        config4.getStringOrDefault(META_SYNC_PARTITION_EXTRACTOR_CLASS));

    Properties props5 = new Properties();
    props5.setProperty(HoodieTableConfig.PARTITION_FIELDS.key(), "foo");
    props5.setProperty(HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE.key(), "false");
    HoodieSyncConfig config5 = new HoodieSyncConfig(props5, new Configuration());
    assertEquals("org.apache.hudi.hive.SinglePartPartitionValueExtractor",
        config5.getStringOrDefault(META_SYNC_PARTITION_EXTRACTOR_CLASS));
  }

  @Test
  void testInferDecodePartition() {
    Properties props1 = new Properties();
    props1.setProperty(HoodieTableConfig.URL_ENCODE_PARTITIONING.key(), "true");
    HoodieSyncConfig config1 = new HoodieSyncConfig(props1, new Configuration());
    assertTrue(config1.getBoolean(META_SYNC_DECODE_PARTITION));
  }

  @Test
  void testInferUseFileListingFromMetadata() {
    HoodieSyncConfig config1 = new HoodieSyncConfig(new Properties(), new Configuration());
    assertEquals(DEFAULT_METADATA_ENABLE_FOR_READERS, config1.getBoolean(META_SYNC_USE_FILE_LISTING_FROM_METADATA));

    Properties props2 = new Properties();
    props2.setProperty(HoodieMetadataConfig.ENABLE.key(), "true");
    HoodieSyncConfig config2 = new HoodieSyncConfig(props2, new Configuration());
    assertTrue(config2.getBoolean(META_SYNC_USE_FILE_LISTING_FROM_METADATA));
  }
}
