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

package org.apache.hudi.snowflake.sync;

import org.apache.hudi.common.config.TypedProperties;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;


import static org.apache.hudi.snowflake.sync.SnowflakeSyncConfig.SNOWFLAKE_SYNC_PARTITION_EXTRACT_EXPRESSION;
import static org.apache.hudi.snowflake.sync.SnowflakeSyncConfig.SNOWFLAKE_SYNC_PARTITION_FIELDS;
import static org.apache.hudi.snowflake.sync.SnowflakeSyncConfig.SNOWFLAKE_SYNC_PROPERTIES_FILE;
import static org.apache.hudi.snowflake.sync.SnowflakeSyncConfig.SNOWFLAKE_SYNC_STORAGE_INTEGRATION;
import static org.apache.hudi.snowflake.sync.SnowflakeSyncConfig.SNOWFLAKE_SYNC_SYNC_BASE_FILE_FORMAT;
import static org.apache.hudi.snowflake.sync.SnowflakeSyncConfig.SNOWFLAKE_SYNC_SYNC_BASE_PATH;
import static org.apache.hudi.snowflake.sync.SnowflakeSyncConfig.SNOWFLAKE_SYNC_TABLE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
/*
 * Test class to test all the snowflake sync configs.
 */
public class TestSnowflakeSyncConfig {

  SnowflakeSyncConfig syncConfig;

  @BeforeEach
  void setUp() {
    syncConfig = new SnowflakeSyncConfig();
    syncConfig.propertiesFile = "/var/demo/config/snowflake.properties";
    syncConfig.storageIntegration = "hudi_gcp_integration";
    syncConfig.tableName = "stock_ticks";
    syncConfig.basePath = "gcs://hudi-demo/dwh/stock_ticks";
    syncConfig.baseFileFormat = "PARQUET";
    syncConfig.partitionFields = Arrays.asList("date", "symbol");
    syncConfig.partitionExtractExpr = Arrays.asList(
        "\"date\" date as to_date(substr(metadata$filename, 22, 10), 'YYYY-MM-DD')",
        "\"symbol\" text as to_date(substr(metadata$filename, 32, 10), 'YYYY-MM-DD')");
    syncConfig.help = true;
  }

  @Test
  public void testCopy() {
    SnowflakeSyncConfig copied = SnowflakeSyncConfig.copy(syncConfig);
    assertEquals(copied.propertiesFile, syncConfig.propertiesFile);
    assertEquals(copied.storageIntegration, syncConfig.storageIntegration);
    assertEquals(copied.tableName, syncConfig.tableName);
    assertEquals(copied.basePath, syncConfig.basePath);
    assertEquals(copied.baseFileFormat, syncConfig.baseFileFormat);
    assertEquals(copied.partitionFields, syncConfig.partitionFields);
    assertEquals(copied.partitionExtractExpr, syncConfig.partitionExtractExpr);
    assertEquals(copied.help, syncConfig.help);
  }

  @Test
  public void testToProps() {
    TypedProperties props = syncConfig.toProps();
    assertEquals("/var/demo/config/snowflake.properties", props.getString(SNOWFLAKE_SYNC_PROPERTIES_FILE));
    assertEquals("hudi_gcp_integration", props.getString(SNOWFLAKE_SYNC_STORAGE_INTEGRATION));
    assertEquals("stock_ticks", props.getString(SNOWFLAKE_SYNC_TABLE_NAME));
    assertEquals("gcs://hudi-demo/dwh/stock_ticks", props.getString(SNOWFLAKE_SYNC_SYNC_BASE_PATH));
    assertEquals("PARQUET", props.getString(SNOWFLAKE_SYNC_SYNC_BASE_FILE_FORMAT));
    assertEquals("date,symbol", props.getString(SNOWFLAKE_SYNC_PARTITION_FIELDS));
    assertEquals("\"date\" date as to_date(substr(metadata$filename, 22, 10), 'YYYY-MM-DD'),"
        + "\"symbol\" text as to_date(substr(metadata$filename, 32, 10), 'YYYY-MM-DD')",
        props.getString(SNOWFLAKE_SYNC_PARTITION_EXTRACT_EXPRESSION));
  }

  @Test
  public void fromProps() {
    TypedProperties props = new TypedProperties();
    props.put(SNOWFLAKE_SYNC_PROPERTIES_FILE, "/var/demo/config/snowflake.properties");
    props.put(SNOWFLAKE_SYNC_STORAGE_INTEGRATION, "hudi_gcp_integration");
    props.put(SNOWFLAKE_SYNC_TABLE_NAME, "stock_ticks");
    props.put(SNOWFLAKE_SYNC_SYNC_BASE_PATH, "gcs://hudi-demo/dwh/stock_ticks");
    props.put(SNOWFLAKE_SYNC_SYNC_BASE_FILE_FORMAT, "PARQUET");
    props.put(SNOWFLAKE_SYNC_PARTITION_FIELDS, "date,symbol");
    props.put(SNOWFLAKE_SYNC_PARTITION_EXTRACT_EXPRESSION,
        "\"date\" date as to_date(substr(metadata$filename, 22, 10), 'YYYY-MM-DD'),\"symbol\" text as to_date(substr(metadata$filename, 32, 10), 'YYYY-MM-DD')");
    SnowflakeSyncConfig cfg = SnowflakeSyncConfig.fromProps(props);

    assertEquals(syncConfig.propertiesFile, cfg.propertiesFile);
    assertEquals(syncConfig.storageIntegration, cfg.storageIntegration);
    assertEquals(syncConfig.tableName, cfg.tableName);
    assertEquals(syncConfig.basePath, cfg.basePath);
    assertEquals(syncConfig.baseFileFormat, cfg.baseFileFormat);
    assertEquals(syncConfig.partitionFields.toString(), cfg.partitionFields.toString());
    assertEquals(syncConfig.partitionExtractExpr.toString(), cfg.partitionExtractExpr.toString());
  }
}
