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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Properties;

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
    Properties props = new Properties();
    props.setProperty(SNOWFLAKE_SYNC_PARTITION_EXTRACT_EXPRESSION.key(),
        "\"\"date\" date as to_date(substr(metadata$filename, 22, 10), 'YYYY-MM-DD')\","
            + "\"\"symbol\" text as to_date(substr(metadata$filename, 32, 10), 'YYYY-MM-DD')\"");
    props.setProperty(SNOWFLAKE_SYNC_PARTITION_FIELDS.key(), "date,symbol");
    props.setProperty(SNOWFLAKE_SYNC_PROPERTIES_FILE.key(), "/var/demo/config/snowflake.properties");
    props.setProperty(SNOWFLAKE_SYNC_STORAGE_INTEGRATION.key(), "hudi_gcp_integration");
    props.setProperty(SNOWFLAKE_SYNC_SYNC_BASE_FILE_FORMAT.key(), "PARQUET");
    props.setProperty(SNOWFLAKE_SYNC_SYNC_BASE_PATH.key(), "gcs://hudi-demo/dwh/stock_ticks");
    props.setProperty(SNOWFLAKE_SYNC_TABLE_NAME.key(), "stock_ticks");
    syncConfig = new SnowflakeSyncConfig(props);
  }

  @Test
  public void testGetConfigs() {
    assertEquals(Arrays.asList(
        "\"date\" date as to_date(substr(metadata$filename, 22, 10), 'YYYY-MM-DD')",
        "\"symbol\" text as to_date(substr(metadata$filename, 32, 10), 'YYYY-MM-DD')"
    ), syncConfig.getString(SNOWFLAKE_SYNC_PARTITION_EXTRACT_EXPRESSION));
    assertEquals(Arrays.asList("date", "symbol"), syncConfig.getString(SNOWFLAKE_SYNC_PARTITION_FIELDS));
    assertEquals("/var/demo/config/snowflake.properties", syncConfig.getString(SNOWFLAKE_SYNC_PROPERTIES_FILE));
    assertEquals("hudi_gcp_integration", syncConfig.getString(SNOWFLAKE_SYNC_STORAGE_INTEGRATION));
    assertEquals("PARQUET", syncConfig.getString(SNOWFLAKE_SYNC_SYNC_BASE_FILE_FORMAT));
    assertEquals("gcs://hudi-demo/dwh/stock_ticks", syncConfig.getString(SNOWFLAKE_SYNC_SYNC_BASE_PATH));
    assertEquals("stock_ticks", syncConfig.getString(SNOWFLAKE_SYNC_TABLE_NAME));
  }
}
