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

package org.apache.hudi.sync.adb;

import org.apache.hudi.sync.common.util.ConfigUtils;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.sync.adb.AdbSyncConfig.ADB_SYNC_DB_LOCATION;
import static org.apache.hudi.sync.adb.AdbSyncConfig.ADB_SYNC_JDBC_URL;
import static org.apache.hudi.sync.adb.AdbSyncConfig.ADB_SYNC_PASS;
import static org.apache.hudi.sync.adb.AdbSyncConfig.ADB_SYNC_SERDE_PROPERTIES;
import static org.apache.hudi.sync.adb.AdbSyncConfig.ADB_SYNC_SKIP_RO_SUFFIX;
import static org.apache.hudi.sync.adb.AdbSyncConfig.ADB_SYNC_TABLE_PROPERTIES;
import static org.apache.hudi.sync.adb.AdbSyncConfig.ADB_SYNC_USER;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_ASSUME_DATE_PARTITION;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_FIELDS;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_TABLE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestAdbSyncConfig {

  @Test
  public void testInitConfig() {
    Properties props = new Properties();
    props.setProperty(META_SYNC_PARTITION_FIELDS.key(), "a,b");
    props.setProperty(META_SYNC_BASE_PATH.key(), "/tmp");
    props.setProperty(META_SYNC_ASSUME_DATE_PARTITION.key(), "true");
    props.setProperty(META_SYNC_DATABASE_NAME.key(), "test");
    props.setProperty(META_SYNC_TABLE_NAME.key(), "test");
    props.setProperty(ADB_SYNC_USER.key(), "adb");
    props.setProperty(ADB_SYNC_PASS.key(), "adb");
    props.setProperty(ADB_SYNC_JDBC_URL.key(), "jdbc:mysql://localhost:3306");
    props.setProperty(ADB_SYNC_SKIP_RO_SUFFIX.key(), "false");
    String tableProps = "spark.sql.sources.provider=hudi\n"
        + "spark.sql.sources.schema.numParts=1\n"
        + "spark.sql.sources.schema.part.0=xx\n"
        + "spark.sql.sources.schema.numPartCols=1\n"
        + "spark.sql.sources.schema.partCol.0=dt";
    props.setProperty(ADB_SYNC_TABLE_PROPERTIES.key(), tableProps);
    props.setProperty(ADB_SYNC_SERDE_PROPERTIES.key(), "path=/tmp/test_db/tbl");
    props.setProperty(ADB_SYNC_DB_LOCATION.key(), "file://tmp/test_db");

    AdbSyncConfig config = new AdbSyncConfig(props);
    assertEquals(Arrays.asList("a", "b"), config.getSplitStrings(META_SYNC_PARTITION_FIELDS));
    assertEquals("/tmp", config.getString(META_SYNC_BASE_PATH));
    assertEquals(true, config.getBoolean(META_SYNC_ASSUME_DATE_PARTITION));
    assertEquals("test", config.getString(META_SYNC_DATABASE_NAME));
    assertEquals("test", config.getString(META_SYNC_TABLE_NAME));
    assertEquals("adb", config.getString(ADB_SYNC_USER));
    assertEquals("adb", config.getString(ADB_SYNC_PASS));
    assertEquals("jdbc:mysql://localhost:3306", config.getString(ADB_SYNC_JDBC_URL));
    assertEquals(false, config.getBoolean(ADB_SYNC_SKIP_RO_SUFFIX));
    Map<String, String> tablePropsMap = new HashMap<>();
    tablePropsMap.put("spark.sql.sources.provider", "hudi");
    tablePropsMap.put("spark.sql.sources.schema.numParts", "1");
    tablePropsMap.put("spark.sql.sources.schema.part.0", "xx");
    tablePropsMap.put("spark.sql.sources.schema.numPartCols", "1");
    tablePropsMap.put("spark.sql.sources.schema.partCol.0", "dt");
    assertEquals(tablePropsMap, ConfigUtils.toMap(config.getString(ADB_SYNC_TABLE_PROPERTIES)));
    Map<String, String> serdePropsMap = new HashMap<>();
    serdePropsMap.put("path", "/tmp/test_db/tbl");
    assertEquals(serdePropsMap, ConfigUtils.toMap(config.getString(ADB_SYNC_SERDE_PROPERTIES)));
    assertEquals("file://tmp/test_db", config.getString(ADB_SYNC_DB_LOCATION));
  }
}
