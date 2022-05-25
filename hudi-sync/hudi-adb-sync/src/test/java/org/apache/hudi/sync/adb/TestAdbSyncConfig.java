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

import org.apache.hudi.common.config.TypedProperties;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
public class TestAdbSyncConfig {
  @Test
  public void testCopy() {
    AdbSyncConfig adbSyncConfig = new AdbSyncConfig();
    adbSyncConfig.partitionFields = Arrays.asList("a", "b");
    adbSyncConfig.basePath = "/tmp";
    adbSyncConfig.assumeDatePartitioning = true;
    adbSyncConfig.databaseName = "test";
    adbSyncConfig.tableName = "test";
    adbSyncConfig.adbUser = "adb";
    adbSyncConfig.adbPass = "adb";
    adbSyncConfig.jdbcUrl = "jdbc:mysql://localhost:3306";
    adbSyncConfig.skipROSuffix = false;
    adbSyncConfig.tableProperties = "spark.sql.sources.provider= 'hudi'\\n"
        + "spark.sql.sources.schema.numParts = '1'\\n "
        + "spark.sql.sources.schema.part.0 ='xx'\\n "
        + "spark.sql.sources.schema.numPartCols = '1'\\n"
        + "spark.sql.sources.schema.partCol.0 = 'dt'";
    adbSyncConfig.serdeProperties = "'path'='/tmp/test_db/tbl'";
    adbSyncConfig.dbLocation = "file://tmp/test_db";

    TypedProperties props = AdbSyncConfig.toProps(adbSyncConfig);
    AdbSyncConfig copied = new AdbSyncConfig(props);

    assertEquals(copied.partitionFields, adbSyncConfig.partitionFields);
    assertEquals(copied.basePath, adbSyncConfig.basePath);
    assertEquals(copied.assumeDatePartitioning, adbSyncConfig.assumeDatePartitioning);
    assertEquals(copied.databaseName, adbSyncConfig.databaseName);
    assertEquals(copied.tableName, adbSyncConfig.tableName);
    assertEquals(copied.adbUser, adbSyncConfig.adbUser);
    assertEquals(copied.adbPass, adbSyncConfig.adbPass);
    assertEquals(copied.basePath, adbSyncConfig.basePath);
    assertEquals(copied.jdbcUrl, adbSyncConfig.jdbcUrl);
    assertEquals(copied.skipROSuffix, adbSyncConfig.skipROSuffix);
    assertEquals(copied.supportTimestamp, adbSyncConfig.supportTimestamp);
  }
}
