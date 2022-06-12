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
    adbSyncConfig.hoodieSyncConfigParams.partitionFields = Arrays.asList("a", "b");
    adbSyncConfig.hoodieSyncConfigParams.basePath = "/tmp";
    adbSyncConfig.hoodieSyncConfigParams.assumeDatePartitioning = true;
    adbSyncConfig.hoodieSyncConfigParams.databaseName = "test";
    adbSyncConfig.hoodieSyncConfigParams.tableName = "test";
    adbSyncConfig.adbSyncConfigParams.hiveSyncConfigParams.hiveUser = "adb";
    adbSyncConfig.adbSyncConfigParams.hiveSyncConfigParams.hivePass = "adb";
    adbSyncConfig.adbSyncConfigParams.hiveSyncConfigParams.jdbcUrl = "jdbc:mysql://localhost:3306";
    adbSyncConfig.adbSyncConfigParams.hiveSyncConfigParams.skipROSuffix = false;
    adbSyncConfig.adbSyncConfigParams.tableProperties = "spark.sql.sources.provider= 'hudi'\\n"
            + "spark.sql.sources.schema.numParts = '1'\\n "
            + "spark.sql.sources.schema.part.0 ='xx'\\n "
            + "spark.sql.sources.schema.numPartCols = '1'\\n"
            + "spark.sql.sources.schema.partCol.0 = 'dt'";
    adbSyncConfig.adbSyncConfigParams.serdeProperties = "'path'='/tmp/test_db/tbl'";
    adbSyncConfig.adbSyncConfigParams.dbLocation = "file://tmp/test_db";

    TypedProperties props = AdbSyncConfig.toProps(adbSyncConfig);
    AdbSyncConfig copied = new AdbSyncConfig(props);

    assertEquals(copied.hoodieSyncConfigParams.partitionFields, adbSyncConfig.hoodieSyncConfigParams.partitionFields);
    assertEquals(copied.hoodieSyncConfigParams.basePath, adbSyncConfig.hoodieSyncConfigParams.basePath);
    assertEquals(copied.hoodieSyncConfigParams.assumeDatePartitioning, adbSyncConfig.hoodieSyncConfigParams.assumeDatePartitioning);
    assertEquals(copied.hoodieSyncConfigParams.databaseName, adbSyncConfig.hoodieSyncConfigParams.databaseName);
    assertEquals(copied.hoodieSyncConfigParams.tableName, adbSyncConfig.hoodieSyncConfigParams.tableName);
    assertEquals(copied.adbSyncConfigParams.hiveSyncConfigParams.hiveUser, adbSyncConfig.adbSyncConfigParams.hiveSyncConfigParams.hiveUser);
    assertEquals(copied.adbSyncConfigParams.hiveSyncConfigParams.hivePass, adbSyncConfig.adbSyncConfigParams.hiveSyncConfigParams.hivePass);
    assertEquals(copied.hoodieSyncConfigParams.basePath, adbSyncConfig.hoodieSyncConfigParams.basePath);
    assertEquals(copied.adbSyncConfigParams.hiveSyncConfigParams.jdbcUrl, adbSyncConfig.adbSyncConfigParams.hiveSyncConfigParams.jdbcUrl);
    assertEquals(copied.adbSyncConfigParams.hiveSyncConfigParams.skipROSuffix, adbSyncConfig.adbSyncConfigParams.hiveSyncConfigParams.skipROSuffix);
    assertEquals(copied.adbSyncConfigParams.supportTimestamp, adbSyncConfig.adbSyncConfigParams.supportTimestamp);
  }
}
