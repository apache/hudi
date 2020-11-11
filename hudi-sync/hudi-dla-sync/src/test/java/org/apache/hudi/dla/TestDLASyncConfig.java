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

package org.apache.hudi.dla;

import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
public class TestDLASyncConfig {
  @Test
  public void testCopy() {
    DLASyncConfig dlaSyncConfig = new DLASyncConfig();
    List<String> partitions = Arrays.asList("a", "b");
    dlaSyncConfig.partitionFields = partitions;
    dlaSyncConfig.basePath = "/tmp";
    dlaSyncConfig.assumeDatePartitioning = true;
    dlaSyncConfig.databaseName = "test";
    dlaSyncConfig.tableName = "test";
    dlaSyncConfig.dlaUser = "dla";
    dlaSyncConfig.dlaPass = "dla";
    dlaSyncConfig.jdbcUrl = "jdbc:mysql://localhost:3306";
    dlaSyncConfig.skipROSuffix = false;

    DLASyncConfig copied = DLASyncConfig.copy(dlaSyncConfig);

    assertEquals(copied.partitionFields, dlaSyncConfig.partitionFields);
    assertEquals(copied.basePath, dlaSyncConfig.basePath);
    assertEquals(copied.assumeDatePartitioning, dlaSyncConfig.assumeDatePartitioning);
    assertEquals(copied.databaseName, dlaSyncConfig.databaseName);
    assertEquals(copied.tableName, dlaSyncConfig.tableName);
    assertEquals(copied.dlaUser, dlaSyncConfig.dlaUser);
    assertEquals(copied.dlaPass, dlaSyncConfig.dlaPass);
    assertEquals(copied.basePath, dlaSyncConfig.basePath);
    assertEquals(copied.jdbcUrl, dlaSyncConfig.jdbcUrl);
    assertEquals(copied.skipROSuffix, dlaSyncConfig.skipROSuffix);
    assertEquals(copied.supportTimestamp, dlaSyncConfig.supportTimestamp);
  }
}
