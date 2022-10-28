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

package org.apache.hudi.hive;

import com.beust.jcommander.JCommander;
import org.junit.jupiter.api.Test;

import static org.apache.hudi.hive.HiveSyncConfig.HIVE_SKIP_RO_SUFFIX_FOR_READ_OPTIMIZED_TABLE;
import static org.apache.hudi.hive.HiveSyncConfig.HIVE_SYNC_AS_DATA_SOURCE_TABLE;
import static org.apache.hudi.hive.HiveSyncConfig.HIVE_SYNC_MODE;
import static org.apache.hudi.hive.HiveSyncConfig.HIVE_USE_JDBC;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_TABLE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHiveSyncToolArgs {

  @Test
  public void testArgsParse() {
    HiveSyncConfig.HiveSyncConfigParams params = new HiveSyncConfig.HiveSyncConfigParams();
    JCommander cmd = JCommander.newBuilder().addObject(params).build();
    String[] args = {"--sync-mode", "hms", "--base-path", "/table_path", "--table", "table_name",
        "--spark-datasource", "false"};
    cmd.parse(args);

    assertEquals("hms", params.toProps().getProperty(HIVE_SYNC_MODE.key()));
    assertEquals("/table_path", params.toProps().getProperty(META_SYNC_BASE_PATH.key()));
    assertEquals("table_name", params.toProps().getProperty(META_SYNC_TABLE_NAME.key()));
    assertFalse(params.toProps().getBoolean(HIVE_SYNC_AS_DATA_SOURCE_TABLE.key()));
    // The default value of "--use-jdbc" is true
    assertTrue(params.toProps().getBoolean(HIVE_USE_JDBC.key()));
    assertFalse(params.toProps().getBoolean(HIVE_SKIP_RO_SUFFIX_FOR_READ_OPTIMIZED_TABLE.key()));
  }
}
