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

import org.apache.hudi.common.table.HoodieTableConfig;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_TABLE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
}
