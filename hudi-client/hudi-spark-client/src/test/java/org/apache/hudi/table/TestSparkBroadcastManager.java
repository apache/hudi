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

package org.apache.hudi.table;

import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.hadoop.fs.inline.InLineFileSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestSparkBroadcastManager {
  @Test
  void testGetStorageConfiguration() {
    Configuration config = new Configuration(false);
    Configuration createdConfig = SparkBroadcastManager.getStorageConfiguration(config);
    assertFalse(createdConfig.getBoolean(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED().key(), true));
    assertFalse(createdConfig.getBoolean(SQLConf.CASE_SENSITIVE().key(), true));
    assertFalse(createdConfig.getBoolean(SQLConf.PARQUET_BINARY_AS_STRING().key(), true));
    assertTrue(createdConfig.getBoolean(SQLConf.PARQUET_INT96_AS_TIMESTAMP().key(), false));
    assertFalse(createdConfig.getBoolean("spark.sql.legacy.parquet.nanosAsLong", true));
    if (HoodieSparkUtils.gteqSpark3_4()) {
      assertFalse(createdConfig.getBoolean("spark.sql.parquet.inferTimestampNTZ.enabled", true));
    }

    String inlineClassName = createdConfig.get("fs." + InLineFileSystem.SCHEME + ".impl");
    assertEquals(InLineFileSystem.class.getName(), inlineClassName);
  }
}
