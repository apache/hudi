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

package org.apache.hudi

import org.apache.hudi.common.config.HoodieCommonConfig
import org.apache.hudi.common.table.HoodieTableConfig

import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

class TestDataSourceOptions {
  @Test
  def testAdvancedConfigs(): Unit = {
    assertTrue(DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED.isAdvanced)
    assertEquals(
      HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.defaultValue(),
      DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED.defaultValue())

    assertTrue(DataSourceWriteOptions.RECONCILE_SCHEMA.isAdvanced)
    assertEquals(
      HoodieCommonConfig.RECONCILE_SCHEMA.defaultValue(),
      DataSourceWriteOptions.RECONCILE_SCHEMA.defaultValue())

    assertTrue(DataSourceWriteOptions.DROP_PARTITION_COLUMNS.isAdvanced)
    assertEquals(
      HoodieTableConfig.DROP_PARTITION_COLUMNS.defaultValue(),
      DataSourceWriteOptions.DROP_PARTITION_COLUMNS.defaultValue())
  }
}
