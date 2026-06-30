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

package org.apache.hudi.common.table;

import org.apache.hudi.common.model.debezium.DebeziumConstants;
import org.apache.hudi.common.model.debezium.MySqlDebeziumAvroPayload;
import org.apache.hudi.common.model.debezium.PostgresDebeziumAvroPayload;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests that {@link HoodieTableConfig#inferMergingConfigsForV9TableCreation} resolves the
 * Debezium ordering field against the {@code _debezium_metadata} struct path when nested metadata
 * is enabled. Kept in {@code hudi-common}'s own test source set (rather than alongside the larger
 * {@code TestHoodieTableConfig} suite in {@code hudi-hadoop-common}) so coverage for this
 * hudi-common-owned logic is attributed to the module that owns it.
 */
class TestHoodieTableConfigDebeziumOrdering {

  @Test
  void mysqlOrderingFieldsAreNestedWhenDebeziumMetadataIsNested() {
    Map<String, String> flat = HoodieTableConfig.inferMergingConfigsForV9TableCreation(
        null, MySqlDebeziumAvroPayload.class.getName(), null, "ts", HoodieTableVersion.NINE, false);
    assertEquals(DebeziumConstants.FLATTENED_FILE_COL_NAME + "," + DebeziumConstants.FLATTENED_POS_COL_NAME,
        flat.get(HoodieTableConfig.ORDERING_FIELDS.key()));

    Map<String, String> nested = HoodieTableConfig.inferMergingConfigsForV9TableCreation(
        null, MySqlDebeziumAvroPayload.class.getName(), null, "ts", HoodieTableVersion.NINE, true);
    assertEquals(
        DebeziumConstants.DEBEZIUM_METADATA_FIELD + "." + DebeziumConstants.FLATTENED_FILE_COL_NAME + ","
            + DebeziumConstants.DEBEZIUM_METADATA_FIELD + "." + DebeziumConstants.FLATTENED_POS_COL_NAME,
        nested.get(HoodieTableConfig.ORDERING_FIELDS.key()));
  }

  @Test
  void postgresOrderingFieldStaysAtRootRegardlessOfNesting() {
    Map<String, String> flat = HoodieTableConfig.inferMergingConfigsForV9TableCreation(
        null, PostgresDebeziumAvroPayload.class.getName(), null, "ts", HoodieTableVersion.NINE, false);
    Map<String, String> nested = HoodieTableConfig.inferMergingConfigsForV9TableCreation(
        null, PostgresDebeziumAvroPayload.class.getName(), null, "ts", HoodieTableVersion.NINE, true);

    assertEquals(DebeziumConstants.FLATTENED_LSN_COL_NAME, flat.get(HoodieTableConfig.ORDERING_FIELDS.key()));
    assertEquals(DebeziumConstants.FLATTENED_LSN_COL_NAME, nested.get(HoodieTableConfig.ORDERING_FIELDS.key()));
  }
}
