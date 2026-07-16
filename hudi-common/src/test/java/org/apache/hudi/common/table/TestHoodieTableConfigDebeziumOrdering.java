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
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests that {@link HoodieTableConfig#inferMergingConfigsForV9TableCreation} reconciles a Debezium
 * payload's ordering field to the canonical column(s) the payload merges on: the flat form by
 * default (auto-correcting whatever the caller supplied), or the nested
 * {@code _debezium_metadata.*} form when the caller already resolved it — which only the Hudi
 * Streamer does when its transformer groups the CDC metadata under the nested struct.
 */
class TestHoodieTableConfigDebeziumOrdering {

  private static final String MYSQL_FLAT =
      DebeziumConstants.FLATTENED_FILE_COL_NAME + "," + DebeziumConstants.FLATTENED_POS_COL_NAME;
  private static final String MYSQL_NESTED =
      DebeziumConstants.DEBEZIUM_METADATA_FIELD + "." + DebeziumConstants.FLATTENED_FILE_COL_NAME + ","
          + DebeziumConstants.DEBEZIUM_METADATA_FIELD + "." + DebeziumConstants.FLATTENED_POS_COL_NAME;

  private static String inferredOrderingFields(String payloadClass, String callerOrderingFields) {
    Map<String, String> configs = HoodieTableConfig.inferMergingConfigsForV9TableCreation(
        null, payloadClass, null, callerOrderingFields, HoodieTableVersion.NINE);
    return configs.get(HoodieTableConfig.ORDERING_FIELDS.key());
  }

  @Test
  void mysqlOrderingIsAutoCorrectedToFlatBinlogColumns() {
    String mysql = MySqlDebeziumAvroPayload.class.getName();
    // A non-canonical caller value (arbitrary field, or the legacy _event_seq) is corrected to the
    // flat binlog coordinates the payload merges on.
    assertEquals(MYSQL_FLAT, inferredOrderingFields(mysql, "ts"));
    assertEquals(MYSQL_FLAT, inferredOrderingFields(mysql, DebeziumConstants.ADDED_SEQ_COL_NAME));
    // Passing the flat form back through is idempotent.
    assertEquals(MYSQL_FLAT, inferredOrderingFields(mysql, MYSQL_FLAT));
  }

  @Test
  void mysqlNestedOrderingIsPreservedWhenCallerResolvesIt() {
    // The streamer passes the nested path when its transformer nests the CDC metadata; it must be
    // preserved so the payload orders against the nested columns rather than the (absent) root ones.
    assertEquals(MYSQL_NESTED, inferredOrderingFields(MySqlDebeziumAvroPayload.class.getName(), MYSQL_NESTED));
  }

  @Test
  void postgresOrderingIsAlwaysLsn() {
    String postgres = PostgresDebeziumAvroPayload.class.getName();
    // The Postgres LSN stays at the root level even when nested, so the ordering field is always _event_lsn.
    assertEquals(DebeziumConstants.FLATTENED_LSN_COL_NAME, inferredOrderingFields(postgres, "ts"));
    assertEquals(DebeziumConstants.FLATTENED_LSN_COL_NAME,
        inferredOrderingFields(postgres, DebeziumConstants.FLATTENED_LSN_COL_NAME));
  }

  @Test
  void resolveOrderingFieldsReturnsNullForNonDebeziumPayload() {
    assertNull(DebeziumConstants.resolveOrderingFields("com.example.CustomPayload", false));
    assertNull(DebeziumConstants.resolveOrderingFields(null, true));
  }
}
