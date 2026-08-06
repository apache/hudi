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

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHoodieMetaSyncOperations {

  private final HoodieMetaSyncOperations operations = new HoodieMetaSyncOperations() { };

  @Test
  void defaultOperationsRemainSafeForMinimalImplementations() {
    assertDoesNotThrow(() -> operations.createTable("table", null, null, null, null,
        Collections.emptyMap(), Collections.emptyMap()));
    assertDoesNotThrow(() -> operations.createOrReplaceTable("table", null, null, null, null,
        Collections.emptyMap(), Collections.emptyMap()));
    assertFalse(operations.tableExists("table"));
    assertDoesNotThrow(() -> operations.dropTable("table"));
    assertDoesNotThrow(() -> operations.addPartitionsToTable("table", Collections.emptyList()));
    assertDoesNotThrow(() -> operations.updatePartitionsToTable("table", Collections.emptyList()));
    assertDoesNotThrow(() -> operations.touchPartitionsToTable("table", Collections.emptyList()));
    assertDoesNotThrow(() -> operations.dropPartitions("table", Collections.emptyList()));
    assertTrue(operations.getAllPartitions("table").isEmpty());
    assertTrue(operations.getPartitionsFromList("table", Collections.emptyList()).isEmpty());
    assertFalse(operations.databaseExists("database"));
    assertDoesNotThrow(() -> operations.createDatabase("database"));
    assertTrue(operations.getMetastoreSchema("table").isEmpty());
    assertNull(operations.getStorageSchema());
    assertNull(operations.getStorageSchema(true));
    assertDoesNotThrow(() -> operations.updateTableSchema("table", null, null));
    assertTrue(operations.getMetastoreFieldSchemas("table").isEmpty());
    assertTrue(operations.getStorageFieldSchemas().isEmpty());
    assertTrue(operations.getTableLocation("table").isEmpty());
    assertFalse(operations.updateTableComments("table", Collections.emptyList(), Collections.emptyList()));
    assertFalse(operations.getLastCommitTimeSynced("table").isPresent());
    assertFalse(operations.getLastCommitCompletionTimeSynced("table").isPresent());
    assertDoesNotThrow(() -> operations.updateLastCommitTimeSynced("table"));
    assertFalse(operations.updateTableProperties("table", Collections.emptyMap()));
    assertFalse(operations.updateSerdeProperties("table", Collections.emptyMap(), false));
    assertFalse(operations.getLastReplicatedTime("table").isPresent());
    assertDoesNotThrow(() -> operations.updateLastReplicatedTimeStamp("table", "001"));
    assertDoesNotThrow(() -> operations.deleteLastReplicatedTimeStamp("table"));
    assertThrows(UnsupportedOperationException.class,
        () -> operations.generatePushDownFilter(Collections.emptyList(), Collections.emptyList()));
    assertThrows(UnsupportedOperationException.class, () -> operations.updateHoodieWriterVersion("table"));
  }
}
