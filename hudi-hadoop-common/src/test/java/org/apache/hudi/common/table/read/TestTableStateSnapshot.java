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

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.internal.InternalSchema;
import org.apache.hudi.common.schema.internal.Types;
import org.apache.hudi.common.schema.internal.io.FileBasedInternalSchemaStorageManager;
import org.apache.hudi.common.schema.internal.utils.SerDeHelper;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Path;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that {@link TableState#snapshotOf} captures committed instants and schema versions only where
 * they are consulted.
 */
class TestTableStateSnapshot {

  @TempDir
  Path tempDir;

  @Test
  void capturesCommittedInstantsOnlyForLogReadsBeforeVersionEight() throws Exception {
    HoodieTableMetaClient sixTable = tableWithCommit("six", HoodieTableVersion.SIX);
    TableState withLogs = TableState.snapshotOf(sixTable, true, false);
    assertTrue(withLogs.isCommitted("001"));
    assertFalse(withLogs.isCommitted("002"));
    assertThrows(IllegalStateException.class,
        () -> TableState.snapshotOf(sixTable, false, false).isCommitted("001"));

    HoodieTableMetaClient currentTable = tableWithCommit("current", HoodieTableVersion.current());
    assertThrows(IllegalStateException.class,
        () -> TableState.snapshotOf(currentTable, true, false).isCommitted("001"));
  }

  @Test
  void carriesTheMetaClientOnlyToResolveSchemaVersions() throws Exception {
    HoodieTableMetaClient metaClient = tableWithCommit("schema", HoodieTableVersion.current());
    InternalSchema schema = new InternalSchema(1L,
        Types.RecordType.get(Collections.singletonList(Types.Field.get(1, true, "a", Types.LongType.get()))));
    new FileBasedInternalSchemaStorageManager(metaClient).persistHistorySchemaStr("001", SerDeHelper.inheritSchemas(schema, ""));

    TableState shipped = roundTrip(TableState.snapshotOf(metaClient, true, true));
    assertEquals("a", shipped.getInternalSchema(1L).getRecord().fields().get(0).name());
    assertThrows(IllegalStateException.class,
        () -> roundTrip(TableState.snapshotOf(metaClient, true, false)).getInternalSchema(1L));
  }

  @SuppressWarnings("unchecked")
  private static <T> T roundTrip(T value) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(value);
    }
    try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      return (T) in.readObject();
    }
  }

  private HoodieTableMetaClient tableWithCommit(String name, HoodieTableVersion version) throws Exception {
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(
        tempDir.resolve(name).toString(), HoodieTableType.MERGE_ON_READ, version);
    HoodieTestTable.of(metaClient).addDeltaCommit("001");
    metaClient.reloadActiveTimeline();
    return metaClient;
  }
}
