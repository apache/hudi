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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.hive;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.hive.ddl.DDLExecutor;
import org.apache.hudi.hive.ddl.JDBCBasedMetadataOperator;
import org.apache.hudi.sync.common.model.FieldSchema;
import org.apache.hudi.sync.common.model.Partition;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.apache.hudi.hadoop.utils.HoodieHiveUtils.GLOBALLY_CONSISTENT_READ_TIMESTAMP;
import static org.apache.hudi.sync.common.HoodieMetaSyncOperations.HOODIE_LAST_COMMIT_COMPLETION_TIME_SYNC;
import static org.apache.hudi.sync.common.HoodieMetaSyncOperations.HOODIE_LAST_COMMIT_TIME_SYNC;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_FILE_FORMAT;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestHoodieHiveSyncClientOperations {

  @Test
  void thriftMismatchActivatesJdbcMetadataFallback() throws Exception {
    TestFixture fixture = newFixture();
    TApplicationException incompatible = new TApplicationException("unknown get_table method");
    when(fixture.metaStoreClient.tableExists("test_db", "table"))
        .thenThrow(new TException(incompatible));
    when(fixture.jdbcMetadataOperator.tableExists("table")).thenReturn(true);
    when(fixture.jdbcMetadataOperator.databaseExists("test_db")).thenReturn(true);
    when(fixture.jdbcMetadataOperator.getTableProperty("table", HOODIE_LAST_COMMIT_TIME_SYNC))
        .thenReturn(Option.of("100"));
    when(fixture.jdbcMetadataOperator.getTableProperty("table", HOODIE_LAST_COMMIT_COMPLETION_TIME_SYNC))
        .thenReturn(Option.of("101"));
    when(fixture.jdbcMetadataOperator.getTableProperty("table", GLOBALLY_CONSISTENT_READ_TIMESTAMP))
        .thenReturn(Option.of("099"));
    when(fixture.jdbcMetadataOperator.getTableLocation("table")).thenReturn("/warehouse/table");
    List<FieldSchema> fields = Collections.singletonList(new FieldSchema("id", "string", Option.empty()));
    when(fixture.jdbcMetadataOperator.getFieldSchemas("table")).thenReturn(fields);
    List<Partition> partitions = Collections.singletonList(
        new Partition(Collections.singletonList("2026-08-06"), "/base/datestr=2026-08-06"));
    when(fixture.jdbcMetadataOperator.getAllPartitions("table", "/base")).thenReturn(partitions);

    assertTrue(fixture.syncClient.tableExists("table"));
    assertTrue(fixture.syncClient.updateTableProperties("table", Collections.singletonMap("owner", "hudi")));
    assertTrue(fixture.syncClient.updateSerdeProperties("table",
        new HashMap<>(Collections.singletonMap("compression", "none")), false));
    assertEquals(partitions, fixture.syncClient.getAllPartitions("table"));
    assertEquals(partitions,
        fixture.syncClient.getPartitionsFromList("table", Collections.singletonList("datestr=2026-08-06")));
    assertTrue(fixture.syncClient.databaseExists("test_db"));
    assertEquals("100", fixture.syncClient.getLastCommitTimeSynced("table").get());
    assertEquals("101", fixture.syncClient.getLastCommitCompletionTimeSynced("table").get());
    assertEquals("099", fixture.syncClient.getLastReplicatedTime("table").get());
    assertEquals(fields, fixture.syncClient.getMetastoreFieldSchemas("table"));
    assertEquals("/warehouse/table", fixture.syncClient.getTableLocation("table"));

    fixture.syncClient.deleteLastReplicatedTimeStamp("table");
    fixture.syncClient.createOrReplaceTable("table", null, "input", "output", "serde",
        Collections.emptyMap(), Collections.emptyMap());

    ArgumentCaptor<String> temporaryTableNameCaptor = ArgumentCaptor.forClass(String.class);
    InOrder replacementOrder = inOrder(fixture.ddlExecutor, fixture.jdbcMetadataOperator);
    replacementOrder.verify(fixture.ddlExecutor).createTable(temporaryTableNameCaptor.capture(),
        isNull(), eq("input"), eq("output"), eq("serde"), eq(Collections.emptyMap()), eq(Collections.emptyMap()));
    String temporaryTableName = temporaryTableNameCaptor.getValue();
    replacementOrder.verify(fixture.jdbcMetadataOperator).dropTable("table");
    replacementOrder.verify(fixture.jdbcMetadataOperator).renameTable(temporaryTableName, "table");

    verify(fixture.jdbcMetadataOperator).unsetTableProperty("table", GLOBALLY_CONSISTENT_READ_TIMESTAMP);
    verify(fixture.jdbcMetadataOperator).setStorageFormat(eq("table"), anyString(), anyString(), anyString(), any());
  }

  @Test
  void metastoreFailuresAreWrappedWhenNoFallbackIsConfigured() throws Exception {
    TestFixture fixture = newFixtureWithoutJdbc();

    assertFalse(fixture.syncClient.updateTableProperties("table", Collections.emptyMap()));
    assertFalse(fixture.syncClient.updateSerdeProperties("table", Collections.emptyMap(), false));

    when(fixture.metaStoreClient.tableExists("test_db", "missing")).thenReturn(false);
    assertThrows(IllegalArgumentException.class, () -> fixture.syncClient.getMetastoreSchema("missing"));

    when(fixture.metaStoreClient.tableExists("test_db", "table")).thenThrow(new TException("unavailable"));
    assertThrows(HoodieHiveSyncException.class, () -> fixture.syncClient.tableExists("table"));

    when(fixture.metaStoreClient.getDatabase("test_db")).thenThrow(new TException("unavailable"));
    assertThrows(HoodieHiveSyncException.class, () -> fixture.syncClient.databaseExists("test_db"));

    when(fixture.metaStoreClient.getTable("test_db", "absent"))
        .thenThrow(new NoSuchObjectException("absent"));
    assertFalse(fixture.syncClient.getLastReplicatedTime("absent").isPresent());
    fixture.syncClient.deleteLastReplicatedTimeStamp("absent");

    when(fixture.metaStoreClient.getTable("test_db", "table")).thenThrow(new TException("unavailable"));
    assertThrows(HoodieHiveSyncException.class, () -> fixture.syncClient.getLastCommitTimeSynced("table"));
    assertThrows(HoodieHiveSyncException.class,
        () -> fixture.syncClient.getLastCommitCompletionTimeSynced("table"));
    assertThrows(HoodieHiveSyncException.class, () -> fixture.syncClient.getLastReplicatedTime("table"));
    assertThrows(HoodieHiveSyncException.class, () -> fixture.syncClient.deleteLastReplicatedTimeStamp("table"));
    assertThrows(HoodieHiveSyncException.class,
        () -> fixture.syncClient.updateTableProperties("table", Collections.singletonMap("key", "value")));
    assertThrows(HoodieHiveSyncException.class,
        () -> fixture.syncClient.updateSerdeProperties("table", new HashMap<>(Collections.singletonMap("key", "value")), false));
    assertThrows(HoodieHiveSyncException.class, () -> fixture.syncClient.getTableLocation("table"));
    assertThrows(HoodieHiveSyncException.class, () -> fixture.syncClient.updateHoodieWriterVersion("table"));

    when(fixture.metaStoreClient.getSchema("test_db", "table")).thenThrow(new TException("unavailable"));
    assertThrows(HoodieHiveSyncException.class, () -> fixture.syncClient.getMetastoreFieldSchemas("table"));
    org.mockito.Mockito.doThrow(new TException("unavailable"))
        .when(fixture.metaStoreClient).dropTable("test_db", "table");
    assertThrows(HoodieHiveSyncException.class, () -> fixture.syncClient.dropTable("table"));
  }

  @Test
  void metastoreCreateReplaceAndSerdeAlterPathsUpdateMetadata() throws Exception {
    TestFixture fixture = newFixtureWithoutJdbc();
    when(fixture.metaStoreClient.tableExists("test_db", "table")).thenReturn(true);
    Table temporaryTable = new Table();
    when(fixture.metaStoreClient.getTable(eq("test_db"), anyString())).thenReturn(temporaryTable);

    fixture.syncClient.createOrReplaceTable("table", null, "input", "output", "serde",
        Collections.emptyMap(), Collections.emptyMap());

    ArgumentCaptor<String> temporaryTableNameCaptor = ArgumentCaptor.forClass(String.class);
    InOrder replacementOrder = inOrder(fixture.ddlExecutor, fixture.metaStoreClient);
    replacementOrder.verify(fixture.ddlExecutor).createTable(temporaryTableNameCaptor.capture(),
        isNull(), eq("input"), eq("output"), eq("serde"), eq(Collections.emptyMap()), eq(Collections.emptyMap()));
    String temporaryTableName = temporaryTableNameCaptor.getValue();
    replacementOrder.verify(fixture.metaStoreClient).dropTable("test_db", "table");
    replacementOrder.verify(fixture.metaStoreClient).alter_table("test_db", temporaryTableName, temporaryTable);

    Table serdeTable = new Table();
    StorageDescriptor storageDescriptor = new StorageDescriptor();
    storageDescriptor.setInputFormat("old-input");
    storageDescriptor.setOutputFormat("old-output");
    storageDescriptor.setSerdeInfo((SerDeInfo) null);
    serdeTable.setSd(storageDescriptor);
    when(fixture.metaStoreClient.getTable("test_db", "serde_table")).thenReturn(serdeTable);

    assertTrue(fixture.syncClient.updateSerdeProperties("serde_table",
        new HashMap<>(Collections.singletonMap("compression", "none")), false));
    assertTrue(storageDescriptor.getSerdeInfo().getParameters().containsKey("serialization.format"));
    assertFalse(fixture.syncClient.updateTableComments("table", Collections.emptyList(), Collections.emptyList()));
  }

  private static TestFixture newFixture() throws Exception {
    TestFixture fixture = newFixtureWithoutJdbc();
    fixture.jdbcMetadataOperator = mock(JDBCBasedMetadataOperator.class);
    setField(fixture.syncClient, "jdbcMetadataOperator", fixture.jdbcMetadataOperator);
    return fixture;
  }

  private static TestFixture newFixtureWithoutJdbc() throws Exception {
    TestFixture fixture = new TestFixture();
    fixture.syncClient = mock(HoodieHiveSyncClient.class, CALLS_REAL_METHODS);
    fixture.config = mock(HiveSyncConfig.class);
    fixture.metaStoreClient = mock(IMetaStoreClient.class);
    fixture.ddlExecutor = mock(DDLExecutor.class);
    when(fixture.config.getString(META_SYNC_BASE_PATH)).thenReturn("/base");
    when(fixture.config.getStringOrDefault(META_SYNC_BASE_FILE_FORMAT)).thenReturn("PARQUET");
    when(fixture.ddlExecutor.supportsUpdatingPartitionColumnComments()).thenReturn(true);
    setField(fixture.syncClient, "config", fixture.config);
    setField(fixture.syncClient, "databaseName", "test_db");
    setField(fixture.syncClient, "client", fixture.metaStoreClient);
    setField(fixture.syncClient, "ddlExecutor", fixture.ddlExecutor);
    setField(fixture.syncClient, "initialTableByName", new HashMap<String, Table>());
    return fixture;
  }

  private static void setField(Object target, String name, Object value) throws Exception {
    Field field = HoodieHiveSyncClient.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }

  private static class TestFixture {
    private HoodieHiveSyncClient syncClient;
    private HiveSyncConfig config;
    private IMetaStoreClient metaStoreClient;
    private DDLExecutor ddlExecutor;
    private JDBCBasedMetadataOperator jdbcMetadataOperator;
  }
}
