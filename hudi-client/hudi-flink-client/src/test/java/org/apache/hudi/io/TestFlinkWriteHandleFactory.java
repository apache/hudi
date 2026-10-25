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

package org.apache.hudi.io;

import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.v2.RowDataInlineLogWriteHandle;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.commit.BucketInfo;
import org.apache.hudi.table.action.commit.BucketType;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Verifies operation-specific Flink write handle selection. */
@SuppressWarnings({"rawtypes", "unchecked"})
class TestFlinkWriteHandleFactory {

  private HoodieTableConfig tableConfig;
  private HoodieWriteConfig writeConfig;
  private HoodieTable table;
  private Iterator<HoodieRecord<Object>> records;

  @BeforeEach
  void setUp() {
    tableConfig = mock(HoodieTableConfig.class);
    writeConfig = mock(HoodieWriteConfig.class);
    table = mock(HoodieTable.class);
    records = Collections.<HoodieRecord<Object>>emptyList().iterator();
    when(table.getTaskContextSupplier()).thenReturn(mock(TaskContextSupplier.class));
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(table.getMetaClient()).thenReturn(metaClient);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.isLSMTreeStorageLayout()).thenReturn(false);
    when(writeConfig.getBasePath()).thenReturn("/tmp/flink-handle-factory-test");
    when(writeConfig.getWriteVersion()).thenReturn(HoodieTableVersion.NINE);
  }

  @Test
  void testCommitFactoryCreatesBaseFileHandle() {
    when(tableConfig.getTableType()).thenReturn(HoodieTableType.COPY_ON_WRITE);
    when(tableConfig.isCDCEnabled()).thenReturn(false);
    BucketInfo bucketInfo = new BucketInfo(BucketType.INSERT, "file-1", "partition");
    Map<String, Path> handles = new HashMap<>();
    StoragePath writePath = new StoragePath("/tmp/flink-handle-factory-test/file-1.parquet");

    try (MockedConstruction<FlinkCreateHandle> mocked = Mockito.mockConstruction(
        FlinkCreateHandle.class,
        (handle, context) -> when(handle.getWritePath()).thenReturn(writePath))) {
      HoodieWriteHandle handle = factory(false).create(
          handles, bucketInfo, writeConfig, "001", table, records);
      assertSame(mocked.constructed().get(0), handle);
      assertEquals(writePath.toUri(), handles.get("file-1").toUri());
    }
  }

  @Test
  void testClusterFactoryCreatesConcatAndIncrementalConcatHandles() {
    when(writeConfig.allowDuplicateInserts()).thenReturn(true);
    BucketInfo bucketInfo = new BucketInfo(BucketType.UPDATE, "file-1", "partition");
    Map<String, Path> handles = new HashMap<>();
    StoragePath firstPath = new StoragePath("/tmp/flink-handle-factory-test/first.parquet");

    try (MockedConstruction<FlinkConcatHandle> mocked = Mockito.mockConstruction(
        FlinkConcatHandle.class,
        (handle, context) -> when(handle.getWritePath()).thenReturn(firstPath))) {
      HoodieWriteHandle handle = factory(false).create(
          handles, bucketInfo, writeConfig, "001", table, records);
      assertSame(mocked.constructed().get(0), handle);
    }

    StoragePath replacementPath = new StoragePath("/tmp/flink-handle-factory-test/replacement.parquet");
    try (MockedConstruction<FlinkIncrementalConcatHandle> mocked = Mockito.mockConstruction(
        FlinkIncrementalConcatHandle.class,
        (handle, context) -> when(handle.getWritePath()).thenReturn(replacementPath))) {
      HoodieWriteHandle handle = factory(false).create(
          handles, bucketInfo, writeConfig, "001", table, records);
      assertSame(mocked.constructed().get(0), handle);
      assertEquals(replacementPath.toUri(), handles.get("file-1").toUri());
    }
  }

  @Test
  void testCdcFactoryCreatesIncrementalChangeLogHandle() {
    when(tableConfig.getTableType()).thenReturn(HoodieTableType.COPY_ON_WRITE);
    when(tableConfig.isCDCEnabled()).thenReturn(true);
    BucketInfo bucketInfo = new BucketInfo(BucketType.UPDATE, "file-1", "partition");
    Map<String, Path> handles = new HashMap<>();
    handles.put("file-1", new Path("/tmp/flink-handle-factory-test/original.parquet"));
    StoragePath replacementPath = new StoragePath("/tmp/flink-handle-factory-test/cdc.parquet");

    try (MockedConstruction<FlinkIncrementalMergeHandleWithChangeLog> mocked = Mockito.mockConstruction(
        FlinkIncrementalMergeHandleWithChangeLog.class,
        (handle, context) -> when(handle.getWritePath()).thenReturn(replacementPath))) {
      HoodieWriteHandle handle = factory(false).create(
          handles, bucketInfo, writeConfig, "001", table, records);
      assertSame(mocked.constructed().get(0), handle);
      assertEquals(replacementPath.toUri(), handles.get("file-1").toUri());
    }
  }

  @Test
  void testCommitFactoryCreatesFileGroupReaderMergeHandle() {
    when(tableConfig.getTableType()).thenReturn(HoodieTableType.COPY_ON_WRITE);
    when(writeConfig.getMergeHandleClassName()).thenReturn(FileGroupReaderBasedMergeHandle.class.getName());
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(table.getMetaClient()).thenReturn(metaClient);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.isLSMTreeStorageLayout()).thenReturn(false);
    BucketInfo bucketInfo = new BucketInfo(BucketType.UPDATE, "file-1", "partition");
    StoragePath writePath = new StoragePath("/tmp/flink-handle-factory-test/reader.parquet");

    try (MockedConstruction<FlinkFileGroupReaderBasedMergeHandle> mocked = Mockito.mockConstruction(
        FlinkFileGroupReaderBasedMergeHandle.class,
        (handle, context) -> when(handle.getWritePath()).thenReturn(writePath))) {
      HoodieWriteHandle handle = factory(false).create(
          new HashMap<>(), bucketInfo, writeConfig, "001", table, records);
      assertSame(mocked.constructed().get(0), handle);
    }
  }

  @Test
  void testMergeOnReadSelectsMetadataAndRowDataLogHandles() {
    when(tableConfig.getTableType()).thenReturn(HoodieTableType.MERGE_ON_READ);
    BucketInfo bucketInfo = new BucketInfo(BucketType.UPDATE, "file-1", "partition");

    when(writeConfig.getBasePath()).thenReturn("/tmp/table/.hoodie/metadata");
    try (MockedConstruction<FlinkInlineLogAppendHandle> mocked =
             Mockito.mockConstruction(FlinkInlineLogAppendHandle.class)) {
      HoodieWriteHandle handle = factory(false).create(
          new HashMap<>(), bucketInfo, writeConfig, "001", table, records);
      assertSame(mocked.constructed().get(0), handle);
    }

    when(writeConfig.getBasePath()).thenReturn("/tmp/table");
    try (MockedConstruction<RowDataInlineLogWriteHandle> mocked =
             Mockito.mockConstruction(RowDataInlineLogWriteHandle.class)) {
      HoodieWriteHandle handle = factory(false).create(
          new HashMap<>(), bucketInfo, writeConfig, "002", table, records);
      assertSame(mocked.constructed().get(0), handle);
    }
  }

  @Test
  void testLsmTreeStorageLayoutDetection() {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(table.getMetaClient()).thenReturn(metaClient);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.isLSMTreeStorageLayout()).thenReturn(false);
    assertFalse(FlinkWriteHandleFactory.isLsmTreeStorageLayout(table));

    when(tableConfig.isLSMTreeStorageLayout()).thenReturn(true);
    assertTrue(FlinkWriteHandleFactory.isLsmTreeStorageLayout(table));
  }

  private FlinkWriteHandleFactory.Factory factory(boolean overwrite) {
    return FlinkWriteHandleFactory.getFactory(tableConfig, writeConfig, overwrite);
  }
}
