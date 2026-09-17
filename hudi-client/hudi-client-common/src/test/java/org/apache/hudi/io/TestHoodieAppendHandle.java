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

package org.apache.hudi.io;

import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.OrderingValues;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Properties;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link HoodieAppendHandle}, covering the ordering value stamped on delete records.
 */
public class TestHoodieAppendHandle extends HoodieCommonTestHarness {

  private static final String TEST_INSTANT_TIME = "20231201120000";
  private static final String TEST_PARTITION_PATH = "partition1";
  private static final String TEST_FILE_ID = "file-001";
  private static final Long INGESTION_ORDERING_VALUE = 150L;

  private HoodieTable<Object, Object, Object, Object> mockHoodieTable;
  private TaskContextSupplier taskContextSupplier;

  @BeforeEach
  public void setUp() throws IOException {
    initPath();
    initMetaClient();
    taskContextSupplier = new LocalTaskContextSupplier();
    mockHoodieTable = mock(HoodieTable.class);
    when(mockHoodieTable.getMetaClient()).thenReturn(metaClient);
    when(mockHoodieTable.getStorage())
        .thenReturn(HoodieStorageUtils.getStorage(basePath, metaClient.getStorageConf()));
  }

  @AfterEach
  public void cleanUp() {
    cleanMetaClient();
  }

  private HoodieWriteConfig writeConfigFor(boolean sqlMergeIntoWrite) {
    Properties props = new Properties();
    props.setProperty(HoodieWriteConfig.SPARK_SQL_MERGE_INTO_WRITES_KEY, String.valueOf(sqlMergeIntoWrite));
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withMarkersType("DIRECT")
        .withWriteRecordPositionsEnabled(false)
        .withProperties(props)
        .build();
    when(mockHoodieTable.getConfig()).thenReturn(config);
    return config;
  }

  private HoodieAppendHandle<Object, Object, Object, Object> plainHandle(HoodieWriteConfig config) {
    return new HoodieAppendHandle<>(config, TEST_INSTANT_TIME, mockHoodieTable, TEST_PARTITION_PATH,
        TEST_FILE_ID, taskContextSupplier);
  }

  /**
   * The handle log compaction really builds. Constructing the parent's log compaction constructor
   * instead would exercise a path no production code calls, and the test would then pass whatever
   * the ordering gate did.
   */
  private HoodieAppendHandle<Object, Object, Object, Object> logCompactionHandle(HoodieWriteConfig config) {
    CompactionOperation operation = mock(CompactionOperation.class);
    when(operation.getPartitionPath()).thenReturn(TEST_PARTITION_PATH);
    when(operation.getFileId()).thenReturn(TEST_FILE_ID);
    return new FileGroupReaderBasedAppendHandle<>(config, TEST_INSTANT_TIME, mockHoodieTable, operation,
        taskContextSupplier, mock(HoodieReaderContext.class));
  }

  /**
   * A delete issued by the MERGE INTO statement itself is unconditional, so it carries the default
   * ordering value and the record's own ordering value is never consulted.
   */
  @Test
  void testStatementIssuedDeleteGetsDefaultOrderingValue() {
    HoodieRecord<Object> record = mock(HoodieRecord.class);

    Comparable<?> orderingValue = plainHandle(writeConfigFor(true)).getDeleteOrderingValue(record);

    assertEquals(OrderingValues.getDefault(), orderingValue);
    verify(record, never()).getOrderingValueAsJava(any(), any(), any());
  }

  /**
   * Log compaction inherits the write config of the client that scheduled it, so under a MERGE INTO
   * write its config says merge-into while the records it rewrites are pre-existing log records.
   * Their ordering values must survive, or a later upsert older than an ingestion delete would
   * resurrect the record.
   */
  @Test
  void testLogCompactionUnderMergeIntoConfigKeepsOrderingValues() {
    HoodieRecord<Object> record = mock(HoodieRecord.class);
    doReturn(INGESTION_ORDERING_VALUE).when(record).getOrderingValueAsJava(any(), any(), any());

    Comparable<?> orderingValue =
        logCompactionHandle(writeConfigFor(true)).getDeleteOrderingValue(record);

    assertEquals(INGESTION_ORDERING_VALUE, orderingValue);
  }

  /**
   * An ordinary ingestion or CDC delete keeps event time semantics, whichever handle writes it.
   */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testDeleteOutsideMergeIntoKeepsItsOrderingValue(boolean logCompaction) {
    HoodieRecord<Object> record = mock(HoodieRecord.class);
    doReturn(INGESTION_ORDERING_VALUE).when(record).getOrderingValueAsJava(any(), any(), any());
    HoodieWriteConfig config = writeConfigFor(false);

    Comparable<?> orderingValue = logCompaction
        ? logCompactionHandle(config).getDeleteOrderingValue(record)
        : plainHandle(config).getDeleteOrderingValue(record);

    assertEquals(INGESTION_ORDERING_VALUE, orderingValue);
  }
}
