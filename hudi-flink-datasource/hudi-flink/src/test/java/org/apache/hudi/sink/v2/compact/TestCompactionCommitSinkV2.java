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

package org.apache.hudi.sink.v2.compact;

import org.apache.hudi.avro.model.HoodieCompactionOperation;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.compact.CompactionCommitEvent;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.action.compact.CompactHelpers;
import org.apache.hudi.util.CompactionUtil;
import org.apache.hudi.util.FlinkWriteClients;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.Arrays;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests {@link CompactionCommitSinkV2} with Flink's operator test harness. */
class TestCompactionCommitSinkV2 {

  private static final String INSTANT = "20240101000000000";

  private Configuration conf;
  private HoodieFlinkWriteClient writeClient;
  private HoodieFlinkTable table;
  private HoodieTableMetaClient metaClient;
  private TransactionManager transactionManager;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    conf = new Configuration();
    conf.set(FlinkOptions.CLEAN_ASYNC_ENABLED, false);
    writeClient = mock(HoodieFlinkWriteClient.class);
    table = mock(HoodieFlinkTable.class);
    metaClient = mock(HoodieTableMetaClient.class);
    transactionManager = mock(TransactionManager.class);

    when(writeClient.getHoodieTable()).thenReturn(table);
    when(writeClient.getTransactionManager()).thenReturn(transactionManager);
    when(table.getMetaClient()).thenReturn(metaClient);
  }

  @Test
  void testWaitForEveryOperationThenRollbackFailedCompaction() throws Exception {
    HoodieCompactionPlan plan = planWithOperations(2);
    try (OneInputStreamOperatorTestHarness<CompactionCommitEvent, RowData> harness = openHarness();
         MockedStatic<CompactionUtils> compactionUtils = mockStatic(CompactionUtils.class);
         MockedStatic<CompactionUtil> compactionUtil = mockStatic(CompactionUtil.class)) {
      compactionUtils.when(() -> CompactionUtils.getCompactionPlan(metaClient, INSTANT))
          .thenReturn(plan);

      process(harness, successEvent("file-1", new WriteStatus()));
      compactionUtil.verifyNoInteractions();

      process(harness, failedEvent("file-2"));
      compactionUtil.verify(
          () -> CompactionUtil.rollbackCompaction(table, INSTANT, transactionManager), times(1));

      // Resetting after rollback makes the next event reload the plan.
      process(harness, successEvent("file-1", new WriteStatus()));
      compactionUtils.verify(
          () -> CompactionUtils.getCompactionPlan(metaClient, INSTANT), times(2));
    }
  }

  @Test
  void testRollbackWriteStatusesWithErrorsUnlessConfiguredToIgnore() throws Exception {
    WriteStatus failedStatus = new WriteStatus();
    failedStatus.setTotalErrorRecords(3);
    HoodieCompactionPlan plan = planWithOperations(1);

    try (OneInputStreamOperatorTestHarness<CompactionCommitEvent, RowData> harness = openHarness();
         MockedStatic<CompactionUtils> compactionUtils = mockStatic(CompactionUtils.class);
         MockedStatic<CompactionUtil> compactionUtil = mockStatic(CompactionUtil.class)) {
      compactionUtils.when(() -> CompactionUtils.getCompactionPlan(metaClient, INSTANT))
          .thenReturn(plan);

      process(harness, successEvent("file-1", failedStatus));

      compactionUtil.verify(
          () -> CompactionUtil.rollbackCompaction(table, INSTANT, transactionManager));
      verify(writeClient, never()).completeCompaction(any(), any(), any());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void testCommitWriteStatusesWithErrorsWhenConfiguredToIgnore() throws Exception {
    conf.set(FlinkOptions.IGNORE_FAILED, true);
    WriteStatus failedStatus = writeStatus("partition", "file-1", 3);
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    CompactHelpers compactHelpers = mock(CompactHelpers.class);
    HoodieCompactionPlan plan = planWithOperations(1);
    stubWriteConfig();

    try (OneInputStreamOperatorTestHarness<CompactionCommitEvent, RowData> harness = openHarness();
         MockedStatic<CompactionUtils> compactionUtils = mockStatic(CompactionUtils.class);
         MockedStatic<CompactionUtil> compactionUtil = mockStatic(CompactionUtil.class);
         MockedStatic<CompactHelpers> helpersFactory = mockStatic(CompactHelpers.class)) {
      compactionUtils.when(() -> CompactionUtils.getCompactionPlan(metaClient, INSTANT))
          .thenReturn(plan);
      helpersFactory.when(CompactHelpers::getInstance).thenReturn(compactHelpers);
      when(compactHelpers.createCompactionMetadata(
          same(table), eq(INSTANT), any(), eq("{}"))).thenReturn(metadata);

      process(harness, successEvent("file-1", failedStatus));

      compactionUtil.verifyNoInteractions();
      verify(writeClient).completeCompaction(same(metadata), same(table), eq(INSTANT));
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void testCommitSuccessfulCompactionAndCleanInline() throws Exception {
    WriteStatus status = writeStatus("partition", "file-1", 0);
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    CompactHelpers compactHelpers = mock(CompactHelpers.class);
    HoodieCompactionPlan plan = planWithOperations(1);
    stubWriteConfig();

    try (OneInputStreamOperatorTestHarness<CompactionCommitEvent, RowData> harness = openHarness();
         MockedStatic<CompactionUtils> compactionUtils = mockStatic(CompactionUtils.class);
         MockedStatic<CompactHelpers> helpersFactory = mockStatic(CompactHelpers.class)) {
      compactionUtils.when(() -> CompactionUtils.getCompactionPlan(metaClient, INSTANT))
          .thenReturn(plan);
      helpersFactory.when(CompactHelpers::getInstance).thenReturn(compactHelpers);
      when(compactHelpers.createCompactionMetadata(
          same(table), eq(INSTANT), any(), eq("{}"))).thenReturn(metadata);

      process(harness, successEvent("file-1", status));
    }

    verify(writeClient).completeCompaction(same(metadata), same(table), eq(INSTANT));
    verify(writeClient).clean();
  }

  private OneInputStreamOperatorTestHarness<CompactionCommitEvent, RowData> openHarness() throws Exception {
    ProcessOperator<CompactionCommitEvent, RowData> operator =
        new ProcessOperator<>(new CompactionCommitSinkV2(conf));
    OneInputStreamOperatorTestHarness<CompactionCommitEvent, RowData> harness =
        new OneInputStreamOperatorTestHarness<>(operator, 1, 1, 0);
    try (MockedStatic<FlinkWriteClients> writeClients = mockStatic(FlinkWriteClients.class)) {
      writeClients.when(() -> FlinkWriteClients.createWriteClient(
          eq(conf), any())).thenReturn(writeClient);
      harness.open();
    }
    return harness;
  }

  private void process(
      OneInputStreamOperatorTestHarness<CompactionCommitEvent, RowData> harness,
      CompactionCommitEvent event) throws Exception {
    harness.processElement(new StreamRecord<>(event));
  }

  private HoodieCompactionPlan planWithOperations(int numOperations) {
    HoodieCompactionPlan plan = mock(HoodieCompactionPlan.class);
    HoodieCompactionOperation[] operations = new HoodieCompactionOperation[numOperations];
    Arrays.setAll(operations, ignored -> mock(HoodieCompactionOperation.class));
    when(plan.getOperations()).thenReturn(Arrays.asList(operations));
    return plan;
  }

  private WriteStatus writeStatus(String partition, String fileId, long errors) {
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setPartitionPath(partition);
    writeStat.setFileId(fileId);
    WriteStatus status = new WriteStatus();
    status.setStat(writeStat);
    status.setTotalErrorRecords(errors);
    return status;
  }

  private void stubWriteConfig() {
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    when(writeConfig.getSchema()).thenReturn("{}");
    when(writeClient.getConfig()).thenReturn(writeConfig);
  }

  private CompactionCommitEvent successEvent(String fileId, WriteStatus... statuses) {
    List<WriteStatus> statusList = Arrays.asList(statuses);
    return new CompactionCommitEvent(INSTANT, fileId, statusList, 0, false, false);
  }

  private CompactionCommitEvent failedEvent(String fileId) {
    return new CompactionCommitEvent(INSTANT, fileId, 0, false, false);
  }
}
