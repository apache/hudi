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

package org.apache.hudi.client;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestSparkStreamingMetadataWriteHandler extends SparkClientFunctionalTestHarness {

  @Override
  public SparkConf conf() {
    // Permit one retry so the regression can distinguish a failed task attempt from the
    // scheduler-selected successful output consumed by the metadata completion path.
    return conf(java.util.Collections.singletonMap("spark.master", "local[8,2]"));
  }

  private final HoodieTable<?, ?, ?, ?> mockHoodieTable = mock(HoodieTable.class);
  private HoodieTableMetaClient metaClient;

  @BeforeEach
  void setUp() {
    metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getBasePath()).thenReturn(new StoragePath("/tmp/"));
    when(mockHoodieTable.getMetaClient()).thenReturn(metaClient);
    HoodieEngineContext engineContext = mock(HoodieEngineContext.class);
    when(mockHoodieTable.getContext()).thenReturn(engineContext);
  }

  private static Stream<Arguments> coalesceDivisorTestArgs() {
    return Arrays.stream(new Object[][] {
        {100, 1000},
        {1, 1},
        {10000, 1},
        {10000, 5000},
        {10001, 5000},
        {10000, 20000}
    }).map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("coalesceDivisorTestArgs")
  public void testCoalesceDividentConfig(int numDataTableWriteStatuses, int coalesceDividentForDataTableWrites) {
    HoodieData<WriteStatus> dataTableWriteStatus = mockWriteStatuses(numDataTableWriteStatuses);
    HoodieTableMetadataWriter mdtWriter = mock(HoodieTableMetadataWriter.class);
    SparkStreamingMetadataWriteHandler metadataWriteHandler = new MockSparkStreamingMetadataWriteHandler(mdtWriter);

    HoodieData<WriteStatus> allWriteStatuses = metadataWriteHandler.streamWriteToMetadataTable(mockHoodieTable, dataTableWriteStatus, "00001",
        coalesceDividentForDataTableWrites);
    assertEquals(Math.max(1, numDataTableWriteStatuses / coalesceDividentForDataTableWrites), allWriteStatuses.getNumPartitions());
    verify(mdtWriter, never()).streamWriteToMetadataPartitions(any(), any());
  }

  @Test
  void testSparkCollectsOnlySuccessfulRetriedTaskAttempt() {
    HoodieData<WriteStatus> retryingDataWriteStatus = HoodieJavaRDD.of(jsc().parallelize(Arrays.asList(0, 1), 2)
        .map(partition -> {
          int attempt = TaskContext.get().attemptNumber();
          if (partition == 0 && attempt == 0) {
            throw new IllegalStateException("intentional first-attempt failure");
          }
          return writeStatus("file-" + partition + "-attempt-" + attempt, false);
        }));
    HoodieTableMetadataWriter mdtWriter = mock(HoodieTableMetadataWriter.class);

    List<WriteStatus> statuses = new MockSparkStreamingMetadataWriteHandler(mdtWriter)
        .streamWriteToMetadataTable(mockHoodieTable, retryingDataWriteStatus, "00001", 1000)
        .collectAsList();
    List<String> paths = statuses.stream().map(status -> status.getStat().getPath()).collect(java.util.stream.Collectors.toList());

    assertTrue(paths.contains("file-0-attempt-1"), "Retry output must come from the successful attempt");
    assertTrue(paths.stream().noneMatch(path -> path.equals("file-0-attempt-0")));
    verify(mdtWriter, never()).streamWriteToMetadataPartitions(any(), any());
  }

  @Test
  void testSparkDefersMetadataGenerationUntilCommittedWriteStatsAreAvailable() {
    HoodieData<WriteStatus> lazyDataWriteStatus = HoodieJavaRDD.of(jsc().parallelize(Arrays.asList(0, 1), 2)
        .map(partition -> writeStatus("file-" + partition + "-stage-" + org.apache.spark.TaskContext.get().stageId(), false)));
    HoodieTableMetadataWriter mdtWriter = mock(HoodieTableMetadataWriter.class);

    HoodieData<WriteStatus> allWriteStatuses = new MockSparkStreamingMetadataWriteHandler(mdtWriter)
        .streamWriteToMetadataTable(mockHoodieTable, lazyDataWriteStatus, "00001", 1000);
    List<WriteStatus> statuses = allWriteStatuses.collectAsList();

    assertTrue(statuses.stream().noneMatch(WriteStatus::isMetadataTable));
    assertTrue(statuses.size() > 1, "Test must exercise multiple source partitions");
    verify(mdtWriter, never()).streamWriteToMetadataPartitions(any(), any());
  }

  private static WriteStatus writeStatus(String path, boolean isMetadataTable) {
    HoodieWriteStat stat = new HoodieWriteStat();
    stat.setPath(path);
    WriteStatus status = new WriteStatus(true, 0.0, isMetadataTable);
    status.setStat(stat);
    return status;
  }

  private HoodieData<WriteStatus> mockWriteStatuses(int size) {
    List<WriteStatus> writeStatuses = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      writeStatuses.add(mock(WriteStatus.class));
    }
    return HoodieJavaRDD.of(jsc().parallelize(writeStatuses, size));
  }

  class MockSparkStreamingMetadataWriteHandler extends SparkStreamingMetadataWriteHandler {

    private HoodieTableMetadataWriter mdtWriter;

    MockSparkStreamingMetadataWriteHandler(HoodieTableMetadataWriter mdtWriter) {
      this.mdtWriter = mdtWriter;
    }

    @Override
    protected synchronized Option<HoodieTableMetadataWriter> getMetadataWriter(String triggeringInstant, HoodieTable table) {
      return Option.of(mdtWriter);
    }
  }
}
