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
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestStreamingMetadataWriteHandler extends SparkClientFunctionalTestHarness {

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

  private static Stream<Arguments> coalesceDividentTestArgs() {
    return Arrays.stream(new Object[][] {
        {100, 20, 1000, true},
        {100, 20, 1000, false},
        {1, 1, 1000, true},
        {1, 1, 1000, false},
        {10000, 100, 5000, true},
        {10000, 100, 5000, true},
        {10000, 100, 20000, true},
        {10000, 100, 20000, true}
    }).map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("coalesceDividentTestArgs")
  public void testCoalesceDividentConfig(int numDataTableWriteStatuses, int numMdtWriteStatus, int coalesceDividentForDataTableWrites,
                                         boolean enforceCoalesceWithRepartition) {
    HoodieData<WriteStatus> dataTableWriteStatus = mockWriteStatuses(numDataTableWriteStatuses);
    HoodieData<WriteStatus> mdtWriteStatus = mockWriteStatuses(numMdtWriteStatus);
    HoodieTableMetadataWriter mdtWriter = mock(HoodieTableMetadataWriter.class);
    when(mdtWriter.streamWriteToMetadataPartitions(any(), any())).thenReturn(mdtWriteStatus);
    StreamingMetadataWriteHandler metadataWriteHandler = new MockStreamingMetadataWriteHandler(mdtWriter);

    HoodieData<WriteStatus> allWriteStatuses = metadataWriteHandler.streamWriteToMetadataTable(mockHoodieTable, dataTableWriteStatus, "00001", enforceCoalesceWithRepartition,
        coalesceDividentForDataTableWrites);
    assertEquals(Math.max(1, numDataTableWriteStatuses / coalesceDividentForDataTableWrites) + numMdtWriteStatus, allWriteStatuses.getNumPartitions());
  }

  private HoodieData<WriteStatus> mockWriteStatuses(int size) {
    List<WriteStatus> writeStatuses = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      writeStatuses.add(mock(WriteStatus.class));
    }
    return HoodieJavaRDD.of(jsc().parallelize(writeStatuses, size));
  }

  class MockStreamingMetadataWriteHandler extends StreamingMetadataWriteHandler {

    private HoodieTableMetadataWriter mdtWriter;

    MockStreamingMetadataWriteHandler(HoodieTableMetadataWriter mdtWriter) {
      this.mdtWriter = mdtWriter;
    }

    @Override
    synchronized Option<HoodieTableMetadataWriter> getMetadataWriter(String triggeringInstant, HoodieTable table) {
      return Option.of(mdtWriter);
    }
  }
}
