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

import org.apache.hudi.common.FormatMetaColumn;
import org.apache.hudi.common.HoodieTableFormat;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.compact.HoodieFlinkMergeOnReadTableCompactor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests that the Flink merge handle and compactor refuse a table format that declares format meta columns, as the
 * Flink record context cannot set their values.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
class TestFlinkFormatMetaColumnRefusal {

  private HoodieTable table;

  @BeforeEach
  void setUp() {
    HoodieTableFormat tableFormat = mock(HoodieTableFormat.class);
    when(tableFormat.getName()).thenReturn("test-format");
    when(tableFormat.getFormatMetaColumns()).thenReturn(Collections.singletonList(mock(FormatMetaColumn.class)));
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getTableFormat()).thenReturn(tableFormat);
    table = mock(HoodieTable.class);
    when(table.getMetaClient()).thenReturn(metaClient);
  }

  @Test
  void testMergeHandleRefusesFormatMetaColumns() {
    HoodieWriteConfig config = mock(HoodieWriteConfig.class);
    when(config.getWriteSchema()).thenReturn("{\"type\":\"record\",\"name\":\"rec\",\"fields\":[{\"name\":\"key\",\"type\":\"string\"}]}");
    HoodieNotSupportedException e = assertThrows(HoodieNotSupportedException.class,
        () -> new FlinkFileGroupReaderBasedMergeHandle<>(config, "001", table, Collections.emptyIterator(), "partition", "file-1",
            mock(TaskContextSupplier.class), mock(HoodieBaseFile.class)));
    assertTrue(e.getMessage().contains("FlinkFileGroupReaderBasedMergeHandle does not carry them over"), e.getMessage());
  }

  @Test
  void testCompactorRefusesFormatMetaColumns() {
    HoodieNotSupportedException e = assertThrows(HoodieNotSupportedException.class,
        () -> new HoodieFlinkMergeOnReadTableCompactor<>().compact(mock(HoodieWriteConfig.class), mock(CompactionOperation.class),
            "001", mock(TaskContextSupplier.class), mock(HoodieReaderContext.class), table));
    assertTrue(e.getMessage().contains("HoodieFlinkMergeOnReadTableCompactor does not carry them over"), e.getMessage());
  }
}
