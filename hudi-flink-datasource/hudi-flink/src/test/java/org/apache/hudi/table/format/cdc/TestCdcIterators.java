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

package org.apache.hudi.table.format.cdc;

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.cdc.HoodieCDCFileSplit;
import org.apache.hudi.common.table.cdc.HoodieCDCInferenceCase;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests the lightweight iterator composition in {@link CdcIterators}.
 */
class TestCdcIterators {

  @Test
  void testAddBaseFileIteratorSetsInsertKindAndClosesNestedIterator() {
    GenericRowData row = GenericRowData.of(StringData.fromString("value"));
    ClosableIterator<RowData> nested = mockIterator();
    when(nested.hasNext()).thenReturn(true, false);
    when(nested.next()).thenReturn(row);

    CdcIterators.AddBaseFileIterator iterator =
        new CdcIterators.AddBaseFileIterator(nested);
    assertTrue(iterator.hasNext());
    assertSame(row, iterator.next());
    assertEquals(RowKind.INSERT, row.getRowKind());
    assertFalse(iterator.hasNext());

    iterator.close();
    verify(nested).close();
  }

  @Test
  void testRemoveBaseFileIteratorProjectsAndSetsDeleteKind() {
    GenericRowData row = GenericRowData.of(
        StringData.fromString("ignored"), StringData.fromString("selected"));
    ClosableIterator<RowData> nested = mockIterator();
    when(nested.hasNext()).thenReturn(true);
    when(nested.next()).thenReturn(row);

    CdcIterators.RemoveBaseFileIterator iterator =
        new CdcIterators.RemoveBaseFileIterator(
            rowType("selected"),
            new int[] {1},
            nested);
    assertTrue(iterator.hasNext());
    RowData projected = iterator.next();
    assertEquals(RowKind.DELETE, projected.getRowKind());
    assertEquals("selected", projected.getString(0).toString());

    iterator.close();
    verify(nested).close();
  }

  @Test
  void testCdcFileSplitsIteratorMovesAcrossEmptySplitsAndClosesResources() {
    HoodieCDCFileSplit first = new HoodieCDCFileSplit(
        "001", HoodieCDCInferenceCase.BASE_FILE_INSERT, "first.parquet");
    HoodieCDCFileSplit second = new HoodieCDCFileSplit(
        "002", HoodieCDCInferenceCase.BASE_FILE_INSERT, "second.parquet");
    ClosableIterator<RowData> firstIterator = mockIterator();
    ClosableIterator<RowData> secondIterator = mockIterator();
    GenericRowData row = GenericRowData.of(StringData.fromString("row"));
    when(firstIterator.hasNext()).thenReturn(false);
    when(secondIterator.hasNext()).thenReturn(true, false);
    when(secondIterator.next()).thenReturn(row);
    CdcImageManager imageManager = mock(CdcImageManager.class);

    CdcIterators.CdcFileSplitsIterator iterator =
        new CdcIterators.CdcFileSplitsIterator(
            new HoodieCDCFileSplit[] {first, second},
            imageManager,
            split -> split == first ? firstIterator : secondIterator);

    assertTrue(iterator.hasNext());
    assertSame(row, iterator.next());
    assertFalse(iterator.hasNext());
    iterator.close();

    verify(firstIterator).close();
    verify(secondIterator).close();
    verify(imageManager).close();
  }

  @Test
  void testReplaceCommitIteratorReadsBeforeSlice() {
    FileSlice beforeSlice = fileSlice();
    HoodieCDCFileSplit fileSplit = new HoodieCDCFileSplit(
        "002",
        HoodieCDCInferenceCase.REPLACE_COMMIT,
        Collections.emptyList(),
        Option.of(beforeSlice),
        Option.empty());
    GenericRowData row = GenericRowData.of(
        StringData.fromString("ignored"), StringData.fromString("selected"));
    ClosableIterator<RowData> nested = mockIterator();
    when(nested.hasNext()).thenReturn(true);
    when(nested.next()).thenReturn(row);
    AtomicReference<MergeOnReadInputSplit> capturedSplit = new AtomicReference<>();

    CdcIterators.ReplaceCommitIterator iterator =
        new CdcIterators.ReplaceCommitIterator(
            "/table",
            rowType("selected"),
            new int[] {1},
            1024L,
            fileSplit,
            split -> {
              capturedSplit.set(split);
              return nested;
            });

    assertEquals("/table", capturedSplit.get().getTablePath());
    assertTrue(iterator.hasNext());
    RowData projected = iterator.next();
    assertEquals(RowKind.DELETE, projected.getRowKind());
    assertEquals("selected", projected.getString(0).toString());
    iterator.close();
    verify(nested).close();

    HoodieCDCFileSplit missingBeforeSlice = new HoodieCDCFileSplit(
        "003",
        HoodieCDCInferenceCase.REPLACE_COMMIT,
        Collections.emptyList());
    assertThrows(
        IllegalStateException.class,
        () -> new CdcIterators.ReplaceCommitIterator(
            "/table",
            rowType("selected"),
            new int[] {0},
            1024L,
            missingBeforeSlice,
            split -> nested));
  }

  @Test
  void testFileSliceAndSingleLogFileSplitConversion() {
    FileSlice fileSlice = fileSlice();
    fileSlice.addLogFile(new HoodieLogFile(
        new StoragePath("/table/region=us/.file-id_002.log.1_1-0-1")));
    fileSlice.addLogFile(new HoodieLogFile(
        new StoragePath("/table/region=us/.file-id_002.log.2_1-0-1.cdc")));

    MergeOnReadInputSplit split =
        CdcIterators.fileSlice2Split("/table", fileSlice, 4096L);
    assertEquals(
        "/table/region=us/file-id_1-0-1_001.parquet",
        split.getBasePath().get());
    assertEquals(
        Collections.singletonList("/table/region=us/.file-id_002.log.1_1-0-1"),
        split.getLogPaths().get());
    assertEquals("file-id", split.getFileId());
    assertEquals("region=us", split.getPartitionPath());
    assertEquals(4096L, split.getMaxCompactionMemoryInBytes());

    MergeOnReadInputSplit logSplit = CdcIterators.singleLogFile2Split(
        "/table",
        "/table/region=us/.file-id_003.log.1_1-0-1",
        8192L);
    assertFalse(logSplit.getBasePath().isPresent());
    assertEquals(
        Collections.singletonList("/table/region=us/.file-id_003.log.1_1-0-1"),
        logSplit.getLogPaths().get());
    assertEquals("003", logSplit.getLatestCommit());
    assertEquals("file-id", logSplit.getFileId());
    assertEquals("region=us", logSplit.getPartitionPath());
  }

  private static FileSlice fileSlice() {
    FileSlice fileSlice = new FileSlice("region=us", "001", "file-id");
    fileSlice.setBaseFile(new HoodieBaseFile(
        "/table/region=us/file-id_1-0-1_001.parquet"));
    return fileSlice;
  }

  @SuppressWarnings("unchecked")
  private static ClosableIterator<RowData> mockIterator() {
    return mock(ClosableIterator.class);
  }

  private static RowType rowType(String fieldName) {
    return RowType.of(
        new LogicalType[] {new VarCharType()},
        new String[] {fieldName});
  }
}
