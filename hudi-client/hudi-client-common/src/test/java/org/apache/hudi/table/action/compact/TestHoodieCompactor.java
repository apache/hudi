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

package org.apache.hudi.table.action.compact;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.FileGroupReaderBasedInlineLogAppendHandle;
import org.apache.hudi.io.HoodieAppendHandle;
import org.apache.hudi.io.HoodieMergeHandle;
import org.apache.hudi.io.HoodieMergeHandleFactory;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests that {@link HoodieCompactor#compact} and {@link HoodieCompactor#logCompact} close their
 * write handle on a best-effort basis when the merge/append step fails, so that the underlying
 * file writer/output stream isn't leaked, while still surfacing the original failure (with any
 * close failure attached as a suppressed exception).
 */
class TestHoodieCompactor {

  private final HoodieCompactor<Object, Object, Object, Object> compactor = new HoodieCompactor<Object, Object, Object, Object>() {
    @Override
    public void preCompact(HoodieTable table, HoodieTimeline pendingCompactionTimeline, WriteOperationType operationType, String instantTime) {
    }

    @Override
    public void maybePersist(HoodieData<WriteStatus> writeStatus, HoodieEngineContext context, HoodieWriteConfig config, String instantTime) {
    }

    @Override
    protected HoodieRecord.HoodieRecordType getEngineRecordType() {
      return HoodieRecord.HoodieRecordType.AVRO;
    }
  };

  @SuppressWarnings("unchecked")
  private MockedStatic<HoodieMergeHandleFactory> mockMergeHandleFactory(HoodieMergeHandle<Object, ?, ?, ?> mergeHandle) {
    MockedStatic<HoodieMergeHandleFactory> factory = mockStatic(HoodieMergeHandleFactory.class);
    factory.when(() -> HoodieMergeHandleFactory.create(
        any(HoodieWriteConfig.class), anyString(), any(HoodieTable.class), any(CompactionOperation.class),
        any(TaskContextSupplier.class), any(HoodieReaderContext.class), anyString(), any(HoodieRecord.HoodieRecordType.class)))
        .thenReturn(mergeHandle);
    return factory;
  }

  private List<WriteStatus> invokeCompact() throws IOException {
    return compactor.compact(mock(HoodieWriteConfig.class), mock(CompactionOperation.class), "20240101000000",
        mock(HoodieReaderContext.class), mock(HoodieTable.class), "20231231000000", mock(TaskContextSupplier.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  void testCompactClosesMergeHandleWhenDoMergeFails() throws IOException {
    HoodieMergeHandle<Object, ?, ?, ?> mergeHandle = mock(HoodieMergeHandle.class);
    IOException failure = new IOException("simulated doMerge failure");
    doThrow(failure).when(mergeHandle).doMerge();

    try (MockedStatic<HoodieMergeHandleFactory> ignored = mockMergeHandleFactory(mergeHandle)) {
      IOException thrown = assertThrows(IOException.class, this::invokeCompact);
      assertSame(failure, thrown);
      verify(mergeHandle).close();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void testCompactAttachesSuppressedExceptionWhenCloseAlsoFailsAfterDoMergeFailure() throws IOException {
    HoodieMergeHandle<Object, ?, ?, ?> mergeHandle = mock(HoodieMergeHandle.class);
    IOException failure = new IOException("simulated doMerge failure");
    RuntimeException closeFailure = new RuntimeException("simulated close failure");
    doThrow(failure).when(mergeHandle).doMerge();
    doThrow(closeFailure).when(mergeHandle).close();

    try (MockedStatic<HoodieMergeHandleFactory> ignored = mockMergeHandleFactory(mergeHandle)) {
      IOException thrown = assertThrows(IOException.class, this::invokeCompact);
      assertSame(failure, thrown);
      assertArrayEquals(new Throwable[] {closeFailure}, thrown.getSuppressed());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void testCompactDoesNotDoubleCloseMergeHandleOnSuccess() throws IOException {
    HoodieMergeHandle<Object, ?, ?, ?> mergeHandle = mock(HoodieMergeHandle.class);
    List<WriteStatus> expected = Collections.singletonList(new WriteStatus());
    when(mergeHandle.close()).thenReturn(expected);

    try (MockedStatic<HoodieMergeHandleFactory> ignored = mockMergeHandleFactory(mergeHandle)) {
      List<WriteStatus> result = invokeCompact();
      assertEquals(expected, result);
      verify(mergeHandle, times(1)).close();
    }
  }

  private HoodieWriteConfig configForInlineLogAppend() {
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    // Write version below TEN routes logCompact() through FileGroupReaderBasedInlineLogAppendHandle
    // rather than the native-log append handle.
    when(writeConfig.getWriteVersion()).thenReturn(HoodieTableVersion.EIGHT);
    return writeConfig;
  }

  private List<WriteStatus> invokeLogCompact(HoodieWriteConfig writeConfig) throws IOException {
    return compactor.logCompact(writeConfig, mock(CompactionOperation.class), "20240101000000",
        mock(HoodieTable.class), mock(TaskContextSupplier.class), mock(HoodieReaderContext.class));
  }

  @Test
  void testLogCompactClosesAppendHandleWhenDoAppendFails() throws IOException {
    // HoodieAppendHandle#doAppend() is declared without a throws clause, so the injected failure
    // must be unchecked here (Mockito rejects a checked exception the method can't actually throw).
    RuntimeException failure = new RuntimeException("simulated doAppend failure");
    try (MockedConstruction<FileGroupReaderBasedInlineLogAppendHandle> ignored = mockConstruction(
        FileGroupReaderBasedInlineLogAppendHandle.class,
        (mockHandle, context) -> doThrow(failure).when(mockHandle).doAppend())) {
      RuntimeException thrown = assertThrows(RuntimeException.class, () -> invokeLogCompact(configForInlineLogAppend()));
      assertSame(failure, thrown);
      HoodieAppendHandle<?, ?, ?, ?> constructedHandle = ignored.constructed().get(0);
      verify(constructedHandle).close();
    }
  }

  @Test
  void testLogCompactAttachesSuppressedExceptionWhenCloseAlsoFailsAfterDoAppendFailure() throws IOException {
    RuntimeException failure = new RuntimeException("simulated doAppend failure");
    RuntimeException closeFailure = new RuntimeException("simulated close failure");
    try (MockedConstruction<FileGroupReaderBasedInlineLogAppendHandle> ignored = mockConstruction(
        FileGroupReaderBasedInlineLogAppendHandle.class,
        (mockHandle, context) -> {
          doThrow(failure).when(mockHandle).doAppend();
          doThrow(closeFailure).when(mockHandle).close();
        })) {
      RuntimeException thrown = assertThrows(RuntimeException.class, () -> invokeLogCompact(configForInlineLogAppend()));
      assertSame(failure, thrown);
      assertArrayEquals(new Throwable[] {closeFailure}, thrown.getSuppressed());
    }
  }

  @Test
  void testLogCompactDoesNotDoubleCloseAppendHandleOnSuccess() throws IOException {
    List<WriteStatus> expected = Collections.singletonList(new WriteStatus());
    try (MockedConstruction<FileGroupReaderBasedInlineLogAppendHandle> ignored = mockConstruction(
        FileGroupReaderBasedInlineLogAppendHandle.class,
        (mockHandle, context) -> when(mockHandle.close()).thenReturn(expected))) {
      List<WriteStatus> result = invokeLogCompact(configForInlineLogAppend());
      assertEquals(expected, result);
      HoodieAppendHandle<?, ?, ?, ?> constructedHandle = ignored.constructed().get(0);
      verify(constructedHandle, times(1)).close();
    }
  }
}
