/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table.lookup;

import org.apache.hudi.exception.HoodieIOException;

import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link HoodieLookupTableReader}.
 */
class TestHoodieLookupTableReader {

  @Test
  @SuppressWarnings("unchecked")
  void testOpenRollsBackPartiallyOpenedInputFormat() throws Exception {
    RichInputFormat<RowData, InputSplit> inputFormat = mock(RichInputFormat.class);
    InputSplit inputSplit = mock(InputSplit.class);
    when(inputFormat.createInputSplits(1)).thenReturn(new InputSplit[] {inputSplit});
    IOException openException = new IOException("expected open failure");
    doThrow(openException).when(inputFormat).open(inputSplit);

    HoodieLookupTableReader reader =
        new HoodieLookupTableReader(() -> inputFormat, new Configuration());

    assertSame(openException, assertThrows(IOException.class, reader::open));
    verify(inputFormat).close();
    verify(inputFormat).closeInputFormat();

    reader.close();
    verify(inputFormat, times(1)).close();
    verify(inputFormat, times(1)).closeInputFormat();
  }

  @Test
  @SuppressWarnings("unchecked")
  void testOpenPreservesFailureWhenRuntimeRollbackFails() throws Exception {
    RichInputFormat<RowData, InputSplit> inputFormat = mock(RichInputFormat.class);
    InputSplit inputSplit = mock(InputSplit.class);
    when(inputFormat.createInputSplits(1)).thenReturn(new InputSplit[] {inputSplit});
    IOException openException = new IOException("expected open failure");
    HoodieIOException splitCloseException =
        new HoodieIOException("expected runtime split close failure");
    doThrow(openException).when(inputFormat).open(inputSplit);
    doThrow(splitCloseException).when(inputFormat).close();

    HoodieLookupTableReader reader =
        new HoodieLookupTableReader(() -> inputFormat, new Configuration());

    IOException exception = assertThrows(IOException.class, reader::open);
    assertSame(openException, exception);
    assertEquals(1, exception.getSuppressed().length);
    assertSame(splitCloseException, exception.getSuppressed()[0]);
    verify(inputFormat).closeInputFormat();
  }

  @Test
  @SuppressWarnings("unchecked")
  void testCloseReleasesInputFormatWhenRuntimeSplitCloseFails() throws Exception {
    RichInputFormat<RowData, InputSplit> inputFormat = mock(RichInputFormat.class);
    InputSplit inputSplit = mock(InputSplit.class);
    when(inputFormat.createInputSplits(1)).thenReturn(new InputSplit[] {inputSplit});
    HoodieIOException splitCloseException =
        new HoodieIOException("expected runtime split close failure");
    IOException formatCloseException = new IOException("expected format close failure");
    doThrow(splitCloseException).when(inputFormat).close();
    doThrow(formatCloseException).when(inputFormat).closeInputFormat();

    HoodieLookupTableReader reader =
        new HoodieLookupTableReader(() -> inputFormat, new Configuration());
    reader.open();

    HoodieIOException exception = assertThrows(HoodieIOException.class, reader::close);
    assertSame(splitCloseException, exception);
    assertEquals(1, exception.getSuppressed().length);
    assertSame(formatCloseException, exception.getSuppressed()[0]);
    verify(inputFormat).closeInputFormat();
  }
}
