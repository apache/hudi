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

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link AsyncLookupFunctionWrapper}.
 */
class TestAsyncLookupFunctionWrapper {

  @Test
  void testLifecycleAndAsyncLookup() throws Exception {
    TestingLookupFunction function = new TestingLookupFunction();
    AsyncLookupFunctionWrapper wrapper = new AsyncLookupFunctionWrapper(function, 2);
    RowData key = GenericRowData.of(1);

    wrapper.open(null);
    Collection<RowData> result = wrapper.asyncLookup(key).join();

    assertEquals(1, result.size());
    assertSame(key, result.iterator().next());
    assertTrue(function.opened);
    wrapper.close();
    assertTrue(function.closed);
  }

  @Test
  void testIOExceptionIsPropagatedAsUncheckedIOException() throws Exception {
    AsyncLookupFunctionWrapper wrapper =
        new AsyncLookupFunctionWrapper(new FailingLookupFunction(), 1);

    CompletionException exception = assertThrows(
        CompletionException.class,
        () -> wrapper.asyncLookup(GenericRowData.of(1)).join());

    assertInstanceOf(UncheckedIOException.class, exception.getCause());
    wrapper.close();
  }

  private static class TestingLookupFunction extends LookupFunction {
    private boolean opened;
    private boolean closed;

    @Override
    public void open(FunctionContext context) {
      opened = true;
    }

    @Override
    public Collection<RowData> lookup(RowData keyRow) {
      return Collections.singletonList(keyRow);
    }

    @Override
    public void close() {
      closed = true;
    }
  }

  private static class FailingLookupFunction extends LookupFunction {
    @Override
    public Collection<RowData> lookup(RowData keyRow) throws IOException {
      throw new IOException("expected");
    }
  }
}
