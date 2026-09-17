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

package org.apache.hudi.sink.partitioner.index;

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.sink.utils.BufferUtils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.table.runtime.operators.sort.BinaryInMemorySortBuffer;
import org.apache.flink.table.runtime.util.LazyMemorySegmentPool;
import org.apache.flink.table.runtime.util.MemorySegmentPool;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class TestIndexWriteFunction {

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testCloseAfterPartialInitialization(boolean poolCreated) throws Exception {
    HoodieFlinkWriteClient writeClient = mock(HoodieFlinkWriteClient.class);
    IndexWriteFunction function = new TestFunction(writeClient);
    CloseableMemorySegmentPool pool = mock(CloseableMemorySegmentPool.class);
    if (poolCreated) {
      setField(function, "memorySegmentPool", pool);
    }

    function.close();

    if (poolCreated) {
      verify(pool).close();
    }
    verify(writeClient).close();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testCloseContinuesAfterCleanupFailure(boolean bufferFails) throws Exception {
    HoodieFlinkWriteClient writeClient = mock(HoodieFlinkWriteClient.class);
    IndexWriteFunction function = new TestFunction(writeClient);
    BinaryInMemorySortBuffer buffer = mock(BinaryInMemorySortBuffer.class);
    CloseableMemorySegmentPool pool = mock(CloseableMemorySegmentPool.class);
    setField(function, "indexDataBuffer", buffer);
    setField(function, "memorySegmentPool", pool);
    RuntimeException bufferFailure = new RuntimeException("buffer disposal failed");
    IOException poolFailure = new IOException("pool close failed");
    RuntimeException clientFailure = new RuntimeException("client close failed");
    if (bufferFails) {
      doThrow(bufferFailure).when(buffer).dispose();
    }
    doThrow(poolFailure).when(pool).close();
    doThrow(clientFailure).when(writeClient).close();

    Exception failure = assertThrows(Exception.class, function::close);

    assertSame(bufferFails ? bufferFailure : poolFailure, failure);
    assertArrayEquals(bufferFails ? new Throwable[] {poolFailure, clientFailure} : new Throwable[] {clientFailure},
        failure.getSuppressed());
    InOrder order = inOrder(buffer, pool, writeClient);
    order.verify(buffer).dispose();
    order.verify(pool).close();
    order.verify(writeClient).close();
  }

  @Test
  void testCloseReturnsManagedMemoryBeforeClosingPool() throws Exception {
    int pageSize = 32 * 1024;
    MemoryManager memoryManager = MemoryManager.create(8L * pageSize, pageSize);
    try {
      LazyMemorySegmentPool pool = new LazyMemorySegmentPool(new Object(), memoryManager, 8);
      BinaryInMemorySortBuffer buffer = BufferUtils.createBuffer(IndexRowUtils.INDEX_ROW_TYPE, pool);
      HoodieFlinkWriteClient writeClient = mock(HoodieFlinkWriteClient.class);
      IndexWriteFunction function = new TestFunction(writeClient);
      setField(function, "indexDataBuffer", buffer);
      setField(function, "memorySegmentPool", pool);

      function.close();

      assertTrue(memoryManager.verifyEmpty(), "All managed pages should be released");
      verify(writeClient).close();
    } finally {
      memoryManager.shutdown();
    }
  }

  private static void setField(IndexWriteFunction function, String name, Object value) throws Exception {
    Field field = IndexWriteFunction.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(function, value);
  }

  private interface CloseableMemorySegmentPool extends MemorySegmentPool, Closeable {
  }

  private static class TestFunction extends IndexWriteFunction {
    private TestFunction(HoodieFlinkWriteClient writeClient) {
      super(new Configuration());
      this.writeClient = writeClient;
    }
  }
}
