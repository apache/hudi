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

package org.apache.hudi.sink.append;

import org.apache.hudi.sink.bulk.BulkInsertWriterHelper;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Test cases for {@link AppendWriteFunctionWithBIMBufferSort}.
 */
public class TestAppendWriteFunctionWithBIMBufferSort {

  @Test
  @Timeout(10)
  void testCloseWaitsForAsyncWriteBeforeClosingWriterHelper() throws Exception {
    AppendWriteFunctionWithBIMBufferSort<Object> function =
        new AppendWriteFunctionWithBIMBufferSort<>(new Configuration(), RowType.of(VarCharType.STRING_TYPE));
    ExecutorService asyncWriteExecutor = Executors.newSingleThreadExecutor();
    CountDownLatch asyncWriteStarted = new CountDownLatch(1);
    CountDownLatch releaseAsyncWrite = new CountDownLatch(1);
    AtomicBoolean writerHelperClosed = new AtomicBoolean(false);
    BulkInsertWriterHelper writerHelper = mock(BulkInsertWriterHelper.class);
    doAnswer(invocation -> {
      writerHelperClosed.set(true);
      return null;
    }).when(writerHelper).close();
    function.writerHelper = writerHelper;

    CompletableFuture<Void> asyncWriteTask = CompletableFuture.runAsync(() -> {
      asyncWriteStarted.countDown();
      try {
        releaseAsyncWrite.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }, asyncWriteExecutor);
    assertTrue(asyncWriteStarted.await(1, TimeUnit.SECONDS));

    setField(function, "asyncWriteExecutor", asyncWriteExecutor);
    setField(function, "asyncWriteTask", new AtomicReference<>(asyncWriteTask));
    setField(function, "isBackgroundBufferBeingProcessed", new AtomicBoolean(true));

    CountDownLatch closeStarted = new CountDownLatch(1);
    CompletableFuture<Void> closeTask = CompletableFuture.runAsync(() -> {
      try {
        closeStarted.countDown();
        function.close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    assertTrue(closeStarted.await(1, TimeUnit.SECONDS));
    Thread.sleep(100);
    assertFalse(closeTask.isDone());
    assertFalse(writerHelperClosed.get());

    releaseAsyncWrite.countDown();
    closeTask.get(1, TimeUnit.SECONDS);

    assertTrue(writerHelperClosed.get());
    verify(writerHelper).close();
  }

  private static void setField(Object target, String fieldName, Object value) throws Exception {
    Field field = AppendWriteFunctionWithBIMBufferSort.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }
}
