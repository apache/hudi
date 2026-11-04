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

package org.apache.hudi.async;

import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TestHoodieAsyncTableService {

  @Test
  void futureFailureIsRecordedWithoutUnwrapping() {
    CompletionException failure = new CompletionException(new IllegalStateException("original failure"));
    CompletableFuture<Boolean> future = new CompletableFuture<>();
    future.completeExceptionally(failure);
    HoodieAsyncService service = new HoodieAsyncService() {
      @Override
      protected Pair<CompletableFuture, ExecutorService> startService() {
        return Pair.of(future, null);
      }
    };

    service.start(null);

    assertTrue(service.hasError());
    assertSame(failure, service.getFailure().get());
  }

  @Test
  void tableServiceShouldNotStartIfDisabled(@Mock HoodieWriteConfig config) {
    when(config.areTableServicesEnabled()).thenReturn(false);
    HoodieAsyncTableService service = new DummyAsyncTableService(config);
    service.start(null);
    assertFalse(service.isStarted());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void restartingServiceClearsPreviousFailure(boolean failAgain) {
    AtomicReference<CompletableFuture<Boolean>> next = new AtomicReference<>(new CompletableFuture<>());
    HoodieAsyncService service = new HoodieAsyncService() {
      @Override
      protected Pair<CompletableFuture, ExecutorService> startService() {
        return Pair.of(next.get(), null);
      }
    };
    RuntimeException firstFailure = new IllegalStateException("first run failed");
    next.get().completeExceptionally(firstFailure);
    service.start(null);
    assertSame(firstFailure, service.getFailure().get());
    assertFalse(service.isStarted());

    next.set(new CompletableFuture<>());
    service.start(null);
    assertFalse(service.hasError());
    assertFalse(service.getFailure().isPresent());
    if (failAgain) {
      RuntimeException secondFailure = new IllegalStateException("second run failed");
      next.get().completeExceptionally(secondFailure);
      assertTrue(service.hasError());
      assertSame(secondFailure, service.getFailure().get());
    } else {
      next.get().complete(true);
      assertFalse(service.hasError());
      assertFalse(service.getFailure().isPresent());
    }
  }

  private static class DummyAsyncTableService extends HoodieAsyncTableService {

    protected DummyAsyncTableService(HoodieWriteConfig writeConfig) {
      super(writeConfig);
    }

    @Override
    protected Pair<CompletableFuture, ExecutorService> startService() {
      return null;
    }
  }
}
