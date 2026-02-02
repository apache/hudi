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

package org.apache.hudi.sink.compact.handler;

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sink.utils.NonThrownExecutor;

import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;

/**
 * Handler for managing table cleaning operations in clean operator.
 *
 * <p>This class provides mechanisms to execute cleaning operations asynchronously,
 * ensuring proper coordination with Flink's checkpointing mechanism.
 *
 * <p>The handler uses a {@link NonThrownExecutor} to manage cleaning tasks and tracks
 * the cleaning state to prevent concurrent cleaning operations.
 */
@Slf4j
public class CleanHandler implements Closeable {
  private final HoodieFlinkWriteClient writeClient;
  private volatile boolean isCleaning;
  private final NonThrownExecutor executor;

  public CleanHandler(HoodieFlinkWriteClient writeClient) {
    this.writeClient = writeClient;
    this.executor = NonThrownExecutor.builder(log).waitForTasksFinish(true).build();
  }

  /**
   * Executes a synchronous cleaning operation.
   *
   * <p>The actual cleaning is performed by the underlying write client, which removes
   * old file versions based on the configured retention policy.
   */
  public void clean() {
    executor.execute(() -> {
      this.isCleaning = true;
      try {
        this.writeClient.clean();
      } finally {
        this.isCleaning = false;
      }
    }, "wait for cleaning finish");
  }

  /**
   * Waits for an ongoing cleaning operation to finish.
   *
   * <p>If no cleaning operation is in progress, this method returns immediately
   * without performing any action.
   */
  public void waitForCleaningFinish() {
    if (isCleaning) {
      executor.execute(() -> {
        try {
          this.writeClient.waitForCleaningFinish();
        } finally {
          // ensure to switch the isCleaning flag
          this.isCleaning = false;
        }
      }, "wait for cleaning finish");
    }
  }

  /**
   * Starts an asynchronous cleaning operation.
   *
   * <p>Any exceptions thrown during the start of async cleaning are caught and logged
   * as warnings to prevent interference with normal Flink checkpointing operations.
   * This ensures that cleaning failures do not disrupt the streaming job's progress.
   */
  public void startAsyncCleaning() {
    if (!isCleaning) {
      try {
        this.writeClient.startAsyncCleaning();
        this.isCleaning = true;
      } catch (Throwable throwable) {
        // catch the exception to not affect the normal checkpointing
        log.warn("Failed to start async cleaning", throwable);
      }
    }
  }

  /**
   * Closes the CleanHandler and releases associated resources.
   */
  @Override
  public void close() {
    if (executor != null) {
      try {
        executor.close();
      } catch (Exception e) {
        throw new HoodieException("Failed to close executor of clean handler.", e);
      }
    }
    this.writeClient.clean();
  }
}
