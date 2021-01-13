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

package org.apache.hudi.operator;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.operator.event.BatchWriteSuccessEvent;
import org.apache.hudi.operator.utils.TestConfigurations;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for StreamingSinkOperatorCoordinator.
 */
public class StreamWriteOperatorCoordinatorTest {
  private StreamWriteOperatorCoordinator coordinator;

  @TempDir
  File tempFile;

  @BeforeEach
  public void before() throws Exception {
    coordinator = new StreamWriteOperatorCoordinator(
        TestConfigurations.getDefaultConf(tempFile.getAbsolutePath()), 2);
    coordinator.start();
  }

  @AfterEach
  public void after() {
    coordinator.close();
  }

  @Test
  public void testTableInitialized() throws IOException {
    final org.apache.hadoop.conf.Configuration hadoopConf = StreamerUtil.getHadoopConf();
    String basePath = tempFile.getAbsolutePath();
    try (FileSystem fs = FSUtils.getFs(basePath, hadoopConf)) {
      assertTrue(fs.exists(new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME)));
    }
  }

  @Test
  public void testCheckpointAndRestore() throws Exception {
    CompletableFuture<byte[]> future = new CompletableFuture<>();
    coordinator.checkpointCoordinator(1, future);
    coordinator.resetToCheckpoint(future.get());
  }

  @Test
  public void testReceiveInvalidEvent() {
    CompletableFuture<byte[]> future = new CompletableFuture<>();
    coordinator.checkpointCoordinator(1, future);
    OperatorEvent event = new BatchWriteSuccessEvent(0, "abc", Collections.emptyList());
    assertThrows(IllegalStateException.class,
        () -> coordinator.handleEventFromOperator(0, event),
        "Receive an unexpected event for instant abc from task 0");
  }

  @Test
  public void testCheckpointInvalid() {
    final CompletableFuture<byte[]> future = new CompletableFuture<>();
    coordinator.checkpointCoordinator(1, future);
    String inflightInstant = coordinator.getInFlightInstant();
    OperatorEvent event = new BatchWriteSuccessEvent(0, inflightInstant, Collections.emptyList());
    coordinator.handleEventFromOperator(0, event);
    final CompletableFuture<byte[]> future2 = new CompletableFuture<>();
    coordinator.checkpointCoordinator(2, future2);
    assertTrue(future2.isCompletedExceptionally());
  }
}
