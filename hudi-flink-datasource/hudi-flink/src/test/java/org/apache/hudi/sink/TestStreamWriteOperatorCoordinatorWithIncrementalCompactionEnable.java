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

package org.apache.hudi.sink;

import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.event.WriteMetadataEvent;
import org.apache.hudi.sink.utils.MockCoordinatorExecutor;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import java.io.File;
import static org.junit.jupiter.api.Assertions.assertEquals;
/**
 * Test cases for StreamingSinkOperatorCoordinator.
  */
public class TestStreamWriteOperatorCoordinatorWithIncrementalCompactionEnable {
  private StreamWriteOperatorCoordinator coordinator;
  @TempDir
  File tempFile;
  @BeforeEach
  public void before() throws Exception {
    OperatorCoordinator.Context context = new MockOperatorCoordinatorContext(new OperatorID(), 2);
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.setBoolean(FlinkOptions.COMPACTION_SCHEDULE_INCREMENTAL_PARTITIONS, true);
    coordinator = new StreamWriteOperatorCoordinator(conf, context);
    coordinator.start();
    coordinator.setExecutor(new MockCoordinatorExecutor(context));
    coordinator.handleEventFromOperator(0, WriteMetadataEvent.emptyBootstrap(0));
    coordinator.handleEventFromOperator(1, WriteMetadataEvent.emptyBootstrap(1));
  }

  @AfterEach
  public void after() throws Exception {
    coordinator.close();
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "20241028145729909"})
  void testStreamOperatorCheckpoint(String lastInstant) throws Exception {
    byte[] cpStatus = coordinator.doCheckpointCoordinatorStatus(lastInstant);
    String res = coordinator.resetCheckpointCoordinatorStatus(cpStatus);
    assertEquals(res, lastInstant);
  }
}