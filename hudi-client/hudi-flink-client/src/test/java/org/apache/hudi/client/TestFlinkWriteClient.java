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

package org.apache.hudi.client;

import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.HoodieFlinkClientTestHarness;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class TestFlinkWriteClient extends HoodieFlinkClientTestHarness {

  @BeforeEach
  private void setup() throws IOException {
    initPath();
    initFileSystem();
    initMetaClient();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testWriteClientAndTableServiceClientWithTimelineServer(
      boolean enableEmbeddedTimelineServer) throws IOException {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(metaClient.getBasePathV2().toString())
        .withEmbeddedTimelineServerEnabled(enableEmbeddedTimelineServer)
        .build();

    HoodieFlinkWriteClient writeClient = new HoodieFlinkWriteClient(context, writeConfig);
    // Only one timeline server should be instantiated, and the same timeline server
    // should be used by both the write client and the table service client.
    assertEquals(
        writeClient.getTimelineServer(),
        writeClient.getTableServiceClient().getTimelineServer());
    if (!enableEmbeddedTimelineServer) {
      assertFalse(writeClient.getTimelineServer().isPresent());
    }

    writeClient.close();
  }

  @ParameterizedTest
  @MethodSource("generateParametersForValidateTimestampInternal")
  void testValidateTimestampInternal(
      boolean shouldEnableTimestampOrderinValidation,
      boolean supportsOcc
  ) throws IOException {
    initMetaClient();
    HoodieWriteConfig.Builder writeConfigBuilder = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withStorageType(FileSystemViewStorageType.MEMORY)
            .build())
        .withEnableTimestampOrderingValidation(shouldEnableTimestampOrderinValidation);
    if (supportsOcc) {
      writeConfigBuilder.withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL);
    }

    HoodieFlinkWriteClient writeClient = new HoodieFlinkWriteClient(context, writeConfigBuilder.build());

    HoodieActiveTimeline timeline = new HoodieActiveTimeline(metaClient);
    HoodieInstant instant1 = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "002");
    timeline.createNewInstant(instant1);
    assertDoesNotThrow(() -> writeClient.validateTimestamp(metaClient, "002"));
  }

  private static Stream<Arguments> generateParametersForValidateTimestampInternal() {
    return Stream.of(
        Arguments.of(true, true),
        Arguments.of(true, false),
        Arguments.of(false, false)
    );
  }
}
