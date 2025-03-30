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

package org.apache.hudi.table.action.rollback;

import org.apache.hudi.avro.model.HoodieRollbackRequest;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.common.testutils.FileCreateUtils.createLogFileMarker;
import static org.apache.hudi.common.testutils.FileCreateUtils.createMarkerFile;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.table.action.rollback.TestRollbackUtils.assertRollbackRequestListEquals;

class TestMarkerBasedRollbackStrategy extends TestRollbackHelper {
  private static final int ROLLBACK_LOG_VERSION = 10;

  @Override
  @BeforeEach
  void setup() throws IOException {
    super.setup();
  }

  @ParameterizedTest
  @ValueSource(strings = {"SIX", "EIGHT"})
  void testGetRollbackRequestsWithMultipleLogFilesInOneFileGroup(HoodieTableVersion tableVersion) throws IOException {
    prepareMetaClient(tableVersion);
    HoodieEngineContext context = new HoodieLocalEngineContext(storage.getConf());
    String rollbackInstantTime = "003";
    String instantToRollbackTs = "002";
    String baseInstantTimeOfLogFiles = "001";
    String partition1 = "partition1";
    String partition2 = "partition2";
    String baseFileId1 = UUID.randomUUID().toString();
    String baseFileId2 = UUID.randomUUID().toString();
    String baseFileId3 = UUID.randomUUID().toString();
    String logFileId1 = UUID.randomUUID().toString();
    String logFileId2 = UUID.randomUUID().toString();
    // Base files to roll back
    StoragePath baseFilePath1 = createBaseFileAndMarkerToRollback(partition1, baseFileId1, instantToRollbackTs);
    StoragePath baseFilePath2 = createBaseFileAndMarkerToRollback(partition2, baseFileId2, instantToRollbackTs);
    StoragePath baseFilePath3 = createBaseFileAndMarkerToRollback(partition2, baseFileId3, instantToRollbackTs);
    // Log files to roll back. logFilesToRollback1 is in new file group part of instant being rolled back
    Map<String, Long> logFilesToRollback1 = createLogFilesAndMarkersToRollback(
        partition2, logFileId1, instantToRollbackTs, instantToRollbackTs, IntStream.of(1));
    // Multiple rollback requests of log files belonging to the same file group
    Map<String, Long> logFilesToRollback2 = IntStream.range(1, ROLLBACK_LOG_VERSION).boxed()
        .flatMap(version -> createLogFilesAndMarkersToRollback(
            partition2, logFileId2, baseInstantTimeOfLogFiles, instantToRollbackTs, IntStream.of(version))
            .entrySet().stream())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    HoodieInstant instantToRollback = INSTANT_GENERATOR.createNewInstant(
        HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, instantToRollbackTs);

    List<HoodieRollbackRequest> actual =
        new MarkerBasedRollbackStrategy(table, context, config, rollbackInstantTime)
            .getRollbackRequests(instantToRollback);
    List<HoodieRollbackRequest> expected = new ArrayList<>();
    expected.add(new HoodieRollbackRequest(
        partition1, baseFileId1, instantToRollbackTs, Collections.singletonList(baseFilePath1.toString()), Collections.emptyMap()));
    expected.add(new HoodieRollbackRequest(
        partition2, baseFileId2, instantToRollbackTs, Collections.singletonList(baseFilePath2.toString()), Collections.emptyMap()));
    expected.add(new HoodieRollbackRequest(
        partition2, baseFileId3, instantToRollbackTs, Collections.singletonList(baseFilePath3.toString()), Collections.emptyMap()));
    if (tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT)) {
      logFilesToRollback1.keySet().forEach(logFilePath ->
          expected.add(new HoodieRollbackRequest(
              partition2, logFileId1, instantToRollbackTs,
              Collections.singletonList(logFilePath), Collections.emptyMap())));
      logFilesToRollback2.keySet().forEach(logFilePath ->
          expected.add(new HoodieRollbackRequest(
              partition2, logFileId2, instantToRollbackTs,
              Collections.singletonList(logFilePath), Collections.emptyMap())));
    } else {
      String logFile1Path = new StoragePath(new StoragePath(basePath, partition2), logFilesToRollback1.keySet().stream().findFirst().get()).toString();
      // Since logFile1 is part of a new file group, it can be deleted as part of rollback
      // logFile2 would be rolled back using a rollback command block
      expected.add(new HoodieRollbackRequest(partition2, "", "", Collections.singletonList(logFile1Path), Collections.emptyMap()));
      expected.add(new HoodieRollbackRequest(partition2, logFileId2, baseInstantTimeOfLogFiles, Collections.emptyList(), logFilesToRollback2));
    }
    assertRollbackRequestListEquals(expected, actual);
  }

  private StoragePath createBaseFileAndMarkerToRollback(String partition,
                                                 String fileId,
                                                 String instantTime) throws IOException {
    StoragePath baseFilePath = createBaseFileToRollback(partition, fileId, instantTime);
    createMarkerFile(metaClient, partition, instantTime, fileId, IOType.CREATE);
    return baseFilePath;
  }

  private Map<String, Long> createLogFilesAndMarkersToRollback(String partition,
                                                               String fileId,
                                                               String baseInstantTime,
                                                               String currentInstantTime,
                                                               IntStream logVersions) {
    Map<String, Long> logFilesToRollback = createLogFilesToRollback(
        partition, fileId, baseInstantTime, logVersions, 0L);
    return logFilesToRollback.keySet().stream().map(logFileName -> {
      try {
        createLogFileMarker(metaClient, partition, currentInstantTime, logFileName);
        if (metaClient.getTableConfig().getTableVersion()
            .greaterThanOrEquals(HoodieTableVersion.EIGHT)) {
          return new StoragePath(new StoragePath(basePath, partition), logFileName).toString();
        }
        return logFileName;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toMap(Function.identity(), e -> 0L));
  }
}

