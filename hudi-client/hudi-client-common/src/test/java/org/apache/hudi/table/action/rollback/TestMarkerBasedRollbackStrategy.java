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
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.common.testutils.FileCreateUtils.createLogFileMarker;
import static org.apache.hudi.common.testutils.FileCreateUtils.createMarkerFile;
import static org.apache.hudi.table.action.rollback.TestRollbackUtils.assertRollbackRequestListEquals;

class TestMarkerBasedRollbackStrategy extends TestBaseRollbackHelper {
  private static final int ROLLBACK_LOG_VERSION = 10;

  @Override
  @BeforeEach
  void setup() throws IOException {
    super.setup();
  }

  @Test
  void testGetRollbackRequestsWithMultipleLogFilesInOneFileGroup() throws IOException {
    Configuration hadoopConf = HoodieTestUtils.getDefaultHadoopConf();
    HoodieEngineContext context = new HoodieLocalEngineContext(hadoopConf);
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
    Path baseFilePath1 = createBaseFileAndMarkerToRollback(partition1, baseFileId1, instantToRollbackTs);
    Path baseFilePath2 = createBaseFileAndMarkerToRollback(partition2, baseFileId2, instantToRollbackTs);
    Path baseFilePath3 = createBaseFileAndMarkerToRollback(partition2, baseFileId3, instantToRollbackTs);
    // Log files to roll back
    Map<String, Long> logFilesToRollback1 = createLogFilesAndMarkersToRollback(
        partition2, logFileId1, baseInstantTimeOfLogFiles, instantToRollbackTs, IntStream.of(1));
    // Multiple rollback requests of log files belonging to the same file group
    Map<String, Long> logFilesToRollback2 = IntStream.range(1, ROLLBACK_LOG_VERSION).boxed()
        .flatMap(version -> createLogFilesAndMarkersToRollback(
            partition2, logFileId2, baseInstantTimeOfLogFiles, instantToRollbackTs, IntStream.of(version))
            .entrySet().stream())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    HoodieInstant instantToRollback = new HoodieInstant(
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
    expected.add(new HoodieRollbackRequest(
        partition2, logFileId1, baseInstantTimeOfLogFiles, Collections.emptyList(), logFilesToRollback1));
    expected.add(new HoodieRollbackRequest(
        partition2, logFileId2, baseInstantTimeOfLogFiles, Collections.emptyList(), logFilesToRollback2));
    assertRollbackRequestListEquals(expected, actual);
  }

  private Path createBaseFileAndMarkerToRollback(String partition,
                                                 String fileId,
                                                 String instantTime) throws IOException {
    Path baseFilePath = createBaseFileToRollback(partition, fileId, instantTime);
    createMarkerFile(basePath.toString(), partition, instantTime, fileId, IOType.CREATE);
    return baseFilePath;
  }

  private Map<String, Long> createLogFilesAndMarkersToRollback(String partition,
                                                               String fileId,
                                                               String baseInstantTime,
                                                               String currentInstantTime,
                                                               IntStream logVersions) {
    Map<String, Long> logFilesToRollback = createLogFilesToRollback(
        partition, fileId, baseInstantTime, logVersions, 0L);
    logFilesToRollback.keySet().forEach(logFileName -> {
      try {
        createLogFileMarker(basePath.toString(), partition, currentInstantTime, logFileName);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    return logFilesToRollback;
  }
}

