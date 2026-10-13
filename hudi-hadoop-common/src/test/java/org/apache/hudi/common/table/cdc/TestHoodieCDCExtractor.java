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

package org.apache.hudi.common.table.cdc;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.testutils.FileCreateUtils;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests {@link HoodieCDCExtractor}.
 */
class TestHoodieCDCExtractor extends HoodieCommonTestHarness {

  private static final String PARTITION = "p1";
  private static final int NUM_FILE_GROUPS = 3;

  /**
   * A delta commit writing log files of several file groups is read once to infer the changes of all of them.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testReadsDeltaCommitMetadataOncePerInstant(boolean preTableVersion8) throws Exception {
    initMetaClient(preTableVersion8, HoodieTableType.MERGE_ON_READ);
    String instantTime = "20260101000000000";
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    metadata.setOperationType(WriteOperationType.UPSERT);
    for (int i = 0; i < NUM_FILE_GROUPS; i++) {
      String fileId = "file-" + i;
      String logFileName = FSUtils.makeInlineLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, instantTime, 1, "1-0-1");
      HoodieDeltaWriteStat writeStat = new HoodieDeltaWriteStat();
      writeStat.setFileId(fileId);
      writeStat.setPartitionPath(PARTITION);
      writeStat.setPath(PARTITION + "/" + logFileName);
      writeStat.setPrevCommit(instantTime);
      writeStat.setBaseFile("");
      writeStat.setLogFiles(Collections.singletonList(logFileName));
      metadata.addWriteStat(PARTITION, writeStat);
    }
    FileCreateUtils.createDeltaCommit(metaClient, metaClient.getCommitMetadataSerDe(), instantTime, Option.empty(), metadata);

    HoodieTableMetaClient reloadedMetaClient = HoodieTestUtils.createMetaClient(metaClient.getStorageConf(), basePath);
    assertEquals(preTableVersion8 ? HoodieTableVersion.SIX : HoodieTableVersion.current(),
        reloadedMetaClient.getTableConfig().getTableVersion());
    HoodieTableMetaClient metaClientSpy = spy(reloadedMetaClient);
    HoodieActiveTimeline timelineSpy = spy(reloadedMetaClient.getActiveTimeline());
    doReturn(timelineSpy).when(metaClientSpy).getActiveTimeline();

    InstantRange range = InstantRange.builder().rangeType(InstantRange.RangeType.CLOSED_CLOSED)
        .startInstant(instantTime).endInstant(instantTime).build();
    Map<HoodieFileGroupId, List<HoodieCDCFileSplit>> splits =
        new HoodieCDCExtractor(metaClientSpy, range, false).extractCDCFileSplits();

    assertEquals(NUM_FILE_GROUPS, splits.size());
    splits.values().forEach(fileGroupSplits -> {
      assertEquals(1, fileGroupSplits.size());
      assertEquals(HoodieCDCInferenceCase.LOG_FILE, fileGroupSplits.get(0).getCdcInferCase());
    });
    verify(timelineSpy, times(1)).readCommitMetadataToAvro(any(HoodieInstant.class));
  }
}
