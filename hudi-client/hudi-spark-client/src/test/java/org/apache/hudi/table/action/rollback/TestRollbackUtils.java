/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table.action.rollback;

import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestRollbackUtils {
  private static final String BASE_FILE_EXTENSION = HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().getFileExtension();

  private StoragePathInfo generateFileStatus(String filePath) {
    return new StoragePathInfo(new StoragePath(filePath), 1, true, (short) 2, 1000000L, 1);
  }

  @Test
  public void testGenerateHeader() {
    HoodieInstant hoodieInstant = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "101");
    String instantToRollback = "1";
    Map<HoodieLogBlock.HeaderMetadataType, String> header = RollbackUtils.generateHeader(instantToRollback, hoodieInstant.requestedTime());
    Map<HoodieLogBlock.HeaderMetadataType, String> headerExpect = new HashMap<>(3);
    headerExpect.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "101");
    headerExpect.put(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME, "1");
    headerExpect.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE, "0");
    assertEquals(header, headerExpect);
  }

  @Test
  public void testMergeRollbackStat() {
    String partitionPath1 = "/partitionPath1/";
    String partitionPath2 = "/partitionPath2/";
    //prepare HoodieRollbackStat for different partition
    Map<StoragePathInfo, Boolean> dataFilesOnlyStat1Files = new HashMap<>();
    dataFilesOnlyStat1Files.put(generateFileStatus(partitionPath1 + "dataFile1" + BASE_FILE_EXTENSION), true);
    dataFilesOnlyStat1Files.put(generateFileStatus(partitionPath1 + "dataFile2" + BASE_FILE_EXTENSION), true);
    HoodieRollbackStat dataFilesOnlyStat1 = HoodieRollbackStat.newBuilder()
        .withPartitionPath(partitionPath1)
        .withDeletedFileResults(dataFilesOnlyStat1Files).build();

    Map<StoragePathInfo, Boolean> dataFilesOnlyStat2Files = new HashMap<>();
    dataFilesOnlyStat2Files.put(generateFileStatus(partitionPath2 + "dataFile1" + BASE_FILE_EXTENSION), true);
    dataFilesOnlyStat2Files.put(generateFileStatus(partitionPath2 + "dataFile2" + BASE_FILE_EXTENSION), true);
    HoodieRollbackStat dataFilesOnlyStat2 = HoodieRollbackStat.newBuilder()
        .withPartitionPath(partitionPath2)
        .withDeletedFileResults(dataFilesOnlyStat1Files).build();
    //1. test different partitionpath merge
    assertThrows(IllegalArgumentException.class, () -> {
      RollbackUtils.mergeRollbackStat(dataFilesOnlyStat1,
          dataFilesOnlyStat2);
    }, "different partition rollbackstat merge will failed");

    //prepare HoodieRollbackStat for failed and block append
    Map<StoragePathInfo, Boolean> dataFilesOnlyStat3Files = new HashMap<>();
    dataFilesOnlyStat3Files.put(generateFileStatus(partitionPath1 + "dataFile1.log"), true);
    dataFilesOnlyStat3Files.put(generateFileStatus(partitionPath1 + "dataFile3" + BASE_FILE_EXTENSION), false);
    HoodieRollbackStat dataFilesOnlyStat3 = HoodieRollbackStat.newBuilder()
        .withPartitionPath(partitionPath1)
        .withDeletedFileResults(dataFilesOnlyStat3Files).build();

    Map<StoragePathInfo, Long> dataFilesOnlyStat4Files = new HashMap<>();
    dataFilesOnlyStat4Files.put(generateFileStatus(partitionPath1 + "dataFile1.log"), 10L);
    HoodieRollbackStat dataFilesOnlyStat4 = HoodieRollbackStat.newBuilder()
        .withPartitionPath(partitionPath1)
        .withRollbackBlockAppendResults(dataFilesOnlyStat4Files).build();

    //2. test merge dataFilesOnlyStat1 and dataFilesOnlyStat3
    HoodieRollbackStat dataFilesOnlyStatMerge1 =
        RollbackUtils.mergeRollbackStat(dataFilesOnlyStat1, dataFilesOnlyStat3);
    assertEquals(partitionPath1, dataFilesOnlyStatMerge1.getPartitionPath());
    assertIterableEquals(CollectionUtils.createImmutableList(partitionPath1 + "dataFile3" + BASE_FILE_EXTENSION),
        dataFilesOnlyStatMerge1.getFailedDeleteFiles());
    assertIterableEquals(CollectionUtils.createImmutableList(partitionPath1 + "dataFile1" + BASE_FILE_EXTENSION,
        partitionPath1 + "dataFile2" + BASE_FILE_EXTENSION, partitionPath1 + "dataFile1.log").stream().sorted().collect(Collectors.toList()),
        dataFilesOnlyStatMerge1.getSuccessDeleteFiles().stream().sorted().collect(Collectors.toList()));
    assertEquals(0, dataFilesOnlyStatMerge1.getCommandBlocksCount().size());

    //3. test merge dataFilesOnlyStatMerge1 and dataFilesOnlyStat4
    HoodieRollbackStat dataFilesOnlyStatMerge2 =
        RollbackUtils.mergeRollbackStat(dataFilesOnlyStatMerge1, dataFilesOnlyStat4);
    assertEquals(partitionPath1, dataFilesOnlyStatMerge1.getPartitionPath());
    assertIterableEquals(CollectionUtils.createImmutableList(partitionPath1 + "dataFile3" + BASE_FILE_EXTENSION).stream().sorted().collect(Collectors.toList()),
        dataFilesOnlyStatMerge2.getFailedDeleteFiles().stream().sorted().collect(Collectors.toList()));
    assertIterableEquals(CollectionUtils.createImmutableList(partitionPath1 + "dataFile1" + BASE_FILE_EXTENSION,
        partitionPath1 + "dataFile2" + BASE_FILE_EXTENSION, partitionPath1 + "dataFile1.log").stream().sorted().collect(Collectors.toList()),
        dataFilesOnlyStatMerge2.getSuccessDeleteFiles().stream().sorted().collect(Collectors.toList()));
    assertEquals(Collections.singletonMap(generateFileStatus(partitionPath1 + "dataFile1.log"), 10L),
        dataFilesOnlyStatMerge2.getCommandBlocksCount());
  }
}
