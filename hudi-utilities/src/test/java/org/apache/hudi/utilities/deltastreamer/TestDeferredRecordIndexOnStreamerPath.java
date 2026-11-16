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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deferred record index initialization on the HoodieStreamer path: the first sync leaves the index unbuilt and the
 * second one builds it, with no duplicates across the switch from the fallback index.
 */
@Tag("functional")
public class TestDeferredRecordIndexOnStreamerPath extends HoodieDeltaStreamerTestBase {

  private static void enableDeferredRecordIndex(HoodieDeltaStreamer.Config cfg, boolean partitioned, int tableVersion) {
    cfg.configs.add(HoodieMetadataConfig.ENABLE.key() + "=true");
    cfg.configs.add(HoodieWriteConfig.WRITE_TABLE_VERSION.key() + "=" + tableVersion);
    cfg.configs.add(HoodieWriteConfig.AUTO_UPGRADE_VERSION.key() + "=false");
    cfg.configs.add(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key() + "=" + !partitioned);
    cfg.configs.add(HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key() + "=" + partitioned);
    cfg.configs.add(HoodieIndexConfig.INDEX_TYPE.key() + "="
        + (partitioned ? "RECORD_LEVEL_INDEX" : "GLOBAL_RECORD_LEVEL_INDEX"));
    cfg.configs.add(HoodieMetadataConfig.DEFER_RLI_INIT_FOR_FRESH_TABLE.key() + "=true");
    // Pin the sizing inputs so the expected file group count follows from the record count alone: 9600 bytes over
    // 48 bytes per entry is 200 entries per file group, and the floor is kept out of the way.
    cfg.configs.add(HoodieMetadataConfig.RECORD_INDEX_MAX_FILE_GROUP_SIZE_BYTES_PROP.key() + "=9600");
    cfg.configs.add(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP.key() + "=1");
    cfg.configs.add(HoodieMetadataConfig.RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP.key() + "=1");
    cfg.configs.add(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MAX_FILE_GROUP_COUNT_PROP.key() + "=1000");
    cfg.configs.add(HoodieMetadataConfig.RECORD_LEVEL_INDEX_MAX_FILE_GROUP_COUNT_PROP.key() + "=1000");
  }

  /**
   * The file group count the record index is sized to for the given number of records, applying the growth factor and
   * the entries that fit in one file group.
   */
  private static int expectedFileGroupCount(long records) {
    long estimated = (long) Math.ceil(records * 2.0) / 200;
    return (int) Math.max(1, Math.min(1000, estimated));
  }

  private static List<String> recordIndexFileGroupIds(String tableBasePath) {
    HoodieTableMetaClient metadataMetaClient = HoodieTableMetaClient.builder()
        .setBasePath(HoodieTableMetadata.getMetadataTableBasePath(tableBasePath))
        .setConf(HoodieTestUtils.getDefaultStorageConf())
        .build();
    return HoodieTableMetadataUtil.getPartitionLatestFileSlices(metadataMetaClient, Option.empty(),
            MetadataPartitionType.RECORD_INDEX.getPartitionPath())
        .stream().map(FileSlice::getFileId).collect(Collectors.toList());
  }

  private static boolean isRecordIndexBuilt(String tableBasePath) {
    return HoodieTableMetaClient.builder()
        .setBasePath(tableBasePath)
        .setConf(HoodieTestUtils.getDefaultStorageConf())
        .build()
        .getTableConfig().getMetadataPartitions()
        .contains(MetadataPartitionType.RECORD_INDEX.getPartitionPath());
  }

  static Stream<Arguments> testArgs() {
    return Stream.of(
        Arguments.arguments(false, HoodieTableVersion.SIX.versionCode()),
        Arguments.arguments(false, HoodieTableVersion.TEN.versionCode()),
        Arguments.arguments(true, HoodieTableVersion.TEN.versionCode()));
  }

  @ParameterizedTest
  @MethodSource("testArgs")
  public void testRecordIndexDeferredUntilSecondSync(boolean partitioned, int tableVersion) throws Exception {
    String tableBasePath = basePath + "/test_rli_deferred_streamer_"
        + (partitioned ? "partitioned" : "global") + "_v" + tableVersion;

    HoodieDeltaStreamer.Config insertCfg =
        TestHoodieDeltaStreamer.TestHelpers.makeConfig(tableBasePath, WriteOperationType.INSERT);
    enableDeferredRecordIndex(insertCfg, partitioned, tableVersion);
    new HoodieDeltaStreamer(insertCfg, jsc).sync();
    assertEquals(tableVersion, HoodieTableMetaClient.builder().setBasePath(tableBasePath)
        .setConf(HoodieTestUtils.getDefaultStorageConf()).build().getTableConfig().getTableVersion().versionCode());
    assertFalse(isRecordIndexBuilt(tableBasePath), "Record index should not be built by the first sync");

    // The index is sized from the data the first sync left behind, so capture it before the second sync adds to it.
    assertRecordCount(1000, tableBasePath, sqlContext);
    sqlContext.clearCache();
    Map<String, Long> rowsPerPartition = sqlContext.read().format("hudi").load(tableBasePath)
        .groupBy("_hoodie_partition_path").count()
        .collectAsList().stream().collect(Collectors.toMap(row -> row.getString(0), row -> row.getLong(1)));
    long rowsBeforeBuild = rowsPerPartition.values().stream().mapToLong(Long::longValue).sum();

    HoodieDeltaStreamer.Config upsertCfg =
        TestHoodieDeltaStreamer.TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT);
    enableDeferredRecordIndex(upsertCfg, partitioned, tableVersion);
    new HoodieDeltaStreamer(upsertCfg, jsc).sync();
    assertTrue(isRecordIndexBuilt(tableBasePath), "Record index should be built by the second sync");

    // The second sync carries 1000 records from the test source: 500 inserts, 450 updates and 50 deletes, so the
    // 1000 the first sync wrote become 1450, one row per key, and the deletes make this cover a key removed through
    // the fallback index as well. The same 1450 comes out with the deferral turned off, so this pins what the two
    // syncs write rather than anything the deferral does, and a row lost or duplicated across the switch from the
    // fallback index to the record index fails here.
    assertRecordCount(1450, tableBasePath, sqlContext);
    assertDistinctRecordCount(1450, tableBasePath, sqlContext);

    List<String> fileGroupIds = recordIndexFileGroupIds(tableBasePath);
    if (partitioned) {
      Map<String, Integer> expected = rowsPerPartition.entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getKey, entry -> expectedFileGroupCount(entry.getValue())));
      Map<String, Integer> actual = fileGroupIds.stream()
          .collect(Collectors.groupingBy(HoodieTableMetadataUtil::getDataTablePartitionNameFromFileGroupName,
              Collectors.reducing(0, fileGroupId -> 1, Integer::sum)));
      assertEquals(expected, actual, "Each partition's record index should be sized from that partition's records");
    } else {
      assertEquals(expectedFileGroupCount(rowsBeforeBuild), fileGroupIds.size(),
          "The record index should be sized from the records the first sync left behind");
    }
  }
}
