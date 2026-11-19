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
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.StorageConfiguration;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.functional.DeferredRecordIndexTestUtils.assertRecordIndexBuilt;
import static org.apache.hudi.functional.DeferredRecordIndexTestUtils.assertRecordIndexMatchesTable;
import static org.apache.hudi.functional.DeferredRecordIndexTestUtils.assertRecordIndexNotBuilt;
import static org.apache.hudi.functional.DeferredRecordIndexTestUtils.assertRecordIndexSizedFrom;
import static org.apache.hudi.functional.DeferredRecordIndexTestUtils.rowsPerPartition;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Deferred record index initialization on the HoodieStreamer path: the first sync leaves the index unbuilt, the second
 * builds it, and the index agrees with the table after the second sync and the third.
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
    // A small file group size and a floor of one keep the sizing result away from the floor.
    cfg.configs.add(HoodieMetadataConfig.RECORD_INDEX_MAX_FILE_GROUP_SIZE_BYTES_PROP.key() + "=9600");
    cfg.configs.add(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP.key() + "=1");
    cfg.configs.add(HoodieMetadataConfig.RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP.key() + "=1");
    cfg.configs.add(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MAX_FILE_GROUP_COUNT_PROP.key() + "=1000");
    cfg.configs.add(HoodieMetadataConfig.RECORD_LEVEL_INDEX_MAX_FILE_GROUP_COUNT_PROP.key() + "=1000");
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
    StorageConfiguration<?> storageConf = HoodieTestUtils.getDefaultStorageConf();

    HoodieDeltaStreamer.Config insertCfg =
        TestHoodieDeltaStreamer.TestHelpers.makeConfig(tableBasePath, WriteOperationType.INSERT);
    enableDeferredRecordIndex(insertCfg, partitioned, tableVersion);
    new HoodieDeltaStreamer(insertCfg, jsc).sync();
    assertEquals(tableVersion, HoodieTableMetaClient.builder().setBasePath(tableBasePath)
        .setConf(storageConf).build().getTableConfig().getTableVersion().versionCode());
    assertRecordIndexNotBuilt(tableBasePath, storageConf);
    assertRecordCount(1000, tableBasePath, sqlContext);
    sqlContext.clearCache();
    Map<String, Long> rowsBeforeBuild = rowsPerPartition(sqlContext.sparkSession(), tableBasePath);

    // Every later sync carries 1000 records from the test source: 500 inserts, 450 updates and 50 deletes, so the
    // 1000 rows the first sync wrote become 1450 after the second and 1900 after the third. The second sync builds
    // the index from what the first left, then writes its own records; the third is tagged through the index alone.
    HoodieDeltaStreamer.Config upsertCfg =
        TestHoodieDeltaStreamer.TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT);
    enableDeferredRecordIndex(upsertCfg, partitioned, tableVersion);
    new HoodieDeltaStreamer(upsertCfg, jsc).sync();
    assertRecordIndexBuilt(tableBasePath, storageConf, partitioned);
    assertRecordIndexSizedFrom(tableBasePath, storageConf, writeProps(upsertCfg), partitioned, rowsBeforeBuild);
    assertSyncedTable(tableBasePath, storageConf, 1450);

    new HoodieDeltaStreamer(upsertCfg, jsc).sync();
    assertSyncedTable(tableBasePath, storageConf, 1900);
  }

  /**
   * The same deferral in continuous mode, where one write client outlives the commit that defers the build, the commit
   * that runs it and the commits tagged through it.
   */
  @ParameterizedTest
  @MethodSource("testArgs")
  public void testRecordIndexDeferredInContinuousMode(boolean partitioned, int tableVersion) throws Exception {
    String tableBasePath = basePath + "/test_rli_deferred_continuous_streamer_"
        + (partitioned ? "partitioned" : "global") + "_v" + tableVersion;
    StorageConfiguration<?> storageConf = HoodieTestUtils.getDefaultStorageConf();
    HoodieDeltaStreamer.Config cfg = TestHoodieDeltaStreamer.TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT);
    cfg.continuousMode = true;
    enableDeferredRecordIndex(cfg, partitioned, tableVersion);
    TestHoodieDeltaStreamer.deltaStreamerTestRunner(new HoodieDeltaStreamer(cfg, jsc), cfg, ignored -> {
      TestHoodieDeltaStreamer.TestHelpers.assertAtLeastNCommits(3, tableBasePath);
      return true;
    });

    // The index must have been sized from the rows the first commit wrote, which a build in that commit could not do.
    HoodieTimeline commits = HoodieTableMetaClient.builder().setBasePath(tableBasePath).setConf(storageConf).build()
        .getCommitsTimeline().filterCompletedInstants();
    Map<String, Long> rowsOfFirstCommit = commits.readCommitMetadata(commits.firstInstant().get())
        .getPartitionToWriteStats().entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().stream().mapToLong(HoodieWriteStat::getNumWrites).sum()));
    assertRecordIndexBuilt(tableBasePath, storageConf, partitioned);
    assertRecordIndexSizedFrom(tableBasePath, storageConf, writeProps(cfg), partitioned, rowsOfFirstCommit);
    sqlContext.clearCache();
    long rows = sqlContext.read().format("hudi").load(tableBasePath).count();
    assertDistinctRecordCount(rows, tableBasePath, sqlContext);
    assertRecordIndexMatchesTable(sqlContext.sparkSession(), tableBasePath, storageConf);
  }

  private static Properties writeProps(HoodieDeltaStreamer.Config cfg) {
    Properties props = new Properties();
    cfg.configs.forEach(config -> {
      String[] keyAndValue = config.split("=", 2);
      props.setProperty(keyAndValue[0], keyAndValue[1]);
    });
    return props;
  }

  private void assertSyncedTable(String tableBasePath, StorageConfiguration<?> storageConf, long expectedRows) {
    sqlContext.clearCache();
    assertRecordCount(expectedRows, tableBasePath, sqlContext);
    assertDistinctRecordCount(expectedRows, tableBasePath, sqlContext);
    assertRecordIndexMatchesTable(sqlContext.sparkSession(), tableBasePath, storageConf);
  }
}
