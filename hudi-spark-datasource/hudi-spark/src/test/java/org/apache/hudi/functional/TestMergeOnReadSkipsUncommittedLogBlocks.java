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

package org.apache.hudi.functional;

import org.apache.hudi.DataSourceReadOptions;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Log files of tables before version 8 are named after the base instant, so a file slice also lists log blocks of
 * delta commits that never completed. The snapshot read must skip those blocks by checking each block's instant
 * against the committed instants, including a block whose instant is older than the latest completed commit.
 */
@Tag("functional")
class TestMergeOnReadSkipsUncommittedLogBlocks extends SparkClientFunctionalTestHarness {

  private static final int NUM_KEYS = 20;
  private static final int NUM_PARTITIONS = 2;

  private static final StructType SCHEMA = DataTypes.createStructType(new StructField[] {
      DataTypes.createStructField("key", DataTypes.StringType, false),
      DataTypes.createStructField("part", DataTypes.StringType, false),
      DataTypes.createStructField("ts", DataTypes.LongType, false),
      DataTypes.createStructField("value", DataTypes.StringType, true)});

  @Test
  void testSnapshotReadSkipsLogBlocksOfInflightDeltaCommitOnTableVersionSix() throws IOException {
    String basePath = basePath();
    Map<String, String> options = new HashMap<>();
    options.put(HoodieTableConfig.NAME.key(), "uncommitted_log_blocks");
    options.put(DataSourceWriteOptions.TABLE_TYPE().key(), HoodieTableType.MERGE_ON_READ.name());
    options.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "key");
    options.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "part");
    options.put(DataSourceWriteOptions.ORDERING_FIELDS().key(), "ts");
    options.put(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), String.valueOf(HoodieTableVersion.SIX.versionCode()));
    options.put(HoodieWriteConfig.AUTO_UPGRADE_VERSION.key(), "false");
    // Without the metadata table the file slices come from listing, which includes every log file on storage.
    options.put(HoodieMetadataConfig.ENABLE.key(), "false");
    options.put("hoodie.insert.shuffle.parallelism", "2");
    options.put("hoodie.upsert.shuffle.parallelism", "2");

    write(rows(0, NUM_KEYS, 1L, "base"), options, basePath);
    // The first update goes to keys 0-9, the second to keys 10-19; both write log blocks into the same file groups.
    write(rows(0, NUM_KEYS / 2, 2L, "first_update"), options, basePath);
    write(rows(NUM_KEYS / 2, NUM_KEYS, 3L, "second_update"), options, basePath);

    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf()).build();
    assertEquals(HoodieTableVersion.SIX, metaClient.getTableConfig().getTableVersion());
    List<HoodieInstant> deltaCommits = metaClient.getActiveTimeline().getDeltaCommitTimeline().filterCompletedInstants().getInstants();
    assertEquals(3, deltaCommits.size());
    Map<String, String> committedValues = readValues(basePath);
    assertEquals(NUM_KEYS, committedValues.size());
    assertTrue(committedValues.entrySet().stream().allMatch(e -> e.getValue().equals(keyIndex(e.getKey()) < NUM_KEYS / 2 ? "first_update" : "second_update")),
        "With every delta commit completed, the read should see both updates: " + committedValues);

    // Turn the first update into a writer that has not completed: its log blocks stay on storage, and its instant is
    // older than the latest completed delta commit the read is planned against.
    HoodieInstant firstUpdate = deltaCommits.get(1);
    assertTrue(metaClient.getStorage().deleteFile(
        new StoragePath(metaClient.getTimelinePath(), metaClient.getInstantFileNameGenerator().getFileName(firstUpdate))));
    metaClient.reloadActiveTimeline();
    HoodieTimeline commitsTimeline = metaClient.getCommitsTimeline();
    assertTrue(commitsTimeline.filterInflights().containsInstant(firstUpdate.requestedTime()));
    assertEquals(deltaCommits.get(2).requestedTime(), commitsTimeline.filterCompletedInstants().lastInstant().get().requestedTime());

    Map<String, String> values = readValues(basePath);
    assertEquals(NUM_KEYS, values.size());
    List<String> unexpected = values.entrySet().stream()
        .filter(e -> !e.getValue().equals(keyIndex(e.getKey()) < NUM_KEYS / 2 ? "base" : "second_update"))
        .map(e -> e.getKey() + "=" + e.getValue())
        .collect(Collectors.toList());
    assertTrue(unexpected.isEmpty(), "Log blocks of the inflight delta commit " + firstUpdate.requestedTime()
        + " must not be read, but these keys returned other values: " + unexpected);
  }

  private Map<String, String> readValues(String basePath) {
    return spark().read().format("hudi")
        .option(DataSourceReadOptions.QUERY_TYPE().key(), DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL())
        .load(basePath)
        .select("key", "value")
        .collectAsList()
        .stream()
        .collect(Collectors.toMap(row -> row.getString(0), row -> row.getString(1), (a, b) -> a + "," + b, TreeMap::new));
  }

  private void write(List<Row> rows, Map<String, String> options, String basePath) {
    spark().createDataset(rows, SparkAdapterSupport$.MODULE$.sparkAdapter().getCatalystExpressionUtils().getEncoder(SCHEMA))
        .write()
        .format("hudi")
        .options(options)
        .mode(SaveMode.Append)
        .save(basePath);
  }

  private static List<Row> rows(int from, int to, long ts, String value) {
    return IntStream.range(from, to)
        .mapToObj(i -> RowFactory.create(String.format("key%02d", i), "p" + (i % NUM_PARTITIONS), ts, value))
        .collect(Collectors.toList());
  }

  private static int keyIndex(String key) {
    return Integer.parseInt(key.substring("key".length()));
  }
}
