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

package org.apache.hudi.client.functional;

import org.apache.hudi.DefaultSparkRecordMerger;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.tableformat.TestTableFormat;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.tableformat.TestTableFormat.FIRST_INSTANT_TIME_COLUMN;
import static org.apache.hudi.tableformat.TestTableFormat.INSTANT_TIME_COLUMN;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Functional test of the format meta columns of {@link TestTableFormat} for records whose new version a merge-on-read
 * upsert written as deletes and inserts puts into another file group, through the Spark write client: the new
 * version keeps the first instant time of the record it replaces and gets the instant time of the write.
 */
@Tag("functional")
public class TestSparkMovedRecordFormatMetaColumnsForTestFormat extends SparkClientFunctionalTestHarness {

  private static final String PARTITION = HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
  private static final String OTHER_PARTITION = HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;

  @AfterAll
  static void tearDownTableFormat() {
    TestTableFormat.tearDown();
  }

  /**
   * Returns the type of the records the test writes.
   */
  protected HoodieRecordType getRecordType() {
    return HoodieRecordType.AVRO;
  }

  /**
   * The simple index locates a record in its partition by reading the base files; the record index looks the record
   * up across partitions without reading them, and also moves a record to another partition. Each record is updated
   * twice in the first batch and again in a second write before any compaction.
   */
  @ParameterizedTest
  @EnumSource(value = HoodieIndex.IndexType.class, names = {"SIMPLE", "RECORD_INDEX"})
  void testMovedRecordKeepsFirstInstantTime(HoodieIndex.IndexType indexType) throws IOException {
    boolean moveAcrossPartitions = indexType == HoodieIndex.IndexType.RECORD_INDEX;
    // The record index lives in the metadata table
    Map<String, Object> tableFormatConfig = new HashMap<>();
    tableFormatConfig.put(TestTableFormat.INSTANT_TIME_COLUMN_ENABLED, "true");
    tableFormatConfig.put(TestTableFormat.METADATA_TABLE_ENABLED, String.valueOf(moveAcrossPartitions));
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.newTableBuilder()
        .setTableName("test-trip-table")
        .setTableType(HoodieTableType.MERGE_ON_READ)
        .setTableFormat("test-format")
        .setRecordKeyFields("_row_key")
        .setPartitionFields("partition_path")
        .set(tableFormatConfig)
        .initTable(storageConf().newInstance(), basePath());
    assertEquals(2, metaClient.getTableFormat().getFormatMetaColumns().size());
    Properties keyGenProps = new Properties();
    keyGenProps.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    keyGenProps.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partition_path");
    HoodieWriteConfig.Builder configBuilder = getConfigBuilder(true, indexType)
        .withProperties(keyGenProps)
        .withIndexConfig(HoodieIndexConfig.newBuilder().fromProperties(keyGenProps).withIndexType(indexType)
            .withRecordIndexUpdatePartitionPath(true).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(moveAcrossPartitions)
            .withEnableGlobalRecordLevelIndex(moveAcrossPartitions).build())
        .withWriteUpdatesAsDeletesAndInserts(true);
    if (getRecordType() == HoodieRecordType.SPARK) {
      configBuilder.withRecordMergeImplClasses(DefaultSparkRecordMerger.class.getName());
    }
    HoodieWriteConfig config = configBuilder.build();
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {PARTITION, OTHER_PARTITION});
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      List<HoodieRecord> inserts = new ArrayList<>(dataGen.generateInsertsForPartition("001", 3, PARTITION));
      inserts.addAll(dataGen.generateInsertsForPartition("001", 3, OTHER_PARTITION));
      String insertTime = write(client, inserts);
      Map<String, Pair<Long, Long>> expected = new HashMap<>();
      inserts.forEach(record -> expected.put(record.getRecordKey(), Pair.of(Long.parseLong(insertTime), Long.parseLong(insertTime))));
      assertEquals(expected, readLiveFirstAndInstantTimes(metaClient));

      HoodieRecord updated = inserts.get(0);
      HoodieRecord moved = inserts.get(1);
      List<HoodieRecord> moves = moveAcrossPartitions
          ? dataGen.generateUpdatesForDifferentPartition("002", Collections.singletonList(moved), 0L, OTHER_PARTITION) : Collections.emptyList();
      List<HoodieRecord> batch = new ArrayList<>(dataGen.generateUpdates("002", Arrays.asList(updated, updated)));
      batch.addAll(moves);
      List<HoodieRecord> newInserts = dataGen.generateInsertsForPartition("002", 1, PARTITION);
      batch.addAll(newInserts);
      String updateTime = write(client, batch);
      expected.put(updated.getRecordKey(), Pair.of(Long.parseLong(insertTime), Long.parseLong(updateTime)));
      if (moveAcrossPartitions) {
        expected.put(moved.getRecordKey(), Pair.of(Long.parseLong(insertTime), Long.parseLong(updateTime)));
      }
      expected.put(newInserts.get(0).getRecordKey(), Pair.of(Long.parseLong(updateTime), Long.parseLong(updateTime)));
      assertEquals(expected, readLiveFirstAndInstantTimes(metaClient));

      List<HoodieRecord> secondBatch = new ArrayList<>(dataGen.generateUpdates("003", Collections.singletonList(updated)));
      if (moveAcrossPartitions) {
        secondBatch.addAll(dataGen.generateUpdatesForDifferentPartition("003", moves, 0L, PARTITION));
      }
      String secondUpdateTime = write(client, secondBatch);
      expected.put(updated.getRecordKey(), Pair.of(Long.parseLong(insertTime), Long.parseLong(secondUpdateTime)));
      if (moveAcrossPartitions) {
        expected.put(moved.getRecordKey(), Pair.of(Long.parseLong(insertTime), Long.parseLong(secondUpdateTime)));
      }
      assertEquals(expected, readLiveFirstAndInstantTimes(metaClient));
    }
  }

  private String write(SparkRDDWriteClient client, List<HoodieRecord> records) {
    String instantTime = client.startCommit();
    JavaRDD<HoodieRecord> recordRDD = jsc().parallelize(TestSparkCopyOnWriteRowLineageForTestFormat.toRecordType(records, getRecordType()), 2);
    List<WriteStatus> statuses = ((JavaRDD<WriteStatus>) client.upsert(recordRDD, instantTime)).collect();
    assertNoWriteErrors(statuses);
    assertTrue(client.commit(instantTime, jsc().parallelize(statuses, 1)));
    return instantTime;
  }

  /**
   * Returns the first instant time and the instant time of the live copy of each record of the latest base files of
   * both partitions, which is the copy with the latest commit time when an update left older copies deleted by
   * position.
   */
  private Map<String, Pair<Long, Long>> readLiveFirstAndInstantTimes(HoodieTableMetaClient metaClient) throws IOException {
    Map<String, Pair<String, Pair<Long, Long>>> liveCopies = new HashMap<>();
    HoodieTableFileSystemView fsView = HoodieTableFileSystemView.fileListingBasedFileSystemView(
        context(), metaClient, metaClient.reloadActiveTimeline().filterCompletedInstants());
    try {
      for (String partition : Arrays.asList(PARTITION, OTHER_PARTITION)) {
        for (FileSlice fileSlice : fsView.getLatestFileSlices(partition).collect(Collectors.toList())) {
          try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(fileSlice.getBaseFile().get().getPath()))
              .withConf(storageConf().unwrapAs(Configuration.class)).build()) {
            Group row;
            while ((row = reader.read()) != null) {
              String commitTime = row.getString(HoodieRecord.COMMIT_TIME_METADATA_FIELD, 0);
              Pair<Long, Long> values = Pair.of(readLong(row, FIRST_INSTANT_TIME_COLUMN), readLong(row, INSTANT_TIME_COLUMN));
              liveCopies.merge(row.getString(HoodieRecord.RECORD_KEY_METADATA_FIELD, 0), Pair.of(commitTime, values),
                  (copy1, copy2) -> copy1.getLeft().compareTo(copy2.getLeft()) >= 0 ? copy1 : copy2);
            }
          }
        }
      }
    } finally {
      fsView.close();
    }
    return liveCopies.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getRight()));
  }

  private static Long readLong(Group row, String column) {
    return row.getType().containsField(column) && row.getFieldRepetitionCount(column) > 0 ? row.getLong(column, 0) : null;
  }
}
