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

package org.apache.hudi.client.functional;

import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.clustering.plan.strategy.JavaSizeBasedClusteringPlanStrategy;
import org.apache.hudi.client.clustering.run.strategy.JavaSortAndSizeExecutionStrategy;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.tableformat.TestTableFormat;
import org.apache.hudi.testutils.HoodieJavaClientTestHarness;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.tableformat.TestTableFormat.FIRST_INSTANT_TIME_COLUMN;
import static org.apache.hudi.tableformat.TestTableFormat.INSTANT_TIME_COLUMN;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Functional test of format meta columns with a value on write, see
 * {@link org.apache.hudi.common.HoodieTableFormat#getFormatMetaColumns()}: the instant time columns of
 * {@link TestTableFormat}. A record written as new or changed into a base file gets the instant time of the write,
 * and a record carried over unchanged keeps the value its base file stores, or gets the instant time of that file.
 * The first instant time column has a value on update, so a changed version of a record of the base file keeps the
 * value of that record.
 */
public class TestHoodieJavaClientFormatMetaColumnForTestFormat extends HoodieJavaClientTestHarness {

  private static final String PARTITION = HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
  private static final String OTHER_PARTITION = HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;

  @Override
  protected void initMetaClient() {
    // The test creates its table through initTable
  }

  @AfterAll
  public static void tearDownAll() throws IOException {
    TestTableFormat.tearDown();
    FileSystem.closeAll();
  }

  /**
   * Two file groups. On copy-on-write, an upsert rewrites each of them with the merge handle; on merge-on-read, the
   * first one is compacted after an upsert, and the second one has a log file with an update when the clustering
   * merges both into one base file.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  void testInstantTimeColumnThroughUpsertCompactionAndClustering(HoodieTableType tableType) throws Exception {
    initTable(tableType);
    HoodieJavaWriteClient client = getHoodieWriteClient(buildConfig());

    List<HoodieRecord> inserts1 = dataGen.generateInsertsForPartition("001", 5, PARTITION);
    String insertTime1 = write(client, inserts1, false);
    List<HoodieRecord> inserts2 = dataGen.generateInsertsForPartition("002", 5, PARTITION);
    String insertTime2 = write(client, inserts2, false);
    Map<String, String> fileIds = getFileIdsByRecordKey();
    String fileId1 = fileIds.get(inserts1.get(0).getRecordKey());
    String fileId2 = fileIds.get(inserts2.get(0).getRecordKey());

    // Rewrite the first file group: the updated records get the instant time of the rewrite, the others the one of
    // the base file that stores no values
    // The batch updates one of the records twice, which the write combines into one update
    List<HoodieRecord> updates1 = Arrays.asList(inserts1.get(1), inserts1.get(3));
    List<HoodieRecord> batch1 = new ArrayList<>(dataGen.generateUpdates("003", updates1));
    batch1.addAll(dataGen.generateUpdates("003", updates1.subList(0, 1)));
    String updateTime1 = write(client, batch1, true);
    String rewriteTime1 = tableType == HoodieTableType.MERGE_ON_READ ? compact(client) : updateTime1;
    Map<String, Long> expected = expectedValues(inserts1, insertTime1);
    Map<String, Long> expectedFirst = new HashMap<>(expected);
    keys(updates1).forEach(key -> expected.put(key, Long.parseLong(rewriteTime1)));
    assertEquals(expected, readColumn(getLatestBaseFile(fileId1), INSTANT_TIME_COLUMN));
    assertEquals(expectedFirst, readColumn(getLatestBaseFile(fileId1), FIRST_INSTANT_TIME_COLUMN));

    // Update the second file group: copy-on-write rewrites it, merge-on-read writes the update to a log file and
    // keeps the base file the insert wrote with its values
    List<HoodieRecord> updates2 = Collections.singletonList(inserts2.get(2));
    String updateTime2 = write(client, dataGen.generateUpdates("004", updates2), true);
    Map<String, Long> expected2 = expectedValues(inserts2, insertTime2);
    expectedFirst.putAll(expected2);
    if (tableType == HoodieTableType.COPY_ON_WRITE) {
      keys(updates2).forEach(key -> expected2.put(key, Long.parseLong(updateTime2)));
    }
    assertEquals(expected2, readColumn(getLatestBaseFile(fileId2), INSTANT_TIME_COLUMN));

    // Clustering carries every record over with its value; a record a log file updates gets the clustering instant time
    Option<String> clusteringTime = client.scheduleClustering(Option.empty());
    assertTrue(clusteringTime.isPresent());
    client.cluster(clusteringTime.get(), true);
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      keys(updates2).forEach(key -> expected2.put(key, Long.parseLong(clusteringTime.get())));
    }
    expected.putAll(expected2);
    List<FileSlice> fileSlices = getLatestFileSlices();
    assertEquals(1, fileSlices.size());
    assertEquals(expected, readColumn(fileSlices.get(0).getBaseFile().get(), INSTANT_TIME_COLUMN));
    assertEquals(expectedFirst, readColumn(fileSlices.get(0).getBaseFile().get(), FIRST_INSTANT_TIME_COLUMN));

    metaClient.reloadActiveTimeline();
    TableSchemaResolver schemaResolver = new TableSchemaResolver(metaClient);
    assertFalse(schemaResolver.getTableSchemaFromDataFile().getField(INSTANT_TIME_COLUMN).isPresent());
    assertFalse(schemaResolver.getTableSchema().getField(INSTANT_TIME_COLUMN).isPresent());
    assertFalse(schemaResolver.getTableSchema().getField(FIRST_INSTANT_TIME_COLUMN).isPresent());
  }

  /**
   * Table type, index type, whether updates are written as deletes and inserts, and the small file size, which routes
   * the inserts of a partition into its existing base file when it is not 0.
   */
  private static Stream<Arguments> movedRecordCases() {
    return Stream.of(
        // A merge-on-read update written as a delete and an insert into a new file group
        Arguments.of(HoodieTableType.MERGE_ON_READ, HoodieIndex.IndexType.SIMPLE, true, 0L),
        // The same with a global index, which also moves a record to another partition
        Arguments.of(HoodieTableType.MERGE_ON_READ, HoodieIndex.IndexType.GLOBAL_SIMPLE, true, 0L),
        // A copy-on-write update in place, and a move to another partition into a new file group
        Arguments.of(HoodieTableType.COPY_ON_WRITE, HoodieIndex.IndexType.GLOBAL_SIMPLE, false, 0L),
        // The same with the move into the existing base file of the other partition, through its merge handle
        Arguments.of(HoodieTableType.COPY_ON_WRITE, HoodieIndex.IndexType.GLOBAL_SIMPLE, false, 1024 * 1024 * 1024L));
  }

  /**
   * A record whose new version a write puts into another file group keeps the first instant time of the record it
   * replaces and gets the instant time of the write, also when the batch updates it twice and when a second write
   * moves it again before any compaction. A new record gets the instant time of its write in both columns.
   */
  @ParameterizedTest
  @MethodSource("movedRecordCases")
  void testMovedRecordKeepsFirstInstantTime(HoodieTableType tableType, HoodieIndex.IndexType indexType,
                                            boolean updatesAsDeletesAndInserts, long smallFileSize) throws Exception {
    initTable(tableType);
    boolean moveAcrossPartitions = indexType == HoodieIndex.IndexType.GLOBAL_SIMPLE;
    Properties keyGenProps = new Properties();
    keyGenProps.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    keyGenProps.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partition_path");
    HoodieJavaWriteClient client = getHoodieWriteClient(getConfigBuilder(TRIP_EXAMPLE_SCHEMA, indexType)
        .withProperties(keyGenProps)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(indexType).withGlobalSimpleIndexUpdatePartitionPath(true).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(smallFileSize).withInlineCompaction(false).build())
        .withWriteUpdatesAsDeletesAndInserts(updatesAsDeletesAndInserts)
        .build());

    List<HoodieRecord> inserts = new ArrayList<>(dataGen.generateInsertsForPartition("001", 3, PARTITION));
    inserts.addAll(dataGen.generateInsertsForPartition("001", 3, OTHER_PARTITION));
    String insertTime = write(client, inserts, false);
    Map<String, Pair<Long, Long>> expected = new HashMap<>();
    inserts.forEach(record -> expected.put(record.getRecordKey(), Pair.of(Long.parseLong(insertTime), Long.parseLong(insertTime))));
    assertEquals(expected, readLiveFirstAndInstantTimes());

    HoodieRecord updated = inserts.get(0);
    HoodieRecord moved = inserts.get(1);
    List<HoodieRecord> moves = moveAcrossPartitions
        ? dataGen.generateUpdatesForDifferentPartition("002", Collections.singletonList(moved), 0L, OTHER_PARTITION) : Collections.emptyList();
    List<HoodieRecord> batch = new ArrayList<>(dataGen.generateUpdates("002", Arrays.asList(updated, updated)));
    batch.addAll(moves);
    List<HoodieRecord> newInserts = dataGen.generateInsertsForPartition("002", 1, PARTITION);
    batch.addAll(newInserts);
    String updateTime = write(client, batch, true);
    expected.put(updated.getRecordKey(), Pair.of(Long.parseLong(insertTime), Long.parseLong(updateTime)));
    if (moveAcrossPartitions) {
      expected.put(moved.getRecordKey(), Pair.of(Long.parseLong(insertTime), Long.parseLong(updateTime)));
    }
    expected.put(newInserts.get(0).getRecordKey(), Pair.of(Long.parseLong(updateTime), Long.parseLong(updateTime)));
    assertEquals(expected, readLiveFirstAndInstantTimes());

    List<HoodieRecord> secondBatch = new ArrayList<>(dataGen.generateUpdates("003", Collections.singletonList(updated)));
    if (moveAcrossPartitions) {
      secondBatch.addAll(dataGen.generateUpdatesForDifferentPartition("003", moves, 0L, PARTITION));
    }
    String secondUpdateTime = write(client, secondBatch, true);
    expected.put(updated.getRecordKey(), Pair.of(Long.parseLong(insertTime), Long.parseLong(secondUpdateTime)));
    if (moveAcrossPartitions) {
      expected.put(moved.getRecordKey(), Pair.of(Long.parseLong(insertTime), Long.parseLong(secondUpdateTime)));
    }
    assertEquals(expected, readLiveFirstAndInstantTimes());
  }

  /**
   * Returns the first instant time and the instant time of the live copy of each record of the latest base files of
   * both partitions, which is the copy with the latest commit time when an update left older copies deleted by
   * position.
   */
  private Map<String, Pair<Long, Long>> readLiveFirstAndInstantTimes() throws IOException {
    Map<String, Pair<String, Pair<Long, Long>>> liveCopies = new HashMap<>();
    try (SyncableFileSystemView fsView = getFileSystemView(metaClient.reloadActiveTimeline())) {
      for (String partition : Arrays.asList(PARTITION, OTHER_PARTITION)) {
        for (FileSlice fileSlice : fsView.getLatestFileSlices(partition).collect(Collectors.toList())) {
          try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(fileSlice.getBaseFile().get().getPath()))
              .withConf(storageConf.unwrapAs(Configuration.class)).build()) {
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
    }
    return liveCopies.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getRight()));
  }

  private static Long readLong(Group row, String column) {
    return row.getType().containsField(column) && row.getFieldRepetitionCount(column) > 0 ? row.getLong(column, 0) : null;
  }

  private void initTable(HoodieTableType tableType) throws IOException {
    storageConf.set(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key(), "false");
    storageConf.set(HoodieTableConfig.TABLE_FORMAT.key(), "test-format");
    storageConf.set(HoodieMetadataConfig.ENABLE.key(), "false");
    Properties properties = new Properties();
    properties.setProperty(HoodieTableConfig.TABLE_FORMAT.key(), "test-format");
    properties.setProperty(HoodieTableConfig.RECORDKEY_FIELDS.key(), "_row_key");
    metaClient = HoodieTestUtils.getMetaClientBuilder(tableType, properties, null)
        .set(Collections.singletonMap(TestTableFormat.INSTANT_TIME_COLUMN_ENABLED, "true"))
        .initTable(storageConf.newInstance(), basePath);
    assertEquals(2, metaClient.getTableFormat().getFormatMetaColumns().size());
  }

  private HoodieWriteConfig buildConfig() {
    return getConfigBuilder(TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.SIMPLE)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .compactionSmallFileSize(0).withInlineCompaction(false).withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withClusteringPlanStrategyClass(JavaSizeBasedClusteringPlanStrategy.class.getName())
            .withClusteringExecutionStrategyClass(JavaSortAndSizeExecutionStrategy.class.getName()).build())
        .build();
  }

  private String write(HoodieJavaWriteClient client, List<HoodieRecord> records, boolean upsert) {
    String instantTime = WriteClientTestUtils.createNewInstantTime();
    WriteClientTestUtils.startCommitWithTime(client, instantTime);
    List<WriteStatus> statuses = upsert ? client.upsert(records, instantTime) : client.insert(records, instantTime);
    assertNoWriteErrors(statuses);
    assertTrue(client.commit(instantTime, statuses));
    return instantTime;
  }

  private String compact(HoodieJavaWriteClient client) {
    Option<String> compactionTime = client.scheduleCompaction(Option.empty());
    assertTrue(compactionTime.isPresent());
    HoodieWriteMetadata writeMetadata = client.compact(compactionTime.get());
    client.commitCompaction(compactionTime.get(), writeMetadata, Option.empty());
    return compactionTime.get();
  }

  private static Map<String, Long> expectedValues(List<HoodieRecord> records, String instantTime) {
    Map<String, Long> values = new HashMap<>();
    records.forEach(record -> values.put(record.getRecordKey(), Long.parseLong(instantTime)));
    return values;
  }

  private static List<String> keys(List<HoodieRecord> records) {
    return records.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toList());
  }

  private List<FileSlice> getLatestFileSlices() throws IOException {
    try (SyncableFileSystemView fsView = getFileSystemView(metaClient.reloadActiveTimeline())) {
      return fsView.getLatestFileSlices(PARTITION).collect(Collectors.toList());
    }
  }

  private HoodieBaseFile getLatestBaseFile(String fileId) throws IOException {
    return getLatestFileSlices().stream().filter(fileSlice -> fileSlice.getFileId().equals(fileId))
        .findFirst().get().getBaseFile().get();
  }

  private Map<String, String> getFileIdsByRecordKey() throws IOException {
    Map<String, String> fileIds = new HashMap<>();
    for (FileSlice fileSlice : getLatestFileSlices()) {
      readColumn(fileSlice.getBaseFile().get(), INSTANT_TIME_COLUMN).keySet().forEach(key -> fileIds.put(key, fileSlice.getFileId()));
    }
    assertEquals(2, fileIds.values().stream().distinct().count());
    return fileIds;
  }

  /**
   * Returns the value of the column the base file stores for each of its records by record key, null for a record
   * that stores none.
   */
  private Map<String, Long> readColumn(HoodieBaseFile baseFile, String column) throws IOException {
    Map<String, Long> values = new HashMap<>();
    try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(baseFile.getPath()))
        .withConf(storageConf.unwrapAs(Configuration.class)).build()) {
      Group row;
      while ((row = reader.read()) != null) {
        boolean stored = row.getType().containsField(column) && row.getFieldRepetitionCount(column) > 0;
        values.put(row.getString(HoodieRecord.RECORD_KEY_METADATA_FIELD, 0), stored ? row.getLong(column, 0) : null);
      }
    }
    return values;
  }
}
