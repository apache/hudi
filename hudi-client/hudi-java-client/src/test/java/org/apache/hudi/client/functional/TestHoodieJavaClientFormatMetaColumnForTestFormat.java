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
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
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
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

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

    // Update the second file group: copy-on-write rewrites it, merge-on-read writes the update to a log file
    List<HoodieRecord> updates2 = Collections.singletonList(inserts2.get(2));
    String updateTime2 = write(client, dataGen.generateUpdates("004", updates2), true);
    Map<String, Long> expected2 = expectedValues(inserts2, insertTime2);
    expectedFirst.putAll(expected2);
    if (tableType == HoodieTableType.COPY_ON_WRITE) {
      keys(updates2).forEach(key -> expected2.put(key, Long.parseLong(updateTime2)));
      assertEquals(expected2, readColumn(getLatestBaseFile(fileId2), INSTANT_TIME_COLUMN));
    } else {
      assertTrue(readColumn(getLatestBaseFile(fileId2), INSTANT_TIME_COLUMN).values().stream().allMatch(Objects::isNull));
    }

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

  private void initTable(HoodieTableType tableType) throws IOException {
    storageConf.set(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key(), "false");
    storageConf.set(HoodieTableConfig.TABLE_FORMAT.key(), "test-format");
    storageConf.set(HoodieMetadataConfig.ENABLE.key(), "false");
    Properties properties = new Properties();
    properties.setProperty(HoodieTableConfig.TABLE_FORMAT.key(), "test-format");
    metaClient = TestTableFormat.setTableFormatConfigs(HoodieTestUtils.getMetaClientBuilder(tableType, properties, null)
        .initTable(storageConf.newInstance(), basePath), Collections.singletonMap(TestTableFormat.INSTANT_TIME_COLUMN_ENABLED, "true"));
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
