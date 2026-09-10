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

import org.apache.hudi.DataSourceReadOptions;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ExternalFilePathUtil;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.execution.FileSourceScanExec;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.index.HoodieIndex.IndexType.INMEMORY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Asserts that the record index and the secondary index are maintained for parquet files written outside Hudi
 * and registered in the table through replace commits, the way tables converted from other formats are.
 * Such a table has no record key, so every row is keyed by the file path relative to the table and the row position.
 * Some writers, e.g. Paimon, place their files in a directory below the partition; that prefix is part of the file id.
 */
public class TestExternalFileRecordAndSecondaryIndex extends HoodieClientTestBase {

  private static final String ID_FIELD = "id";
  private static final String NAME_FIELD = "name";
  private static final String PARTITION = "americas/brazil";
  private static final String PREFIX = "bucket-0";
  private static final HoodieSchema SCHEMA = HoodieSchema.createRecord("external", null, null, false, Arrays.asList(
      HoodieSchemaField.of(ID_FIELD, HoodieSchema.create(HoodieSchemaType.INT)),
      HoodieSchemaField.of(NAME_FIELD, HoodieSchema.create(HoodieSchemaType.STRING))));

  /** Partitioned and unpartitioned tables, with the files placed directly in the partition or below a prefix. */
  static Stream<Arguments> partitionsAndPrefixes() {
    return Stream.of(
        Arguments.of(PARTITION, Option.empty()),
        Arguments.of("", Option.empty()),
        Arguments.of(PARTITION, Option.of(PREFIX)),
        Arguments.of("", Option.of(PREFIX)));
  }

  /** The shapes above with a global record index, plus a partitioned record index, which the secondary index does not build on. */
  static Stream<Arguments> partitionsPrefixesAndRecordIndexKinds() {
    return Stream.concat(
        partitionsAndPrefixes().map(arguments -> Arguments.of(arguments.get()[0], arguments.get()[1], false)),
        Stream.of(Arguments.of(PARTITION, Option.empty(), true)));
  }

  @ParameterizedTest
  @MethodSource("partitionsPrefixesAndRecordIndexKinds")
  public void testRecordAndSecondaryIndexForExternalFiles(String partitionPath, Option<String> prefix, boolean partitionedRecordIndex) throws Exception {
    initExternalTable(partitionPath);
    HoodieWriteConfig writeConfig = writeConfig(true, partitionedRecordIndex);
    writeClient = getHoodieWriteClient(writeConfig);
    writeClient.setOperationType(WriteOperationType.UNKNOWN);
    Option<String> recordIndexPartition = partitionedRecordIndex ? Option.of(partitionPath) : Option.empty();

    // first commit registers one file with three rows
    ExternalFile file1 = new ExternalFile(partitionPath, prefix, "file_1.parquet");
    commitReplace(Collections.singletonList(Pair.of(file1, rows(1, "alice", 2, "bob", 3, "alice"))), Collections.emptyMap());

    HoodieBackedTableMetadata tableMetadata = tableMetadata(writeConfig);
    assertRecordIndex(tableMetadata, recordIndexPartition, file1, 3);
    if (!partitionedRecordIndex) {
      assertEquals(mapOf("alice", setOf(file1.key(0), file1.key(2)), "bob", setOf(file1.key(1))),
          readSecondaryIndex(tableMetadata, secondaryIndexPartition(), Arrays.asList("alice", "bob", "carol")));
    }

    // second commit replaces the file with one that keeps bob and adds carol
    ExternalFile file2 = new ExternalFile(partitionPath, prefix, "file_2.parquet");
    commitReplace(Collections.singletonList(Pair.of(file2, rows(2, "bob", 4, "carol"))),
        Collections.singletonMap(partitionPath, Collections.singletonList(file1.fileId())));

    tableMetadata = tableMetadata(writeConfig);
    assertTrue(readRecordIndex(tableMetadata, recordIndexPartition, file1.keys(3)).isEmpty());
    assertRecordIndex(tableMetadata, recordIndexPartition, file2, 2);
    if (!partitionedRecordIndex) {
      assertEquals(mapOf("bob", setOf(file2.key(0)), "carol", setOf(file2.key(1))),
          readSecondaryIndex(tableMetadata, secondaryIndexPartition(), Arrays.asList("alice", "bob", "carol")));
    }
  }

  @ParameterizedTest
  @MethodSource("partitionsAndPrefixes")
  public void testIndexesAreBuiltFromRegisteredFilesWhenEnabledLater(String partitionPath, Option<String> prefix) throws Exception {
    initExternalTable(partitionPath);
    // the first file is registered while both indexes are off
    writeClient = getHoodieWriteClient(writeConfig(false, false));
    writeClient.setOperationType(WriteOperationType.UNKNOWN);
    ExternalFile file1 = new ExternalFile(partitionPath, prefix, "file_1.parquet");
    commitReplace(Collections.singletonList(Pair.of(file1, rows(1, "alice", 2, "bob", 3, "alice"))), Collections.emptyMap());

    // the second file is registered with both indexes on, which first builds them from the registered file
    HoodieWriteConfig indexedWriteConfig = writeConfig(true, false);
    writeClient = getHoodieWriteClient(indexedWriteConfig);
    writeClient.setOperationType(WriteOperationType.UNKNOWN);
    ExternalFile file2 = new ExternalFile(partitionPath, prefix, "file_2.parquet");
    commitReplace(Collections.singletonList(Pair.of(file2, rows(4, "carol"))), Collections.emptyMap());

    HoodieBackedTableMetadata tableMetadata = tableMetadata(indexedWriteConfig);
    assertRecordIndex(tableMetadata, Option.empty(), file1, 3);
    assertRecordIndex(tableMetadata, Option.empty(), file2, 1);
    String secondaryIndexPartition = secondaryIndexPartition();
    assertEquals(mapOf("alice", setOf(file1.key(0), file1.key(2)), "bob", setOf(file1.key(1)), "carol", setOf(file2.key(0))),
        readSecondaryIndex(tableMetadata, secondaryIndexPartition, Arrays.asList("alice", "bob", "carol")));
    if (partitionPath.isEmpty()) {
      // a query on the indexed column reads only the file the secondary index points at. The partitioned shape is not
      // read here: the test harness declares a partition field that the schema of the external files does not carry.
      assertSecondaryIndexPrunesRead("carol", setOf(4), 1);
      assertSecondaryIndexPrunesRead("alice", setOf(1, 3), 1);
    }

    // the third commit only drops the first file
    commitReplace(Collections.emptyList(), Collections.singletonMap(partitionPath, Collections.singletonList(file1.fileId())));

    tableMetadata = tableMetadata(indexedWriteConfig);
    assertTrue(readRecordIndex(tableMetadata, Option.empty(), file1.keys(3)).isEmpty());
    assertRecordIndex(tableMetadata, Option.empty(), file2, 1);
    assertEquals(mapOf("carol", setOf(file2.key(0))),
        readSecondaryIndex(tableMetadata, secondaryIndexPartition, Arrays.asList("alice", "bob", "carol")));
  }

  @Test
  public void testClusteringIsRejectedBecausePositionalKeysDoNotSurviveIt() throws Exception {
    initExternalTable("");
    HoodieWriteConfig writeConfig = writeConfig(true, false);
    writeClient = getHoodieWriteClient(writeConfig);
    writeClient.setOperationType(WriteOperationType.UNKNOWN);
    commitReplace(Arrays.asList(
        Pair.of(new ExternalFile("", Option.empty(), "file_1.parquet"), rows(1, "alice", 2, "bob")),
        Pair.of(new ExternalFile("", Option.empty(), "file_2.parquet"), rows(3, "carol"))), Collections.emptyMap());

    // clustering rewrites the rows into a new file, so their keys, which are file path and position, would change.
    // The clustering commit is replayed the way clustering completes it: a replace commit of the rewritten file that
    // carries the CLUSTER operation type and updates the indexes from its commit metadata.
    writeClient.setOperationType(WriteOperationType.CLUSTER);
    Exception clustering = assertThrows(Exception.class, () -> commitReplace(
        Collections.singletonList(Pair.of(new ExternalFile("", Option.empty(), "file_3.parquet"), rows(1, "alice", 2, "bob", 3, "carol"))),
        Collections.singletonMap("", Arrays.asList("file_1.parquet", "file_2.parquet"))));
    Throwable cause = clustering;
    while (cause.getCause() != null && (cause.getMessage() == null || !cause.getMessage().contains("cannot be clustered"))) {
      cause = cause.getCause();
    }
    assertTrue(String.valueOf(cause.getMessage()).contains("cannot be clustered because it has no record key"), String.valueOf(clustering));
  }

  /**
   * Tables registered from other formats carry neither meta fields nor record key fields. An unpartitioned table
   * declares no partition field either; the test harness otherwise declares one that the schema does not carry.
   */
  private void initExternalTable(String partitionPath) throws IOException {
    Properties tableProperties = new Properties();
    tableProperties.setProperty(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");
    tableProperties.setProperty(HoodieTableConfig.RECORDKEY_FIELDS.key(), "");
    if (partitionPath.isEmpty()) {
      tableProperties.setProperty(HoodieTableConfig.PARTITION_FIELDS.key(), "");
    }
    initMetaClient(tableProperties);
  }

  private HoodieWriteConfig writeConfig(boolean withIndexes, boolean partitionedRecordIndex) {
    HoodieMetadataConfig.Builder metadataConfig = HoodieMetadataConfig.newBuilder()
        .enable(true)
        // the indexes are updated from the commit metadata, the way replace commits and table services update them
        .withStreamingWriteEnabled(false)
        .withMetadataIndexColumnStats(false);
    if (withIndexes && partitionedRecordIndex) {
      // the secondary index builds on the global record index only
      metadataConfig.withEnableRecordLevelIndex(true);
    } else if (withIndexes) {
      metadataConfig.withEnableGlobalRecordLevelIndex(true)
          .withSecondaryIndexEnabled(true)
          .withSecondaryIndexForColumn(NAME_FIELD);
    }
    return HoodieWriteConfig.newBuilder()
        .withPath(metaClient.getBasePath())
        .withSchema(SCHEMA.toString())
        .withPopulateMetaFields(false)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(INMEMORY).build())
        .withEmbeddedTimelineServerEnabled(false)
        .withMetadataConfig(metadataConfig.build())
        .build();
  }

  /**
   * Registers the given files in one replace commit, the way an external writer does: the files are written to
   * their location below the table, and the commit records them with the external file marker.
   */
  private void commitReplace(List<Pair<ExternalFile, List<Pair<Integer, String>>>> newFiles,
                             Map<String, List<String>> partitionToReplacedFileIds) throws IOException {
    String instantTime = writeClient.startCommit(HoodieTimeline.REPLACE_COMMIT_ACTION, metaClient);
    List<WriteStatus> writeStatuses = new ArrayList<>();
    for (Pair<ExternalFile, List<Pair<Integer, String>>> fileAndRows : newFiles) {
      ExternalFile file = fileAndRows.getLeft();
      long fileSize = writeParquetFile(new Path(metaClient.getBasePath().toString(), file.relativePath()), fileAndRows.getRight());
      WriteStatus writeStatus = new WriteStatus();
      writeStatus.setFileId(file.fileId());
      writeStatus.setPartitionPath(file.partitionPath);
      HoodieDeltaWriteStat writeStat = new HoodieDeltaWriteStat();
      writeStat.setFileId(file.fileId());
      writeStat.setPath(file.markedPath(instantTime));
      writeStat.setPartitionPath(file.partitionPath);
      writeStat.setNumWrites(fileAndRows.getRight().size());
      writeStat.setNumInserts(fileAndRows.getRight().size());
      writeStat.setTotalWriteBytes(fileSize);
      writeStat.setFileSizeInBytes(fileSize);
      writeStatus.setStat(writeStat);
      writeStatuses.add(writeStatus);
    }
    metaClient.getActiveTimeline().transitionReplaceRequestedToInflight(
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.REPLACE_COMMIT_ACTION, instantTime), Option.empty());
    writeClient.commit(instantTime, jsc.parallelize(writeStatuses, 1), Option.empty(),
        HoodieTimeline.REPLACE_COMMIT_ACTION, partitionToReplacedFileIds);
    metaClient = HoodieTableMetaClient.reload(metaClient);
  }

  private long writeParquetFile(Path path, List<Pair<Integer, String>> rows) throws IOException {
    Configuration conf = metaClient.getStorageConf().unwrapAs(Configuration.class);
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
        .withSchema(SCHEMA.toAvroSchema()).withConf(conf).build()) {
      for (Pair<Integer, String> row : rows) {
        GenericRecord record = new GenericData.Record(SCHEMA.toAvroSchema());
        record.put(ID_FIELD, row.getLeft());
        record.put(NAME_FIELD, row.getRight());
        writer.write(record);
      }
    }
    return path.getFileSystem(conf).getFileStatus(path).getLen();
  }

  /**
   * Reads the table through Spark with a filter on the indexed column and asserts the rows returned and the number of
   * files scanned, which the secondary index prunes to the files that hold the secondary key.
   */
  private void assertSecondaryIndexPrunesRead(String secondaryKey, Set<Integer> expectedIds, long expectedFileCount) {
    Dataset<Row> rows = sparkSession.read().format("hudi")
        .option(HoodieMetadataConfig.ENABLE.key(), "true")
        .option(DataSourceReadOptions.ENABLE_DATA_SKIPPING().key(), "true")
        .load(metaClient.getBasePath().toString())
        .where(NAME_FIELD + " = '" + secondaryKey + "'");
    assertEquals(expectedIds, rows.collectAsList().stream().map(row -> row.getInt(row.fieldIndex(ID_FIELD))).collect(Collectors.toSet()));
    SparkPlan scan = rows.queryExecution().executedPlan().collectLeaves().head();
    if (scan instanceof AdaptiveSparkPlanExec) {
      scan = ((AdaptiveSparkPlanExec) scan).executedPlan().collectLeaves().head();
    }
    assertEquals(expectedFileCount, ((FileSourceScanExec) scan).metrics().apply("numFiles").value());
  }

  private HoodieBackedTableMetadata tableMetadata(HoodieWriteConfig writeConfig) {
    return new HoodieBackedTableMetadata(context, metaClient.getStorage(), writeConfig.getMetadataConfig(), writeConfig.getBasePath(), true);
  }

  private String secondaryIndexPartition() {
    return metaClient.getIndexMetadata().get().getIndexDefinitions().keySet().stream()
        .filter(indexName -> indexName.startsWith(HoodieTableMetadataUtil.PARTITION_NAME_SECONDARY_INDEX_PREFIX))
        .findFirst().get();
  }

  /** Asserts that every row of the file is in the record index and points at the file. */
  private void assertRecordIndex(HoodieBackedTableMetadata tableMetadata, Option<String> recordIndexPartition, ExternalFile file, int rowCount) {
    Map<String, HoodieRecordGlobalLocation> locations = readRecordIndex(tableMetadata, recordIndexPartition, file.keys(rowCount));
    assertEquals(rowCount, locations.size());
    locations.values().forEach(location -> {
      assertEquals(file.partitionPath, location.getPartitionPath());
      assertEquals(file.fileId(), location.getFileId());
    });
  }

  private Map<String, HoodieRecordGlobalLocation> readRecordIndex(HoodieBackedTableMetadata tableMetadata, Option<String> recordIndexPartition,
                                                                  List<String> recordKeys) {
    return tableMetadata.readRecordIndexLocationsWithKeys(HoodieJavaRDD.of(jsc.parallelize(recordKeys, 1)), recordIndexPartition).collectAsList().stream()
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }

  private Map<String, Set<String>> readSecondaryIndex(HoodieBackedTableMetadata tableMetadata, String partitionName, List<String> secondaryKeys) {
    return tableMetadata.readSecondaryIndexDataTableRecordKeysWithKeys(HoodieJavaRDD.of(jsc.parallelize(secondaryKeys, 1)), partitionName)
        .collectAsList().stream()
        .collect(Collectors.groupingBy(Pair::getKey, Collectors.mapping(Pair::getValue, Collectors.toSet())));
  }

  private static List<Pair<Integer, String>> rows(Object... idsAndNames) {
    List<Pair<Integer, String>> rows = new ArrayList<>();
    for (int i = 0; i < idsAndNames.length; i += 2) {
      rows.add(Pair.of((Integer) idsAndNames[i], (String) idsAndNames[i + 1]));
    }
    return rows;
  }

  private static Map<String, Set<String>> mapOf(Object... keysAndValues) {
    Map<String, Set<String>> map = new HashMap<>();
    for (int i = 0; i < keysAndValues.length; i += 2) {
      @SuppressWarnings("unchecked")
      Set<String> value = (Set<String>) keysAndValues[i + 1];
      map.put((String) keysAndValues[i], value);
    }
    return map;
  }

  @SafeVarargs
  private static <T> Set<T> setOf(T... values) {
    return Arrays.stream(values).collect(Collectors.toSet());
  }

  /** A parquet file written outside Hudi: where it lives below the table, and the file id Hudi registers it under. */
  private static final class ExternalFile {
    private final String partitionPath;
    private final Option<String> prefix;
    private final String fileName;

    ExternalFile(String partitionPath, Option<String> prefix, String fileName) {
      this.partitionPath = partitionPath;
      this.prefix = prefix;
      this.fileName = fileName;
    }

    /** The file id of an external file is its path below the partition. */
    String fileId() {
      return prefix.map(p -> p + "/" + fileName).orElse(fileName);
    }

    String relativePath() {
      return partitionPath.isEmpty() ? fileId() : partitionPath + "/" + fileId();
    }

    /** The path the commit records: the file name carries the commit time, the prefix and the external file marker. */
    String markedPath(String instantTime) {
      String markedFileName = prefix.isPresent()
          ? ExternalFilePathUtil.appendCommitTimeAndExternalFileMarker(fileName, instantTime, prefix.get())
          : ExternalFilePathUtil.appendCommitTimeAndExternalFileMarker(fileName, instantTime);
      String markedFileId = prefix.map(p -> p + "/" + markedFileName).orElse(markedFileName);
      return partitionPath.isEmpty() ? markedFileId : partitionPath + "/" + markedFileId;
    }

    /** Every row is keyed by the file path relative to the table and the row position. */
    String key(long rowPosition) {
      return ExternalFilePathUtil.generateRecordKeyForRow(relativePath(), rowPosition);
    }

    List<String> keys(int rowCount) {
      return IntStream.range(0, rowCount).mapToObj(this::key).collect(Collectors.toList());
    }
  }
}
