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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.index.HoodieIndex.IndexType.INMEMORY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Asserts that the record index and the secondary index are maintained for parquet files written outside Hudi
 * and registered in the table through replace commits, the way tables converted from other formats are.
 * Such files carry no record key, so every row is keyed by the file path relative to the table and the row position.
 */
public class TestExternalFileRecordAndSecondaryIndex extends HoodieClientTestBase {

  private static final String ID_FIELD = "id";
  private static final String NAME_FIELD = "name";
  private static final HoodieSchema SCHEMA = HoodieSchema.createRecord("external", null, null, false, Arrays.asList(
      HoodieSchemaField.of(ID_FIELD, HoodieSchema.create(HoodieSchemaType.INT)),
      HoodieSchemaField.of(NAME_FIELD, HoodieSchema.create(HoodieSchemaType.STRING))));

  @ParameterizedTest
  @ValueSource(strings = {"americas/brazil", ""})
  public void testRecordAndSecondaryIndexForExternalFiles(String partitionPath) throws Exception {
    // tables registered from other formats carry neither meta fields nor record key fields
    Properties tableProperties = new Properties();
    tableProperties.setProperty(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");
    tableProperties.setProperty(HoodieTableConfig.RECORDKEY_FIELDS.key(), "");
    initMetaClient(tableProperties);
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(metaClient.getBasePath())
        .withSchema(SCHEMA.toString())
        .withPopulateMetaFields(false)
        // file ids of external files are file names, not UUIDs, so the record index stores them as raw strings
        .withWritesFileIdEncoding(1)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(INMEMORY).build())
        .withEmbeddedTimelineServerEnabled(false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .withMetadataIndexColumnStats(false)
            .withEnableGlobalRecordLevelIndex(true)
            .withSecondaryIndexEnabled(true)
            .withSecondaryIndexForColumn(NAME_FIELD)
            .build())
        .build();
    writeClient = getHoodieWriteClient(writeConfig);
    writeClient.setOperationType(WriteOperationType.UNKNOWN);

    // first commit registers one file with three rows
    String fileName1 = "file_1.parquet";
    List<Pair<Integer, String>> rows1 = Arrays.asList(Pair.of(1, "alice"), Pair.of(2, "bob"), Pair.of(3, "alice"));
    commitExternalFile(partitionPath, fileName1, rows1, Collections.emptyMap());

    String relativePath1 = relativeFilePath(partitionPath, fileName1);
    HoodieBackedTableMetadata tableMetadata = new HoodieBackedTableMetadata(
        context, metaClient.getStorage(), writeConfig.getMetadataConfig(), writeConfig.getBasePath(), true);
    Map<String, HoodieRecordGlobalLocation> recordIndex = readRecordIndex(tableMetadata, generatedKeys(relativePath1, 3));
    assertEquals(3, recordIndex.size());
    recordIndex.values().forEach(location -> {
      assertEquals(partitionPath, location.getPartitionPath());
      assertEquals(fileName1, location.getFileId());
    });
    String secondaryIndexPartition = secondaryIndexPartition();
    assertEquals(new HashMap<String, Set<String>>() {
      {
        put("alice", setOf(generatedKey(relativePath1, 0), generatedKey(relativePath1, 2)));
        put("bob", setOf(generatedKey(relativePath1, 1)));
      }
    }, readSecondaryIndex(tableMetadata, secondaryIndexPartition, Arrays.asList("alice", "bob", "carol")));

    // second commit replaces the file with one that keeps bob and adds carol
    String fileName2 = "file_2.parquet";
    List<Pair<Integer, String>> rows2 = Arrays.asList(Pair.of(2, "bob"), Pair.of(4, "carol"));
    commitExternalFile(partitionPath, fileName2, rows2, Collections.singletonMap(partitionPath, Collections.singletonList(fileName1)));

    String relativePath2 = relativeFilePath(partitionPath, fileName2);
    tableMetadata = new HoodieBackedTableMetadata(
        context, metaClient.getStorage(), writeConfig.getMetadataConfig(), writeConfig.getBasePath(), true);
    assertTrue(readRecordIndex(tableMetadata, generatedKeys(relativePath1, 3)).isEmpty());
    recordIndex = readRecordIndex(tableMetadata, generatedKeys(relativePath2, 2));
    assertEquals(2, recordIndex.size());
    recordIndex.values().forEach(location -> assertEquals(fileName2, location.getFileId()));
    assertEquals(new HashMap<String, Set<String>>() {
      {
        put("bob", setOf(generatedKey(relativePath2, 0)));
        put("carol", setOf(generatedKey(relativePath2, 1)));
      }
    }, readSecondaryIndex(tableMetadata, secondaryIndexPartition, Arrays.asList("alice", "bob", "carol")));
  }

  private void commitExternalFile(String partitionPath, String fileName, List<Pair<Integer, String>> rows,
                                  Map<String, List<String>> partitionToReplacedFileIds) throws IOException {
    String instantTime = writeClient.startCommit(HoodieTimeline.REPLACE_COMMIT_ACTION, metaClient);
    String relativePath = relativeFilePath(partitionPath, fileName);
    long fileSize = writeParquetFile(new Path(metaClient.getBasePath().toString(), relativePath), rows);
    WriteStatus writeStatus = new WriteStatus();
    writeStatus.setFileId(fileName);
    writeStatus.setPartitionPath(partitionPath);
    HoodieDeltaWriteStat writeStat = new HoodieDeltaWriteStat();
    writeStat.setFileId(fileName);
    writeStat.setPath(ExternalFilePathUtil.appendCommitTimeAndExternalFileMarker(relativePath, instantTime));
    writeStat.setPartitionPath(partitionPath);
    writeStat.setNumWrites(rows.size());
    writeStat.setNumInserts(rows.size());
    writeStat.setTotalWriteBytes(fileSize);
    writeStat.setFileSizeInBytes(fileSize);
    writeStatus.setStat(writeStat);
    metaClient.getActiveTimeline().transitionReplaceRequestedToInflight(
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.REPLACE_COMMIT_ACTION, instantTime), Option.empty());
    writeClient.commit(instantTime, jsc.parallelize(Collections.singletonList(writeStatus), 1), Option.empty(),
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

  private String secondaryIndexPartition() {
    return metaClient.getIndexMetadata().get().getIndexDefinitions().keySet().stream()
        .filter(indexName -> indexName.startsWith(HoodieTableMetadataUtil.PARTITION_NAME_SECONDARY_INDEX_PREFIX))
        .findFirst().get();
  }

  private Map<String, HoodieRecordGlobalLocation> readRecordIndex(HoodieBackedTableMetadata tableMetadata, List<String> recordKeys) {
    return tableMetadata.readRecordIndexLocationsWithKeys(HoodieJavaRDD.of(jsc.parallelize(recordKeys, 1))).collectAsList().stream()
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }

  private Map<String, Set<String>> readSecondaryIndex(HoodieBackedTableMetadata tableMetadata, String partitionName, List<String> secondaryKeys) {
    return tableMetadata.readSecondaryIndexDataTableRecordKeysWithKeys(HoodieJavaRDD.of(jsc.parallelize(secondaryKeys, 1)), partitionName)
        .collectAsList().stream()
        .collect(Collectors.groupingBy(Pair::getKey, Collectors.mapping(Pair::getValue, Collectors.toSet())));
  }

  private static String relativeFilePath(String partitionPath, String fileName) {
    return partitionPath.isEmpty() ? fileName : partitionPath + "/" + fileName;
  }

  private static String generatedKey(String relativeFilePath, long rowPosition) {
    return ExternalFilePathUtil.generateRecordKeyForRow(relativeFilePath, rowPosition);
  }

  private static List<String> generatedKeys(String relativeFilePath, int rowCount) {
    return java.util.stream.IntStream.range(0, rowCount).mapToObj(row -> generatedKey(relativeFilePath, row)).collect(Collectors.toList());
  }

  private static Set<String> setOf(String... values) {
    return Arrays.stream(values).collect(Collectors.toSet());
  }
}
