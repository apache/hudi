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

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.DefaultSparkRecordMerger;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.SerializableIndexedRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.tableformat.TestTableFormat;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.HoodieInternalRowUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.common.testutils.RowLineageTestUtils.NO_ROW_LINEAGE;
import static org.apache.hudi.common.testutils.RowLineageTestUtils.assertNewRowLineage;
import static org.apache.hudi.common.testutils.RowLineageTestUtils.assertRowLineageChangesWithCommitSeqNo;
import static org.apache.hudi.common.testutils.RowLineageTestUtils.assertUpdatedRowLineage;
import static org.apache.hudi.common.testutils.RowLineageTestUtils.deriveRowLineage;
import static org.apache.hudi.common.testutils.RowLineageTestUtils.hasRowLineageColumns;
import static org.apache.hudi.common.testutils.RowLineageTestUtils.readStoredRowLineage;
import static org.apache.hudi.common.testutils.RowLineageTestUtils.storedOnUpdate;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Functional test for the row lineage columns of an external table format, see
 * {@link org.apache.hudi.common.HoodieTableFormat#getFormatMetaColumns()}, on a copy-on-write table written
 * with the Spark write client. Reading and writing Spark records needs the Spark version adapter of the
 * Spark datasource modules, where a subclass runs this test with Spark records.
 */
@Tag("functional")
public class TestSparkCopyOnWriteRowLineageForTestFormat extends SparkClientFunctionalTestHarness {

  private static final String PARTITION = HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;

  @AfterAll
  static void tearDownTableFormat() {
    TestTableFormat.tearDown();
  }

  /**
   * Returns the type of the records the merge reads and writes.
   */
  protected HoodieRecordType getRecordType() {
    return HoodieRecordType.AVRO;
  }

  protected HoodieTableMetaClient initTable() throws IOException {
    return initTable(HoodieTableType.COPY_ON_WRITE);
  }

  protected HoodieTableMetaClient initTable(HoodieTableType tableType) throws IOException {
    HoodieTableMetaClient metaClient = TestTableFormat.setTableFormatConfigs(HoodieTableMetaClient.newTableBuilder()
        .setTableName("test-trip-table")
        .setTableType(tableType)
        .setTableFormat("test-format")
        .initTable(storageConf().newInstance(), basePath()), Collections.singletonMap(TestTableFormat.ROW_LINEAGE_ENABLED, "true"));
    assertEquals(2, metaClient.getTableFormat().getFormatMetaColumns().size());
    return metaClient;
  }

  protected HoodieWriteConfig buildConfig(HoodieTableMetaClient metaClient, HoodieRecordType recordType, long smallFileSize,
                                          HoodieClusteringConfig clusteringConfig) {
    HoodieWriteConfig.Builder configBuilder = getConfigBuilder(true, false, HoodieIndex.IndexType.SIMPLE, smallFileSize, clusteringConfig)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .withEmbeddedTimelineServerEnabled(false);
    if (recordType == HoodieRecordType.SPARK) {
      configBuilder.withRecordMergeImplClasses(DefaultSparkRecordMerger.class.getName());
    }
    HoodieWriteConfig config = configBuilder.build();
    // The merge reads and writes records of the given type
    assertEquals(recordType, HoodieSparkTable.create(config, context(), metaClient).getRecordContextForWrite().getEngineRecordType());
    return config;
  }

  @Test
  void testUpsertKeepsRowLineageOfUnchangedRecords() throws IOException {
    HoodieRecordType recordType = getRecordType();
    HoodieTableMetaClient metaClient = initTable();
    // The table format keeps the row lineage it assigns by the base path of the meta client
    String tableBasePath = metaClient.getBasePath().toString();
    HoodieWriteConfig config = buildConfig(metaClient, recordType, 1024 * 1024 * 1024L, HoodieClusteringConfig.newBuilder().build());
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {PARTITION});
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      List<HoodieRecord> inserts = dataGen.generateInsertsForPartition("001", 20, PARTITION);
      write(client, toRecordType(inserts, recordType));
      HoodieBaseFile baseFile1 = getOnlyLatestBaseFile(metaClient);
      assertFalse(hasRowLineageColumns(storageConf(), baseFile1));
      Map<String, Pair<Long, Long>> rowLineage1 = deriveRowLineage(storageConf(), tableBasePath, baseFile1);
      assertEquals(keys(inserts), rowLineage1.keySet());

      // The rewrite updates and deletes records in the middle of the base file, and adds new records
      List<HoodieRecord> updates = Arrays.asList(inserts.get(1), inserts.get(7), inserts.get(13));
      List<HoodieRecord> deletes = Arrays.asList(inserts.get(4), inserts.get(10));
      List<HoodieRecord> newInserts = dataGen.generateInsertsForPartition("002", 4, PARTITION);
      List<HoodieRecord> records = new ArrayList<>(dataGen.generateUpdates("002", updates));
      records.addAll(dataGen.generateDeletesFromExistingRecords(deletes));
      records.addAll(newInserts);
      write(client, toRecordType(records, recordType));
      HoodieBaseFile baseFile2 = getOnlyLatestBaseFile(metaClient);
      assertEquals(baseFile1.getFileId(), baseFile2.getFileId());
      assertNotEquals(baseFile1.getFileName(), baseFile2.getFileName());

      Map<String, Pair<Long, Long>> expectedStored = new LinkedHashMap<>(rowLineage1);
      expectedStored.keySet().removeAll(keys(deletes));
      keys(updates).forEach(key -> expectedStored.put(key, storedOnUpdate(rowLineage1.get(key))));
      keys(newInserts).forEach(key -> expectedStored.put(key, NO_ROW_LINEAGE));
      assertEquals(expectedStored, readStoredRowLineage(storageConf(), baseFile2));
      Map<String, Pair<Long, Long>> rowLineage2 = deriveRowLineage(storageConf(), tableBasePath, baseFile2);
      assertNewRowLineage(tableBasePath, baseFile2, rowLineage2, keys(newInserts), rowLineage1.values());
      assertUpdatedRowLineage(tableBasePath, baseFile2, rowLineage2, keys(updates), rowLineage1);
      assertRowLineageChangesWithCommitSeqNo(storageConf(), tableBasePath, baseFile1, baseFile2, 15, 3);
    }
  }

  /**
   * An insert of new records only into the existing small base file goes through the concat handle, which carries
   * the records of the base file over unchanged and appends the new ones.
   */
  @Test
  void testInsertIntoSmallFileKeepsRowLineageOfExistingRecords() throws IOException {
    HoodieRecordType recordType = getRecordType();
    HoodieTableMetaClient metaClient = initTable();
    String tableBasePath = metaClient.getBasePath().toString();
    HoodieWriteConfig config = buildConfig(metaClient, recordType, 1024 * 1024 * 1024L, HoodieClusteringConfig.newBuilder().build());
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {PARTITION});
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      List<HoodieRecord> inserts = dataGen.generateInsertsForPartition("001", 10, PARTITION);
      write(client, toRecordType(inserts, recordType), false);
      HoodieBaseFile baseFile1 = getOnlyLatestBaseFile(metaClient);
      Map<String, Pair<Long, Long>> rowLineage1 = deriveRowLineage(storageConf(), tableBasePath, baseFile1);

      List<HoodieRecord> newInserts = dataGen.generateInsertsForPartition("002", 5, PARTITION);
      write(client, toRecordType(newInserts, recordType), false);
      HoodieBaseFile baseFile2 = getOnlyLatestBaseFile(metaClient);
      assertEquals(baseFile1.getFileId(), baseFile2.getFileId());
      assertNotEquals(baseFile1.getFileName(), baseFile2.getFileName());

      Map<String, Pair<Long, Long>> expectedStored = new LinkedHashMap<>(rowLineage1);
      keys(newInserts).forEach(key -> expectedStored.put(key, NO_ROW_LINEAGE));
      assertEquals(expectedStored, readStoredRowLineage(storageConf(), baseFile2));
      assertNewRowLineage(tableBasePath, baseFile2, deriveRowLineage(storageConf(), tableBasePath, baseFile2), keys(newInserts), rowLineage1.values());
      assertRowLineageChangesWithCommitSeqNo(storageConf(), tableBasePath, baseFile1, baseFile2, 10, 0);
    }
  }

  protected void write(SparkRDDWriteClient client, List<HoodieRecord> records) {
    write(client, records, true);
  }

  protected void write(SparkRDDWriteClient client, List<HoodieRecord> records, boolean upsert) {
    String instantTime = client.startCommit();
    JavaRDD<HoodieRecord> recordRDD = jsc().parallelize(records, 1);
    List<WriteStatus> statuses = ((JavaRDD<WriteStatus>) (upsert ? client.upsert(recordRDD, instantTime) : client.insert(recordRDD, instantTime))).collect();
    assertNoWriteErrors(statuses);
    assertTrue(client.commit(instantTime, jsc().parallelize(statuses, 1)));
  }

  protected static List<HoodieRecord> toRecordType(List<HoodieRecord> records, HoodieRecordType recordType) {
    if (recordType == HoodieRecordType.AVRO) {
      return records;
    }
    HoodieSchema schema = HoodieSchema.parse(TRIP_EXAMPLE_SCHEMA);
    StructType structType = HoodieInternalRowUtils.getCachedSchema(schema);
    scala.Function1<GenericRecord, scala.Option<InternalRow>> converter = AvroConversionUtils.createAvroToInternalRowConverter(schema, structType);
    return records.stream().map(record -> record instanceof HoodieEmptyRecord
        ? new HoodieEmptyRecord<>(record.getKey(), HoodieRecordType.SPARK)
        : new HoodieSparkRecord(record.getKey(), converter.apply((GenericRecord) ((SerializableIndexedRecord) record.getData()).getData()).get().copy(),
            structType, false)).collect(Collectors.toList());
  }

  protected static Set<String> keys(List<HoodieRecord> records) {
    return records.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toSet());
  }

  protected HoodieBaseFile getOnlyLatestBaseFile(HoodieTableMetaClient metaClient) {
    List<HoodieBaseFile> baseFiles = getLatestBaseFiles(metaClient);
    assertEquals(1, baseFiles.size(), "The records of every write are expected in one file group");
    return baseFiles.get(0);
  }

  protected List<HoodieBaseFile> getLatestBaseFiles(HoodieTableMetaClient metaClient) {
    HoodieTableFileSystemView fsView = HoodieTableFileSystemView.fileListingBasedFileSystemView(
        context(), metaClient, metaClient.reloadActiveTimeline().filterCompletedInstants());
    try {
      return fsView.getLatestFileSlices(PARTITION).map(fileSlice -> fileSlice.getBaseFile().get()).collect(Collectors.toList());
    } finally {
      fsView.close();
    }
  }
}
