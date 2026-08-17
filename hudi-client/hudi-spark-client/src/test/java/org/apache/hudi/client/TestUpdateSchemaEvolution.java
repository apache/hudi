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

package org.apache.hudi.client;

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.schema.internal.InternalSchema;
import org.apache.hudi.common.schema.internal.action.TableChanges;
import org.apache.hudi.common.schema.internal.convert.InternalSchemaConverter;
import org.apache.hudi.common.schema.internal.utils.SchemaChangeUtils;
import org.apache.hudi.common.schema.internal.utils.SerDeHelper;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.FileCreateUtilsLegacy;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.InProcessTimeGenerator;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.io.storage.HoodieIOFactory;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.io.CreateHandleFactory;
import org.apache.hudi.io.HoodieWriteHandle;
import org.apache.hudi.io.HoodieWriteMergeHandle;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.io.ParquetDecodingException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestUtils.COMMIT_METADATA_SER_DE;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_FILE_NAME_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.createSimpleRecord;
import static org.apache.hudi.common.testutils.HoodieTestUtils.extractPartitionFromTimeField;
import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestUpdateSchemaEvolution extends HoodieSparkClientTestHarness implements Serializable {

  @BeforeEach
  public void setUp() throws Exception {
    initPath();
    HoodieTestUtils.init(HoodieTestUtils.getDefaultStorageConf(), basePath);
    initSparkContexts("TestUpdateSchemaEvolution");
    initHoodieStorage();
    initTimelineService();
  }

  @AfterEach
  public void tearDown() throws IOException {
    cleanupResources();
  }

  private WriteStatus prepareFirstRecordCommit(List<HoodieRecord> insertRecords) throws IOException {
    return prepareFirstRecordCommit(insertRecords, Option.empty(), false);
  }

  /**
   * Writes the first commit. When {@code writeCommitMetadata} is set the commit file holds the schema the
   * records were written with, and {@code internalSchema}, if given, is recorded as the latest schema of that
   * commit. That is how {@code HoodieMergeHelper} resolves the internal schema of the base file being merged.
   */
  private WriteStatus prepareFirstRecordCommit(List<HoodieRecord> insertRecords,
                                               Option<InternalSchema> internalSchema,
                                               boolean writeCommitMetadata) throws IOException {
    // Create a bunch of records with an old version of schema
    final HoodieWriteConfig config = makeHoodieClientConfig("/exampleSchema.avsc");
    final HoodieSparkTable table = HoodieSparkTable.create(config, context);
    final List<WriteStatus> statuses = jsc.parallelize(Arrays.asList(1)).map(x -> {
      Map<String, HoodieRecord> insertRecordMap = insertRecords.stream()
          .collect(Collectors.toMap(r -> r.getRecordKey(), Function.identity()));
      HoodieWriteHandle<?,?,?,?> createHandle = new CreateHandleFactory<>(false)
          .create(config, "100", table, insertRecords.get(0).getPartitionPath(), "f1-0", supplier);
      for (HoodieRecord record : insertRecordMap.values()) {
        createHandle.write(record, createHandle.getWriterSchema(), createHandle.getConfig().getProps());
      }
      return createHandle.close().get(0);
    }).collect();

    if (writeCommitMetadata) {
      HoodieCommitMetadata commitMetadata = CommitUtils.buildMetadata(Collections.emptyList(), Collections.emptyMap(),
          Option.empty(), WriteOperationType.INSERT, config.getSchema(), HoodieTimeline.COMMIT_ACTION);
      if (internalSchema.isPresent()) {
        commitMetadata.addMetadata(SerDeHelper.LATEST_SCHEMA, SerDeHelper.toJson(internalSchema.get()));
      }
      FileCreateUtilsLegacy.createCommit(COMMIT_METADATA_SER_DE, basePath, "100", Option.of(commitMetadata));
    } else {
      final Path commitFile = new Path(config.getBasePath() + "/.hoodie/timeline/"
          + INSTANT_FILE_NAME_GENERATOR.makeCommitFileName("100" + "_" + InProcessTimeGenerator.createNewInstantTime()));
      HadoopFSUtils.getFs(basePath, HoodieTestUtils.getDefaultStorageConf()).create(commitFile);
    }
    return statuses.get(0);
  }

  private List<HoodieRecord> generateMultipleRecordsForExampleSchema() {
    List<HoodieRecord> records = new ArrayList<>();
    records.add(createSimpleRecord("8eb5b87a-1feh-4edd-87b4-6ec96dc405a0", "2016-01-31T03:16:41.415Z", 12));
    records.add(createSimpleRecord("8eb5b87b-1feu-4edd-87b4-6ec96dc405a0", "2016-01-31T03:20:41.415Z", 100));
    records.add(createSimpleRecord("8eb5b87c-1fej-4edd-87b4-6ec96dc405a0", "2016-01-31T03:16:41.415Z", 15));
    return records;
  }

  private List<HoodieRecord> generateOneRecordForExampleSchema() {
    return Collections.singletonList(createSimpleRecord("8eb5b87c-1fej-4edd-87b4-6ec96dc405a0", "2016-01-31T03:16:41.415Z", 15));
  }

  private void assertSchemaEvolutionOnUpdateResult(WriteStatus insertResult, HoodieSparkTable updateTable,
                                                   List<HoodieRecord> updateRecords, String assertMsg, boolean isAssertThrow, Class expectedExceptionType) {
    jsc.parallelize(Arrays.asList(1)).map(x -> {
      Executable executable = () -> {
        HoodieWriteMergeHandle mergeHandle = new HoodieWriteMergeHandle(updateTable.getConfig(), "101", updateTable,
            updateRecords.iterator(), updateRecords.get(0).getPartitionPath(), insertResult.getFileId(), supplier, Option.empty());
        List<GenericRecord> oldRecords = HoodieIOFactory.getIOFactory(updateTable.getStorage())
            .getFileFormatUtils(updateTable.getBaseFileFormat())
            .readAvroRecords(updateTable.getStorage(),
                new StoragePath(updateTable.getConfig().getBasePath() + "/" + insertResult.getStat().getPath()),
                mergeHandle.getWriterSchemaWithMetaFields());
        for (GenericRecord rec : oldRecords) {
          // TODO create hoodie record with rec can getRecordKey
          mergeHandle.write(new HoodieAvroIndexedRecord(rec));
        }
        mergeHandle.close();
      };
      if (isAssertThrow) {
        assertThrows(expectedExceptionType, executable, assertMsg);
      } else {
        assertDoesNotThrow(executable, assertMsg);
      }
      return 1;
    }).collect();
  }

  private List<HoodieRecord> buildUpdateRecords(String recordStr, String insertFileId, String schema) throws IOException {
    HoodieSchema hoodieSchema = HoodieSchema.parse(schema);
    GenericRecord data = new GenericData.Record(hoodieSchema.getAvroSchema());
    Map<String, Object> json = JsonUtils.getObjectMapper().readValue(recordStr, Map.class);
    json.forEach(data::put);
    String key = json.get("_row_key").toString();
    String partition = extractPartitionFromTimeField(json.get("time").toString());
    HoodieRecord record = new HoodieAvroRecord<>(new HoodieKey(key, partition), new HoodieAvroPayload(Option.of(data)));
    record.setCurrentLocation(new HoodieRecordLocation("101", insertFileId));
    record.seal();
    return Collections.singletonList(record);
  }

  @Test
  public void testSchemaEvolutionOnUpdateSuccessWithAddColumnHaveDefault() throws Exception {
    final WriteStatus insertResult = prepareFirstRecordCommit(generateMultipleRecordsForExampleSchema());
    // Now try an update with an evolved schema
    // Evolved schema does not have guarantee on preserving the original field ordering
    final HoodieWriteConfig config = makeHoodieClientConfig("/exampleEvolvedSchema.avsc");
    final HoodieSparkTable table = HoodieSparkTable.create(config, context);
    // New content with values for the newly added field
    String recordStr = "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12,\"added_field\":1}";
    List<HoodieRecord> updateRecords = buildUpdateRecords(recordStr, insertResult.getFileId(), config.getSchema());
    String assertMsg = "UpdateFunction could not read records written with exampleSchema.avsc using the "
        + "exampleEvolvedSchema.avsc";
    assertSchemaEvolutionOnUpdateResult(insertResult, table, updateRecords, assertMsg, false, null);
  }

  @Test
  public void testSchemaEvolutionOnUpdateSuccessWithChangeColumnOrder() throws Exception {
    final WriteStatus insertResult = prepareFirstRecordCommit(generateMultipleRecordsForExampleSchema());
    // Now try an update with an evolved schema
    // Evolved schema does not have guarantee on preserving the original field ordering
    final HoodieWriteConfig config = makeHoodieClientConfig("/exampleEvolvedSchemaChangeOrder.avsc");
    final HoodieSparkTable table = HoodieSparkTable.create(config, context);
    String recordStr = "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"added_field\":1,\"number\":12}";
    List<HoodieRecord> updateRecords = buildUpdateRecords(recordStr, insertResult.getFileId(), config.getSchema());
    String assertMsg = "UpdateFunction could not read records written with exampleSchema.avsc using the "
        + "exampleEvolvedSchemaChangeOrder.avsc as column order change";
    assertSchemaEvolutionOnUpdateResult(insertResult, table, updateRecords, assertMsg, false, null);
  }

  @Test
  public void testSchemaEvolutionOnUpdateMisMatchWithDeleteColumn() throws Exception {
    final WriteStatus insertResult = prepareFirstRecordCommit(generateOneRecordForExampleSchema());
    // Now try an update with an evolved schema
    // Evolved schema does not have guarantee on preserving the original field ordering
    final HoodieWriteConfig config = makeHoodieClientConfig("/exampleEvolvedSchemaDeleteColumn.avsc");
    final HoodieSparkTable table = HoodieSparkTable.create(config, context);
    String recordStr = "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\"}";
    List<HoodieRecord> updateRecords = buildUpdateRecords(recordStr, insertResult.getFileId(), config.getSchema());
    String assertMsg = "UpdateFunction when delete column, Parquet/Avro schema mismatch: Avro field 'xxx' not found";
    assertSchemaEvolutionOnUpdateResult(insertResult, table, updateRecords, assertMsg, true, InvalidRecordException.class);
  }

  @Test
  public void testSchemaEvolutionOnUpdateMisMatchWithAddColumnNotHaveDefault() throws Exception {
    final WriteStatus insertResult = prepareFirstRecordCommit(generateOneRecordForExampleSchema());
    // Now try an update with an evolved schema
    // Evolved schema does not have guarantee on preserving the original field ordering
    final HoodieWriteConfig config = makeHoodieClientConfig("/exampleEvolvedSchemaColumnRequire.avsc");
    final HoodieSparkTable table = HoodieSparkTable.create(config, context);
    String recordStr = "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12,\"added_field\":1}";
    List<HoodieRecord> updateRecords = buildUpdateRecords(recordStr, insertResult.getFileId(), config.getSchema());
    String assertMsg = "UpdateFunction could not read records written with exampleSchema.avsc using the "
        + "exampleEvolvedSchemaColumnRequire.avsc, because old records do not have required column added_field";
    assertSchemaEvolutionOnUpdateResult(insertResult, table, updateRecords, assertMsg, true, HoodieUpsertException.class);
  }

  @Test
  public void testSchemaEvolutionOnUpdateMisMatchWithChangeColumnType() throws Exception {
    final WriteStatus insertResult = prepareFirstRecordCommit(generateOneRecordForExampleSchema());
    // Now try an update with an evolved schema
    // Evolved schema does not have guarantee on preserving the original field ordering
    final HoodieWriteConfig config = makeHoodieClientConfig("/exampleEvolvedSchemaColumnType.avsc");
    final HoodieSparkTable table = HoodieSparkTable.create(config, context);
    String recordStr = "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":\"12\"}";
    List<HoodieRecord> updateRecords = buildUpdateRecords(recordStr, insertResult.getFileId(), config.getSchema());
    String assertMsg = "UpdateFunction when change column type, org.apache.parquet.avro.AvroConverters$FieldUTF8Converter";
    assertSchemaEvolutionOnUpdateResult(insertResult, table, updateRecords, assertMsg, true, ParquetDecodingException.class);
  }

  /**
   * When the write config carries an internal (schema-on-read) schema, {@code HoodieMergeHelper} reconciles it
   * against the internal schema of the base file and rewrites every record read out of that file. Here the
   * column {@code number} was renamed, so the values have to be carried over to the renamed column.
   */
  @Test
  public void testMergeHelperRewritesRecordsOnRenamedColumn() throws Exception {
    HoodieSchema fileSchema = HoodieSchemaUtils.addMetadataFields(getSchemaFromResource(getClass(), "/exampleSchema.avsc"));
    InternalSchema fileInternalSchema = InternalSchemaConverter.convert(fileSchema).setSchemaId(100L);
    WriteStatus insertResult =
        prepareFirstRecordCommit(generateMultipleRecordsForExampleSchema(), Option.of(fileInternalSchema), true);

    // rename `number` to `numberx`, which is what `alter table rename column` leaves on the write config
    TableChanges.ColumnUpdateChange renameChange = TableChanges.ColumnUpdateChange.get(fileInternalSchema)
        .renameColumn("number", "numberx");
    InternalSchema querySchema = SchemaChangeUtils.applyTableChanges2Schema(fileInternalSchema, renameChange).setSchemaId(101L);
    HoodieSchema renamedSchema =
        HoodieSchemaUtils.removeMetadataFields(InternalSchemaConverter.convert(querySchema, fileSchema.getFullName()));

    HoodieWriteConfig config = makeHoodieClientConfig(renamedSchema, Option.of(querySchema), false);
    String recordStr = "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"numberx\":34}";
    List<GenericRecord> mergedRecords = runMerge(config, insertResult, buildUpdateRecords(recordStr, insertResult.getFileId(), config.getSchema()));

    // the two records that were not updated keep the value of the column they were written with
    assertMergedNumbers(mergedRecords, "numberx");
  }

  /**
   * The commit of the base file has no internal schema recorded, which is how a table looks when schema on
   * read is turned on after some inserts. With reconciliation enabled the merge helper falls back to the
   * table schema resolver to get one, without it there is nothing to reconcile the query schema against.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testMergeHelperWithoutInternalSchemaOnCommit(boolean reconcileSchema) throws Exception {
    // the table schema resolver needs the commit to carry the schema the records were written with
    WriteStatus insertResult =
        prepareFirstRecordCommit(generateMultipleRecordsForExampleSchema(), Option.empty(), reconcileSchema);

    HoodieSchema schema = getSchemaFromResource(getClass(), "/exampleSchema.avsc");
    InternalSchema querySchema = InternalSchemaConverter.convert(HoodieSchemaUtils.addMetadataFields(schema)).setSchemaId(101L);
    HoodieWriteConfig config = makeHoodieClientConfig(schema, Option.of(querySchema), reconcileSchema);
    String recordStr = "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":34}";
    List<GenericRecord> mergedRecords = runMerge(config, insertResult, buildUpdateRecords(recordStr, insertResult.getFileId(), config.getSchema()));

    assertMergedNumbers(mergedRecords, "number");
  }

  private void assertMergedNumbers(List<GenericRecord> mergedRecords, String numberColumn) {
    Map<String, Integer> numberByKey = new HashMap<>();
    for (GenericRecord record : mergedRecords) {
      numberByKey.put(record.get("_row_key").toString(), (Integer) record.get(numberColumn));
    }
    assertEquals(3, numberByKey.size());
    assertEquals(34, numberByKey.get("8eb5b87a-1feh-4edd-87b4-6ec96dc405a0"));
    assertEquals(100, numberByKey.get("8eb5b87b-1feu-4edd-87b4-6ec96dc405a0"));
    assertEquals(15, numberByKey.get("8eb5b87c-1fej-4edd-87b4-6ec96dc405a0"));
  }

  /**
   * Merges {@code updateRecords} into the base file written by {@code insertResult} through
   * {@code HoodieMergeHelper} and returns the records of the file it produced.
   */
  private List<GenericRecord> runMerge(HoodieWriteConfig config, WriteStatus insertResult,
                                       List<HoodieRecord> updateRecords) {
    HoodieSparkTable table = HoodieSparkTable.create(config, context);
    List<String> mergedFilePaths = jsc.parallelize(Arrays.asList(1)).map(x -> {
      HoodieWriteMergeHandle mergeHandle = new HoodieWriteMergeHandle(config, "101", table,
          updateRecords.iterator(), updateRecords.get(0).getPartitionPath(), insertResult.getFileId(), supplier, Option.empty());
      // `doMerge` is the only entry point into HoodieMergeHelper: it reads the base file and feeds the handle
      mergeHandle.doMerge();
      return ((WriteStatus) mergeHandle.close().get(0)).getStat().getPath();
    }).collect();

    return HoodieIOFactory.getIOFactory(table.getStorage())
        .getFileFormatUtils(table.getBaseFileFormat())
        .readAvroRecords(table.getStorage(), new StoragePath(config.getBasePath() + "/" + mergedFilePaths.get(0)));
  }

  private HoodieWriteConfig makeHoodieClientConfig(String name) {
    return makeHoodieClientConfig(getSchemaFromResource(getClass(), name), Option.empty(), false);
  }

  private HoodieWriteConfig makeHoodieClientConfig(HoodieSchema schema, Option<InternalSchema> internalSchema,
                                                   boolean reconcileSchema) {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withRemoteServerPort(timelineServicePort).build())
        .withProps(Collections.singletonMap(HoodieCommonConfig.RECONCILE_SCHEMA.key(), String.valueOf(reconcileSchema)))
        .withSchema(schema.toString()).build();
    if (internalSchema.isPresent()) {
      config.setInternalSchemaString(SerDeHelper.toJson(internalSchema.get()));
    }
    return config;
  }
}
