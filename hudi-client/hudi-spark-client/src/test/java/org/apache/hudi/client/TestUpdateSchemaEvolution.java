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

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.common.util.BaseFileUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.io.HoodieCreateHandle;
import org.apache.hudi.io.HoodieMergeHandle;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.testutils.HoodieClientTestHarness;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.io.ParquetDecodingException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestUpdateSchemaEvolution extends HoodieClientTestHarness {

  @BeforeEach
  public void setUp() throws Exception {
    initPath();
    HoodieTestUtils.init(HoodieTestUtils.getDefaultHadoopConf(), basePath);
    initSparkContexts("TestUpdateSchemaEvolution");
    initFileSystem();
    initTimelineService();
  }

  @AfterEach
  public void tearDown() throws IOException {
    cleanupResources();
  }

  private WriteStatus prepareFirstRecordCommit(List<String> recordsStrs) throws IOException {
    // Create a bunch of records with an old version of schema
    final HoodieWriteConfig config = makeHoodieClientConfig("/exampleSchema.avsc");
    final HoodieSparkTable table = HoodieSparkTable.create(config, context);
    final List<WriteStatus> statuses = jsc.parallelize(Arrays.asList(1)).map(x -> {
      List<HoodieRecord> insertRecords = new ArrayList<>();
      for (String recordStr : recordsStrs) {
        RawTripTestPayload rowChange = new RawTripTestPayload(recordStr);
        insertRecords
            .add(new HoodieRecord(new HoodieKey(rowChange.getRowKey(), rowChange.getPartitionPath()), rowChange));
      }
      Map<String, HoodieRecord> insertRecordMap = insertRecords.stream()
          .collect(Collectors.toMap(r -> r.getRecordKey(), Function.identity()));
      HoodieCreateHandle<?,?,?,?> createHandle =
          new HoodieCreateHandle(config, "100", table, insertRecords.get(0).getPartitionPath(), "f1-0", insertRecordMap, supplier);
      createHandle.write();
      return createHandle.close().get(0);
    }).collect();

    final Path commitFile = new Path(config.getBasePath() + "/.hoodie/" + HoodieTimeline.makeCommitFileName("100"));
    FSUtils.getFs(basePath, HoodieTestUtils.getDefaultHadoopConf()).create(commitFile);
    return statuses.get(0);
  }

  private List<String> generateMultipleRecordsForExampleSchema() {
    List<String> recordsStrs = new ArrayList<>();
    String recordStr1 = "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}";
    String recordStr2 = "{\"_row_key\":\"8eb5b87b-1feu-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:20:41.415Z\",\"number\":100}";
    String recordStr3 = "{\"_row_key\":\"8eb5b87c-1fej-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":15}";
    recordsStrs.add(recordStr1);
    recordsStrs.add(recordStr2);
    recordsStrs.add(recordStr3);
    return recordsStrs;
  }

  private List<String> generateOneRecordForExampleSchema() {
    List<String> recordsStrs = new ArrayList<>();
    String recordStr = "{\"_row_key\":\"8eb5b87c-1fej-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":15}";
    recordsStrs.add(recordStr);
    return recordsStrs;
  }

  private void assertSchemaEvolutionOnUpdateResult(WriteStatus insertResult, HoodieSparkTable updateTable,
                                                   List<HoodieRecord> updateRecords, String assertMsg, boolean isAssertThrow, Class expectedExceptionType) {
    jsc.parallelize(Arrays.asList(1)).map(x -> {
      Executable executable = () -> {
        HoodieMergeHandle mergeHandle = new HoodieMergeHandle(updateTable.getConfig(), "101", updateTable,
            updateRecords.iterator(), updateRecords.get(0).getPartitionPath(), insertResult.getFileId(), supplier, Option.empty());
        List<GenericRecord> oldRecords = BaseFileUtils.getInstance(updateTable.getBaseFileFormat())
            .readAvroRecords(updateTable.getHadoopConf(),
                new Path(updateTable.getConfig().getBasePath() + "/" + insertResult.getStat().getPath()),
                mergeHandle.getWriterSchemaWithMetaFields());
        for (GenericRecord rec : oldRecords) {
          mergeHandle.write(rec);
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

  private List<HoodieRecord> buildUpdateRecords(String recordStr, String insertFileId) throws IOException {
    List<HoodieRecord> updateRecords = new ArrayList<>();
    RawTripTestPayload rowChange = new RawTripTestPayload(recordStr);
    HoodieRecord record =
        new HoodieRecord(new HoodieKey(rowChange.getRowKey(), rowChange.getPartitionPath()), rowChange);
    record.setCurrentLocation(new HoodieRecordLocation("101", insertFileId));
    record.seal();
    updateRecords.add(record);
    return updateRecords;
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
    List<HoodieRecord> updateRecords = buildUpdateRecords(recordStr, insertResult.getFileId());
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
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"added_field\":1},\"number\":12";
    List<HoodieRecord> updateRecords = buildUpdateRecords(recordStr, insertResult.getFileId());
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
    List<HoodieRecord> updateRecords = buildUpdateRecords(recordStr, insertResult.getFileId());
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
    List<HoodieRecord> updateRecords = buildUpdateRecords(recordStr, insertResult.getFileId());
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
    List<HoodieRecord> updateRecords = buildUpdateRecords(recordStr, insertResult.getFileId());
    String assertMsg = "UpdateFunction when change column type, org.apache.parquet.avro.AvroConverters$FieldUTF8Converter";
    assertSchemaEvolutionOnUpdateResult(insertResult, table, updateRecords, assertMsg, true, ParquetDecodingException.class);
  }

  private HoodieWriteConfig makeHoodieClientConfig(String name) {
    Schema schema = getSchemaFromResource(getClass(), name);
    return HoodieWriteConfig.newBuilder().withPath(basePath)
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withRemoteServerPort(timelineServicePort).build())
        .withSchema(schema.toString()).build();
  }
}
