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
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.io.HoodieCreateHandle;
import org.apache.hudi.io.HoodieMergeHandle;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.testutils.HoodieClientTestHarness;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.io.ParquetDecodingException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
  }

  @AfterEach
  public void tearDown() throws IOException {
    cleanupResources();
  }

  private WriteStatus prepareFirstRecordCommit(List<String> recordsStrs) throws IOException {
    // Create a bunch of records with a old version of schema
    final HoodieWriteConfig config = makeHoodieClientConfig("/exampleSchema.txt");
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
      HoodieCreateHandle createHandle =
          new HoodieCreateHandle(config, "100", table, insertRecords.get(0).getPartitionPath(), "f1-0", insertRecordMap, supplier);
      createHandle.write();
      return createHandle.close();
    }).collect();

    final Path commitFile = new Path(config.getBasePath() + "/.hoodie/" + HoodieTimeline.makeCommitFileName("100"));
    FSUtils.getFs(basePath, HoodieTestUtils.getDefaultHadoopConf()).create(commitFile);
    return statuses.get(0);
  }

  private List<String> generateMultiRecordsForExampleSchema() {
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

  @Test
  public void testSchemaEvolutionOnUpdateSuccessWithAddColumnHaveDefault() throws Exception {
    final WriteStatus insertResult = prepareFirstRecordCommit(generateMultiRecordsForExampleSchema());
    String fileId = insertResult.getFileId();

    // Now try an update with an evolved schema
    // Evolved schema does not have guarantee on preserving the original field ordering
    final HoodieWriteConfig config = makeHoodieClientConfig("/exampleEvolvedSchema.txt");
    final HoodieSparkTable table = HoodieSparkTable.create(config, context);
    jsc.parallelize(Arrays.asList(1)).map(x -> {
      // New content with values for the newly added field
      String recordStr = "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
          + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12,\"added_field\":1}";
      List<HoodieRecord> updateRecords = new ArrayList<>();
      RawTripTestPayload rowChange = new RawTripTestPayload(recordStr);
      HoodieRecord record =
          new HoodieRecord(new HoodieKey(rowChange.getRowKey(), rowChange.getPartitionPath()), rowChange);
      record.unseal();
      record.setCurrentLocation(new HoodieRecordLocation("101", fileId));
      record.seal();
      updateRecords.add(record);
      assertDoesNotThrow(() -> {
        HoodieMergeHandle mergeHandle = new HoodieMergeHandle(config, "101", table,
            updateRecords.iterator(), record.getPartitionPath(), fileId, supplier);
        Configuration conf = new Configuration();
        AvroReadSupport.setAvroReadSchema(conf, mergeHandle.getWriterSchemaWithMetafields());
        List<GenericRecord> oldRecords = ParquetUtils.readAvroRecords(conf,
            new Path(config.getBasePath() + "/" + insertResult.getStat().getPath()));
        for (GenericRecord rec : oldRecords) {
          mergeHandle.write(rec);
        }
        mergeHandle.close();
      }, "UpdateFunction could not read records written with exampleSchema.txt using the "
          + "exampleEvolvedSchema.txt");
      return 1;
    }).collect();
  }

  @Test
  public void testSchemaEvolutionOnUpdateSuccessWithChangeColumnOrder() throws Exception {
    final WriteStatus insertResult = prepareFirstRecordCommit(generateMultiRecordsForExampleSchema());
    String fileId = insertResult.getFileId();

    // Now try an update with an evolved schema
    // Evolved schema does not have guarantee on preserving the original field ordering
    final HoodieWriteConfig config = makeHoodieClientConfig("/exampleEvolvedSchemaChangeOrder.txt");
    final HoodieSparkTable table = HoodieSparkTable.create(config, context);
    jsc.parallelize(Arrays.asList(1)).map(x -> {
      // New content with values for the newly added field
      String recordStr = "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
          + "\"time\":\"2016-01-31T03:16:41.415Z\",\"added_field\":1},\"number\":12";
      List<HoodieRecord> updateRecords = new ArrayList<>();
      RawTripTestPayload rowChange = new RawTripTestPayload(recordStr);
      HoodieRecord record =
          new HoodieRecord(new HoodieKey(rowChange.getRowKey(), rowChange.getPartitionPath()), rowChange);
      record.unseal();
      record.setCurrentLocation(new HoodieRecordLocation("101", fileId));
      record.seal();
      updateRecords.add(record);
      assertDoesNotThrow(() -> {
        HoodieMergeHandle mergeHandle = new HoodieMergeHandle(config, "101", table,
            updateRecords.iterator(), record.getPartitionPath(), fileId, supplier);
        Configuration conf = new Configuration();
        AvroReadSupport.setAvroReadSchema(conf, mergeHandle.getWriterSchemaWithMetafields());
        List<GenericRecord> oldRecords = ParquetUtils.readAvroRecords(conf,
            new Path(config.getBasePath() + "/" + insertResult.getStat().getPath()));
        for (GenericRecord rec : oldRecords) {
          mergeHandle.write(rec);
        }
        mergeHandle.close();
      }, "UpdateFunction could not read records written with exampleSchema.txt using the "
          + "exampleEvolvedSchemaChangeOrder.txt as column order change");
      return 1;
    }).collect();
  }

  @Test
  public void testSchemaEvolutionOnUpdateMisMatchWithDeleteColumn() throws Exception {
    final WriteStatus insertResult = prepareFirstRecordCommit(generateOneRecordForExampleSchema());
    String fileId = insertResult.getFileId();

    // Now try an update with an evolved schema
    // Evolved schema does not have guarantee on preserving the original field ordering
    final HoodieWriteConfig config = makeHoodieClientConfig("/exampleEvolvedSchemaDeleteColumn.txt");
    final HoodieSparkTable table = HoodieSparkTable.create(config, context);
    jsc.parallelize(Arrays.asList(1)).map(x -> {
      // New content with values for the newly added field
      String recordStr = "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
          + "\"time\":\"2016-01-31T03:16:41.415Z\"}";
      List<HoodieRecord> updateRecords = new ArrayList<>();
      RawTripTestPayload rowChange = new RawTripTestPayload(recordStr);
      HoodieRecord record =
          new HoodieRecord(new HoodieKey(rowChange.getRowKey(), rowChange.getPartitionPath()), rowChange);
      record.unseal();
      record.setCurrentLocation(new HoodieRecordLocation("101", fileId));
      record.seal();
      updateRecords.add(record);
      HoodieMergeHandle mergeHandle = new HoodieMergeHandle(config, "101", table,
          updateRecords.iterator(), record.getPartitionPath(), fileId, supplier);
      Configuration conf = new Configuration();
      AvroReadSupport.setAvroReadSchema(conf, mergeHandle.getWriterSchemaWithMetafields());
      assertThrows(InvalidRecordException.class, () -> {
        List<GenericRecord> oldRecords = ParquetUtils.readAvroRecords(conf,
            new Path(config.getBasePath() + "/" + insertResult.getStat().getPath()));
      }, "UpdateFunction when delete column ,Parquet/Avro schema mismatch: Avro field 'xxx' not found");
      mergeHandle.close();
      return 1;
    }).collect();
  }

  @Test
  public void testSchemaEvolutionOnUpdateMisMatchWithAddColumnNotHaveDefault() throws Exception {
    final WriteStatus insertResult = prepareFirstRecordCommit(generateOneRecordForExampleSchema());
    String fileId = insertResult.getFileId();

    // Now try an update with an evolved schema
    // Evolved schema does not have guarantee on preserving the original field ordering
    final HoodieWriteConfig config = makeHoodieClientConfig("/exampleEvolvedSchemaColumnRequire.txt");
    final HoodieSparkTable table = HoodieSparkTable.create(config, context);
    jsc.parallelize(Arrays.asList(1)).map(x -> {
      // New content with values for the newly added field
      String recordStr = "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
          + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12,\"added_field\":1}";
      List<HoodieRecord> updateRecords = new ArrayList<>();
      RawTripTestPayload rowChange = new RawTripTestPayload(recordStr);
      HoodieRecord record =
          new HoodieRecord(new HoodieKey(rowChange.getRowKey(), rowChange.getPartitionPath()), rowChange);
      record.unseal();
      record.setCurrentLocation(new HoodieRecordLocation("101", fileId));
      record.seal();
      updateRecords.add(record);
      assertThrows(HoodieUpsertException.class, () -> {
        HoodieMergeHandle mergeHandle = new HoodieMergeHandle(config, "101", table,
            updateRecords.iterator(), record.getPartitionPath(), fileId, supplier);
        Configuration conf = new Configuration();
        AvroReadSupport.setAvroReadSchema(conf, mergeHandle.getWriterSchemaWithMetafields());
        List<GenericRecord> oldRecords = ParquetUtils.readAvroRecords(conf,
            new Path(config.getBasePath() + "/" + insertResult.getStat().getPath()));
        for (GenericRecord rec : oldRecords) {
          mergeHandle.write(rec);
        }
        mergeHandle.close();
      }, "UpdateFunction could not read records written with exampleSchema.txt using the "
          + "exampleEvolvedSchemaColumnRequire.txt ,because oldrecords do not have required column added_field");
      return 1;
    }).collect();
  }

  @Test
  public void testSchemaEvolutionOnUpdateMisMatchWithChangeColumnType() throws Exception {
    final WriteStatus insertResult = prepareFirstRecordCommit(generateOneRecordForExampleSchema());
    String fileId = insertResult.getFileId();

    // Now try an update with an evolved schema
    // Evolved schema does not have guarantee on preserving the original field ordering
    final HoodieWriteConfig config = makeHoodieClientConfig("/exampleEvolvedSchemaColumnType.txt");
    final HoodieSparkTable table = HoodieSparkTable.create(config, context);
    jsc.parallelize(Arrays.asList(1)).map(x -> {
      // New content with values for the newly added field
      String recordStr = "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
          + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":\"12\"}";
      List<HoodieRecord> updateRecords = new ArrayList<>();
      RawTripTestPayload rowChange = new RawTripTestPayload(recordStr);
      HoodieRecord record =
          new HoodieRecord(new HoodieKey(rowChange.getRowKey(), rowChange.getPartitionPath()), rowChange);
      record.unseal();
      record.setCurrentLocation(new HoodieRecordLocation("101", fileId));
      record.seal();
      updateRecords.add(record);
      HoodieMergeHandle mergeHandle = new HoodieMergeHandle(config, "101", table,
          updateRecords.iterator(), record.getPartitionPath(), fileId, supplier);
      Configuration conf = new Configuration();
      AvroReadSupport.setAvroReadSchema(conf, mergeHandle.getWriterSchemaWithMetafields());
      assertThrows(ParquetDecodingException.class, () -> {
        List<GenericRecord> oldRecords = ParquetUtils.readAvroRecords(conf,
            new Path(config.getBasePath() + "/" + insertResult.getStat().getPath()));
      }, "UpdateFunction when change column type ,org.apache.parquet.avro.AvroConverters$FieldUTF8Converter");
      mergeHandle.close();
      return 1;
    }).collect();
  }

  private HoodieWriteConfig makeHoodieClientConfig(String name) {
    Schema schema = getSchemaFromResource(getClass(), name);
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(schema.toString()).build();
  }
}
