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
import org.apache.hudi.table.HoodieTable;
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
import static org.junit.jupiter.api.Assertions.assertEquals;
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

  private WriteStatus prepareFirstCommitData(List<String> recordsStrs) throws IOException {
    // Create a bunch of records with a old version of schema
    final HoodieWriteConfig config = makeHoodieClientConfig("/exampleSchema.txt");
    final HoodieTable<?> table = HoodieTable.create(config, hadoopConf);
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

  @Test
  public void testSchemaEvolutionOnUpdateSuccessWithAddColumnHaveDefault() throws Exception {
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

    // Now try an update with an evolved schema
    // Evolved schema does not have guarantee on preserving the original field ordering
    final HoodieWriteConfig config2 = makeHoodieClientConfig("/exampleEvolvedSchema.txt");
    final WriteStatus insertResult = prepareFirstCommitData(recordsStrs);
    String fileId = insertResult.getFileId();

    final HoodieTable table2 = HoodieTable.create(config2, hadoopConf);
    assertEquals(1, jsc.parallelize(Arrays.asList(1)).map(x -> {
      // New content with values for the newly added field
      String recordStr4 = "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
          + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12,\"added_field\":1}";
      List<HoodieRecord> updateRecords = new ArrayList<>();
      RawTripTestPayload rowChange1 = new RawTripTestPayload(recordStr4);
      HoodieRecord record1 =
          new HoodieRecord(new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath()), rowChange1);
      record1.unseal();
      record1.setCurrentLocation(new HoodieRecordLocation("101", fileId));
      record1.seal();
      updateRecords.add(record1);
      assertDoesNotThrow(() -> {
        HoodieMergeHandle mergeHandle = new HoodieMergeHandle(config2, "101", table2,
            updateRecords.iterator(), record1.getPartitionPath(), fileId, supplier);
        Configuration conf = new Configuration();
        AvroReadSupport.setAvroReadSchema(conf, mergeHandle.getWriterSchemaWithMetafields());
        List<GenericRecord> oldRecords = ParquetUtils.readAvroRecords(conf,
            new Path(config2.getBasePath() + "/" + insertResult.getStat().getPath()));
        for (GenericRecord rec : oldRecords) {
          mergeHandle.write(rec);
        }
        mergeHandle.close();
      }, "UpdateFunction could not read records written with exampleSchema.txt using the "
          + "exampleEvolvedSchema.txt");
      return 1;
    }).collect().size());
  }

  @Test
  public void testSchemaEvolutionOnUpdateSuccessWithChangeColumnOrder() throws Exception {
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

    // Now try an update with an evolved schema
    // Evolved schema does not have guarantee on preserving the original field ordering
    final HoodieWriteConfig config2 = makeHoodieClientConfig("/exampleEvolvedSchemaChangeOrder.txt");
    final WriteStatus insertResult = prepareFirstCommitData(recordsStrs);
    String fileId = insertResult.getFileId();

    final HoodieTable table2 = HoodieTable.create(config2, hadoopConf);
    assertEquals(1, jsc.parallelize(Arrays.asList(1)).map(x -> {
      // New content with values for the newly added field
      String recordStr4 = "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
          + "\"time\":\"2016-01-31T03:16:41.415Z\",\"added_field\":1},\"number\":12";
      List<HoodieRecord> updateRecords = new ArrayList<>();
      RawTripTestPayload rowChange1 = new RawTripTestPayload(recordStr4);
      HoodieRecord record1 =
          new HoodieRecord(new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath()), rowChange1);
      record1.unseal();
      record1.setCurrentLocation(new HoodieRecordLocation("101", fileId));
      record1.seal();
      updateRecords.add(record1);
      assertDoesNotThrow(() -> {
        HoodieMergeHandle mergeHandle = new HoodieMergeHandle(config2, "101", table2,
            updateRecords.iterator(), record1.getPartitionPath(), fileId, supplier);
        Configuration conf = new Configuration();
        AvroReadSupport.setAvroReadSchema(conf, mergeHandle.getWriterSchemaWithMetafields());
        List<GenericRecord> oldRecords = ParquetUtils.readAvroRecords(conf,
            new Path(config2.getBasePath() + "/" + insertResult.getStat().getPath()));
        for (GenericRecord rec : oldRecords) {
          mergeHandle.write(rec);
        }
        mergeHandle.close();
      }, "UpdateFunction could not read records written with exampleSchema.txt using the "
          + "exampleEvolvedSchemaChangeOrder.txt as column order change");
      return 1;
    }).collect().size());
  }

  @Test
  public void testSchemaEvolutionOnUpdateMisMatchWithDeleteColumn() throws Exception {
    List<String> recordsStrs = new ArrayList<>();
    String recordStr3 = "{\"_row_key\":\"8eb5b87c-1fej-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":15}";
    recordsStrs.add(recordStr3);

    // Now try an update with an evolved schema
    // Evolved schema does not have guarantee on preserving the original field ordering
    final HoodieWriteConfig config2 = makeHoodieClientConfig("/exampleEvolvedSchemaDeleteColumn.txt");
    final WriteStatus insertResult = prepareFirstCommitData(recordsStrs);
    String fileId = insertResult.getFileId();

    final HoodieTable table2 = HoodieTable.create(config2, hadoopConf);
    assertEquals(1, jsc.parallelize(Arrays.asList(1)).map(x -> {
      // New content with values for the newly added field
      String recordStr4 = "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
          + "\"time\":\"2016-01-31T03:16:41.415Z\"}";
      List<HoodieRecord> updateRecords = new ArrayList<>();
      RawTripTestPayload rowChange1 = new RawTripTestPayload(recordStr4);
      HoodieRecord record1 =
          new HoodieRecord(new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath()), rowChange1);
      record1.unseal();
      record1.setCurrentLocation(new HoodieRecordLocation("101", fileId));
      record1.seal();
      updateRecords.add(record1);

      HoodieMergeHandle mergeHandle = new HoodieMergeHandle(config2, "101", table2,
          updateRecords.iterator(), record1.getPartitionPath(), fileId, supplier);
      Configuration conf = new Configuration();
      AvroReadSupport.setAvroReadSchema(conf, mergeHandle.getWriterSchemaWithMetafields());
      assertThrows(InvalidRecordException.class, () -> {
        List<GenericRecord> oldRecords = ParquetUtils.readAvroRecords(conf,
            new Path(config2.getBasePath() + "/" + insertResult.getStat().getPath()));
      }, "UpdateFunction when delete column ,Parquet/Avro schema mismatch: Avro field 'xxx' not found");
      mergeHandle.close();
      return 1;
    }).collect().size());
  }

  @Test
  public void testSchemaEvolutionOnUpdateMisMatchWithAddColumnNotHaveDefault() throws Exception {
    List<String> recordsStrs = new ArrayList<>();
    String recordStr3 = "{\"_row_key\":\"8eb5b87c-1fej-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":15}";
    recordsStrs.add(recordStr3);

    // Now try an update with an evolved schema
    // Evolved schema does not have guarantee on preserving the original field ordering
    final HoodieWriteConfig config2 = makeHoodieClientConfig("/exampleEvolvedSchemaColumnRequire.txt");
    final WriteStatus insertResult = prepareFirstCommitData(recordsStrs);
    String fileId = insertResult.getFileId();

    final HoodieTable table2 = HoodieTable.create(config2, hadoopConf);
    assertEquals(1, jsc.parallelize(Arrays.asList(1)).map(x -> {
      // New content with values for the newly added field
      String recordStr4 = "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
          + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12,\"added_field\":1}";
      List<HoodieRecord> updateRecords = new ArrayList<>();
      RawTripTestPayload rowChange1 = new RawTripTestPayload(recordStr4);
      HoodieRecord record1 =
          new HoodieRecord(new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath()), rowChange1);
      record1.unseal();
      record1.setCurrentLocation(new HoodieRecordLocation("101", fileId));
      record1.seal();
      updateRecords.add(record1);
      assertThrows(HoodieUpsertException.class, () -> {
        HoodieMergeHandle mergeHandle = new HoodieMergeHandle(config2, "101", table2,
            updateRecords.iterator(), record1.getPartitionPath(), fileId, supplier);
        Configuration conf = new Configuration();
        AvroReadSupport.setAvroReadSchema(conf, mergeHandle.getWriterSchemaWithMetafields());
        List<GenericRecord> oldRecords = ParquetUtils.readAvroRecords(conf,
            new Path(config2.getBasePath() + "/" + insertResult.getStat().getPath()));
        for (GenericRecord rec : oldRecords) {
          mergeHandle.write(rec);
        }
        mergeHandle.close();
      }, "UpdateFunction could not read records written with exampleSchema.txt using the "
          + "exampleEvolvedSchemaColumnRequire.txt ,because oldrecords do not have required column added_field");
      return 1;
    }).collect().size());
  }

  @Test
  public void testSchemaEvolutionOnUpdateMisMatchWithChangeColumnType() throws Exception {
    List<String> recordsStrs = new ArrayList<>();
    String recordStr3 = "{\"_row_key\":\"8eb5b87c-1fej-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":15}";
    recordsStrs.add(recordStr3);

    // Now try an update with an evolved schema
    // Evolved schema does not have guarantee on preserving the original field ordering
    final HoodieWriteConfig config2 = makeHoodieClientConfig("/exampleEvolvedSchemaColumnType.txt");
    final WriteStatus insertResult = prepareFirstCommitData(recordsStrs);
    String fileId = insertResult.getFileId();

    final HoodieTable table2 = HoodieTable.create(config2, hadoopConf);
    assertEquals(1, jsc.parallelize(Arrays.asList(1)).map(x -> {
      // New content with values for the newly added field
      String recordStr4 = "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
          + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":\"12\"}";
      List<HoodieRecord> updateRecords = new ArrayList<>();
      RawTripTestPayload rowChange1 = new RawTripTestPayload(recordStr4);
      HoodieRecord record1 =
          new HoodieRecord(new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath()), rowChange1);
      record1.unseal();
      record1.setCurrentLocation(new HoodieRecordLocation("101", fileId));
      record1.seal();
      updateRecords.add(record1);
      HoodieMergeHandle mergeHandle = new HoodieMergeHandle(config2, "101", table2,
          updateRecords.iterator(), record1.getPartitionPath(), fileId, supplier);
      Configuration conf = new Configuration();
      AvroReadSupport.setAvroReadSchema(conf, mergeHandle.getWriterSchemaWithMetafields());
      assertThrows(ParquetDecodingException.class, () -> {
        List<GenericRecord> oldRecords = ParquetUtils.readAvroRecords(conf,
            new Path(config2.getBasePath() + "/" + insertResult.getStat().getPath()));
      }, "UpdateFunction when change column type ,org.apache.parquet.avro.AvroConverters$FieldUTF8Converter");
      mergeHandle.close();
      return 1;
    }).collect().size());
  }

  private HoodieWriteConfig makeHoodieClientConfig(String name) {
    Schema schema = getSchemaFromResource(getClass(), name);
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(schema.toString()).build();
  }
}
