/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi;

import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.read.HoodiePositionBasedFileGroupRecordBuffer;
import org.apache.hudi.common.table.read.TestHoodieFileGroupReaderOnSpark;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.engine.HoodieReaderContext.INTERNAL_META_RECORD_KEY;
import static org.apache.hudi.common.model.WriteOperationType.INSERT;
import static org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodiePositionBasedFileGroupRecordBuffer extends TestHoodieFileGroupReaderOnSpark {
  private final HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(0xDEEF);
  private HoodieTableMetaClient metaClient;
  private Schema avroSchema;
  private HoodiePositionBasedFileGroupRecordBuffer<InternalRow> buffer;
  private String partitionPath;

  public void prepareBuffer(boolean useCustomMerger) throws Exception {
    Map<String, String> writeConfigs = new HashMap<>();
    writeConfigs.put(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), "parquet");
    writeConfigs.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    writeConfigs.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partition_path");
    writeConfigs.put("hoodie.datasource.write.precombine.field", "timestamp");
    writeConfigs.put("hoodie.payload.ordering.field", "timestamp");
    writeConfigs.put(HoodieTableConfig.HOODIE_TABLE_NAME_KEY, "hoodie_test");
    writeConfigs.put("hoodie.insert.shuffle.parallelism", "4");
    writeConfigs.put("hoodie.upsert.shuffle.parallelism", "4");
    writeConfigs.put("hoodie.bulkinsert.shuffle.parallelism", "2");
    writeConfigs.put("hoodie.delete.shuffle.parallelism", "1");
    writeConfigs.put("hoodie.merge.small.file.group.candidates.limit", "0");
    writeConfigs.put("hoodie.compact.inline", "false");
    commitToTable(recordsToStrings(dataGen.generateInserts("001", 100)), INSERT.value(), writeConfigs);

    String[] partitionPaths = dataGen.getPartitionPaths();
    String[] partitionValues = new String[1];
    partitionPath = partitionPaths[0];
    partitionValues[0] = partitionPath;

    metaClient = HoodieTableMetaClient.builder().setConf(getHadoopConf()).setBasePath(getBasePath()).build();
    avroSchema = new TableSchemaResolver(metaClient).getTableAvroSchema();
    Option<String[]> partitionFields = metaClient.getTableConfig().getPartitionFields();
    Option<String> partitionNameOpt = StringUtils.isNullOrEmpty(partitionPaths[0])
        ? Option.empty() : Option.of(partitionPaths[0]);

    buffer = new HoodiePositionBasedFileGroupRecordBuffer<>(
        getHoodieReaderContext(getBasePath(), partitionValues),
        avroSchema,
        avroSchema,
        partitionNameOpt,
        partitionFields,
        useCustomMerger ? new CustomMerger() : new HoodieSparkRecordMerger(),
        new TypedProperties());
  }

  public Map<HoodieLogBlock.HeaderMetadataType, String> getHeader() {
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, avroSchema.toString());
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    return header;
  }

  public List<DeleteRecord> getDeleteRecords() throws IOException, URISyntaxException {
    SchemaTestUtil testUtil = new SchemaTestUtil();
    List<IndexedRecord> records = testUtil.generateHoodieTestRecords(0, 100);

    List<DeleteRecord> deletedRecords = records.stream()
        .map(s -> (DeleteRecord.create(((GenericRecord) s).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString(),
            ((GenericRecord) s).get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString())))
        .collect(Collectors.toList()).subList(0, 50);
    return deletedRecords;
  }

  public HoodieDeleteBlock getDeleteBlockWithPositions() throws IOException, URISyntaxException {
    List<DeleteRecord> deletedRecords = getDeleteRecords();
    List<Pair<DeleteRecord, Long>> deleteRecordList = new ArrayList<>();

    long position = 0;
    for (DeleteRecord dr : deletedRecords) {
      deleteRecordList.add(Pair.of(dr, position++));
    }
    return new HoodieDeleteBlock(deleteRecordList, true, getHeader());
  }

  public HoodieDeleteBlock getDeleteBlockWithoutPositions() throws IOException, URISyntaxException {
    List<DeleteRecord> deletedRecords = getDeleteRecords();
    List<Pair<DeleteRecord, Long>> deleteRecordList = new ArrayList<>();

    for (DeleteRecord dr : deletedRecords) {
      deleteRecordList.add(Pair.of(dr, -1L));
    }
    return new HoodieDeleteBlock(deleteRecordList, true, getHeader());
  }

  @Test
  public void testProcessDeleteBlockWithPositions() throws Exception {
    prepareBuffer(false);
    HoodieDeleteBlock deleteBlock = getDeleteBlockWithPositions();
    buffer.processDeleteBlock(deleteBlock);
    assertEquals(50, buffer.getLogRecords().size());
    // With record positions, we do not need the record keys.
    assertNull(buffer.getLogRecords().get(0L).getRight().get(INTERNAL_META_RECORD_KEY));
  }

  @Test
  public void testProcessDeleteBlockWithCustomMerger() throws Exception {
    prepareBuffer(true);
    HoodieDeleteBlock deleteBlock = getDeleteBlockWithPositions();
    buffer.processDeleteBlock(deleteBlock);
    assertEquals(50, buffer.getLogRecords().size());
    assertNotNull(buffer.getLogRecords().get(0L).getRight().get(INTERNAL_META_RECORD_KEY));
  }

  @Test
  public void testProcessDeleteBlockWithoutPositions() throws Exception {
    prepareBuffer(false);
    HoodieDeleteBlock deleteBlock = getDeleteBlockWithoutPositions();
    Exception exception = assertThrows(
        HoodieValidationException.class, () -> buffer.processDeleteBlock(deleteBlock));
    assertTrue(exception.getMessage().contains("No record position info is found"));
  }

  public static class CustomMerger implements HoodieRecordMerger {
    @Override
    public String getMergingStrategy() {
      return "random_strategy";
    }

    @Override
    public Option<Pair<HoodieRecord, Schema>> merge(
        HoodieRecord older, Schema oldSchema, HoodieRecord newer, Schema newSchema, TypedProperties props
    ) throws IOException {
      throw new IOException("Not implemented");
    }

    @Override
    public HoodieRecord.HoodieRecordType getRecordType() {
      return HoodieRecord.HoodieRecordType.SPARK;
    }
  }
}

