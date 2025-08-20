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

import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.read.CustomPayloadForTesting;
import org.apache.hudi.common.table.read.FileGroupReaderSchemaHandler;
import org.apache.hudi.common.table.read.ParquetRowIndexBasedSchemaHandler;
import org.apache.hudi.common.table.read.buffer.PositionBasedFileGroupRecordBuffer;
import org.apache.hudi.common.table.read.UpdateProcessor;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.SparkColumnarFileReader;
import org.apache.spark.sql.sources.Filter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import scala.collection.JavaConverters;
import scala.collection.immutable.Map$;

import static org.apache.hudi.common.model.WriteOperationType.INSERT;
import static org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType.BASE_FILE_INSTANT_TIME_OF_RECORD_POSITIONS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

public class TestPositionBasedFileGroupRecordBuffer extends SparkClientFunctionalTestHarness {
  private final HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(0xDEEF);
  private final UpdateProcessor updateProcessor = mock(UpdateProcessor.class);
  private Schema avroSchema;
  private PositionBasedFileGroupRecordBuffer<InternalRow> buffer;

  private void prepareBuffer(RecordMergeMode mergeMode, String baseFileInstantTime) throws Exception {
    Map<String, String> writeConfigs = new HashMap<>();
    writeConfigs.put(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), "parquet");
    writeConfigs.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    writeConfigs.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partition_path");
    writeConfigs.put(HoodieTableConfig.ORDERING_FIELDS.key(), mergeMode.equals(RecordMergeMode.COMMIT_TIME_ORDERING) ? "" : "timestamp");
    writeConfigs.put("hoodie.payload.ordering.field", "timestamp");
    writeConfigs.put(HoodieTableConfig.HOODIE_TABLE_NAME_KEY, "hoodie_test");
    writeConfigs.put("hoodie.insert.shuffle.parallelism", "4");
    writeConfigs.put("hoodie.upsert.shuffle.parallelism", "4");
    writeConfigs.put("hoodie.bulkinsert.shuffle.parallelism", "2");
    writeConfigs.put("hoodie.delete.shuffle.parallelism", "1");
    writeConfigs.put("hoodie.merge.small.file.group.candidates.limit", "0");
    writeConfigs.put("hoodie.compact.inline", "false");
    writeConfigs.put(HoodieWriteConfig.WRITE_RECORD_POSITIONS.key(), "true");
    writeConfigs.put(HoodieWriteConfig.RECORD_MERGE_MODE.key(), mergeMode.name());
    if (mergeMode.equals(RecordMergeMode.CUSTOM)) {
      writeConfigs.put(HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key(), CustomPayloadForTesting.class.getName());
      writeConfigs.put(HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key(), HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID);
    }
    commitToTable(dataGen.generateInserts("001", 100), INSERT.value(), writeConfigs);

    String[] partitionPaths = dataGen.getPartitionPaths();
    String[] partitionValues = new String[1];
    String partitionPath = partitionPaths[0];
    partitionValues[0] = partitionPath;

    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setBasePath(basePath())
        .setConf(storageConf())
        .build();
    avroSchema = new TableSchemaResolver(metaClient).getTableAvroSchema();

    SparkColumnarFileReader reader = SparkAdapterSupport$.MODULE$.sparkAdapter().createParquetFileReader(false, spark().sessionState().conf(),
        Map$.MODULE$.empty(), storageConf().unwrapAs(Configuration.class));
    HoodieReaderContext<InternalRow> ctx = new SparkFileFormatInternalRowReaderContext(reader, JavaConverters.asScalaBufferConverter(Collections.<Filter>emptyList()).asScala().toSeq(),
        JavaConverters.asScalaBufferConverter(Collections.<Filter>emptyList()).asScala().toSeq(), storageConf(), metaClient.getTableConfig());
    ctx.setTablePath(basePath());
    ctx.setLatestCommitTime(WriteClientTestUtils.createNewInstantTime());
    ctx.setShouldMergeUseRecordPosition(true);
    ctx.setHasBootstrapBaseFile(false);
    ctx.setHasLogFiles(true);
    ctx.setNeedsBootstrapMerge(false);
    if (mergeMode == RecordMergeMode.CUSTOM) {
      ctx.setRecordMerger(Option.of(new CustomMerger()));
    } else {
      ctx.setRecordMerger(Option.empty());
    }
    ctx.setSchemaHandler(HoodieSparkUtils.gteqSpark3_5()
        ? new ParquetRowIndexBasedSchemaHandler<>(ctx, avroSchema, avroSchema, Option.empty(), new TypedProperties(), metaClient)
        : new FileGroupReaderSchemaHandler<>(ctx, avroSchema, avroSchema, Option.empty(), new TypedProperties(), metaClient));
    TypedProperties props = new TypedProperties();
    props.put("hoodie.write.record.merge.mode", mergeMode.name());
    props.setProperty(HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE.key(),String.valueOf(HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE.defaultValue()));
    props.setProperty(HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH.key(), metaClient.getTempFolderPath());
    props.setProperty(HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.key(), ExternalSpillableMap.DiskMapType.ROCKS_DB.name());
    props.setProperty(HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED.key(), "false");
    if (mergeMode.equals(RecordMergeMode.CUSTOM)) {
      writeConfigs.put(HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key(), CustomPayloadForTesting.class.getName());
      writeConfigs.put(HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key(), HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID);
    }
    buffer = new PositionBasedFileGroupRecordBuffer<>(
        ctx,
        metaClient,
        mergeMode,
        metaClient.getTableConfig().getPartialUpdateMode(),
        baseFileInstantTime,
        props,
        Collections.singletonList("timestamp"),
        updateProcessor);
  }

  private void commitToTable(List<HoodieRecord> recordList, String operation, Map<String, String> options) {
    List<String> recs = HoodieTestDataGenerator.recordsToStrings(recordList);
    Dataset<Row> inputDF = spark().read().json(jsc().parallelize(recs, 2));

    inputDF.write().format("hudi")
        .options(options)
        .option("hoodie.compact.inline", "false") // else fails due to compaction & deltacommit instant times being same
        .option("hoodie.datasource.write.operation", operation)
        .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
        .mode(operation.equalsIgnoreCase(WriteOperationType.INSERT.value()) ? SaveMode.Overwrite : SaveMode.Append)
      .save(basePath());
  }

  private Map<HoodieLogBlock.HeaderMetadataType, String> getHeader(boolean shouldWriteRecordPositions,
                                                                  String baseFileInstantTime) {
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, avroSchema.toString());
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    if (shouldWriteRecordPositions) {
      header.put(BASE_FILE_INSTANT_TIME_OF_RECORD_POSITIONS, baseFileInstantTime);
    }
    return header;
  }

  private List<DeleteRecord> getDeleteRecords() throws IOException, URISyntaxException {
    SchemaTestUtil testUtil = new SchemaTestUtil();
    List<IndexedRecord> records = testUtil.generateHoodieTestRecords(0, 100);

    List<DeleteRecord> deletedRecords = records.stream()
        .map(s -> (DeleteRecord.create(((GenericRecord) s).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString(),
            ((GenericRecord) s).get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString())))
        .collect(Collectors.toList()).subList(0, 50);
    return deletedRecords;
  }

  private HoodieDeleteBlock getDeleteBlockWithPositions(String baseFileInstantTime)
      throws IOException, URISyntaxException {
    List<DeleteRecord> deletedRecords = getDeleteRecords();
    List<Pair<DeleteRecord, Long>> deleteRecordList = new ArrayList<>();

    long position = 0;
    for (DeleteRecord dr : deletedRecords) {
      deleteRecordList.add(Pair.of(dr, position++));
    }
    return new HoodieDeleteBlock(deleteRecordList, getHeader(true, baseFileInstantTime));
  }

  private HoodieDeleteBlock getDeleteBlockWithoutPositions() throws IOException, URISyntaxException {
    List<DeleteRecord> deletedRecords = getDeleteRecords();
    List<Pair<DeleteRecord, Long>> deleteRecordList = new ArrayList<>();

    for (DeleteRecord dr : deletedRecords) {
      deleteRecordList.add(Pair.of(dr, -1L));
    }
    return new HoodieDeleteBlock(deleteRecordList, getHeader(false, ""));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testProcessDeleteBlockWithPositions(boolean sameBaseInstantTime) throws Exception {
    String baseFileInstantTime = "090";
    prepareBuffer(RecordMergeMode.COMMIT_TIME_ORDERING, baseFileInstantTime);
    HoodieDeleteBlock deleteBlock = getDeleteBlockWithPositions(
        sameBaseInstantTime ? baseFileInstantTime : baseFileInstantTime + "1");
    buffer.processDeleteBlock(deleteBlock);
    assertEquals(50, buffer.getLogRecords().size());
    if (sameBaseInstantTime) {
      // If the log block's base instant time of record positions match the base file
      // to merge, the log records are stored based on the position
      assertNotNull(buffer.getLogRecords().get(0L).getRecordKey(),
          "the record key is set up for fallback handling");
      assertNotNull(buffer.getLogRecords().get(0L).getOrderingValue(),
          "the ordering value is set up for fallback handling");
    } else {
      // If the log block's base instant time of record positions does not match the
      // base file to merge, the log records are stored based on the record key
      assertNull(buffer.getLogRecords().get(0L));
    }
  }

  @Test
  public void testProcessDeleteBlockWithCustomMerger() throws Exception {
    String baseFileInstantTime = "090";
    prepareBuffer(RecordMergeMode.CUSTOM, baseFileInstantTime);
    HoodieDeleteBlock deleteBlock = getDeleteBlockWithPositions(baseFileInstantTime);
    buffer.processDeleteBlock(deleteBlock);
    assertEquals(50, buffer.getLogRecords().size());
    assertNotNull(buffer.getLogRecords().get(0L).getRecordKey());
  }

  @Test
  public void testProcessDeleteBlockWithoutPositions() throws Exception {
    prepareBuffer(RecordMergeMode.COMMIT_TIME_ORDERING, "090");
    HoodieDeleteBlock deleteBlock = getDeleteBlockWithoutPositions();
    buffer.processDeleteBlock(deleteBlock);
    assertEquals(50, buffer.getLogRecords().size());
  }

  public static class CustomMerger implements HoodieRecordMerger {
    @Override
    public String getMergingStrategy() {
      return "random_strategy";
    }

    @Override
    public Pair<HoodieRecord, Schema> merge(
        HoodieRecord older, Schema oldSchema, HoodieRecord newer, Schema newSchema, TypedProperties props
    ) throws IOException {
      throw new IOException("Not implemented");
    }

    @Override
    public HoodieRecord.HoodieRecordType getRecordType() {
      return HoodieRecord.HoodieRecordType.SPARK;
    }
  }

  @Override
  public SparkConf conf() {
    return super.conf(Collections.singletonMap("spark.sql.parquet.enableVectorizedReader", "false"));
  }
}

