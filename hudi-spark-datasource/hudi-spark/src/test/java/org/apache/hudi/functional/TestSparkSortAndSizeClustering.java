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

package org.apache.hudi.functional;

import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.avro.AvroSchemaUtils;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.io.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.stats.ValueType;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.cluster.ClusteringPlanPartitionFilterMode;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;
import org.apache.hudi.testutils.MetadataMergeWriteStatus;

import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.parquet.avro.AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSparkSortAndSizeClustering extends HoodieSparkClientTestHarness {

  private HoodieWriteConfig config;

  public void setup(int maxFileSize) throws IOException {
    setup(maxFileSize, Collections.emptyMap());
  }

  public void setup(int maxFileSize, Map<String, String> options) throws IOException {
    initPath();
    initSparkContexts();
    initTestDataGenerator();
    initHoodieStorage();
    Properties props = getPropertiesForKeyGen(true);
    props.putAll(options);
    props.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.COPY_ON_WRITE, props);
    config = getConfigBuilder().withProps(props)
        .withStorageConfig(HoodieStorageConfig.newBuilder().parquetMaxFileSize(maxFileSize).build())
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withClusteringPlanPartitionFilterMode(ClusteringPlanPartitionFilterMode.RECENT_DAYS)
            .build())
        .withSchema(getSchema().toString())
        .build();
    context.getStorageConf().set(WRITE_OLD_LIST_STRUCTURE, "false");

    writeClient = getHoodieWriteClient(config);
  }

  @AfterEach
  public void tearDown() throws IOException {
    cleanupResources();
  }

  @Test
  public void testClusteringWithRDD() throws IOException {
    writeAndClustering(false);
  }

  @Test
  public void testClusteringWithRow() throws IOException {
    writeAndClustering(true);
  }

  public void writeAndClustering(boolean isRow) throws IOException {
    setup(102400);
    config.setValue("hoodie.datasource.write.row.writer.enable", String.valueOf(isRow));
    config.setValue("hoodie.metadata.enable", "false");
    config.setValue("hoodie.clustering.plan.strategy.daybased.lookback.partitions", "1");
    config.setValue("hoodie.clustering.plan.strategy.target.file.max.bytes", String.valueOf(1024 * 1024));
    config.setValue("hoodie.clustering.plan.strategy.max.bytes.per.group", String.valueOf(2 * 1024 * 1024));

    int numRecords = 1000;
    long ts = System.currentTimeMillis();
    List<WriteStatus> initialWriteStats = writeData(numRecords, true, ts);
    validateTypes(initialWriteStats.stream().map(WriteStatus::getStat).collect(Collectors.toList()));
    List<Row> rows = readRecords();
    assertEquals(numRecords, rows.size());
    validateDateAndTimestampFields(rows, ts);

    String clusteringTime = (String) writeClient.scheduleClustering(Option.empty()).get();
    HoodieClusteringPlan plan = ClusteringUtils.getClusteringPlan(
        metaClient, INSTANT_GENERATOR.getClusteringCommitRequestedInstant(clusteringTime)).map(Pair::getRight).get();

    List<HoodieClusteringGroup> inputGroups = plan.getInputGroups();
    assertEquals(1, inputGroups.size(), "Clustering plan will contain 1 input group");

    Integer outputFileGroups = plan.getInputGroups().get(0).getNumOutputFileGroups();
    assertEquals(2, outputFileGroups, "Clustering plan will generate 2 output groups");

    HoodieWriteMetadata writeMetadata = writeClient.cluster(clusteringTime, true);
    List<HoodieWriteStat> writeStats = (List<HoodieWriteStat>)writeMetadata.getWriteStats().get();
    assertEquals(2, writeStats.size(), "Clustering should write 2 files");

    rows = readRecords();
    assertEquals(numRecords, rows.size());
    validateTypes(writeStats);
    validateDateAndTimestampFields(rows, ts);
  }

  private void validateDateAndTimestampFields(List<Row> rows, long ts) {
    Schema schema = HoodieAvroUtils.addMetadataFields(getSchema(), false);
    Timestamp timestamp = new Timestamp(ts);
    // sanity check date field is within expected range
    Date startDate = Date.valueOf(LocalDate.now().minusDays(3));
    Date endDate = Date.valueOf(LocalDate.now().plusDays(1));
    int dateFieldIndex = schema.getField("date_nullable_field").pos();
    int tsMillisFieldIndex = schema.getField("timestamp_millis_field").pos();
    int tsMicrosNullableFieldIndex = schema.getField("timestamp_micros_nullable_field").pos();
    int tsLocalMillisFieldIndex = schema.getField("timestamp_local_millis_nullable_field").pos();
    int tsLocalMicrosFieldIndex = schema.getField("timestamp_local_micros_field").pos();
    for (Row row : rows) {
      assertEquals(timestamp, row.get(tsMillisFieldIndex));
      if (!row.isNullAt(tsMicrosNullableFieldIndex)) {
        assertEquals(timestamp, row.get(tsMicrosNullableFieldIndex));
      }
      if (!row.isNullAt(dateFieldIndex)) {
        assertTrue(row.get(dateFieldIndex) instanceof Date);
        Date actualDate = (Date) row.get(dateFieldIndex);
        assertTrue(actualDate.compareTo(startDate) > 0 && actualDate.compareTo(endDate) < 0);
      }
      if (!HoodieSparkUtils.gteqSpark4_0()) {
        if (!row.isNullAt(tsLocalMillisFieldIndex)) {
          assertEquals(ValueType.toLocalTimestampMillis(ts, null), row.get(tsLocalMillisFieldIndex));
        }
        assertEquals(ValueType.toLocalTimestampMicros(Math.multiplyExact(ts, 1000L), null), row.get(tsLocalMicrosFieldIndex));
      }
    }
  }

  // Validate that clustering produces decimals in legacy format and lists in newer format. Assert that the unit is correct on the timestamps
  private void validateTypes(List<HoodieWriteStat> writeStats) {
    writeStats.stream().map(writeStat -> new StoragePath(metaClient.getBasePath(), writeStat.getPath())).forEach(writtenPath -> {
      MessageType schema = ParquetUtils.readMetadata(storage, writtenPath)
          .getFileMetaData().getSchema();
      // validate decimal field
      int decimalFieldIndex = schema.getFieldIndex("decimal_field");
      Type decimalType = schema.getFields().get(decimalFieldIndex);
      assertEquals("DECIMAL", decimalType.getOriginalType().toString());
      assertEquals("FIXED_LEN_BYTE_ARRAY", decimalType.asPrimitiveType().getPrimitiveTypeName().toString());
      LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalTypeAnnotation = ((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) decimalType.getLogicalTypeAnnotation());
      assertEquals(10, decimalLogicalTypeAnnotation.getPrecision());
      assertEquals(6, decimalLogicalTypeAnnotation.getScale());
      // validate array field
      int arrayFieldIndex = schema.getFieldIndex("array_field");
      Type arrayType = schema.getFields().get(arrayFieldIndex);
      assertEquals("list", arrayType.asGroupType().getFields().get(0).getName());
      // validate timestamp field scale
      int timeMillisField = schema.getFieldIndex("timestamp_millis_field");
      assertEquals(LogicalTypeAnnotation.TimeUnit.MILLIS, ((LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) schema.getFields().get(timeMillisField).getLogicalTypeAnnotation()).getUnit());
      int timeMicrosField = schema.getFieldIndex("timestamp_micros_nullable_field");
      assertEquals(LogicalTypeAnnotation.TimeUnit.MICROS, ((LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) schema.getFields().get(timeMicrosField).getLogicalTypeAnnotation()).getUnit());
      // check nested timestamp field scale in record, array and map
      Type nestedFieldType = schema.getFields().get(schema.getFieldIndex("nested_record"));
      int nestedTimeMillisField = nestedFieldType.asGroupType().getFieldIndex("nested_timestamp_millis_field");
      assertEquals(LogicalTypeAnnotation.TimeUnit.MILLIS,
          ((LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) nestedFieldType.asGroupType().getFields().get(nestedTimeMillisField).getLogicalTypeAnnotation()).getUnit());
      Type arrayFieldType = schema.getFields().get(schema.getFieldIndex("array_field"));
      Type arrayElement = arrayFieldType.asGroupType().getFields().get(0).asGroupType().getFields().get(0);
      int arrayElementTimeMillisField = arrayElement.asGroupType().getFieldIndex("nested_timestamp_millis_field");
      assertEquals(LogicalTypeAnnotation.TimeUnit.MILLIS,
          ((LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) arrayElement.asGroupType().getFields().get(arrayElementTimeMillisField).getLogicalTypeAnnotation()).getUnit());
      Type mapValueType = schema.getFields().get(schema.getFieldIndex("nullable_map_field")).asGroupType().getFields().get(0)
          .asGroupType().getFields().get(1);
      int mapValueTimeMillisField = mapValueType.asGroupType().getFieldIndex("nested_timestamp_millis_field");
      assertEquals(LogicalTypeAnnotation.TimeUnit.MILLIS,
          ((LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) mapValueType.asGroupType().getFields().get(mapValueTimeMillisField).getLogicalTypeAnnotation()).getUnit());
    });
  }

  private List<WriteStatus> writeData(int totalRecords, boolean doCommit, long ts) {
    String commitTime = writeClient.startCommit();
    List<HoodieRecord> records = generateInserts(commitTime, ts, totalRecords);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records);
    metaClient = HoodieTableMetaClient.reload(metaClient);

    List<WriteStatus> writeStatues = writeClient.insert(writeRecords, commitTime).collect();
    org.apache.hudi.testutils.Assertions.assertNoWriteErrors(writeStatues);

    if (doCommit) {
      Assertions.assertTrue(writeClient.commitStats(commitTime, writeStatues.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
          Option.empty(), metaClient.getCommitActionType()));
    }

    metaClient = HoodieTableMetaClient.reload(metaClient);
    return writeStatues;
  }

  private List<Row> readRecords() {
    Dataset<Row> roViewDF = sparkSession.read().format("hudi").load(basePath);
    roViewDF.createOrReplaceTempView("clutering_table");
    return sparkSession.sqlContext().sql("select * from clutering_table").collectAsList();
  }

  public HoodieWriteConfig.Builder getConfigBuilder() {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withWriteStatusClass(MetadataMergeWriteStatus.class)
        .forTable("clustering-table")
        .withEmbeddedTimelineServerEnabled(true);
  }

  private List<HoodieRecord> generateInserts(String instant, long ts, int count) {
    Schema schema = getSchema();
    Schema decimalSchema = schema.getField("decimal_field").schema();
    Schema nestedSchema = AvroSchemaUtils.getNonNullTypeFromUnion(schema.getField("nested_record").schema());
    Schema enumSchema = AvroSchemaUtils.getNonNullTypeFromUnion(schema.getField("enum_field").schema());
    Random random = new Random(0);
    return IntStream.range(0, count)
        .mapToObj(i -> {
          GenericRecord record = new GenericData.Record(schema);
          String key = "key_" + i;
          String partition = "partition_" + (i % 3);
          record.put("_row_key", key);
          record.put("ts", ts);
          record.put("partition_path", partition);
          record.put("_hoodie_is_deleted", false);
          record.put("double_field", random.nextDouble());
          record.put("float_field", random.nextFloat());
          record.put("int_field", random.nextInt());
          record.put("long_field", random.nextLong());
          record.put("string_field", instant);
          record.put("bytes_field", ByteBuffer.wrap(instant.getBytes(StandardCharsets.UTF_8)));
          GenericRecord nestedRecord = new GenericData.Record(nestedSchema);
          nestedRecord.put("nested_int", random.nextInt());
          nestedRecord.put("nested_string", "nested_" + instant);
          nestedRecord.put("nested_timestamp_millis_field", ts);
          record.put("nested_record", nestedRecord);
          record.put("array_field", Collections.singletonList(nestedRecord));
          record.put("nullable_map_field", Collections.singletonMap("key_" + instant, nestedRecord));
          // logical types
          BigDecimal bigDecimal = new BigDecimal(String.format(Locale.ENGLISH, "%5f", random.nextFloat()));
          Conversions.DecimalConversion decimalConversions = new Conversions.DecimalConversion();
          GenericFixed genericFixed = decimalConversions.toFixed(bigDecimal, decimalSchema, LogicalTypes.decimal(10, 6));
          record.put("decimal_field", genericFixed);
          record.put("date_nullable_field", random.nextBoolean() ? null : LocalDate.now().minusDays(random.nextInt(3)));
          record.put("timestamp_millis_field", ts);
          record.put("timestamp_micros_nullable_field", random.nextBoolean() ? null : ts * 1000);
          record.put("timestamp_local_millis_nullable_field", random.nextBoolean() ? null : ts);
          record.put("timestamp_local_micros_field", ts * 1000);
          record.put("enum_field", new GenericData.EnumSymbol(
              enumSchema,
              enumSchema
                  .getEnumSymbols()
                  .get(random.nextInt(enumSchema.getEnumSymbols().size()))));
          return new HoodieAvroIndexedRecord(new HoodieKey(key, partition), record, ts);
        })
        .collect(Collectors.toList());
  }

  private Schema getSchema() {
    try {
      String schema = FileIOUtils.readAsUTFString(this.getClass().getClassLoader().getResourceAsStream("schema_with_logical_types.avsc"));
      return new Schema.Parser().parse(schema);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
