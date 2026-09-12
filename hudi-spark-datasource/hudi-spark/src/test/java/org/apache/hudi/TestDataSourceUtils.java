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

package org.apache.hudi;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SerializationUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.execution.bulkinsert.GlobalSortPartitioner;
import org.apache.hudi.execution.bulkinsert.GlobalSortPartitionerWithRows;
import org.apache.hudi.execution.bulkinsert.NonSortPartitioner;
import org.apache.hudi.execution.bulkinsert.NonSortPartitionerWithRows;
import org.apache.hudi.execution.bulkinsert.PartitionPathRepartitionAndSortPartitioner;
import org.apache.hudi.execution.bulkinsert.PartitionPathRepartitionAndSortPartitionerWithRows;
import org.apache.hudi.execution.bulkinsert.PartitionPathRepartitionPartitioner;
import org.apache.hudi.execution.bulkinsert.PartitionPathRepartitionPartitionerWithRows;
import org.apache.hudi.execution.bulkinsert.PartitionSortPartitionerWithRows;
import org.apache.hudi.execution.bulkinsert.RDDCustomColumnsSortPartitioner;
import org.apache.hudi.execution.bulkinsert.RDDPartitionSortPartitioner;
import org.apache.hudi.execution.bulkinsert.RowCustomColumnsSortPartitioner;
import org.apache.hudi.execution.bulkinsert.RowSpatialCurveSortPartitioner;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.metadata.stats.ValueMetadata;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestDataSourceUtils extends HoodieClientTestBase {

  @Mock
  private SparkRDDWriteClient hoodieWriteClient;

  @Mock
  private JavaRDD<HoodieRecord> hoodieRecords;

  @Captor
  private ArgumentCaptor<Option> optionCaptor;
  private HoodieWriteConfig config;

  // There are fields event_date1, event_date2, event_date3 with logical type as Date. event_date1 & event_date3 are
  // of UNION schema type, which is a union of null and date type in different orders. event_date2 is non-union
  // date type. event_cost1, event_cost2, event3 are decimal logical types with UNION schema, which is similar to
  // the event_date.
  private String avroSchemaString = "{\"type\": \"record\"," + "\"name\": \"events\"," + "\"fields\": [ "
          + "{\"name\": \"event_date1\", \"type\" : [{\"type\" : \"int\", \"logicalType\" : \"date\"}, \"null\"]},"
          + "{\"name\": \"event_date2\", \"type\" : {\"type\": \"int\", \"logicalType\" : \"date\"}},"
          + "{\"name\": \"event_date3\", \"type\" : [\"null\", {\"type\" : \"int\", \"logicalType\" : \"date\"}]},"
          + "{\"name\": \"event_name\", \"type\": \"string\"},"
          + "{\"name\": \"event_organizer\", \"type\": \"string\"},"
          + "{\"name\": \"event_cost1\", \"type\": "
          + "[{\"type\": \"fixed\", \"name\": \"dc\", \"size\": 5, \"logicalType\": \"decimal\", \"precision\": 10, \"scale\": 6}, \"null\"]},"
          + "{\"name\": \"event_cost2\", \"type\": "
          + "{\"type\": \"fixed\", \"name\": \"ef\", \"size\": 5, \"logicalType\": \"decimal\", \"precision\": 10, \"scale\": 6}},"
          + "{\"name\": \"event_cost3\", \"type\": "
          + "[\"null\", {\"type\": \"fixed\", \"name\": \"fg\", \"size\": 5, \"logicalType\": \"decimal\", \"precision\": 10, \"scale\": 6}]}"
          + "]}";

  @BeforeEach
  public void setUp() {
    config = HoodieWriteConfig.newBuilder().withPath("/").build();
  }

  @Test
  public void testSparkVersionSpecificParquetCompressionCodecDefault() {
    String expectedCodec = StringUtils.compareVersions(HoodieSparkUtils.getSparkVersion(), "3.5.0") >= 0
        ? "zstd" : "gzip";
    assertEquals(expectedCodec, config.getParquetCompressionCodec());

    HoodieWriteConfig configWithPartialStorage = HoodieWriteConfig.newBuilder()
        .withPath("/")
        .withStorageConfig(HoodieStorageConfig.newBuilder().parquetWriteLegacyFormat("false").build())
        .build();
    assertEquals(expectedCodec, configWithPartialStorage.getParquetCompressionCodec());

    Map<String, String> params = new HashMap<>();
    params.put(DataSourceWriteOptions.TABLE_TYPE().key(), DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL());
    HoodieWriteConfig dataSourceConfig = DataSourceUtils.createHoodieConfig(
        avroSchemaString, config.getBasePath(), "test", params);
    assertEquals(expectedCodec, dataSourceConfig.getParquetCompressionCodec());

    params.put(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME.key(), "snappy");
    dataSourceConfig = DataSourceUtils.createHoodieConfig(
        avroSchemaString, config.getBasePath(), "test", params);
    assertEquals("snappy", dataSourceConfig.getParquetCompressionCodec());
  }

  @Test
  public void testAvroRecordsFieldConversion() {

    HoodieSchema schema = HoodieSchema.parse(avroSchemaString);
    GenericRecord record = new GenericData.Record(schema.toAvroSchema());
    record.put("event_date1", 18000);
    record.put("event_date2", 18001);
    record.put("event_date3", 18002);
    record.put("event_name", "Hudi Meetup");
    record.put("event_organizer", "Hudi PMC");

    BigDecimal bigDecimal = new BigDecimal("123.184331");
    HoodieSchema decimalSchema = schema.getField("event_cost1").get().schema().getNonNullType();
    Conversions.DecimalConversion decimalConversions = new Conversions.DecimalConversion();
    GenericFixed genericFixed = decimalConversions.toFixed(bigDecimal, decimalSchema.toAvroSchema(), LogicalTypes.decimal(10, 6));
    record.put("event_cost1", genericFixed);
    record.put("event_cost2", genericFixed);
    record.put("event_cost3", genericFixed);

    assertEquals(LocalDate.ofEpochDay(18000).toString(), HoodieAvroUtils.getNestedFieldValAsString(record, "event_date1",
        true, false));
    assertEquals(LocalDate.ofEpochDay(18001).toString(), HoodieAvroUtils.getNestedFieldValAsString(record, "event_date2",
        true, false));
    assertEquals(LocalDate.ofEpochDay(18002).toString(), HoodieAvroUtils.getNestedFieldValAsString(record, "event_date3",
        true, false));
    assertEquals("Hudi Meetup", HoodieAvroUtils.getNestedFieldValAsString(record, "event_name", true, false));
    assertEquals("Hudi PMC", HoodieAvroUtils.getNestedFieldValAsString(record, "event_organizer", true, false));
    assertEquals(bigDecimal.toString(), HoodieAvroUtils.getNestedFieldValAsString(record, "event_cost1", true, false));
    assertEquals(bigDecimal.toString(), HoodieAvroUtils.getNestedFieldValAsString(record, "event_cost2", true, false));
    assertEquals(bigDecimal.toString(), HoodieAvroUtils.getNestedFieldValAsString(record, "event_cost3", true, false));
  }

  @Test
  public void testDoWriteOperationWithoutUserDefinedBulkInsertPartitioner() throws HoodieException {
    when(hoodieWriteClient.getConfig()).thenReturn(config);

    DataSourceUtils.doWriteOperation(hoodieWriteClient, hoodieRecords, "test-time",
            WriteOperationType.BULK_INSERT, false);

    verify(hoodieWriteClient, times(1)).bulkInsert(any(hoodieRecords.getClass()), anyString(),
            optionCaptor.capture());
    assertThat(optionCaptor.getValue(), is(equalTo(Option.empty())));
  }

  @Test
  public void testDoWriteOperationWithNonExistUserDefinedBulkInsertPartitioner() throws HoodieException {
    setAndVerifyHoodieWriteClientWith("NonExistClassName");

    Exception exception = assertThrows(HoodieException.class, () -> {
      DataSourceUtils.doWriteOperation(hoodieWriteClient, hoodieRecords, "test-time",
              WriteOperationType.BULK_INSERT, false);
    });

    assertThat(exception.getMessage(), containsString("Could not create UserDefinedBulkInsertPartitioner"));
  }

  @Test
  public void testDoWriteOperationWithUserDefinedBulkInsertPartitioner() throws HoodieException {
    setAndVerifyHoodieWriteClientWith(NoOpBulkInsertPartitioner.class.getName());

    DataSourceUtils.doWriteOperation(hoodieWriteClient, hoodieRecords, "test-time",
            WriteOperationType.BULK_INSERT, false);

    verify(hoodieWriteClient, times(1)).bulkInsert(any(hoodieRecords.getClass()), anyString(),
        optionCaptor.capture());
    assertThat(optionCaptor.getValue().get(), is(instanceOf(NoOpBulkInsertPartitioner.class)));
  }

  @Test
  public void testCreateUserDefinedBulkInsertPartitionerRowsWithInValidPartitioner() throws HoodieException {
    config = HoodieWriteConfig.newBuilder().withPath("/").withUserDefinedBulkInsertPartitionerClass("NonExistentUserDefinedClass").build();

    Exception exception = assertThrows(HoodieException.class, () -> {
      DataSourceUtils.createUserDefinedBulkInsertPartitionerWithRows(config);
    });

    assertThat(exception.getMessage(), containsString("Could not create UserDefinedBulkInsertPartitionerRows"));
  }

  @Test
  public void testCreateUserDefinedBulkInsertPartitionerRowsWithValidPartitioner() throws HoodieException {
    config = HoodieWriteConfig.newBuilder().withPath("/").withUserDefinedBulkInsertPartitionerClass(NoOpBulkInsertPartitionerRows.class.getName()).build();

    Option<BulkInsertPartitioner<Dataset<Row>>> partitioner = DataSourceUtils.createUserDefinedBulkInsertPartitionerWithRows(config);
    assertThat(partitioner.isPresent(), is(true));
  }

  @Test
  public void testCreateRDDCustomColumnsSortPartitionerWithValidPartitioner() throws HoodieException {
    config = HoodieWriteConfig
            .newBuilder()
            .withPath("/")
            .withUserDefinedBulkInsertPartitionerClass(RDDCustomColumnsSortPartitioner.class.getName())
            .withUserDefinedBulkInsertPartitionerSortColumns("column1,column2")
            .withSchema(avroSchemaString)
            .build();

    Option<BulkInsertPartitioner<Dataset<Row>>> partitioner = DataSourceUtils.createUserDefinedBulkInsertPartitionerWithRows(config);
    assertThat(partitioner.isPresent(), is(true));
  }

  /**
   * Every out of the box bulk insert partitioner has to be usable as a user defined partitioner.
   * One is instantiated by reflection with only the write config, so each has to expose a
   * constructor taking only a {@link HoodieWriteConfig}. See HUDI-7526.
   */
  @ParameterizedTest
  @ValueSource(classes = {
      NonSortPartitioner.class,
      GlobalSortPartitioner.class,
      RDDPartitionSortPartitioner.class,
      RDDCustomColumnsSortPartitioner.class,
      PartitionPathRepartitionPartitioner.class,
      PartitionPathRepartitionAndSortPartitioner.class,
      NonSortPartitionerWithRows.class,
      GlobalSortPartitionerWithRows.class,
      PartitionSortPartitionerWithRows.class,
      RowCustomColumnsSortPartitioner.class,
      RowSpatialCurveSortPartitioner.class,
      PartitionPathRepartitionPartitionerWithRows.class,
      PartitionPathRepartitionAndSortPartitionerWithRows.class
  })
  public void testBuiltInPartitionersAreUsableAsUserDefinedPartitioners(Class<?> partitionerClass) {
    Map<String, String> props = new HashMap<>();
    // required by the spatial curve partitioner, ignored by the rest
    props.put(HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS.key(), "column1,column2");
    config = HoodieWriteConfig.newBuilder()
        .withPath("/")
        .withUserDefinedBulkInsertPartitionerClass(partitionerClass.getName())
        .withUserDefinedBulkInsertPartitionerSortColumns("column1,column2")
        .withSchema(avroSchemaString)
        .withProps(props)
        .build();

    assertThat(DataSourceUtils.createUserDefinedBulkInsertPartitioner(config).isPresent(), is(true));
    assertThat(DataSourceUtils.createUserDefinedBulkInsertPartitionerWithRows(config).isPresent(), is(true));
  }

  /**
   * The partition path partitioners take the flag from the table when built by the factory, so
   * check the write config only path agrees with the table config the factory path reads.
   */
  @Test
  public void testIsTablePartitionedPrefersTableConfigOverPartitionPathField() {
    // hoodie.table.partition.fields is what HoodieTable#isPartitioned reads, so it wins when present.
    HoodieWriteConfig fromTableConfig = HoodieWriteConfig.newBuilder().withPath("/")
        .withProps(Collections.singletonMap(
            HoodieTableConfig.PARTITION_FIELDS.key(), "partition_path"))
        .build();
    assertThat(BulkInsertPartitioner.isTablePartitioned(fromTableConfig), is(true));

    // Present but empty means a non partitioned table, even with a write side field configured.
    Map<String, String> emptyTableConfig = new HashMap<>();
    emptyTableConfig.put(HoodieTableConfig.PARTITION_FIELDS.key(), "");
    emptyTableConfig.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partition_path");
    HoodieWriteConfig emptyWins = HoodieWriteConfig.newBuilder().withPath("/")
        .withProps(emptyTableConfig).build();
    assertThat(BulkInsertPartitioner.isTablePartitioned(emptyWins), is(false));

    // The two sources disagreeing resolves to the table config, matching the factory path.
    Map<String, String> disagreeing = new HashMap<>();
    disagreeing.put(HoodieTableConfig.PARTITION_FIELDS.key(), "partition_path");
    disagreeing.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "");
    HoodieWriteConfig tableConfigWins = HoodieWriteConfig.newBuilder().withPath("/")
        .withProps(disagreeing).build();
    assertThat(BulkInsertPartitioner.isTablePartitioned(tableConfigWins), is(true));
  }

  /**
   * Without the table property, which is the case for a write config assembled without the table's
   * properties, the write side partition path field is the only signal left.
   */
  @Test
  public void testIsTablePartitionedFallsBackToPartitionPathField() {
    HoodieWriteConfig partitioned = HoodieWriteConfig.newBuilder().withPath("/")
        .withProps(Collections.singletonMap(
            KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partition_path"))
        .build();
    assertThat(BulkInsertPartitioner.isTablePartitioned(partitioned), is(true));

    HoodieWriteConfig nonPartitioned = HoodieWriteConfig.newBuilder().withPath("/")
        .withProps(Collections.singletonMap(
            KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), ""))
        .build();
    assertThat(BulkInsertPartitioner.isTablePartitioned(nonPartitioned), is(false));

    HoodieWriteConfig unset = HoodieWriteConfig.newBuilder().withPath("/").build();
    assertThat(BulkInsertPartitioner.isTablePartitioned(unset), is(false));
  }

  @Test
  public void testCreateHoodieConfigWithAsyncClustering() {
    ArrayList<ImmutablePair<String, Boolean>> asyncClusteringKeyValues = new ArrayList<>(4);
    asyncClusteringKeyValues.add(new ImmutablePair(DataSourceWriteOptions.ASYNC_CLUSTERING_ENABLE().key(), true));
    asyncClusteringKeyValues.add(new ImmutablePair(HoodieClusteringConfig.ASYNC_CLUSTERING_ENABLE.key(), true));
    asyncClusteringKeyValues.add(new ImmutablePair("hoodie.datasource.clustering.async.enable", true));
    asyncClusteringKeyValues.add(new ImmutablePair("hoodie.clustering.async.enabled", true));

    asyncClusteringKeyValues.stream().forEach(pair -> {
      HashMap<String, String> params = new HashMap<>(3);
      params.put(DataSourceWriteOptions.TABLE_TYPE().key(), DataSourceWriteOptions.TABLE_TYPE().defaultValue());
      params.put(DataSourceWriteOptions.PAYLOAD_CLASS_NAME().key(), DefaultHoodieRecordPayload.class.getName());
      params.put(pair.left, pair.right.toString());
      HoodieWriteConfig hoodieConfig = DataSourceUtils
              .createHoodieConfig(avroSchemaString, config.getBasePath(), "test", params);
      assertEquals(pair.right, hoodieConfig.isAsyncClusteringEnabled());

      TypedProperties prop = new TypedProperties();
      prop.putAll(params);
      assertEquals(pair.right, HoodieClusteringConfig.from(prop).isAsyncClusteringEnabled());
    });
  }

  private void setAndVerifyHoodieWriteClientWith(final String partitionerClassName) {
    config = HoodieWriteConfig.newBuilder().withPath(config.getBasePath())
        .withUserDefinedBulkInsertPartitionerClass(partitionerClassName)
        .build();
    when(hoodieWriteClient.getConfig()).thenReturn(config);

    assertThat(config.getUserDefinedBulkInsertPartitionerClass(), is(equalTo(partitionerClassName)));
  }

  public static class NoOpBulkInsertPartitioner<T extends HoodieRecordPayload>
      implements BulkInsertPartitioner<JavaRDD<HoodieRecord<T>>> {

    public NoOpBulkInsertPartitioner(HoodieWriteConfig config) {
    }

    @Override
    public JavaRDD<HoodieRecord<T>> repartitionRecords(JavaRDD<HoodieRecord<T>> records, int outputSparkPartitions) {
      return records;
    }

    @Override
    public boolean arePartitionRecordsSorted() {
      return false;
    }
  }

  public static class NoOpBulkInsertPartitionerRows
      implements BulkInsertPartitioner<Dataset<Row>> {

    public NoOpBulkInsertPartitionerRows(HoodieWriteConfig config) {
    }

    @Override
    public Dataset<Row> repartitionRecords(Dataset<Row> records, int outputSparkPartitions) {
      return records;
    }

    @Override
    public boolean arePartitionRecordsSorted() {
      return false;
    }
  }

  @Test
  public void testSerHoodieMetadataPayload() throws IOException {
    String partitionPath = "2022/10/01";
    String fileName = "file.parquet";
    String targetColName = "c1";

    HoodieColumnRangeMetadata<Comparable> columnStatsRecord =
        HoodieColumnRangeMetadata.<Comparable>create(fileName, targetColName, 0, 500, 0, 100, 12345, 12345, ValueMetadata.V1EmptyMetadata.get());

    HoodieRecord<HoodieMetadataPayload> hoodieMetadataPayload =
        HoodieMetadataPayload.createColumnStatsRecords(partitionPath, Collections.singletonList(columnStatsRecord), false)
            .findFirst().get();

    IndexedRecord record = hoodieMetadataPayload.getData().getInsertValue(null).get();
    byte[] recordToBytes = HoodieAvroUtils.avroToBytes(record);
    GenericRecord genericRecord = HoodieAvroUtils.bytesToAvro(recordToBytes, record.getSchema());

    HoodieMetadataPayload genericRecordHoodieMetadataPayload = new HoodieMetadataPayload(Option.of(genericRecord));
    byte[] bytes = SerializationUtils.serialize(genericRecordHoodieMetadataPayload);
    HoodieMetadataPayload deserGenericRecordHoodieMetadataPayload = SerializationUtils.deserialize(bytes);

    assertEquals(genericRecordHoodieMetadataPayload, deserGenericRecordHoodieMetadataPayload);
  }

  @Test
  void testDeduplicationAgainstRecordsAlreadyInTable() throws IOException {
    initResources();
    HoodieWriteConfig config = getConfig();
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config)) {
      String newCommitTime = writeClient.startCommit();
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 2);
      List<WriteStatus> statusList = writeClient.bulkInsert(recordsRDD, newCommitTime).collect();
      writeClient.commit(newCommitTime, jsc.parallelize(statusList), Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty());
      assertNoWriteErrors(statusList);

      List<HoodieRecord> newRecords = dataGen.generateInserts(newCommitTime, 10);
      List<HoodieRecord> inputRecords = Stream.concat(records.subList(0, 10).stream(), newRecords.stream()).collect(Collectors.toList());
      // Deduplicate against the committing client's engine context and config, the same wiring the
      // Spark SQL writer uses so the record index lookup registry is owned by the draining context.
      List<HoodieRecord> output = DataSourceUtils.handleDuplicates(context, jsc.parallelize(inputRecords, 1), config, false).collect();
      Set<String> expectedRecordKeys = newRecords.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toSet());
      assertEquals(expectedRecordKeys, output.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toSet()));
    }
  }
}
