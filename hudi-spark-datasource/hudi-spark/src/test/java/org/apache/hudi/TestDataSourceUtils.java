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

import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.execution.bulkinsert.RDDCustomColumnsSortPartitioner;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DecimalType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructType$;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.hudi.DataSourceUtils.tryOverrideParquetWriteLegacyFormatProperty;
import static org.apache.hudi.common.model.HoodieFileFormat.PARQUET;
import static org.apache.hudi.hive.ddl.HiveSyncMode.HMS;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestDataSourceUtils {

  private static final String HIVE_DATABASE = "testdb1";
  private static final String HIVE_TABLE = "hive_trips";

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
  public void testAvroRecordsFieldConversion() {

    Schema avroSchema = new Schema.Parser().parse(avroSchemaString);
    GenericRecord record = new GenericData.Record(avroSchema);
    record.put("event_date1", 18000);
    record.put("event_date2", 18001);
    record.put("event_date3", 18002);
    record.put("event_name", "Hudi Meetup");
    record.put("event_organizer", "Hudi PMC");

    BigDecimal bigDecimal = new BigDecimal("123.184331");
    Schema decimalSchema = avroSchema.getField("event_cost1").schema().getTypes().get(0);
    Conversions.DecimalConversion decimalConversions = new Conversions.DecimalConversion();
    GenericFixed genericFixed = decimalConversions.toFixed(bigDecimal, decimalSchema, LogicalTypes.decimal(10, 6));
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
            WriteOperationType.BULK_INSERT);

    verify(hoodieWriteClient, times(1)).bulkInsert(any(hoodieRecords.getClass()), anyString(),
            optionCaptor.capture());
    assertThat(optionCaptor.getValue(), is(equalTo(Option.empty())));
  }

  @Test
  public void testDoWriteOperationWithNonExistUserDefinedBulkInsertPartitioner() throws HoodieException {
    setAndVerifyHoodieWriteClientWith("NonExistClassName");

    Exception exception = assertThrows(HoodieException.class, () -> {
      DataSourceUtils.doWriteOperation(hoodieWriteClient, hoodieRecords, "test-time",
              WriteOperationType.BULK_INSERT);
    });

    assertThat(exception.getMessage(), containsString("Could not create UserDefinedBulkInsertPartitioner"));
  }

  @Test
  public void testDoWriteOperationWithUserDefinedBulkInsertPartitioner() throws HoodieException {
    setAndVerifyHoodieWriteClientWith(NoOpBulkInsertPartitioner.class.getName());

    DataSourceUtils.doWriteOperation(hoodieWriteClient, hoodieRecords, "test-time",
            WriteOperationType.BULK_INSERT);

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
            .withUserDefinedBulkInsertPartitionerSortColumns("column1, column2")
            .withSchema(avroSchemaString)
            .build();

    Option<BulkInsertPartitioner<Dataset<Row>>> partitioner = DataSourceUtils.createUserDefinedBulkInsertPartitionerWithRows(config);
    assertThat(partitioner.isPresent(), is(true));
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
      params.put(DataSourceWriteOptions.PAYLOAD_CLASS_NAME().key(),
              DataSourceWriteOptions.PAYLOAD_CLASS_NAME().defaultValue());
      params.put(pair.left, pair.right.toString());
      HoodieWriteConfig hoodieConfig = DataSourceUtils
              .createHoodieConfig(avroSchemaString, config.getBasePath(), "test", params);
      assertEquals(pair.right, hoodieConfig.isAsyncClusteringEnabled());

      TypedProperties prop = new TypedProperties();
      prop.putAll(params);
      assertEquals(pair.right, HoodieClusteringConfig.from(prop).isAsyncClusteringEnabled());
    });
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testBuildHiveSyncConfig(boolean useSyncMode) {
    TypedProperties props = new TypedProperties();
    if (useSyncMode) {
      props.setProperty(DataSourceWriteOptions.HIVE_SYNC_MODE().key(), HMS.name());
      props.setProperty(DataSourceWriteOptions.HIVE_USE_JDBC().key(), String.valueOf(false));
    }
    props.setProperty(DataSourceWriteOptions.HIVE_DATABASE().key(), HIVE_DATABASE);
    props.setProperty(DataSourceWriteOptions.HIVE_TABLE().key(), HIVE_TABLE);
    HiveSyncConfig hiveSyncConfig = DataSourceUtils.buildHiveSyncConfig(props, config.getBasePath(), PARQUET.name());

    if (useSyncMode) {
      assertFalse(hiveSyncConfig.getBoolean(HiveSyncConfig.HIVE_USE_JDBC));
      assertEquals(HMS.name(), hiveSyncConfig.getString(HiveSyncConfig.HIVE_SYNC_MODE));
    } else {
      assertTrue(hiveSyncConfig.getBoolean(HiveSyncConfig.HIVE_USE_JDBC));
      assertNull(hiveSyncConfig.getString(HiveSyncConfig.HIVE_SYNC_MODE));
    }
    assertEquals(HIVE_DATABASE, hiveSyncConfig.getString(HiveSyncConfig.META_SYNC_DATABASE_NAME));
    assertEquals(HIVE_TABLE, hiveSyncConfig.getString(HiveSyncConfig.META_SYNC_TABLE_NAME));
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

    public NoOpBulkInsertPartitioner(HoodieWriteConfig config) {}

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

    public NoOpBulkInsertPartitionerRows(HoodieWriteConfig config) {}

    @Override
    public Dataset<Row> repartitionRecords(Dataset<Row> records, int outputSparkPartitions) {
      return records;
    }

    @Override
    public boolean arePartitionRecordsSorted() {
      return false;
    }
  }

  @ParameterizedTest
  @MethodSource("testAutoModifyParquetWriteLegacyFormatParameterParams")
  public void testAutoModifyParquetWriteLegacyFormatParameter(boolean smallDecimal, Boolean propValue, Boolean expectedPropValue) {
    DecimalType decimalType;
    if (smallDecimal) {
      decimalType = DecimalType$.MODULE$.apply(10, 2);
    } else {
      decimalType = DecimalType$.MODULE$.apply(38, 10);
    }

    StructType structType = StructType$.MODULE$.apply(
        Arrays.asList(
            StructField.apply("d1", decimalType, false, Metadata.empty())
        )
    );

    Map<String, String> options = propValue != null
        ? Collections.singletonMap(HoodieStorageConfig.PARQUET_WRITE_LEGACY_FORMAT_ENABLED.key(), String.valueOf(propValue))
        : new HashMap<>();

    tryOverrideParquetWriteLegacyFormatProperty(options, structType);

    Boolean finalPropValue =
        Option.ofNullable(options.get(HoodieStorageConfig.PARQUET_WRITE_LEGACY_FORMAT_ENABLED.key()))
            .map(Boolean::parseBoolean)
            .orElse(null);
    assertEquals(expectedPropValue, finalPropValue);
  }

  private static Stream<Arguments> testAutoModifyParquetWriteLegacyFormatParameterParams() {
    return Arrays.stream(new Object[][] {
        {true, null, true},   {false, null, null},
        {true, false, false}, {true, true, true},
        {false, true, true},  {false, false, false}
    }).map(Arguments::of);
  }
}
