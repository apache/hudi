/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.functional;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.HoodieDatasetBulkInsertHelper;
import org.apache.hudi.HoodieSchemaConversionUtils;
import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.common.config.TimestampKeyGeneratorConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.PartitionPathEncodeUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.execution.bulkinsert.NonSortPartitionerWithRows;
import org.apache.hudi.io.util.FileIOUtils;
import org.apache.hudi.keygen.BuiltinKeyGenerator;
import org.apache.hudi.keygen.ComplexKeyGenerator;
import org.apache.hudi.keygen.CustomKeyGenerator;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;
import org.apache.hudi.keygen.SimpleKeyGenerator;
import org.apache.hudi.keygen.TimestampBasedKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.testutils.DataSourceTestUtils;
import org.apache.hudi.testutils.HoodieSparkClientTestBase;

import lombok.Getter;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import scala.Tuple2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests {@link HoodieDatasetBulkInsertHelper}.
 */
@Tag("functional")
public class TestHoodieDatasetBulkInsertHelper extends HoodieSparkClientTestBase {

  private String schemaStr;
  private transient HoodieSchema schema;
  private StructType structType;

  public TestHoodieDatasetBulkInsertHelper() throws IOException {
    init();
  }

  /**
   * args for schema evolution test.
   */
  private static Stream<Arguments> providePreCombineArgs() {
    return Stream.of(
        Arguments.of(false),
        Arguments.of(true));
  }

  private void init() throws IOException {
    schemaStr = FileIOUtils.readAsUTFString(getClass().getResourceAsStream("/exampleSchema.txt"));
    schema = DataSourceTestUtils.getStructTypeExampleSchema();
    structType = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(schema);
  }

  @Test
  public void testBulkInsertHelperConcurrently() {
    IntStream.range(0, 2).parallel().forEach(i -> {
      if (i % 2 == 0) {
        testBulkInsertHelperFor(SimpleKeyGenerator.class.getName(), "_row_key");
      } else {
        testBulkInsertHelperFor(SimpleKeyGenerator.class.getName(), "ts");
      }
    });
  }

  private static Stream<Arguments> provideKeyGenArgs() {
    return Stream.of(
        Arguments.of(SimpleKeyGenerator.class.getName()),
        Arguments.of(ComplexKeyGenerator.class.getName()),
        Arguments.of(NonpartitionedKeyGenerator.class.getName()));
  }

  @ParameterizedTest
  @MethodSource("provideKeyGenArgs")
  public void testBulkInsertHelper(String keyGenClass) {
    testBulkInsertHelperFor(keyGenClass, "_row_key");
  }

  private void testBulkInsertHelperFor(String keyGenClass, String recordKeyField) {
    Map<String, String> props = null;
    if (keyGenClass.equals(SimpleKeyGenerator.class.getName())) {
      props = getPropsAllSet(recordKeyField);
    } else if (keyGenClass.equals(ComplexKeyGenerator.class.getName())) {
      props = getPropsForComplexKeyGen(recordKeyField);
    } else { // NonPartitioned key gen
      props = getPropsForNonPartitionedKeyGen(recordKeyField);
    }
    HoodieWriteConfig config = getConfigBuilder(schemaStr).withProps(props).combineInput(false, false).build();
    List<Row> rows = DataSourceTestUtils.generateRandomRows(10);
    Dataset<Row> dataset = sqlContext.createDataFrame(rows, structType);
    Dataset<Row> result = HoodieDatasetBulkInsertHelper.prepareForBulkInsert(dataset, config,
        new HoodieTableConfig(), new NonSortPartitionerWithRows(), "0000000001");
    StructType resultSchema = result.schema();

    assertEquals(result.count(), 10);
    assertEquals(resultSchema.fieldNames().length, structType.fieldNames().length + HoodieRecord.HOODIE_META_COLUMNS.size());

    for (Map.Entry<String, Integer> entry : HoodieRecord.HOODIE_META_COLUMNS_NAME_TO_POS.entrySet()) {
      assertEquals(entry.getValue(), resultSchema.fieldIndex(entry.getKey()));
    }

    boolean isNonPartitionedKeyGen = keyGenClass.equals(NonpartitionedKeyGenerator.class.getName());
    boolean isComplexKeyGen = keyGenClass.equals(ComplexKeyGenerator.class.getName());

    TypedProperties keyGenProperties = new TypedProperties();
    keyGenProperties.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), recordKeyField);
    keyGenProperties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partition");
    ComplexKeyGenerator complexKeyGenerator = new ComplexKeyGenerator(keyGenProperties);

    result.toJavaRDD().foreach(entry -> {
      String recordKey = isComplexKeyGen ? complexKeyGenerator.getRecordKey(entry) : entry.getAs(recordKeyField).toString();
      assertEquals(recordKey, entry.get(resultSchema.fieldIndex(HoodieRecord.RECORD_KEY_METADATA_FIELD)));

      String partitionPath = isNonPartitionedKeyGen ? HoodieTableMetadata.EMPTY_PARTITION_NAME : entry.getAs("partition").toString();
      assertEquals(partitionPath, entry.get(resultSchema.fieldIndex(HoodieRecord.PARTITION_PATH_METADATA_FIELD)));

      assertEquals("", entry.get(resultSchema.fieldIndex(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD)));
      assertEquals("", entry.get(resultSchema.fieldIndex(HoodieRecord.COMMIT_TIME_METADATA_FIELD)));
      assertEquals("", entry.get(resultSchema.fieldIndex(HoodieRecord.FILENAME_METADATA_FIELD)));
    });

    Dataset<Row> trimmedOutput = result.drop(HoodieRecord.PARTITION_PATH_METADATA_FIELD).drop(HoodieRecord.RECORD_KEY_METADATA_FIELD)
        .drop(HoodieRecord.FILENAME_METADATA_FIELD).drop(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD).drop(HoodieRecord.COMMIT_TIME_METADATA_FIELD);
    assertTrue(dataset.except(trimmedOutput).count() == 0);
  }

  @Test
  public void testBulkInsertHelperNoMetaFields() {
    List<Row> rows = DataSourceTestUtils.generateRandomRows(10);
    HoodieWriteConfig config = getConfigBuilder(schemaStr)
        .withProps(getPropsAllSet("_row_key"))
        .withPopulateMetaFields(false)
        .build();
    Dataset<Row> dataset = sqlContext.createDataFrame(rows, structType);
    Dataset<Row> result = HoodieDatasetBulkInsertHelper.prepareForBulkInsert(dataset, config,
        new HoodieTableConfig(), new NonSortPartitionerWithRows(), "000001111");
    StructType resultSchema = result.schema();

    assertEquals(result.count(), 10);
    assertEquals(resultSchema.fieldNames().length, structType.fieldNames().length + HoodieRecord.HOODIE_META_COLUMNS.size());

    for (Map.Entry<String, Integer> entry : HoodieRecord.HOODIE_META_COLUMNS_NAME_TO_POS.entrySet()) {
      assertTrue(resultSchema.fieldIndex(entry.getKey()) == entry.getValue());
    }

    result.toJavaRDD().foreach(entry -> {
      assertTrue(entry.get(resultSchema.fieldIndex(HoodieRecord.RECORD_KEY_METADATA_FIELD)).equals(""));
      assertTrue(entry.get(resultSchema.fieldIndex(HoodieRecord.PARTITION_PATH_METADATA_FIELD)).equals(""));
      assertTrue(entry.get(resultSchema.fieldIndex(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD)).equals(""));
      assertTrue(entry.get(resultSchema.fieldIndex(HoodieRecord.COMMIT_TIME_METADATA_FIELD)).equals(""));
      assertTrue(entry.get(resultSchema.fieldIndex(HoodieRecord.FILENAME_METADATA_FIELD)).equals(""));
    });

    Dataset<Row> trimmedOutput = result.drop(HoodieRecord.PARTITION_PATH_METADATA_FIELD).drop(HoodieRecord.RECORD_KEY_METADATA_FIELD)
        .drop(HoodieRecord.FILENAME_METADATA_FIELD).drop(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD).drop(HoodieRecord.COMMIT_TIME_METADATA_FIELD);
    assertTrue(dataset.except(trimmedOutput).count() == 0);
  }

  @ParameterizedTest
  @MethodSource("providePreCombineArgs")
  public void testBulkInsertPreCombine(boolean enablePreCombine) {
    HoodieWriteConfig config = getConfigBuilder(schemaStr).withProps(getPropsAllSet("_row_key"))
            .combineInput(enablePreCombine, enablePreCombine)
            .build();
    config.setValue(HoodieTableConfig.ORDERING_FIELDS, "ts");
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setValue(HoodieTableConfig.ORDERING_FIELDS, "ts");
    List<Row> inserts = DataSourceTestUtils.generateRandomRows(10);
    Dataset<Row> toUpdateDataset = sqlContext.createDataFrame(inserts.subList(0, 5), structType);
    List<Row> updates = DataSourceTestUtils.updateRowsWithUpdatedTs(toUpdateDataset);
    List<Row> rows = new ArrayList<>();
    rows.addAll(inserts);
    rows.addAll(updates);
    Dataset<Row> dataset = sqlContext.createDataFrame(rows, structType);
    Dataset<Row> result = HoodieDatasetBulkInsertHelper.prepareForBulkInsert(dataset, config,
        tableConfig, new NonSortPartitionerWithRows(), "000001111");
    StructType resultSchema = result.schema();

    assertEquals(result.count(), enablePreCombine ? 10 : 15);
    assertEquals(resultSchema.fieldNames().length, structType.fieldNames().length + HoodieRecord.HOODIE_META_COLUMNS.size());

    for (Map.Entry<String, Integer> entry : HoodieRecord.HOODIE_META_COLUMNS_NAME_TO_POS.entrySet()) {
      assertTrue(resultSchema.fieldIndex(entry.getKey()) == entry.getValue());
    }

    int metadataRecordKeyIndex = resultSchema.fieldIndex(HoodieRecord.RECORD_KEY_METADATA_FIELD);
    int metadataPartitionPathIndex = resultSchema.fieldIndex(HoodieRecord.PARTITION_PATH_METADATA_FIELD);
    int metadataCommitTimeIndex = resultSchema.fieldIndex(HoodieRecord.COMMIT_TIME_METADATA_FIELD);
    int metadataCommitSeqNoIndex = resultSchema.fieldIndex(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD);
    int metadataFilenameIndex = resultSchema.fieldIndex(HoodieRecord.FILENAME_METADATA_FIELD);

    result.toJavaRDD()
        .collect()
        .forEach(entry -> {
          assertTrue(entry.get(metadataRecordKeyIndex).equals(entry.getAs("_row_key")));
          assertTrue(entry.get(metadataPartitionPathIndex).equals(entry.getAs("partition")));
          assertTrue(entry.get(metadataCommitSeqNoIndex).equals(""));
          assertTrue(entry.get(metadataCommitTimeIndex).equals(""));
          assertTrue(entry.get(metadataFilenameIndex).equals(""));
        });

    Dataset<Row> trimmedOutput = result.drop(HoodieRecord.PARTITION_PATH_METADATA_FIELD).drop(HoodieRecord.RECORD_KEY_METADATA_FIELD)
        .drop(HoodieRecord.FILENAME_METADATA_FIELD).drop(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD).drop(HoodieRecord.COMMIT_TIME_METADATA_FIELD);

    // find resolved input snapshot
    ExpressionEncoder encoder = getEncoder(dataset.schema());
    if (enablePreCombine) {
      Dataset<Row> inputSnapshotDf = dataset.groupByKey(
          (MapFunction<Row, String>) value -> value.getAs("partition") + ":" + value.getAs("_row_key"), Encoders.STRING())
          .reduceGroups((ReduceFunction<Row>) (v1, v2) -> {
            long ts1 = v1.getAs("ts");
            long ts2 = v2.getAs("ts");
            if (ts1 >= ts2) {
              return v1;
            } else {
              return v2;
            }
          })
          .map((MapFunction<Tuple2<String, Row>, Row>) value -> value._2, encoder);

      assertEquals(0, inputSnapshotDf.except(trimmedOutput).count());
    } else {
      assertEquals(0, dataset.except(trimmedOutput).count());
    }
  }

  private Map<String, String> getPropsAllSet(String recordKey) {
    return getProps(recordKey, true, true, true, true);
  }

  private Map<String, String> getProps(boolean setAll, boolean setKeyGen, boolean setRecordKey, boolean setPartitionPath) {
    return getProps("_row_key", setAll, setKeyGen, setRecordKey, setPartitionPath);
  }

  private Map<String, String> getProps(String recordKey, boolean setAll, boolean setKeyGen, boolean setRecordKey, boolean setPartitionPath) {
    Map<String, String> props = new HashMap<>();
    if (setAll) {
      props.put(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME().key(), "org.apache.hudi.keygen.SimpleKeyGenerator");
      props.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), recordKey);
      props.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "partition");
      props.put(HoodieWriteConfig.TBL_NAME.key(), recordKey + "_table");
    } else {
      if (setKeyGen) {
        props.put(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME().key(), "org.apache.hudi.keygen.SimpleKeyGenerator");
      }
      if (setRecordKey) {
        props.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), recordKey);
      }
      if (setPartitionPath) {
        props.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "partition");
      }
    }
    return props;
  }

  private Map<String, String> getPropsForComplexKeyGen(String recordKey) {
    Map<String, String> props = new HashMap<>();
    props.put(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME().key(), ComplexKeyGenerator.class.getName());
    props.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), recordKey);
    props.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "partition");
    props.put(HoodieWriteConfig.TBL_NAME.key(), recordKey + "_table");
    return props;
  }

  private Map<String, String> getPropsForNonPartitionedKeyGen(String recordKey) {
    Map<String, String> props = new HashMap<>();
    props.put(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME().key(), NonpartitionedKeyGenerator.class.getName());
    props.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), recordKey);
    props.put(HoodieWriteConfig.TBL_NAME.key(), recordKey + "_table");
    return props;
  }

  @Test
  public void testNoPropsSet() {
    HoodieWriteConfig config = getConfigBuilder(schemaStr).build();
    List<Row> rows = DataSourceTestUtils.generateRandomRows(10);
    Dataset<Row> dataset = sqlContext.createDataFrame(rows, structType);
    try {
      Dataset<Row> preparedDF = HoodieDatasetBulkInsertHelper.prepareForBulkInsert(dataset, config,
          new HoodieTableConfig(), new NonSortPartitionerWithRows(), "000001111");
      preparedDF.count();
      fail("Should have thrown exception");
    } catch (Exception e) {
      // ignore
    }

    config = getConfigBuilder(schemaStr).withProps(getProps(false, false, true, true)).build();
    rows = DataSourceTestUtils.generateRandomRows(10);
    dataset = sqlContext.createDataFrame(rows, structType);
    try {
      Dataset<Row> preparedDF = HoodieDatasetBulkInsertHelper.prepareForBulkInsert(dataset, config,
          new HoodieTableConfig(), new NonSortPartitionerWithRows(), "000001111");
      preparedDF.count();
      fail("Should have thrown exception");
    } catch (Exception e) {
      // ignore
    }

    config = getConfigBuilder(schemaStr).withProps(getProps(false, true, true, false)).build();
    rows = DataSourceTestUtils.generateRandomRows(10);
    dataset = sqlContext.createDataFrame(rows, structType);
    try {
      Dataset<Row> preparedDF = HoodieDatasetBulkInsertHelper.prepareForBulkInsert(dataset, config,
          new HoodieTableConfig(), new NonSortPartitionerWithRows(), "000001111");
      preparedDF.count();
      fail("Should have thrown exception");
    } catch (Exception e) {
      // ignore
    }
  }

  private ExpressionEncoder getEncoder(StructType schema) {
    return SparkAdapterSupport$.MODULE$.sparkAdapter().getCatalystExpressionUtils().getEncoder(schema);
  }

  @Test
  public void testBulkInsertParallelismParam() {
    HoodieWriteConfig config = getConfigBuilder(schemaStr).withProps(getPropsAllSet("_row_key"))
        .combineInput(true, true)
        .build();
    config.setValue(HoodieTableConfig.ORDERING_FIELDS, "ts");
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setValue(HoodieTableConfig.ORDERING_FIELDS, "ts");
    int checkParallelism = 7;
    config.setValue("hoodie.bulkinsert.shuffle.parallelism", String.valueOf(checkParallelism));
    StageCheckBulkParallelismListener stageCheckBulkParallelismListener =
        new StageCheckBulkParallelismListener("org.apache.hudi.HoodieDatasetBulkInsertHelper$.dedupeRows");
    sqlContext.sparkContext().addSparkListener(stageCheckBulkParallelismListener);
    List<Row> inserts = DataSourceTestUtils.generateRandomRows(10);
    Dataset<Row> dataset = sqlContext.createDataFrame(inserts, structType).repartition(3);
    assertNotEquals(checkParallelism, SparkAdapterSupport$.MODULE$.sparkAdapter().getUnsafeUtils().getNumPartitions(dataset));
    assertNotEquals(checkParallelism, sqlContext.sparkContext().defaultParallelism());
    Dataset<Row> result = HoodieDatasetBulkInsertHelper.prepareForBulkInsert(dataset, config,
        tableConfig, new NonSortPartitionerWithRows(), "000001111");
    // trigger job
    result.count();
    assertEquals(checkParallelism, stageCheckBulkParallelismListener.getParallelism());
    sqlContext.sparkContext().removeSparkListener(stageCheckBulkParallelismListener);
  }

  private static Stream<Arguments> provideKeyGenParityArgs() {
    // Covers every keygen + partition-formatter combo the dispatcher in
    // HoodieDatasetBulkInsertHelper#buildKeygenColumns can land on. SimpleKeyGenerator cases
    // exercise hive-style / url-encode / slash-separated date flags. Tier 2 covers only the
    // {default, hive-style} subset for SimpleKeyGen; url-encode and slash-separated push to Tier 3.
    return Stream.of(
        // Tier 1
        Arguments.of("nonpartitioned-tier1", NonpartitionedKeyGenerator.class.getName(), "_row_key", "", false, false, false),
        // Tier 2
        Arguments.of("simple-default-tier2", SimpleKeyGenerator.class.getName(), "_row_key", "partition", false, false, false),
        Arguments.of("simple-hive-tier2", SimpleKeyGenerator.class.getName(), "_row_key", "partition", true, false, false),
        // Tier 3 fallbacks: Simple under url-encode / slash-sep, plus every other keygen class.
        Arguments.of("simple-slash-tier3", SimpleKeyGenerator.class.getName(), "_row_key", "partition", false, true, false),
        Arguments.of("simple-hive-slash-tier3", SimpleKeyGenerator.class.getName(), "_row_key", "partition", true, true, false),
        Arguments.of("simple-url-tier3", SimpleKeyGenerator.class.getName(), "_row_key", "partition", false, false, true),
        Arguments.of("simple-hive-url-tier3", SimpleKeyGenerator.class.getName(), "_row_key", "partition", true, false, true),
        Arguments.of("complex-single-tier3", ComplexKeyGenerator.class.getName(), "_row_key", "partition", false, false, false),
        Arguments.of("complex-multi-tier3", ComplexKeyGenerator.class.getName(), "_row_key,ts", "partition,_hoodie_is_deleted", false, false, false),
        Arguments.of("timestamp-based-tier3", TimestampBasedKeyGenerator.class.getName(), "_row_key", "ts", false, false, false),
        Arguments.of("custom-tier3", CustomKeyGenerator.class.getName(), "_row_key", "partition:SIMPLE", false, false, false));
  }

  /**
   * Asserts that record-key / partition-path values produced by {@link HoodieDatasetBulkInsertHelper}
   * match those produced by the canonical Avro path ({@link BuiltinKeyGenerator#getRecordKey(GenericRecord)}).
   *
   * <p>The Avro path is the ground truth shared by read- and write-side keygen invocations, so parity
   * against it (rather than RDD-vs-UDF parity) is what actually matters for correctness.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("provideKeyGenParityArgs")
  public void testKeyGenParityAgainstAvroGroundTruth(String label,
                                                     String keyGenClass,
                                                     String recordKeyFields,
                                                     String partitionPathFields,
                                                     boolean hiveStylePartitioning,
                                                     boolean slashSepPartitioning,
                                                     boolean urlEncodePartitioning) {
    Map<String, String> props = new HashMap<>();
    props.put(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME().key(), keyGenClass);
    props.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), recordKeyFields);
    props.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), partitionPathFields);
    props.put(HoodieWriteConfig.TBL_NAME.key(), label + "_parity_table");
    props.put(KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE.key(), String.valueOf(hiveStylePartitioning));
    props.put(KeyGeneratorOptions.SLASH_SEPARATED_DATE_PARTITIONING.key(), String.valueOf(slashSepPartitioning));
    props.put(KeyGeneratorOptions.URL_ENCODE_PARTITIONING.key(), String.valueOf(urlEncodePartitioning));
    if (keyGenClass.equals(TimestampBasedKeyGenerator.class.getName())) {
      props.put(TimestampKeyGeneratorConfig.TIMESTAMP_TYPE_FIELD.key(), "EPOCHMILLISECONDS");
      props.put(TimestampKeyGeneratorConfig.TIMESTAMP_OUTPUT_DATE_FORMAT.key(), "yyyy-MM-dd");
      props.put(TimestampKeyGeneratorConfig.TIMESTAMP_TIMEZONE_FORMAT.key(), "UTC");
    }
    HoodieWriteConfig config = getConfigBuilder(schemaStr).withProps(props).combineInput(false, false).build();

    List<Row> rows = DataSourceTestUtils.generateRandomRows(20);
    Dataset<Row> dataset = sqlContext.createDataFrame(rows, structType);
    Dataset<Row> result = HoodieDatasetBulkInsertHelper.prepareForBulkInsert(dataset, config,
        new HoodieTableConfig(), new NonSortPartitionerWithRows(), "000000001");

    int recordKeyIdx = result.schema().fieldIndex(HoodieRecord.RECORD_KEY_METADATA_FIELD);
    int partitionPathIdx = result.schema().fieldIndex(HoodieRecord.PARTITION_PATH_METADATA_FIELD);
    int rowKeyIdx = result.schema().fieldIndex("_row_key");

    Map<String, String[]> actual = new HashMap<>();
    for (Row r : result.collectAsList()) {
      actual.put(r.getString(rowKeyIdx),
          new String[] {r.getString(recordKeyIdx), r.getString(partitionPathIdx)});
    }

    TypedProperties keyGenProps = new TypedProperties();
    keyGenProps.putAll(props);
    BuiltinKeyGenerator groundTruthKeyGen =
        (BuiltinKeyGenerator) org.apache.hudi.common.util.ReflectionUtils.loadClass(keyGenClass, keyGenProps);
    scala.Function1<Row, GenericRecord> toAvro = AvroConversionUtils.createConverterToAvro(
        structType, "trip", "example.schema");

    assertEquals(rows.size(), actual.size(), "Row count mismatch — possible duplicate record keys");
    for (Row inputRow : rows) {
      GenericRecord avro = toAvro.apply(inputRow);
      String expectedRecordKey = groundTruthKeyGen.getRecordKey(avro);
      String expectedPartitionPath = groundTruthKeyGen.getPartitionPath(avro);
      String[] observed = actual.get(inputRow.getString(0));
      assertEquals(expectedRecordKey, observed[0],
          "record key mismatch for keygen=" + label + " row=" + inputRow.getString(0));
      assertEquals(expectedPartitionPath, observed[1],
          "partition path mismatch for keygen=" + label + " row=" + inputRow.getString(0));
    }
  }

  /**
   * Tier 1 / Tier 2 fast paths use {@code col(field).cast(String)} for the record key. Confirms that
   * a non-string record-key column (e.g. {@code ts: long}) is materialised as the string form of the
   * underlying value, matching the canonical keygen output.
   */
  @Test
  public void testFastPathCastsNonStringRecordKey() {
    // Tier 1: Nonpartitioned, single record-key field. Using `ts` (long) as the record key.
    Map<String, String> nonpartitionedProps = new HashMap<>();
    nonpartitionedProps.put(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME().key(), NonpartitionedKeyGenerator.class.getName());
    nonpartitionedProps.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "ts");
    nonpartitionedProps.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "");
    nonpartitionedProps.put(HoodieWriteConfig.TBL_NAME.key(), "nonpartitioned_cast_tbl");
    assertFastPathRecordKeyCast(nonpartitionedProps, /* expectedPartitionPath */ "");

    // Tier 2: Simple, single record-key + partition-path. Using `ts` (long) as the record key.
    Map<String, String> simpleProps = new HashMap<>();
    simpleProps.put(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME().key(), SimpleKeyGenerator.class.getName());
    simpleProps.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "ts");
    simpleProps.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "partition");
    simpleProps.put(HoodieWriteConfig.TBL_NAME.key(), "simple_cast_tbl");
    assertFastPathRecordKeyCast(simpleProps, /* expectedPartitionPath */ null);
  }

  private void assertFastPathRecordKeyCast(Map<String, String> props, String expectedPartitionPath) {
    HoodieWriteConfig config = getConfigBuilder(schemaStr).withProps(props).combineInput(false, false).build();
    List<Row> rows = DataSourceTestUtils.generateRandomRows(10);
    Dataset<Row> dataset = sqlContext.createDataFrame(rows, structType);
    Dataset<Row> result = HoodieDatasetBulkInsertHelper.prepareForBulkInsert(dataset, config,
        new HoodieTableConfig(), new NonSortPartitionerWithRows(), "000000001");

    int recordKeyIdx = result.schema().fieldIndex(HoodieRecord.RECORD_KEY_METADATA_FIELD);
    int partitionPathIdx = result.schema().fieldIndex(HoodieRecord.PARTITION_PATH_METADATA_FIELD);
    int tsIdx = result.schema().fieldIndex("ts");
    int partitionIdx = result.schema().fieldIndex("partition");

    for (Row r : result.collectAsList()) {
      Object tsVal = r.get(tsIdx);
      assertEquals(String.valueOf(tsVal), r.getString(recordKeyIdx),
          "record key should be string form of ts column");
      String expected = expectedPartitionPath != null ? expectedPartitionPath : String.valueOf(r.get(partitionIdx));
      assertEquals(expected, r.getString(partitionPathIdx),
          "partition path mismatch");
    }
  }

  /**
   * Sanity check that {@link HoodieDatasetBulkInsertHelper} composes the Catalyst plan from
   * pure column projections for Tier 1 and Tier 2 paths — no UDFs, no toRdd round-trip — so the
   * fast paths actually benefit from Catalyst codegen. We verify this by inspecting the resulting
   * Dataset's logical plan for absence of {@code ScalaUDF}/{@code UserDefinedFunction} nodes.
   */
  @Test
  public void testFastPathAvoidsUdf() {
    // Tier 1
    Map<String, String> tier1Props = new HashMap<>();
    tier1Props.put(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME().key(), NonpartitionedKeyGenerator.class.getName());
    tier1Props.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key");
    tier1Props.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "");
    tier1Props.put(HoodieWriteConfig.TBL_NAME.key(), "tier1_plan_tbl");
    assertNoScalaUdfInPlan(tier1Props, "tier1");

    // Tier 2
    Map<String, String> tier2Props = new HashMap<>();
    tier2Props.put(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME().key(), SimpleKeyGenerator.class.getName());
    tier2Props.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key");
    tier2Props.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "partition");
    tier2Props.put(HoodieWriteConfig.TBL_NAME.key(), "tier2_plan_tbl");
    assertNoScalaUdfInPlan(tier2Props, "tier2");
  }

  private void assertNoScalaUdfInPlan(Map<String, String> props, String label) {
    HoodieWriteConfig config = getConfigBuilder(schemaStr).withProps(props).combineInput(false, false).build();
    List<Row> rows = DataSourceTestUtils.generateRandomRows(5);
    Dataset<Row> dataset = sqlContext.createDataFrame(rows, structType);
    Dataset<Row> result = HoodieDatasetBulkInsertHelper.prepareForBulkInsert(dataset, config,
        new HoodieTableConfig(), new NonSortPartitionerWithRows(), "000000001");
    String plan = result.queryExecution().analyzed().toString();
    assertTrue(!plan.toLowerCase().contains("scalaudf"),
        label + " path should not contain ScalaUDF in its logical plan; plan was:\n" + plan);
  }

  /**
   * The Tier 2 partition-path projection must substitute {@code __HIVE_DEFAULT_PARTITION__} for
   * empty/null partition values, matching {@link org.apache.hudi.keygen.StringPartitionPathFormatter#handleEmpty}.
   * Runs the case under both default and hive-style formatter flags, and against an injected
   * row whose {@code partition} value is the empty string.
   *
   * <p>The slash-separated formatter branch deliberately skips {@code handleEmpty} (per
   * {@code PartitionPathFormatterBase#combine}), so we don't exercise that combo here -- the
   * cross-flag parity test already covers it against the canonical Avro path.
   */
  @Test
  public void testTier2EmptyPartitionValueSubstitutedWithHiveDefault() {
    Row emptyPartitionRow = RowFactory.create("rk-empty", "", 1700000000000L, false);

    Map<String, String> defaultProps = new HashMap<>();
    defaultProps.put(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME().key(), SimpleKeyGenerator.class.getName());
    defaultProps.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key");
    defaultProps.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "partition");
    defaultProps.put(HoodieWriteConfig.TBL_NAME.key(), "empty_partition_default_tbl");
    assertEmptyPartitionSubstituted(defaultProps, emptyPartitionRow,
        PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH);

    Map<String, String> hiveProps = new HashMap<>(defaultProps);
    hiveProps.put(HoodieWriteConfig.TBL_NAME.key(), "empty_partition_hive_tbl");
    hiveProps.put(KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE.key(), "true");
    assertEmptyPartitionSubstituted(hiveProps, emptyPartitionRow,
        "partition=" + PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH);
  }

  private void assertEmptyPartitionSubstituted(Map<String, String> props,
                                               Row inputRow,
                                               String expectedPartitionPath) {
    HoodieWriteConfig config = getConfigBuilder(schemaStr).withProps(props).combineInput(false, false).build();
    Dataset<Row> dataset = sqlContext.createDataFrame(java.util.Collections.singletonList(inputRow), structType);
    Dataset<Row> result = HoodieDatasetBulkInsertHelper.prepareForBulkInsert(dataset, config,
        new HoodieTableConfig(), new NonSortPartitionerWithRows(), "000000001");

    Row out = result.collectAsList().get(0);
    int partitionPathIdx = result.schema().fieldIndex(HoodieRecord.PARTITION_PATH_METADATA_FIELD);
    assertEquals(expectedPartitionPath, out.getString(partitionPathIdx),
        "empty partition value should be substituted with " + PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH);
  }

  /**
   * Guards against a UDF-path-only failure mode: the RDD path threaded the driver's {@link SQLConf}
   * onto executor tasks via {@code injectSQLConf}; the UDF path does not. If any keygen reads
   * {@code spark.sql.session.timeZone} during execution (e.g. when converting {@link java.sql.Timestamp}
   * values through Catalyst), executor-local conf would silently override the driver's choice.
   *
   * <p>This test forces {@code spark.sql.session.timeZone} to a non-default value, runs the helper, and
   * asserts the resulting keys match the Avro ground truth computed under the same timezone. A divergence
   * here would indicate the UDF path is sensitive to executor JVM defaults.
   */
  @Test
  public void testUdfPathRespectsDriverSessionTimezone() {
    String originalTz = sqlContext.getConf("spark.sql.session.timeZone", "UTC");
    sqlContext.setConf("spark.sql.session.timeZone", "America/Los_Angeles");
    try {
      Map<String, String> props = new HashMap<>();
      props.put(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME().key(), TimestampBasedKeyGenerator.class.getName());
      props.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key");
      props.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "ts");
      props.put(HoodieWriteConfig.TBL_NAME.key(), "tz_parity_table");
      props.put(TimestampKeyGeneratorConfig.TIMESTAMP_TYPE_FIELD.key(), "EPOCHMILLISECONDS");
      props.put(TimestampKeyGeneratorConfig.TIMESTAMP_OUTPUT_DATE_FORMAT.key(), "yyyy-MM-dd-HH");
      props.put(TimestampKeyGeneratorConfig.TIMESTAMP_TIMEZONE_FORMAT.key(), "America/Los_Angeles");

      HoodieWriteConfig config = getConfigBuilder(schemaStr).withProps(props).combineInput(false, false).build();
      List<Row> rows = DataSourceTestUtils.generateRandomRows(10);
      Dataset<Row> dataset = sqlContext.createDataFrame(rows, structType);
      Dataset<Row> result = HoodieDatasetBulkInsertHelper.prepareForBulkInsert(dataset, config,
          new HoodieTableConfig(), new NonSortPartitionerWithRows(), "000000001");

      int partitionPathIdx = result.schema().fieldIndex(HoodieRecord.PARTITION_PATH_METADATA_FIELD);
      int rowKeyIdx = result.schema().fieldIndex("_row_key");
      Map<String, String> actualPartition = new HashMap<>();
      for (Row r : result.collectAsList()) {
        actualPartition.put(r.getString(rowKeyIdx), r.getString(partitionPathIdx));
      }

      TypedProperties keyGenProps = new TypedProperties();
      keyGenProps.putAll(props);
      BuiltinKeyGenerator groundTruth =
          (BuiltinKeyGenerator) org.apache.hudi.common.util.ReflectionUtils.loadClass(
              TimestampBasedKeyGenerator.class.getName(), keyGenProps);
      scala.Function1<Row, GenericRecord> toAvro = AvroConversionUtils.createConverterToAvro(
          structType, "trip", "example.schema");

      for (Row inputRow : rows) {
        GenericRecord avro = toAvro.apply(inputRow);
        assertEquals(groundTruth.getPartitionPath(avro), actualPartition.get(inputRow.getString(0)),
            "partition path diverged for row " + inputRow.getString(0) + " under LA timezone");
      }
    } finally {
      sqlContext.setConf("spark.sql.session.timeZone", originalTz);
    }
  }

  class StageCheckBulkParallelismListener extends SparkListener {

    private boolean checkFlag = false;
    private String checkMessage;
    @Getter
    private int parallelism;

    StageCheckBulkParallelismListener(String checkMessage) {
      this.checkMessage = checkMessage;
    }

    @Override
    public void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {
      if (checkFlag) {
        // dedup next stage is reduce task
        this.parallelism = stageSubmitted.stageInfo().numTasks();
        checkFlag = false;
      }
      if (stageSubmitted.stageInfo().details().contains(checkMessage)) {
        checkFlag = true;
      }
    }
  }
}
