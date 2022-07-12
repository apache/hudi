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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.execution.bulkinsert.NonSortPartitionerWithRows;
import org.apache.hudi.keygen.ComplexKeyGenerator;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;
import org.apache.hudi.keygen.SimpleKeyGenerator;
import org.apache.hudi.testutils.DataSourceTestUtils;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.avro.Schema;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer$;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.Attribute;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests {@link HoodieDatasetBulkInsertHelper}.
 */
@Tag("functional")
public class TestHoodieDatasetBulkInsertHelper extends HoodieClientTestBase {

  private String schemaStr;
  private transient Schema schema;
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
    structType = AvroConversionUtils.convertAvroSchemaToStructType(schema);
  }

  @Test
  public void testBulkInsertHelperConcurrently() {
    IntStream.range(0, 2).parallel().forEach(i -> {
      if (i % 2 == 0) {
        testBulkInsertHelperFor(SimpleKeyGenerator.class.getName(),
            "_row_key",
            getProps("_row_key", SimpleKeyGenerator.class.getName())
        );
      } else {
        testBulkInsertHelperFor(SimpleKeyGenerator.class.getName(),
            "ts",
            getProps("ts", SimpleKeyGenerator.class.getName())
        );
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
    testBulkInsertHelperFor(keyGenClass, "_row_key", getProps("_row_key", keyGenClass));
  }

  @ParameterizedTest
  @MethodSource("provideKeyGenArgs")
  public void testBulkInsertWithHiveStylePartition(String keyGenClass) {
    Map<String, String> props = getProps("_row_key", keyGenClass);
    props.put(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING().key(), "true");
    testBulkInsertHelperFor(keyGenClass, "_row_key", props);
  }

  private Map<String, String> getProps(String recordKey, String keyGenClass) {
    Map<String, String> props = null;
    if (keyGenClass.equals(SimpleKeyGenerator.class.getName())) {
      props = getPropsAllSet(recordKey);
    } else if (keyGenClass.equals(ComplexKeyGenerator.class.getName())) {
      props = getPropsForComplexKeyGen(recordKey);
    } else { // NonPartitioned key gen
      props = getPropsForNonPartitionedKeyGen(recordKey);
    }
    return props;
  }

  private void testBulkInsertHelperFor(String keyGenClass, String recordKey, Map<String, String> props) {
    HoodieWriteConfig config = getConfigBuilder(schemaStr).withProps(props).combineInput(false, false).build();
    List<Row> rows = DataSourceTestUtils.generateRandomRows(10);
    Dataset<Row> dataset = sqlContext.createDataFrame(rows, structType);
    Dataset<Row> result = HoodieDatasetBulkInsertHelper.prepareHoodieDatasetForBulkInsert(sqlContext, config, dataset, "testStructName",
        "testNamespace", new NonSortPartitionerWithRows(), false, false);
    StructType resultSchema = result.schema();

    assertEquals(result.count(), 10);
    assertEquals(resultSchema.fieldNames().length, structType.fieldNames().length + HoodieRecord.HOODIE_META_COLUMNS.size());

    for (Map.Entry<String, Integer> entry : HoodieRecord.HOODIE_META_COLUMNS_NAME_TO_POS.entrySet()) {
      assertTrue(resultSchema.fieldIndex(entry.getKey()) == entry.getValue());
    }

    boolean isNonPartitioned = keyGenClass.equals(NonpartitionedKeyGenerator.class.getName());
    boolean isComplexKeyGenerator = keyGenClass.equals(ComplexKeyGenerator.class.getName());
    boolean hiveStylePartition = config.isHiveStylePartitioningEnabled();
    result.toJavaRDD().foreach(entry -> {
      String keyShouldBe = isComplexKeyGenerator ? recordKey + ":" + entry.getAs(recordKey).toString() : entry.getAs(recordKey).toString();
      assertEquals(keyShouldBe, entry.get(resultSchema.fieldIndex(HoodieRecord.RECORD_KEY_METADATA_FIELD)).toString());

      String partitionShouldBe;
      if (isNonPartitioned) {
        partitionShouldBe = "";
      } else if (hiveStylePartition) {
        partitionShouldBe = "partition=" + entry.getAs("partition");
      } else {
        partitionShouldBe = entry.getAs("partition").toString();
      }
      assertEquals(partitionShouldBe, entry.get(resultSchema.fieldIndex(HoodieRecord.PARTITION_PATH_METADATA_FIELD)));

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
    Dataset<Row> dataset = sqlContext.createDataFrame(rows, structType);
    Dataset<Row> result = HoodieDatasetBulkInsertHelper.prepareHoodieDatasetForBulkInsertWithoutMetaFields(dataset);
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
            .withPreCombineField("ts").build();
    List<Row> inserts = DataSourceTestUtils.generateRandomRows(10);
    Dataset<Row> toUpdateDataset = sqlContext.createDataFrame(inserts.subList(0, 5), structType);
    List<Row> updates = DataSourceTestUtils.updateRowsWithHigherTs(toUpdateDataset);
    List<Row> rows = new ArrayList<>();
    rows.addAll(inserts);
    rows.addAll(updates);
    Dataset<Row> dataset = sqlContext.createDataFrame(rows, structType);
    Dataset<Row> result = HoodieDatasetBulkInsertHelper.prepareHoodieDatasetForBulkInsert(sqlContext, config, dataset, "testStructName",
        "testNamespace", new NonSortPartitionerWithRows(), false, false);
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

    result.toJavaRDD().foreach(entry -> {
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
          (MapFunction<Row, String>) value -> value.getAs("partition") + "+" + value.getAs("_row_key"), Encoders.STRING())
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

      assertTrue(inputSnapshotDf.except(trimmedOutput).count() == 0);
    } else {
      assertTrue(dataset.except(trimmedOutput).count() == 0);
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
      HoodieDatasetBulkInsertHelper.prepareHoodieDatasetForBulkInsert(sqlContext, config, dataset, "testStructName",
          "testNamespace", new NonSortPartitionerWithRows(), false, false);
      fail("Should have thrown exception");
    } catch (Exception e) {
      // ignore
    }

    config = getConfigBuilder(schemaStr).withProps(getProps(false, false, true, true)).build();
    rows = DataSourceTestUtils.generateRandomRows(10);
    dataset = sqlContext.createDataFrame(rows, structType);
    try {
      HoodieDatasetBulkInsertHelper.prepareHoodieDatasetForBulkInsert(sqlContext, config, dataset, "testStructName",
          "testNamespace", new NonSortPartitionerWithRows(), false, false);
      fail("Should have thrown exception");
    } catch (Exception e) {
      // ignore
    }

    config = getConfigBuilder(schemaStr).withProps(getProps(false, true, false, true)).build();
    rows = DataSourceTestUtils.generateRandomRows(10);
    dataset = sqlContext.createDataFrame(rows, structType);
    try {
      HoodieDatasetBulkInsertHelper.prepareHoodieDatasetForBulkInsert(sqlContext, config, dataset, "testStructName",
          "testNamespace", new NonSortPartitionerWithRows(), false, false);
      fail("Should have thrown exception");
    } catch (Exception e) {
      // ignore
    }

    config = getConfigBuilder(schemaStr).withProps(getProps(false, true, true, false)).build();
    rows = DataSourceTestUtils.generateRandomRows(10);
    dataset = sqlContext.createDataFrame(rows, structType);
    try {
      HoodieDatasetBulkInsertHelper.prepareHoodieDatasetForBulkInsert(sqlContext, config, dataset, "testStructName",
          "testNamespace", new NonSortPartitionerWithRows(), false, false);
      fail("Should have thrown exception");
    } catch (Exception e) {
      // ignore
    }
  }

  private ExpressionEncoder getEncoder(StructType schema) {
    List<Attribute> attributes = JavaConversions.asJavaCollection(schema.toAttributes()).stream()
        .map(Attribute::toAttribute).collect(Collectors.toList());
    return RowEncoder.apply(schema)
        .resolveAndBind(JavaConverters.asScalaBufferConverter(attributes).asScala().toSeq(),
            SimpleAnalyzer$.MODULE$);
  }
}
