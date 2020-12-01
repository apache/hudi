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

package org.apache.hudi;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.execution.bulkinsert.NonSortPartitionerWithRows;
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
import org.junit.jupiter.api.Assertions;
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
        Arguments.of(false, false),
        Arguments.of(true, false),
        Arguments.of(true, true));
  }

  private void init() throws IOException {
    schemaStr = FileIOUtils.readAsUTFString(getClass().getResourceAsStream("/exampleSchema.txt"));
    schema = DataSourceTestUtils.getStructTypeExampleSchema();
    structType = AvroConversionUtils.convertAvroSchemaToStructType(schema);
  }

  @Test
  public void testBulkInsertHelper() throws IOException {
    HoodieWriteConfig config = getConfigBuilder(schemaStr).withProps(getPropsAllSet()).combineBulkInsertInput(false).build();
    List<Row> rows = DataSourceTestUtils.generateRandomRows(10);
    Dataset<Row> dataset = sqlContext.createDataFrame(rows, structType);
    Dataset<Row> result = HoodieDatasetBulkInsertHelper.prepareHoodieDatasetForBulkInsert(sqlContext, config, dataset, "testStructName",
        "testNamespace", new NonSortPartitionerWithRows(), Option.of(new TestPreCombineRow()));
    StructType resultSchema = result.schema();

    assertEquals(result.count(), 10);
    assertEquals(resultSchema.fieldNames().length, structType.fieldNames().length + HoodieRecord.HOODIE_META_COLUMNS.size());

    for (Map.Entry<String, Integer> entry : HoodieRecord.HOODIE_META_COLUMNS_NAME_TO_POS.entrySet()) {
      assertTrue(resultSchema.fieldIndex(entry.getKey()) == entry.getValue());
    }

    int metadataRecordKeyIndex = resultSchema.fieldIndex(HoodieRecord.RECORD_KEY_METADATA_FIELD);
    int metadataParitionPathIndex = resultSchema.fieldIndex(HoodieRecord.PARTITION_PATH_METADATA_FIELD);
    int metadataCommitTimeIndex = resultSchema.fieldIndex(HoodieRecord.COMMIT_TIME_METADATA_FIELD);
    int metadataCommitSeqNoIndex = resultSchema.fieldIndex(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD);
    int metadataFilenameIndex = resultSchema.fieldIndex(HoodieRecord.FILENAME_METADATA_FIELD);

    result.toJavaRDD().foreach(entry -> {
      assertTrue(entry.get(metadataRecordKeyIndex).equals(entry.getAs("_row_key")));
      assertTrue(entry.get(metadataParitionPathIndex).equals(entry.getAs("partition")));
      assertTrue(entry.get(metadataCommitSeqNoIndex).equals(""));
      assertTrue(entry.get(metadataCommitTimeIndex).equals(""));
      assertTrue(entry.get(metadataFilenameIndex).equals(""));
    });

    Dataset<Row> trimmedOutput = result.drop(HoodieRecord.PARTITION_PATH_METADATA_FIELD).drop(HoodieRecord.RECORD_KEY_METADATA_FIELD)
        .drop(HoodieRecord.FILENAME_METADATA_FIELD).drop(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD).drop(HoodieRecord.COMMIT_TIME_METADATA_FIELD);
    assertTrue(dataset.except(trimmedOutput).count() == 0);
  }

  @ParameterizedTest
  @MethodSource("providePreCombineArgs")
  public void testBulkInsertPreCombineRow(boolean enablePreCombine, boolean useDefaultPreCombine) {
    HoodieWriteConfig config = getConfigBuilder(schemaStr).withProps(getPropsAllSet()).combineBulkInsertInput(enablePreCombine).build();
    List<Row> inserts = DataSourceTestUtils.generateRandomRows(10);
    Dataset<Row> toUpdateDataset = sqlContext.createDataFrame(inserts.subList(0, 5), structType);
    List<Row> updates = DataSourceTestUtils.updateRowsWithHigherTs(toUpdateDataset);
    List<Row> rows = new ArrayList<>();
    rows.addAll(inserts);
    rows.addAll(updates);
    Dataset<Row> dataset = sqlContext.createDataFrame(rows, structType);
    //Dataset<Row> result = HoodieDatasetBulkInsertHelper.prepareHoodieDatasetForBulkInsert(sqlContext, config, dataset, "testStructName",
    //  "testNamespace", Option.of(new TestPreCombineRow()));
    Dataset<Row> result = HoodieDatasetBulkInsertHelper.prepareHoodieDatasetForBulkInsert(sqlContext, config, dataset, "testStructName",
        "testNamespace", new NonSortPartitionerWithRows(), enablePreCombine ? (useDefaultPreCombine ? Option.of(new DefaultPreCombineRow("ts")) :
            Option.of(new TestUserDefinedPreCombineRow())) : Option.empty());
    StructType resultSchema = result.schema();

    assertEquals(result.count(), enablePreCombine ? 10 : 15);
    assertEquals(resultSchema.fieldNames().length, structType.fieldNames().length + HoodieRecord.HOODIE_META_COLUMNS.size());

    for (Map.Entry<String, Integer> entry : HoodieRecord.HOODIE_META_COLUMNS_NAME_TO_POS.entrySet()) {
      assertTrue(resultSchema.fieldIndex(entry.getKey()) == entry.getValue());
    }

    int metadataRecordKeyIndex = resultSchema.fieldIndex(HoodieRecord.RECORD_KEY_METADATA_FIELD);
    int metadataParitionPathIndex = resultSchema.fieldIndex(HoodieRecord.PARTITION_PATH_METADATA_FIELD);
    int metadataCommitTimeIndex = resultSchema.fieldIndex(HoodieRecord.COMMIT_TIME_METADATA_FIELD);
    int metadataCommitSeqNoIndex = resultSchema.fieldIndex(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD);
    int metadataFilenameIndex = resultSchema.fieldIndex(HoodieRecord.FILENAME_METADATA_FIELD);

    result.toJavaRDD().foreach(entry -> {
      assertTrue(entry.get(metadataRecordKeyIndex).equals(entry.getAs("_row_key")));
      assertTrue(entry.get(metadataParitionPathIndex).equals(entry.getAs("partition")));
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

  @Test
  public void testNonExistantPreCombineField() {
    HoodieWriteConfig config = getConfigBuilder(schemaStr).withProps(getPropsAllSet()).combineBulkInsertInput(true).build();
    List<Row> rows = DataSourceTestUtils.generateRandomRows(10);
    Dataset<Row> toUpdateDataset = sqlContext.createDataFrame(rows.subList(0, 5), structType);
    List<Row> updates = DataSourceTestUtils.updateRowsWithHigherTs(toUpdateDataset);
    rows.addAll(updates);
    Dataset<Row> dataset = sqlContext.createDataFrame(rows, structType);
    try {
      Dataset<Row> result = HoodieDatasetBulkInsertHelper.prepareHoodieDatasetForBulkInsert(sqlContext, config, dataset, "testStructName",
          "testNamespace", new NonSortPartitionerWithRows(), Option.of(new DefaultPreCombineRow("ts1")));
      result.count();
      Assertions.fail("non existant preCombine field should have thrown exception");
    } catch (Exception e) {
      assertTrue(e.getCause() instanceof IllegalArgumentException);
    }
  }

  class TestUserDefinedPreCombineRow implements PreCombineRow {

    @Override
    public Row combineTwoRows(Row v1, Row v2) {
      long tsV1 = v1.getAs("ts");
      long tsV2 = v2.getAs("ts");
      return (tsV1 >= tsV2) ? v1 : v2;
    }
  }

  private Map<String, String> getPropsAllSet() {
    return getProps(true, true, true, true);
  }

  private Map<String, String> getProps(boolean setAll, boolean setKeyGen, boolean setRecordKey, boolean setPartitionPath) {
    Map<String, String> props = new HashMap<>();
    if (setAll) {
      props.put(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY().key(), "org.apache.hudi.keygen.SimpleKeyGenerator");
      props.put(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY().key(), "_row_key");
      props.put(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY().key(), "partition");
    } else {
      if (setKeyGen) {
        props.put(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY().key(), "org.apache.hudi.keygen.SimpleKeyGenerator");
      }
      if (setRecordKey) {
        props.put(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY().key(), "_row_key");
      }
      if (setPartitionPath) {
        props.put(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY().key(), "partition");
      }
    }
    return props;
  }

  @Test
  public void testNoPropsSet() {
    HoodieWriteConfig config = getConfigBuilder(schemaStr).build();
    List<Row> rows = DataSourceTestUtils.generateRandomRows(10);
    Dataset<Row> dataset = sqlContext.createDataFrame(rows, structType);
    try {
      HoodieDatasetBulkInsertHelper.prepareHoodieDatasetForBulkInsert(sqlContext, config, dataset, "testStructName",
          "testNamespace", new NonSortPartitionerWithRows(), Option.empty());
      fail("Should have thrown exception");
    } catch (Exception e) {
      // ignore
    }

    config = getConfigBuilder(schemaStr).withProps(getProps(false, false, true, true)).build();
    rows = DataSourceTestUtils.generateRandomRows(10);
    dataset = sqlContext.createDataFrame(rows, structType);
    try {
      HoodieDatasetBulkInsertHelper.prepareHoodieDatasetForBulkInsert(sqlContext, config, dataset, "testStructName",
          "testNamespace", new NonSortPartitionerWithRows(), Option.empty());
      fail("Should have thrown exception");
    } catch (Exception e) {
      // ignore
    }

    config = getConfigBuilder(schemaStr).withProps(getProps(false, true, false, true)).build();
    rows = DataSourceTestUtils.generateRandomRows(10);
    dataset = sqlContext.createDataFrame(rows, structType);
    try {
      HoodieDatasetBulkInsertHelper.prepareHoodieDatasetForBulkInsert(sqlContext, config, dataset, "testStructName",
          "testNamespace", new NonSortPartitionerWithRows(), Option.empty());
      fail("Should have thrown exception");
    } catch (Exception e) {
      // ignore
    }

    config = getConfigBuilder(schemaStr).withProps(getProps(false, true, true, false)).build();
    rows = DataSourceTestUtils.generateRandomRows(10);
    dataset = sqlContext.createDataFrame(rows, structType);
    try {
      HoodieDatasetBulkInsertHelper.prepareHoodieDatasetForBulkInsert(sqlContext, config, dataset, "testStructName",
          "testNamespace", new NonSortPartitionerWithRows(), Option.empty());
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
