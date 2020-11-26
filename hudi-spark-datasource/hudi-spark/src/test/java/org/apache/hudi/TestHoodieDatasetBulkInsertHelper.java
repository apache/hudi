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
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.DataSourceTestUtils;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.avro.Schema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests {@link HoodieDatasetBulkInsertHelper}.
 */
public class TestHoodieDatasetBulkInsertHelper extends HoodieClientTestBase {

  private String schemaStr;
  private Schema schema;
  private StructType structType;

  public TestHoodieDatasetBulkInsertHelper() throws IOException {
    init();
  }

  private void init() throws IOException {
    schemaStr = FileIOUtils.readAsUTFString(getClass().getResourceAsStream("/exampleSchema.txt"));
    schema = DataSourceTestUtils.getStructTypeExampleSchema();
    structType = AvroConversionUtils.convertAvroSchemaToStructType(schema);
  }

  @Test
  public void testBulkInsertHelper() throws IOException {
    HoodieWriteConfig config = getConfigBuilder(schemaStr).withProps(getPropsAllSet()).build();
    List<Row> rows = DataSourceTestUtils.generateRandomRows(10);
    Dataset<Row> dataset = sqlContext.createDataFrame(rows, structType);
    Dataset<Row> result = HoodieDatasetBulkInsertHelper.prepareHoodieDatasetForBulkInsert(sqlContext, config, dataset, "testStructName", "testNamespace");
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
  }

  private Map<String, String> getPropsAllSet() {
    return getProps(true, true, true, true);
  }

  private Map<String, String> getProps(boolean setAll, boolean setKeyGen, boolean setRecordKey, boolean setPartitionPath) {
    Map<String, String> props = new HashMap<>();
    if (setAll) {
      props.put(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY(), "org.apache.hudi.keygen.SimpleKeyGenerator");
      props.put(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "_row_key");
      props.put(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "partition");
    } else {
      if (setKeyGen) {
        props.put(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY(), "org.apache.hudi.keygen.SimpleKeyGenerator");
      }
      if (setRecordKey) {
        props.put(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "_row_key");
      }
      if (setPartitionPath) {
        props.put(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "partition");
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
      HoodieDatasetBulkInsertHelper.prepareHoodieDatasetForBulkInsert(sqlContext, config, dataset, "testStructName", "testNamespace");
      fail("Should have thrown exception");
    } catch (Exception e) {
      // ignore
    }

    config = getConfigBuilder(schemaStr).withProps(getProps(false, false, true, true)).build();
    rows = DataSourceTestUtils.generateRandomRows(10);
    dataset = sqlContext.createDataFrame(rows, structType);
    try {
      HoodieDatasetBulkInsertHelper.prepareHoodieDatasetForBulkInsert(sqlContext, config, dataset, "testStructName", "testNamespace");
      fail("Should have thrown exception");
    } catch (Exception e) {
      // ignore
    }

    config = getConfigBuilder(schemaStr).withProps(getProps(false, true, false, true)).build();
    rows = DataSourceTestUtils.generateRandomRows(10);
    dataset = sqlContext.createDataFrame(rows, structType);
    try {
      HoodieDatasetBulkInsertHelper.prepareHoodieDatasetForBulkInsert(sqlContext, config, dataset, "testStructName", "testNamespace");
      fail("Should have thrown exception");
    } catch (Exception e) {
      // ignore
    }

    config = getConfigBuilder(schemaStr).withProps(getProps(false, true, true, false)).build();
    rows = DataSourceTestUtils.generateRandomRows(10);
    dataset = sqlContext.createDataFrame(rows, structType);
    try {
      HoodieDatasetBulkInsertHelper.prepareHoodieDatasetForBulkInsert(sqlContext, config, dataset, "testStructName", "testNamespace");
      fail("Should have thrown exception");
    } catch (Exception e) {
      // ignore
    }
  }
}
