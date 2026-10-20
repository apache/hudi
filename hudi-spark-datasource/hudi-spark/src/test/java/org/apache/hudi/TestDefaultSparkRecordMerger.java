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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecords;
import org.apache.hudi.common.util.OrderingValues;
import org.apache.hudi.common.util.collection.ArrayComparable;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.Properties;

import static org.apache.hudi.common.table.HoodieTableConfig.ORDERING_FIELDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestDefaultSparkRecordMerger {
  static final String RECORD_KEY_FIELD_NAME = "record_key";
  static final String PARTITION_PATH_FIELD_NAME = "partition_path";
  static final String INT_COLUMN_NAME = "int_column";
  static final String STRING_COLUMN_NAME = "string_column";
  static final String ANY_KEY = "any_key";
  static final String ANY_PARTITION = "any_partition";
  static final String ANY_NAME = "any_name";
  static final String ANY_NAMESPACE = "anh_namespace";
  public static final StructType SPARK_SCHEMA = new StructType(new StructField[] {
      new StructField(HoodieRecord.COMMIT_TIME_METADATA_FIELD, DataTypes.StringType, false, Metadata.empty()),
      new StructField(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, DataTypes.StringType, false, Metadata.empty()),
      new StructField(HoodieRecord.RECORD_KEY_METADATA_FIELD, DataTypes.StringType, false, Metadata.empty()),
      new StructField(HoodieRecord.PARTITION_PATH_METADATA_FIELD, DataTypes.StringType, false, Metadata.empty()),
      new StructField(HoodieRecord.FILENAME_METADATA_FIELD, DataTypes.StringType, false, Metadata.empty()),
      new StructField(RECORD_KEY_FIELD_NAME, DataTypes.StringType, false, Metadata.empty()),
      new StructField(PARTITION_PATH_FIELD_NAME, DataTypes.StringType, false, Metadata.empty()),
      new StructField(INT_COLUMN_NAME, DataTypes.IntegerType, false, Metadata.empty()),
      new StructField(STRING_COLUMN_NAME, DataTypes.StringType, false, Metadata.empty())});
  private final RecordContext<InternalRow> recordContext = SparkFileFormatInternalRecordContext.getFieldAccessorInstance();

  /**
   * If the new record has higher ordering value than old record,
   * then the merged record is the new one.
   */
  @Test
  void testMergerWithNewRecordAccepted() throws IOException {
    HoodieKey key = new HoodieKey(ANY_KEY, ANY_PARTITION);
    Row oldValue = getSpecificValue(key, "001", 1L, "file1", 1, "1");
    Row newValue = getSpecificValue(key, "002", 2L, "file2", 2, "2");
    HoodieSchema schema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(SPARK_SCHEMA, ANY_NAME, ANY_NAMESPACE);
    BufferedRecord<InternalRow> oldRecord = BufferedRecords.fromEngineRecord(InternalRow.apply(oldValue.toSeq()), ANY_KEY, schema, recordContext, Collections.singletonList(INT_COLUMN_NAME), null);
    BufferedRecord<InternalRow> newRecord = BufferedRecords.fromEngineRecord(InternalRow.apply(newValue.toSeq()), ANY_KEY, schema, recordContext, Collections.singletonList(INT_COLUMN_NAME), null);

    DefaultSparkRecordMerger merger = new DefaultSparkRecordMerger();
    TypedProperties props = new TypedProperties();
    props.setProperty(ORDERING_FIELDS.key(), INT_COLUMN_NAME);

    BufferedRecord<InternalRow> merged =
        merger.merge(oldRecord, newRecord, recordContext, props);

    assertEquals(
        InternalRow.apply(newValue.toSeq()),
        merged.getRecord());
  }

  /**
   * If the ordering value of the old record smaller than or equal to that of the new record,
   * the merged record is the old record.
   */
  @Test
  void testMergerWithOldRecordAccepted() throws IOException {
    HoodieKey key = new HoodieKey(ANY_KEY, ANY_PARTITION);
    Row oldValue = getSpecificValue(key, "001", 1L, "file1", 3, "1");
    Row newValue = getSpecificValue(key, "002", 2L, "file2", 2, "2");
    HoodieSchema schema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(SPARK_SCHEMA, ANY_NAME, ANY_NAMESPACE);
    BufferedRecord<InternalRow> oldRecord = BufferedRecords.fromEngineRecord(InternalRow.apply(oldValue.toSeq()), ANY_KEY, schema, recordContext, Collections.singletonList(INT_COLUMN_NAME), null);
    BufferedRecord<InternalRow> newRecord = BufferedRecords.fromEngineRecord(InternalRow.apply(newValue.toSeq()), ANY_KEY, schema, recordContext, Collections.singletonList(INT_COLUMN_NAME), null);

    DefaultSparkRecordMerger merger = new DefaultSparkRecordMerger();
    TypedProperties props = new TypedProperties();
    props.setProperty(ORDERING_FIELDS.key(), INT_COLUMN_NAME);
    BufferedRecord<InternalRow> merged =
        merger.merge(oldRecord, newRecord, recordContext, props);

    assertEquals(
        InternalRow.apply(oldValue.toSeq()),
        merged.getRecord());
  }

  /**
   * If the new record is a delete record, the merged record is empty.
   */
  @Test
  void testMergerWithNewRecordAsDelete() throws IOException {
    HoodieKey key = new HoodieKey(ANY_KEY, ANY_PARTITION);
    Row oldValue = getSpecificValue(key, "001", 1L, "file1", 1, "1");
    HoodieSchema schema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(SPARK_SCHEMA, ANY_NAME, ANY_NAMESPACE);
    BufferedRecord<InternalRow> oldRecord = BufferedRecords.fromEngineRecord(InternalRow.apply(oldValue.toSeq()), ANY_KEY, schema, recordContext, Collections.singletonList(INT_COLUMN_NAME), null);
    BufferedRecord<InternalRow> newRecord = BufferedRecords.createDelete(key.getRecordKey(), OrderingValues.getDefault());

    DefaultSparkRecordMerger merger = new DefaultSparkRecordMerger();
    TypedProperties props = new TypedProperties();
    props.setProperty(ORDERING_FIELDS.key(), INT_COLUMN_NAME);
    BufferedRecord<InternalRow> merged =
        merger.merge(oldRecord, newRecord, recordContext, props);
    assertTrue(merged.isDelete());
  }

  /**
   * If the old record is a delete record, the merged record is the new record.
   */
  @Test
  void testMergerWithOldRecordAsDelete() throws IOException {
    HoodieKey key = new HoodieKey(ANY_KEY, ANY_PARTITION);
    Row newValue = getSpecificValue(key, "001", 1L, "file1", 1, "1");
    HoodieSchema schema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(SPARK_SCHEMA, ANY_NAME, ANY_NAMESPACE);
    BufferedRecord<InternalRow> oldRecord = BufferedRecords.createDelete(key.getRecordKey(), OrderingValues.getDefault());
    BufferedRecord<InternalRow> newRecord = BufferedRecords.fromEngineRecord(InternalRow.apply(newValue.toSeq()), ANY_KEY, schema, recordContext, Collections.singletonList(INT_COLUMN_NAME), null);

    DefaultSparkRecordMerger merger = new DefaultSparkRecordMerger();
    TypedProperties props = new TypedProperties();
    props.setProperty(ORDERING_FIELDS.key(), INT_COLUMN_NAME);
    BufferedRecord<InternalRow> result = merger.merge(oldRecord, newRecord, recordContext, props);
    assertEquals(
        InternalRow.apply(newValue.toSeq()),
        result.getRecord());
  }

  @Test
  void testEmptyRecordOrderingConversion() {
    HoodieUTF8String stringValue = SparkAdapterSupport$.MODULE$.sparkAdapter().getUTF8StringFactory().wrapUTF8String(UTF8String.fromString("060"));
    BigDecimal decimalValue = new BigDecimal("60.25");
    Decimal engineDecimal = Decimal.apply(decimalValue);
    Comparable<?>[] engineValues = {stringValue, UTF8String.fromString("060"), engineDecimal, 60L, null, OrderingValues.getDefault(),
        new ArrayComparable(new Comparable[] {stringValue, engineDecimal, 60L, OrderingValues.getDefault(), OrderingValues.getDefault()})};
    Comparable<?>[] javaValues = {"060", "060", decimalValue, 60L, null, OrderingValues.getDefault(),
        new ArrayComparable(new Comparable[] {"060", decimalValue, 60L, OrderingValues.getDefault(), OrderingValues.getDefault()})};
    Properties props = new Properties();
    String[] orderingFields = {"name", "amount", "ts", "missing", "default"};
    for (int i = 0; i < engineValues.length; i++) {
      HoodieRecord<InternalRow> record = recordContext.constructHoodieRecord(
          new BufferedRecord<>("key", engineValues[i], null, null, HoodieOperation.DELETE), "partition");
      assertEquals(javaValues[i], record.getOrderingValueAsJava(null, props, orderingFields));
      assertSame(engineValues[i], record.getOrderingValue(null, props, orderingFields));
    }
  }

  static Row getSpecificValue(
      HoodieKey key, String commitTime, long seqNo, String filePath, int intValue, String stringValue) {
    Object[] values = new Object[9];
    values[0] = commitTime;
    values[1] = seqNo;
    values[2] = key.getRecordKey();
    values[3] = key.getPartitionPath();
    values[4] = filePath;
    values[5] = key.getRecordKey();
    values[6] = key.getPartitionPath();
    values[7] = intValue;
    values[8] = stringValue;
    return new GenericRow(values);
  }
}
