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
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.apache.hudi.common.model.HoodieRecord.HoodieRecordType.SPARK;
import static org.apache.hudi.common.table.HoodieTableConfig.PRECOMBINE_FIELD;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

  /**
   * If the input records are not Spark record, it throws.
   */
  @Test
  void testMergerWithAvroRecord() {
    try (HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator(0L)) {
      List<HoodieRecord> records = dataGenerator.generateInserts("001", 2);
      DefaultSparkRecordMerger merger = new DefaultSparkRecordMerger();
      TypedProperties props = new TypedProperties();
      Schema recordSchema = new Schema.Parser().parse(TRIP_EXAMPLE_SCHEMA);
      assertThrows(
          IllegalArgumentException.class,
          () -> merger.merge(records.get(0), recordSchema, records.get(1), recordSchema, props));
    }
  }

  /**
   * If the new record has higher ordering value than old record,
   * then the merged record is the new one.
   */
  @Test
  void testMergerWithNewRecordAccepted() throws IOException {
    HoodieKey key = new HoodieKey(ANY_KEY, ANY_PARTITION);
    Row oldValue = getSpecificValue(key, "001", 1L, "file1", 1, "1");
    Row newValue = getSpecificValue(key, "002", 2L, "file2", 2, "2");
    HoodieRecord<InternalRow> oldRecord =
        new HoodieSparkRecord(InternalRow.apply(oldValue.toSeq()), SPARK_SCHEMA);
    HoodieRecord<InternalRow> newRecord =
        new HoodieSparkRecord(InternalRow.apply(newValue.toSeq()), SPARK_SCHEMA);

    DefaultSparkRecordMerger merger = new DefaultSparkRecordMerger();
    TypedProperties props = new TypedProperties();
    props.setProperty(PRECOMBINE_FIELD.key(), INT_COLUMN_NAME);
    Schema avroSchema = AvroConversionUtils.convertStructTypeToAvroSchema(
        SPARK_SCHEMA, ANY_NAME, ANY_NAMESPACE);
    Option<Pair<HoodieRecord, Schema>> merged =
        merger.merge(oldRecord, avroSchema, newRecord, avroSchema, props);

    assertEquals(
        InternalRow.apply(newValue.toSeq()),
        merged.get().getLeft().getData());
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
    HoodieRecord<InternalRow> oldRecord =
        new HoodieSparkRecord(InternalRow.apply(oldValue.toSeq()), SPARK_SCHEMA);
    HoodieRecord<InternalRow> newRecord =
        new HoodieSparkRecord(InternalRow.apply(newValue.toSeq()), SPARK_SCHEMA);

    DefaultSparkRecordMerger merger = new DefaultSparkRecordMerger();
    TypedProperties props = new TypedProperties();
    props.setProperty(PRECOMBINE_FIELD.key(), INT_COLUMN_NAME);
    Schema avroSchema = AvroConversionUtils.convertStructTypeToAvroSchema(
        SPARK_SCHEMA, ANY_NAME, ANY_NAMESPACE);
    Option<Pair<HoodieRecord, Schema>> r =
        merger.merge(oldRecord, avroSchema, newRecord, avroSchema, props);

    assertEquals(
        InternalRow.apply(oldValue.toSeq()),
        r.get().getLeft().getData());
  }

  /**
   * If the new record is a delete record, the merged record is empty.
   */
  @Test
  void testMergerWithNewRecordAsDelete() throws IOException {
    HoodieKey key = new HoodieKey(ANY_KEY, ANY_PARTITION);
    Row oldValue = getSpecificValue(key, "001", 1L, "file1", 1, "1");
    HoodieRecord<InternalRow> oldRecord =
        new HoodieSparkRecord(InternalRow.apply(oldValue.toSeq()), SPARK_SCHEMA);
    HoodieRecord<InternalRow> newRecord = new HoodieEmptyRecord<>(key, SPARK);

    DefaultSparkRecordMerger merger = new DefaultSparkRecordMerger();
    TypedProperties props = new TypedProperties();
    props.setProperty(PRECOMBINE_FIELD.key(), INT_COLUMN_NAME);
    Schema avroSchema = AvroConversionUtils.convertStructTypeToAvroSchema(
        SPARK_SCHEMA, ANY_NAME, ANY_NAMESPACE);
    Option<Pair<HoodieRecord, Schema>> r =
        merger.merge(oldRecord, avroSchema, newRecord, avroSchema, props);
    assertTrue(r.isEmpty());
  }

  /**
   * If the old record is a delete record, the merged record is the new record.
   */
  @Test
  void testMergerWithOldRecordAsDelete() throws IOException {
    HoodieKey key = new HoodieKey(ANY_KEY, ANY_PARTITION);
    Row newValue = getSpecificValue(key, "001", 1L, "file1", 1, "1");
    HoodieRecord<InternalRow> oldRecord = new HoodieEmptyRecord<>(key, SPARK);
    HoodieRecord<InternalRow> newRecord =
        new HoodieSparkRecord(InternalRow.apply(newValue.toSeq()), SPARK_SCHEMA);

    DefaultSparkRecordMerger merger = new DefaultSparkRecordMerger();
    TypedProperties props = new TypedProperties();
    props.setProperty(PRECOMBINE_FIELD.key(), INT_COLUMN_NAME);
    Schema avroSchema = AvroConversionUtils.convertStructTypeToAvroSchema(
        SPARK_SCHEMA, ANY_NAME, ANY_NAMESPACE);
    Option<Pair<HoodieRecord, Schema>> r =
        merger.merge(oldRecord, avroSchema, newRecord, avroSchema, props);
    assertEquals(
        InternalRow.apply(newValue.toSeq()),
        r.get().getLeft().getData());
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
