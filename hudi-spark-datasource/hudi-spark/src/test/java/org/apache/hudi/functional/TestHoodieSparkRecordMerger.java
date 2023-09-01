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

package org.apache.hudi.functional;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.HoodieSparkRecordMerger;
import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.client.utils.SparkRowSerDe;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.testutils.DataSourceTestUtils;

import org.apache.avro.Schema;
import org.apache.spark.sql.Row;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieSparkRecordMerger {
  private static HoodieSparkRecordMerger MERGER = new HoodieSparkRecordMerger();

  private static InternalRow toInternalRow(Row row, StructType schema) {
    SparkRowSerDe encoder = SparkAdapterSupport$.MODULE$.sparkAdapter().createSparkRowSerDe(schema);
    return encoder.serializeRow(row);
  }

  private static HoodieRecord toHoodieRecord(InternalRow row, StructType schema, int i) {
    HoodieKey key = new HoodieKey(String.valueOf(i), String.valueOf(i));
    return new HoodieSparkRecord(key, row, schema, false);
  }

  @Test
  public void testWhenBothSizesAvailable() throws IOException {
    List<Row> oldData = DataSourceTestUtils.generateRandomRows(3);
    List<Row> newData = DataSourceTestUtils.generateRandomRows(3);
    Schema avroSchema = DataSourceTestUtils.getStructTypeExampleSchema();
    StructType scalaSchema = AvroConversionUtils.convertAvroSchemaToStructType(avroSchema);

    TypedProperties props = new TypedProperties();
    props.setPropertyIfNonNull("hoodie.payload.ordering.field", "_row_key");
    for (int i = 0; i < oldData.size(); i++) {
      Option<Pair<HoodieRecord, Schema>> r = MERGER.merge(
          toHoodieRecord(toInternalRow(oldData.get(i), scalaSchema), scalaSchema, i),
          avroSchema,
          toHoodieRecord(toInternalRow(newData.get(i), scalaSchema), scalaSchema, i),
          avroSchema,
          props);
      if (((String) oldData.get(i).get(0)).compareTo((String) newData.get(i).get(0)) > 0) {
        assertEquals(r.get().getLeft().getData(), toInternalRow(oldData.get(i), scalaSchema));
      } else {
        assertEquals(r.get().getLeft().getData(), toInternalRow(newData.get(i), scalaSchema));
      }
    }
  }

  @Test
  public void testWhenOneSideIsEmpty() throws IOException {
    List<Row> data = DataSourceTestUtils.generateRandomRows(3);
    Schema avroSchema = DataSourceTestUtils.getStructTypeExampleSchema();
    StructType scalaSchema = AvroConversionUtils.convertAvroSchemaToStructType(avroSchema);

    TypedProperties props = new TypedProperties();
    props.setPropertyIfNonNull("hoodie.payload.ordering.field", "_row_key");

    // When new data is not given, no data is returned.
    for (int i = 0; i < data.size(); i++) {
      HoodieRecord record = toHoodieRecord(toInternalRow(data.get(i), scalaSchema), scalaSchema, i);
      HoodieRecord emptyRecord = new HoodieEmptyRecord(
          record.getKey(), HoodieOperation.DELETE, 1, HoodieRecord.HoodieRecordType.SPARK);
      Option<Pair<HoodieRecord, Schema>> r = MERGER.merge(
          emptyRecord,
          avroSchema,
          record,
          avroSchema,
          props);
      assertTrue(r.isPresent());
      assertEquals(record, r.get().getLeft());
    }

    // When a delete record is given, return value should be the delete record.
    for (int i = 0; i < data.size(); i++) {
      HoodieRecord record = toHoodieRecord(toInternalRow(data.get(i), scalaSchema), scalaSchema, i);
      HoodieRecord emptyRecord = new HoodieEmptyRecord(
          record.getKey(), HoodieOperation.DELETE, 1, HoodieRecord.HoodieRecordType.SPARK);
      Option<Pair<HoodieRecord, Schema>> r = MERGER.merge(
          record,
          avroSchema,
          emptyRecord,
          avroSchema,
          props);
      assertTrue(r.isPresent());
      assertTrue(r.get().getLeft().isDelete(avroSchema, props));
    }
  }
}
