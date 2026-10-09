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

package org.apache.hudi.merge;

import org.apache.hudi.SparkFileFormatInternalRecordContext$;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecords;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Tests {@link SparkRecordMergingUtils#mergePartialRecords}.
 */
class TestSparkRecordMergingUtils {
  // The reader schema of a position-based merge: the last field carries a default value, which a schema converted
  // back from a Spark struct type does not.
  private static final HoodieSchema READER_SCHEMA = HoodieSchema.createRecord("partial_merge_record", "test", null,
      Arrays.asList(
          HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING)),
          HoodieSchemaField.of("ts", HoodieSchema.create(HoodieSchemaType.LONG)),
          HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING)),
          HoodieSchemaField.of("price", HoodieSchema.create(HoodieSchemaType.DOUBLE)),
          HoodieSchemaField.of("_tmp_metadata_row_index", HoodieSchema.create(HoodieSchemaType.LONG), "", -1L)));
  private static final HoodieSchema PRICE_UPDATE_SCHEMA = HoodieSchema.createRecord("partial_merge_record", "test", null,
      Arrays.asList(
          HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING)),
          HoodieSchemaField.of("ts", HoodieSchema.create(HoodieSchemaType.LONG)),
          HoodieSchemaField.of("price", HoodieSchema.create(HoodieSchemaType.DOUBLE))));
  private static final HoodieSchema NAME_UPDATE_SCHEMA = HoodieSchema.createRecord("partial_merge_record", "test", null,
      Arrays.asList(
          HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING)),
          HoodieSchemaField.of("ts", HoodieSchema.create(HoodieSchemaType.LONG)),
          HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING))));

  private RecordContext<InternalRow> recordContext;

  @BeforeEach
  void setUp() {
    recordContext = (RecordContext<InternalRow>) (Object) SparkFileFormatInternalRecordContext$.MODULE$.apply();
  }

  @Test
  void testPartialUpdateMergedIntoFullRecordKeepsReaderSchema() {
    for (int i = 0; i < 10; i++) {
      String key = "key" + i;
      BufferedRecord<InternalRow> base = record(READER_SCHEMA, 1L, key, 1L, "name" + i, 1.5, (long) i);
      BufferedRecord<InternalRow> update = record(PRICE_UPDATE_SCHEMA, 2L, key, 2L, 9.5);

      BufferedRecord<InternalRow> merged = SparkRecordMergingUtils.mergePartialRecords(
          base, READER_SCHEMA, update, PRICE_UPDATE_SCHEMA, READER_SCHEMA, recordContext);

      assertEquals(base.getSchemaId(), merged.getSchemaId());
      assertSame(READER_SCHEMA, recordContext.getSchemaFromBufferRecord(merged));
      assertRow(merged.getRecord(), key, 2L, "name" + i, 9.5, (long) i);
      assertEquals(key, merged.getRecordKey());
    }
  }

  @Test
  void testFullUpdateIsReturnedAsIs() {
    BufferedRecord<InternalRow> older = record(READER_SCHEMA, 1L, "key0", 1L, "name0", 1.5, 0L);
    BufferedRecord<InternalRow> newer = record(READER_SCHEMA, 2L, "key0", 2L, "name1", 2.5, 3L);

    assertSame(newer, SparkRecordMergingUtils.mergePartialRecords(
        older, READER_SCHEMA, newer, READER_SCHEMA, READER_SCHEMA, recordContext));
  }

  @Test
  void testTwoPartialUpdatesMergeIntoUnionOfFields() {
    BufferedRecord<InternalRow> older = record(PRICE_UPDATE_SCHEMA, 1L, "key0", 1L, 9.5);
    BufferedRecord<InternalRow> newer = record(NAME_UPDATE_SCHEMA, 2L, "key0", 2L, "name1");

    BufferedRecord<InternalRow> merged = SparkRecordMergingUtils.mergePartialRecords(
        older, PRICE_UPDATE_SCHEMA, newer, NAME_UPDATE_SCHEMA, READER_SCHEMA, recordContext);

    HoodieSchema mergedSchema = recordContext.getSchemaFromBufferRecord(merged);
    assertNotEquals(READER_SCHEMA, mergedSchema);
    assertEquals(Arrays.asList("id", "ts", "name", "price"), fieldNames(mergedSchema));
    assertRow(merged.getRecord(), "key0", 2L, "name1", 9.5);
  }

  private BufferedRecord<InternalRow> record(HoodieSchema schema, long orderingValue, Object... values) {
    Object[] rowValues = Arrays.stream(values).map(v -> v instanceof String ? UTF8String.fromString((String) v) : v).toArray();
    return BufferedRecords.fromEngineRecord(new GenericInternalRow(rowValues), schema, recordContext, orderingValue, (String) values[0], false);
  }

  private static void assertRow(InternalRow row, Object... expected) {
    assertEquals(expected.length, row.numFields());
    for (int i = 0; i < expected.length; i++) {
      Object actual = row.get(i, null);
      assertEquals(expected[i], actual instanceof UTF8String ? actual.toString() : actual);
    }
  }

  private static List<String> fieldNames(HoodieSchema schema) {
    return schema.getFields().stream().map(HoodieSchemaField::name).collect(Collectors.toList());
  }
}
