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

package org.apache.hudi.utilities.schema;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestLazyCastingIterator {

  private static final String NESTED_COL_SCHEMA = "{\"type\":\"record\", \"name\":\"nested_col\",\"fields\": ["
      + "{\"name\": \"prop1\",\"type\": [\"null\", \"string\"]},{\"name\": \"prop2\", \"type\": \"long\"}]}";

  private static final String EXAMPLE_SCHEMA = "{\"type\": \"record\",\"name\": \"testrec\",\"fields\": [ "
      + "{\"name\": \"timestamp\",\"type\": \"long\"},{\"name\": \"_row_key\", \"type\": \"string\"},"
      + "{\"name\": \"ts_ms\", \"type\": \"string\"},"
      + "{\"name\": \"int_col\", \"type\": [\"null\", \"int\"], \"default\": null },"
      + "{\"name\": \"long_col\", \"type\": [\"null\", \"long\"], \"default\": null },"
      + "{\"name\": \"nested_col\",\"type\": [\"null\", " + NESTED_COL_SCHEMA + "]}"
      + "]}";

  private static final String EXAMPLE_SCHEMA_WITHOUT_NESTED_COL = "{\"type\": \"record\",\"name\": \"testrec\",\"fields\": [ "
      + "{\"name\": \"timestamp\",\"type\": \"long\"},{\"name\": \"_row_key\", \"type\": \"string\"},"
      + "{\"name\": \"ts_ms\", \"type\": \"string\"},"
      + "{\"name\": \"int_col\", \"type\": [\"null\", \"int\"], \"default\": null },"
      + "{\"name\": \"long_col\", \"type\": [\"null\", \"long\"], \"default\": null }"
      + "]}";

  private static final String EXAMPLE_SCHEMA_INT_COL_AS_LONG = "{\"type\": \"record\",\"name\": \"testrec\",\"fields\": [ "
      + "{\"name\": \"timestamp\",\"type\": \"long\"},{\"name\": \"_row_key\", \"type\": \"string\"},"
      + "{\"name\": \"ts_ms\", \"type\": \"string\"},"
      + "{\"name\": \"int_col\", \"type\": [\"null\", \"long\"], \"default\": null },"
      + "{\"name\": \"long_col\", \"type\": [\"null\", \"long\"], \"default\": null },"
      + "{\"name\": \"nested_col\",\"type\": [\"null\", " + NESTED_COL_SCHEMA + "]}"
      + "]}";

  private static final String EXAMPLE_SCHEMA_LONG_COL_AS_INT = "{\"type\": \"record\",\"name\": \"testrec\",\"fields\": [ "
      + "{\"name\": \"timestamp\",\"type\": \"long\"},{\"name\": \"_row_key\", \"type\": \"string\"},"
      + "{\"name\": \"ts_ms\", \"type\": \"string\"},"
      + "{\"name\": \"int_col\", \"type\": [\"null\", \"int\"], \"default\": null },"
      + "{\"name\": \"long_col\", \"type\": [\"null\", \"int\"], \"default\": null },"
      + "{\"name\": \"nested_col\",\"type\": [\"null\", " + NESTED_COL_SCHEMA + "]}"
      + "]}";

  private static final GenericRecord GEN_RECORD_EXAMPLE_WITH_NESTED = getRecordWithExampleSchema();
  private static final GenericRecord GEN_RECORD_EXAMPLE_WITH_NULL_NESTED = getRecordWithExampleSchemaNullNestedCol();
  private static final GenericRecord GEN_RECORD_EXAMPLE_WITHOUT_NESTED = getRecordWithExampleSchemaWithoutNestedCol();
  private static final GenericRecord GEN_RECORD_EXAMPLE_INT_COL_AS_LONG = getRecordWithExampleSchemaIntColAsLong();
  private static final GenericRecord GEN_RECORD_EXAMPLE_LONG_COL_AS_INT = getRecordWithExampleSchemaLongColAsInt();

  @Test
  // no changes to record
  public void testHappyPath() {
    List<GenericRecord> genericRecords = Collections.singletonList(GEN_RECORD_EXAMPLE_WITH_NESTED);
    LazyCastingIterator itr = new LazyCastingIterator(genericRecords.iterator(), EXAMPLE_SCHEMA);
    GenericRecord outGenRec = itr.next();
    assertEquals(genericRecords.get(0), outGenRec);
  }

  @Test
  // data has 1 additional col compared to schema
  public void testDataWithAdditionalCol() {
    List<GenericRecord> genericRecords = Collections.singletonList(GEN_RECORD_EXAMPLE_WITH_NESTED);
    LazyCastingIterator itr = new LazyCastingIterator(genericRecords.iterator(), EXAMPLE_SCHEMA_WITHOUT_NESTED_COL);
    GenericRecord outGenRec = itr.next();
    // data will be equivalent to not having the additional col.
    assertEquals(GEN_RECORD_EXAMPLE_WITHOUT_NESTED, outGenRec);
  }

  @Test
  // data has 1 col missing compared to schema
  public void testDataWithMissingCol() {
    List<GenericRecord> genericRecords = Collections.singletonList(GEN_RECORD_EXAMPLE_WITHOUT_NESTED);
    LazyCastingIterator itr = new LazyCastingIterator(genericRecords.iterator(), EXAMPLE_SCHEMA);
    GenericRecord outGenRec = itr.next();
    assertEquals(GEN_RECORD_EXAMPLE_WITH_NULL_NESTED, outGenRec);
  }

  @Test
  // data has 1 col as int which is long in target schema. should cast w/o issues.
  public void testDataForIntToLongPromotion() {
    List<GenericRecord> genericRecords = Collections.singletonList(GEN_RECORD_EXAMPLE_LONG_COL_AS_INT);
    LazyCastingIterator itr = new LazyCastingIterator(genericRecords.iterator(), EXAMPLE_SCHEMA);
    GenericRecord outGenRec = itr.next();
    assertEquals(GEN_RECORD_EXAMPLE_WITH_NESTED, outGenRec);
  }

  @Test
  // data has 1 col as long which is int in target schema. casting directly should throw exception
  public void testDataForLongToIntPromotion() {
    List<GenericRecord> genericRecords = Collections.singletonList(GEN_RECORD_EXAMPLE_INT_COL_AS_LONG);
    LazyCastingIterator itr = new LazyCastingIterator(genericRecords.iterator(), EXAMPLE_SCHEMA);
    Exception e = assertThrows(RuntimeException.class, () -> {
      itr.next();
    }, "Should error out since long cannot be promoted to int");
    assertTrue(e.getMessage().contains("cannot support rewrite value for schema type: \"int\" since the old schema type is: \"long\""));
  }

  public static GenericRecord getRecordWithExampleSchema() {
    return getRecordWithExampleSchema(getNestedColRecord("val1", 10L));
  }

  public static GenericRecord getRecordWithExampleSchemaIntColAsLong() {
    return getRecordWithExampleSchemaIntColAsLong(getNestedColRecord("val1", 10L));
  }

  public static GenericRecord getRecordWithExampleSchemaLongColAsInt() {
    return getRecordWithExampleSchemaLongColAsInt(getNestedColRecord("val1", 10L));
  }

  public static GenericRecord getRecordWithExampleSchemaNullNestedCol() {
    return getRecordWithExampleSchema(null);
  }

  public static GenericRecord getNestedColRecord(String prop1Value, Long prop2Value) {
    GenericRecord nestedColRecord = new GenericData.Record(new Schema.Parser().parse(NESTED_COL_SCHEMA));
    nestedColRecord.put("prop1", prop1Value);
    nestedColRecord.put("prop2", prop2Value);
    return nestedColRecord;
  }

  public static GenericRecord getRecordWithExampleSchema(GenericRecord nestedColRecord) {
    GenericRecord record = new GenericData.Record(new Schema.Parser().parse(EXAMPLE_SCHEMA));
    record.put("timestamp", 4357686L);
    record.put("_row_key", "key1");
    record.put("ts_ms", "2020-03-21");
    record.put("int_col", 10);
    record.put("long_col", 100L);
    if (nestedColRecord != null) {
      record.put("nested_col", nestedColRecord);
    }
    return record;
  }

  public static GenericRecord getRecordWithExampleSchemaIntColAsLong(GenericRecord nestedColRecord) {
    GenericRecord record = new GenericData.Record(new Schema.Parser().parse(EXAMPLE_SCHEMA_INT_COL_AS_LONG));
    record.put("timestamp", 4357686L);
    record.put("_row_key", "key1");
    record.put("ts_ms", "2020-03-21");
    record.put("int_col", 10L);
    record.put("long_col", 100L);
    if (nestedColRecord != null) {
      record.put("nested_col", nestedColRecord);
    }
    return record;
  }

  public static GenericRecord getRecordWithExampleSchemaLongColAsInt(GenericRecord nestedColRecord) {
    GenericRecord record = new GenericData.Record(new Schema.Parser().parse(EXAMPLE_SCHEMA_LONG_COL_AS_INT));
    record.put("timestamp", 4357686L);
    record.put("_row_key", "key1");
    record.put("ts_ms", "2020-03-21");
    record.put("int_col", 10);
    record.put("long_col", 100);
    if (nestedColRecord != null) {
      record.put("nested_col", nestedColRecord);
    }
    return record;
  }

  public static GenericRecord getRecordWithExampleSchemaWithoutNestedCol() {
    GenericRecord record = new GenericData.Record(new Schema.Parser().parse(EXAMPLE_SCHEMA_WITHOUT_NESTED_COL));
    record.put("timestamp", 4357686L);
    record.put("_row_key", "key1");
    record.put("ts_ms", "2020-03-21");
    record.put("int_col", 10);
    record.put("long_col", 100L);
    return record;
  }

}
