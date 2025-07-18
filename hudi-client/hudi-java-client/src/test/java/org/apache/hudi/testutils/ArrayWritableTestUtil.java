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

package org.apache.hudi.testutils;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ArrayWritableTestUtil {
  public static void assertArrayWritableEqual(Schema schema, ArrayWritable expected, ArrayWritable actual, boolean isPartitioned) {
    assertArrayWritableEqualInternal(schema, expected, actual, isPartitioned);
  }

  private static void assertArrayWritableEqualInternal(Schema schema, Writable expected, Writable actual, boolean ignoreOneExtraCol) {
    switch (schema.getType()) {
      case RECORD: {
        assertInstanceOf(ArrayWritable.class, expected);
        assertInstanceOf(ArrayWritable.class, actual);
        //adjust for fake partition
        int expectedLen = ((ArrayWritable) expected).get().length - (ignoreOneExtraCol ? 1 : 0);
        int actualLen = ((ArrayWritable) actual).get().length;
        assertEquals(expectedLen, actualLen);
        assertEquals(schema.getFields().size(), expectedLen);
        for (Schema.Field field : schema.getFields()) {
          assertArrayWritableEqualInternal(field.schema(), ((ArrayWritable) expected).get()[field.pos()], ((ArrayWritable) actual).get()[field.pos()], false);
        }
        break;
      }
      case ARRAY: {
        assertInstanceOf(ArrayWritable.class, expected);
        assertInstanceOf(ArrayWritable.class, actual);
        int expectedLen = ((ArrayWritable) expected).get().length;
        int actualLen = ((ArrayWritable) actual).get().length;
        assertEquals(expectedLen, actualLen);
        for (int i = 0; i < expectedLen; i++) {
          assertArrayWritableEqualInternal(schema.getElementType(), ((ArrayWritable) expected).get()[i], ((ArrayWritable) expected).get()[i], false);
        }
        break;
      }
      case MAP: {
        assertInstanceOf(ArrayWritable.class, expected);
        assertInstanceOf(ArrayWritable.class, actual);
        int expectedLen = ((ArrayWritable) expected).get().length;
        int actualLen = ((ArrayWritable) actual).get().length;
        assertEquals(expectedLen, actualLen);
        Map<Writable, Writable> expectedMap = new HashMap<>(expectedLen);
        Map<Writable, Writable> actualMap = new HashMap<>(actualLen);
        for (int i = 0; i < expectedLen; i++) {
          Writable expectedKV = ((ArrayWritable) expected).get()[i];
          assertInstanceOf(ArrayWritable.class, expectedKV);
          assertEquals(2, ((ArrayWritable) expectedKV).get().length);
          expectedMap.put(((ArrayWritable) expectedKV).get()[0], ((ArrayWritable) expectedKV).get()[1]);
          Writable actualKV = ((ArrayWritable) actual).get()[i];
          assertInstanceOf(ArrayWritable.class, actualKV);
          assertEquals(2, ((ArrayWritable) actualKV).get().length);
          actualMap.put(((ArrayWritable) actualKV).get()[0], ((ArrayWritable) actualKV).get()[1]);
        }

        for (Writable key : expectedMap.keySet()) {
          Writable expectedValue = expectedMap.get(key);
          assertNotNull(expectedValue);
          Writable actualValue = actualMap.remove(key);
          assertNotNull(actualValue);
          assertArrayWritableEqualInternal(schema.getValueType(), expectedValue, actualValue, false);
        }
        assertEquals(0, actualMap.size());
        break;
      }
      case UNION:
        if (schema.getTypes().size() == 2
            && schema.getTypes().get(0).getType() == Schema.Type.NULL) {
          assertArrayWritableEqualInternal(schema.getTypes().get(1), expected, actual, false);
        } else if (schema.getTypes().size() == 2
            && schema.getTypes().get(1).getType() == Schema.Type.NULL) {
          assertArrayWritableEqualInternal(schema.getTypes().get(0), expected, actual, false);
        } else if (schema.getTypes().size() == 1) {
          assertArrayWritableEqualInternal(schema.getTypes().get(0), expected, actual, false);
        } else {
          throw new IllegalStateException("Union has more than 2 types or one type is not null: " + schema);
        }
        break;

      default:
        assertWritablePrimaryType(schema, expected, actual);
    }
  }

  private static void assertWritablePrimaryType(Schema schema, Writable expected, Writable actual) {
    switch (schema.getType()) {
      case NULL:
        assertInstanceOf(NullWritable.class, expected);
        assertInstanceOf(NullWritable.class, actual);
        assertEquals(expected, actual);
        break;

      case BOOLEAN:
        assertInstanceOf(BooleanWritable.class, expected);
        assertInstanceOf(BooleanWritable.class, actual);
        assertEquals(expected, actual);
        break;

      case INT:
        if (schema.getLogicalType() instanceof LogicalTypes.Date) {
          assertInstanceOf(DateWritable.class, expected);
          assertInstanceOf(DateWritable.class, actual);
        } else {
          assertInstanceOf(IntWritable.class, expected);
          assertInstanceOf(IntWritable.class, actual);
        }
        assertEquals(expected, actual);
        break;

      case LONG:
        assertInstanceOf(LongWritable.class, expected);
        assertInstanceOf(LongWritable.class, actual);
        assertEquals(expected, actual);
        break;

      case FLOAT:
        assertInstanceOf(FloatWritable.class, expected);
        assertInstanceOf(FloatWritable.class, actual);
        assertEquals(expected, actual);
        break;

      case DOUBLE:
        assertInstanceOf(DoubleWritable.class, expected);
        assertInstanceOf(DoubleWritable.class, actual);
        assertEquals(expected, actual);
        break;

      case BYTES:
      case ENUM:
        assertInstanceOf(BytesWritable.class, expected);
        assertInstanceOf(BytesWritable.class, actual);
        assertEquals(expected, actual);
        break;

      case STRING:
        assertInstanceOf(Text.class, expected);
        assertInstanceOf(Text.class, actual);
        assertEquals(expected, actual);
        break;

      case FIXED:
        if (schema.getLogicalType() instanceof LogicalTypes.Decimal) {
          assertInstanceOf(HiveDecimalWritable.class, expected);
          assertInstanceOf(HiveDecimalWritable.class, actual);
        } else {
          assertEquals(expected.getClass(), actual.getClass());
        }
        assertEquals(expected, actual);
        break;

      default:
        assertEquals(expected.getClass(), actual.getClass());
        assertEquals(expected, actual);
    }
  }

  public static void assertArrayWritableMatchesSchema(Schema schema, Writable writable) {
    switch (schema.getType()) {
      case RECORD: {
        assertInstanceOf(ArrayWritable.class, writable);
        ArrayWritable arrayWritable = (ArrayWritable) writable;
        assertEquals(schema.getFields().size(), arrayWritable.get().length);
        for (Schema.Field field : schema.getFields()) {
          assertArrayWritableMatchesSchema(field.schema(), arrayWritable.get()[field.pos()]);
        }
        break;
      }
      case ARRAY: {
        assertInstanceOf(ArrayWritable.class, writable);
        ArrayWritable arrayWritable = (ArrayWritable) writable;
        for (int i = 0; i < arrayWritable.get().length; i++) {
          assertArrayWritableMatchesSchema(schema.getElementType(), arrayWritable.get()[i]);
        }
        break;
      }
      case MAP: {
        assertInstanceOf(ArrayWritable.class, writable);
        ArrayWritable arrayWritable = (ArrayWritable) writable;
        for (int i = 0; i < arrayWritable.get().length; i++) {
          Writable expectedKV = arrayWritable.get()[i];
          assertInstanceOf(ArrayWritable.class, expectedKV);
          ArrayWritable kv = (ArrayWritable) expectedKV;
          assertEquals(2, kv.get().length);
          assertNotNull(kv.get()[0]);
          assertArrayWritableMatchesSchema(schema.getValueType(), kv.get()[1]);
        }
        break;
      }
      case UNION:
        if (schema.getTypes().size() == 2
            && schema.getTypes().get(0).getType() == Schema.Type.NULL) {
          assertArrayWritableMatchesSchema(schema.getTypes().get(1), writable);
        } else if (schema.getTypes().size() == 2
            && schema.getTypes().get(1).getType() == Schema.Type.NULL) {
          assertArrayWritableMatchesSchema(schema.getTypes().get(0), writable);
        } else if (schema.getTypes().size() == 1) {
          assertArrayWritableMatchesSchema(schema.getTypes().get(0), writable);
        } else {
          throw new IllegalStateException("Union has more than 2 types or one type is not null: " + schema);
        }
        break;

      default:
        assertWritablePrimaryTypeMatchesSchema(schema, writable);
    }
  }

  private static void assertWritablePrimaryTypeMatchesSchema(Schema schema, Writable writable) {
    switch (schema.getType()) {
      case NULL:
        assertInstanceOf(NullWritable.class, writable);
        break;

      case BOOLEAN:
        assertInstanceOf(BooleanWritable.class, writable);
        break;

      case INT:
        if (schema.getLogicalType() instanceof LogicalTypes.Date) {
          assertInstanceOf(DateWritable.class, writable);
        } else {
          assertInstanceOf(IntWritable.class, writable);
        }
        break;

      case LONG:
        assertInstanceOf(LongWritable.class, writable);
        break;

      case FLOAT:
        assertInstanceOf(FloatWritable.class, writable);
        break;

      case DOUBLE:
        assertInstanceOf(DoubleWritable.class, writable);
        break;

      case BYTES:
      case ENUM:
        assertInstanceOf(BytesWritable.class, writable);
        break;

      case STRING:
        assertInstanceOf(Text.class, writable);
        break;

      case FIXED:
        if (schema.getLogicalType() instanceof LogicalTypes.Decimal) {
          assertInstanceOf(HiveDecimalWritable.class, writable);
        } else {
          throw new IllegalStateException("Unexpected schema type: " + schema);
        }
        break;

      default:
        throw new IllegalStateException("Unexpected schema type: " + schema);
    }
  }
}
