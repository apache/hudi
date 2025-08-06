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
 * KIND, either express or implied.  See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.hadoop;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.hadoop.utils.ObjectInspectorCache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.math.BigDecimal;
import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class TestHoodieHiveRecord {
  private HoodieHiveRecord hoodieHiveRecord;
  @Mock
  private ObjectInspectorCache mockObjectInspectorCache;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    
    // Create a minimal HoodieHiveRecord instance with mocked dependencies
    HoodieKey key = new HoodieKey("test-key", "test-partition");
    ArrayWritable data = new ArrayWritable(Writable.class, new Writable[]{new Text("test")});
    Schema schema = Schema.create(Schema.Type.STRING);
    
    // Create HoodieHiveRecord with mocked dependencies
    hoodieHiveRecord = new HoodieHiveRecord(key, data, schema, mockObjectInspectorCache);
  }

  @Test
  void testConvertColumnValueForLogicalTypeWithNullValue() {
    Schema dateSchema = Schema.create(Schema.Type.INT);
    LogicalTypes.date().addToSchema(dateSchema);
    
    Object result = hoodieHiveRecord.convertColumnValueForLogicalType(dateSchema, null, true);
    assertNull(result);
  }

  @Test
  void testConvertColumnValueForLogicalTypeWithDate() {
    Schema dateSchema = Schema.create(Schema.Type.INT);
    LogicalTypes.date().addToSchema(dateSchema);
    
    IntWritable dateValue = new IntWritable(18628); // 2021-01-01
    Object result = hoodieHiveRecord.convertColumnValueForLogicalType(dateSchema, dateValue, true);
    
    assertEquals(LocalDate.class, result.getClass());
    assertEquals("2021-01-01", String.valueOf(result));
  }

  @Test
  void testConvertColumnValueForLogicalTypeWithTimestampMillis() {
    Schema timestampMillisSchema = Schema.create(Schema.Type.LONG);
    LogicalTypes.timestampMillis().addToSchema(timestampMillisSchema);
    
    LongWritable timestampValue = new LongWritable(1609459200000L); // 2021-01-01 00:00:00 UTC
    Object result = hoodieHiveRecord.convertColumnValueForLogicalType(timestampMillisSchema, timestampValue, true);
    
    assertEquals(Long.class, result.getClass());
    assertEquals("1609459200000", String.valueOf(result));
  }

  @Test
  void testConvertColumnValueForLogicalTypeWithTimestampMillisDisabled() {
    Schema timestampMillisSchema = Schema.create(Schema.Type.LONG);
    LogicalTypes.timestampMillis().addToSchema(timestampMillisSchema);
    
    LongWritable timestampValue = new LongWritable(1609459200000L);
    Object result = hoodieHiveRecord.convertColumnValueForLogicalType(timestampMillisSchema, timestampValue, false);
    
    // Should return original value when keepConsistentLogicalTimestamp is false
    assertEquals(LongWritable.class, result.getClass());
    assertEquals("1609459200000", String.valueOf(result));
  }

  @Test
  void testConvertColumnValueForLogicalTypeWithTimestampMicros() {
    Schema timestampMicrosSchema = Schema.create(Schema.Type.LONG);
    LogicalTypes.timestampMicros().addToSchema(timestampMicrosSchema);
    
    LongWritable timestampValue = new LongWritable(1609459200000000L); // 2021-01-01 00:00:00 UTC in microseconds
    Object result = hoodieHiveRecord.convertColumnValueForLogicalType(timestampMicrosSchema, timestampValue, true);
    
    assertEquals(Long.class, result.getClass());
    assertEquals("1609459200000", String.valueOf(result));
  }

  @Test
  void testConvertColumnValueForLogicalTypeWithTimestampMicrosDisabled() {
    Schema timestampMicrosSchema = Schema.create(Schema.Type.LONG);
    LogicalTypes.timestampMicros().addToSchema(timestampMicrosSchema);
    
    LongWritable timestampValue = new LongWritable(1609459200000000L);
    Object result = hoodieHiveRecord.convertColumnValueForLogicalType(timestampMicrosSchema, timestampValue, false);
    
    // Should return original value when keepConsistentLogicalTimestamp is false
    assertEquals(LongWritable.class, result.getClass());
    assertEquals("1609459200000000", String.valueOf(result));
  }

  @Test
  void testConvertColumnValueForLogicalTypeWithDecimal() {
    Schema decimalSchema = Schema.create(Schema.Type.BYTES);
    LogicalTypes.decimal(10, 2).addToSchema(decimalSchema);
    
    HiveDecimalWritable decimalValue = new HiveDecimalWritable("123.45");
    Object result = hoodieHiveRecord.convertColumnValueForLogicalType(decimalSchema, decimalValue, true);
    
    assertEquals(BigDecimal.class, result.getClass());
    assertEquals("123.45", String.valueOf(result));
  }

  @Test
  void testConvertColumnValueForLogicalTypeWithString() {
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    
    Text stringValue = new Text("test string");
    Object result = hoodieHiveRecord.convertColumnValueForLogicalType(stringSchema, stringValue, true);
    
    // Should return original value for non-logical types
    assertEquals(Text.class, result.getClass());
    assertEquals("test string", result.toString());
  }

  @Test
  void testConvertColumnValueForLogicalTypeWithIntWritable() {
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    
    IntWritable intValue = new IntWritable(42);
    Object result = hoodieHiveRecord.convertColumnValueForLogicalType(stringSchema, intValue, true);
    
    // Should return original value for non-logical types
    assertEquals(IntWritable.class, result.getClass());
    assertEquals("42", String.valueOf(result));
  }

  @Test
  void testConvertColumnValueForLogicalTypeWithLongWritable() {
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    
    LongWritable longValue = new LongWritable(12345L);
    Object result = hoodieHiveRecord.convertColumnValueForLogicalType(stringSchema, longValue, true);
    
    // Should return original value for non-logical types
    assertEquals(LongWritable.class, result.getClass());
    assertEquals("12345", String.valueOf(result));
  }
}
