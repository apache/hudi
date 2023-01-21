/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.keygen;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class TestAutoRecordKeyGenerator {
  private static final long TIME = 1672265446090L;
  private static final Schema SCHEMA;

  static {
    try {
      SCHEMA = new Schema.Parser().parse(TestAutoRecordKeyGenerator.class.getClassLoader().getResourceAsStream("keyless_schema.avsc"));
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Test
  public void createKeyWithoutPartitionColumn() {
    ComplexAvroKeyGenerator keyGenerator = new ComplexAvroKeyGenerator(getKeyGenProperties("", 3));
    GenericRecord record = createRecord("partition1", "value1", 123, 456L, TIME, null);
    String actualForRecord = keyGenerator.getRecordKey(record);
    Assertions.assertEquals("952f0fd4-17b6-3762-b0ea-aa76d36377f1", actualForRecord);
  }

  @Test
  public void createKeyWithPartition() {
    ComplexAvroKeyGenerator keyGenerator = new ComplexAvroKeyGenerator(getKeyGenProperties("integer_field:SIMPLE,partition_field:SIMPLE,nested_struct.doubly_nested:SIMPLE", 3));
    GenericRecord record = createRecord("partition1", "value1", 123, 456L, TIME, null);
    String actualForRecord = keyGenerator.getRecordKey(record);
    Assertions.assertEquals("5c1f9cac-c45d-3b57-9bf7-f745a4bb35c4", actualForRecord);
  }

  @Test
  public void nullFieldsProperlyHandled() {
    ComplexAvroKeyGenerator keyGenerator = new ComplexAvroKeyGenerator(getKeyGenProperties("", 3));
    GenericRecord record = createRecord("partition1", "value1", null, null, null, null);
    String actualForRecord = keyGenerator.getRecordKey(record);
    Assertions.assertEquals("22dee533-e64f-3694-8242-5ec5f25e6d11", actualForRecord);
  }

  @Test
  public void assertOnlySubsetOfFieldsUsed() {
    ComplexAvroKeyGenerator keyGenerator = new ComplexAvroKeyGenerator(getKeyGenProperties("", 3));
    GenericRecord record1 = createRecord("partition1", "value1", 123, 456L, TIME, null);
    String actualForRecord1 = keyGenerator.getRecordKey(record1);
    GenericRecord record2 = createRecord("partition2", "value2", 123, 456L, TIME, null);
    String actualForRecord2 = keyGenerator.getRecordKey(record2);
    Assertions.assertEquals(actualForRecord2, actualForRecord1);
  }

  @Test
  public void numFieldsImpactsKeyGen() {
    ComplexAvroKeyGenerator keyGenerator1 = new ComplexAvroKeyGenerator(getKeyGenProperties("", 3));
    ComplexAvroKeyGenerator keyGenerator2 = new ComplexAvroKeyGenerator(getKeyGenProperties("", 10));
    GenericRecord record = createRecord("partition1", "value1", 123, 456L, TIME, null);
    Assertions.assertNotEquals(keyGenerator1.getRecordKey(record), keyGenerator2.getRecordKey(record));
  }

  @Test
  public void nestedColumnsUsed() {
    ComplexAvroKeyGenerator keyGenerator = new  ComplexAvroKeyGenerator(getKeyGenProperties("", 10));
    GenericRecord record = createRecord("partition1", "value1", 123, 456L, TIME, 20.1);
    String actualForRecord = keyGenerator.getRecordKey(record);
    Assertions.assertEquals("6bbd8811-6ea1-3ef1-840c-f7a51d8f378c", actualForRecord);
  }

  protected GenericRecord createRecord(String partitionField, String stringValue, Integer integerValue, Long longValue, Long timestampValue, Double nestedDouble) {
    GenericRecord nestedRecord = null;
    if (nestedDouble != null) {
      nestedRecord = new GenericRecordBuilder(SCHEMA.getField("nested_struct").schema().getTypes().get(1))
          .set("doubly_nested", nestedDouble)
          .build();
    }

    return new GenericRecordBuilder(SCHEMA)
        .set("partition_field", partitionField)
        .set("string_field", stringValue)
        .set("integer_field", integerValue)
        .set("long_field", longValue)
        .set("timestamp_field", timestampValue)
        .set("nested_struct", nestedRecord)
        .build();
  }

  protected TypedProperties getKeyGenProperties(String partitionPathField, int numFieldsInKeyGen) {
    TypedProperties properties = new TypedProperties();
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), partitionPathField);
    properties.put(KeyGeneratorOptions.NUM_FIELDS_IN_AUTO_RECORDKEY_GENERATION.key(), numFieldsInKeyGen);
    properties.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "");
    properties.put(KeyGeneratorOptions.AUTO_GENERATE_RECORD_KEYS.key(),"true");
    return properties;
  }
}