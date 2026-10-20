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

package org.apache.hudi.common.model;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.OrderingValueJavaTypeConverter;
import org.apache.hudi.common.util.OrderingValues;
import org.apache.hudi.common.util.SerializationUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHoodieEmptyRecord {

  @ParameterizedTest
  @MethodSource("orderedDeletes")
  void testKeyCopyPreservesDeleteOrdering(HoodieOperation operation, Comparable<?> orderingValue,
                                         HoodieRecord.HoodieRecordType recordType) {
    HoodieKey originalKey = new HoodieKey("old-key", "old-partition");
    HoodieKey newKey = new HoodieKey("new-key", "new-partition");
    Properties props = new Properties();
    String[] orderingFields = {"ts"};
    HoodieEmptyRecord<Object> original = new HoodieEmptyRecord<>(originalKey, operation, orderingValue, recordType);
    assertEquals(orderingValue, original.getOrderingValue(null, props, orderingFields));

    HoodieRecord<Object> copy = original.newInstance(newKey);

    assertAll(
        () -> assertNotSame(original, copy),
        () -> assertEquals(newKey, copy.getKey()),
        () -> assertEquals(originalKey, original.getKey()),
        () -> assertEquals(operation, copy.getOperation()),
        () -> assertEquals(orderingValue, copy.getOrderingValue(null, props, orderingFields)),
        () -> assertEquals(recordType, copy.getRecordType()),
        () -> assertTrue(copy.isDelete(null, props)),
        () -> assertNull(copy.getData()));
  }

  @Test
  void testConverterSurvivesCopiesAndSerialization() throws Exception {
    HoodieKey key = new HoodieKey("key", "partition");
    Properties props = new Properties();
    props.setProperty("prefix", "java-");
    String[] orderingFields = {"ts"};
    HoodieEmptyRecord<Object> original = new HoodieEmptyRecord<>(key, HoodieOperation.DELETE, "engine",
        HoodieRecord.HoodieRecordType.SPARK, TestConverter.INSTANCE);
    HoodieEmptyRecord<Object> second = new HoodieEmptyRecord<>(key, HoodieOperation.DELETE, "engine",
        HoodieRecord.HoodieRecordType.SPARK, TestConverter.INSTANCE);
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
      output.writeObject(original);
    }
    HoodieRecord<?> javaCopy;
    try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      javaCopy = (HoodieRecord<?>) input.readObject();
    }
    HoodieRecord<?> kryoCopy = SerializationUtils.deserialize(SerializationUtils.serialize(original));
    Field converter = HoodieEmptyRecord.class.getDeclaredField("orderingValueJavaTypeConverter");
    converter.setAccessible(true);
    for (HoodieRecord<?> record : Arrays.asList(original, second, original.newInstance(key),
        original.newInstance(key, HoodieOperation.UPDATE_BEFORE), javaCopy, kryoCopy)) {
      assertSame(TestConverter.INSTANCE, converter.get(record));
      assertEquals("java-ts-engine", record.getOrderingValueAsJava(null, props, orderingFields));
      assertEquals("engine", record.getOrderingValue(null, props, orderingFields));
    }
  }

  @Test
  void testLegacyConstructorsKeepIdentityConversion() throws Exception {
    HoodieKey key = new HoodieKey("key", "partition");
    for (Comparable<?> value : Arrays.asList("value", OrderingValues.getDefault(), null)) {
      HoodieEmptyRecord<Object> original = new HoodieEmptyRecord<>(key, HoodieOperation.DELETE, value, HoodieRecord.HoodieRecordType.AVRO);
      assertSame(value, original.getOrderingValueAsJava(null, new Properties(), new String[] {"ts"}));
      HoodieRecord<?> restored = SerializationUtils.deserialize(SerializationUtils.serialize(original));
      assertEquals(value, restored.getOrderingValueAsJava(null, new Properties(), new String[] {"ts"}));
    }
    HoodieEmptyRecord<Object> naturalOrder = new HoodieEmptyRecord<>(key, HoodieRecord.HoodieRecordType.AVRO);
    assertEquals(OrderingValues.getDefault(), naturalOrder.getOrderingValueAsJava(null, new Properties(), new String[0]));
  }

  private enum TestConverter implements OrderingValueJavaTypeConverter {
    INSTANCE;

    @Override
    public Comparable<?> convert(Comparable<?> orderingValue, HoodieSchema schema, Properties props, String[] orderingFields) {
      return props.getProperty("prefix") + orderingFields[0] + "-" + orderingValue;
    }
  }

  private static Stream<Arguments> orderedDeletes() {
    return Stream.of(
        Arguments.of(HoodieOperation.DELETE, 60L, HoodieRecord.HoodieRecordType.FLINK),
        Arguments.of(HoodieOperation.UPDATE_BEFORE, 60L, HoodieRecord.HoodieRecordType.FLINK),
        Arguments.of(HoodieOperation.DELETE, "060", HoodieRecord.HoodieRecordType.AVRO),
        Arguments.of(HoodieOperation.UPDATE_BEFORE, "060", HoodieRecord.HoodieRecordType.SPARK),
        Arguments.of(HoodieOperation.DELETE, OrderingValues.getDefault(), HoodieRecord.HoodieRecordType.HIVE),
        Arguments.of(HoodieOperation.UPDATE_BEFORE, OrderingValues.getDefault(), HoodieRecord.HoodieRecordType.FLINK));
  }

  @ParameterizedTest
  @EnumSource(HoodieRecord.HoodieRecordType.class)
  void testKeyCopyPreservesNaturalOrderDelete(HoodieRecord.HoodieRecordType recordType) {
    HoodieEmptyRecord<Object> original = new HoodieEmptyRecord<>(new HoodieKey("key", "partition"), recordType);
    HoodieKey newKey = new HoodieKey("key", "new-partition");
    Properties props = new Properties();

    HoodieRecord<Object> copy = original.newInstance(newKey);

    assertEquals(newKey, copy.getKey());
    assertEquals(recordType, copy.getRecordType());
    assertNull(copy.getOperation());
    assertTrue(OrderingValues.isDefault(copy.getOrderingValue(null, props, new String[0])));
    assertTrue(copy.isDelete(null, props));
  }
}
