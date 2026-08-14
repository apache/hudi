/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.util;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.collection.ArrayComparable;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Unit tests for {@link OrderingValueEngineTypeConverter}.
 */
public class TestOrderingValueEngineTypeConverter {

  @Test
  public void testCreateWithMultipleOrderingFields() {
    HoodieSchema schema = HoodieSchema.createRecord("test", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
        HoodieSchemaField.of("timestamp", HoodieSchema.create(HoodieSchemaType.LONG), null, null),
        HoodieSchemaField.of("amount", HoodieSchema.create(HoodieSchemaType.DOUBLE), null, null)
    ));

    List<String> orderingFieldNames = Arrays.asList("id", "timestamp");
    boolean utcTimezone = true;

    OrderingValueEngineTypeConverter converter =
        OrderingValueEngineTypeConverter.create(schema, orderingFieldNames, utcTimezone);

    Assertions.assertEquals(orderingFieldNames.size(), converter.getConverters().size());
  }

  @Test
  public void testConvertWithSingleValue() {
    HoodieSchema schema = HoodieSchema.createRecord("test", null, null, Collections.singletonList(
        HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING), null, null)));

    List<String> orderingFieldNames = Collections.singletonList("name");
    boolean utcTimezone = true;

    OrderingValueEngineTypeConverter converter =
        OrderingValueEngineTypeConverter.create(schema, orderingFieldNames, utcTimezone);

    String originalValue = "test";
    Comparable converted = converter.convert(originalValue);

    Assertions.assertEquals(BinaryStringData.fromString("test"), converted);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testConvertWithTimestampValue(boolean utcTimezone) {
    HoodieSchema schema = HoodieSchema.createRecord("test", null, null, Collections.singletonList(
        HoodieSchemaField.of("timestamp", HoodieSchema.createTimestampMillis(), null, null)));

    List<String> orderingFieldNames = Collections.singletonList("timestamp");

    OrderingValueEngineTypeConverter converter =
        OrderingValueEngineTypeConverter.create(schema, orderingFieldNames, utcTimezone);

    Long originalValue = 1234567890000L;
    Comparable converted = converter.convert(originalValue);

    TimestampData expected = utcTimezone ? TimestampData.fromEpochMillis(originalValue) : TimestampData.fromTimestamp(new Timestamp(originalValue));
    Assertions.assertEquals(expected, converted);
  }

  @Test
  public void testConvertWithArrayComparable() {
    HoodieSchema schema = HoodieSchema.createRecord("test", null, null, Arrays.asList(
        HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
        HoodieSchemaField.of("timestamp", HoodieSchema.createTimestampMillis(), null, null)));

    List<String> orderingFieldNames = Arrays.asList("name", "timestamp");
    boolean utcTimezone = true;

    OrderingValueEngineTypeConverter converter =
        OrderingValueEngineTypeConverter.create(schema, orderingFieldNames, utcTimezone);

    Comparable[] originalValues = {"test", 1234567890000L};
    ArrayComparable originalArray = new ArrayComparable(originalValues);

    Comparable converted = converter.convert(originalArray);

    Assertions.assertTrue(converted instanceof ArrayComparable);
    ArrayComparable convertedArray = (ArrayComparable) converted;
    Assertions.assertEquals(2, convertedArray.getValues().size());

    Assertions.assertEquals(BinaryStringData.fromString((String) originalValues[0]), ((ArrayComparable) converted).getValues().get(0));
    Assertions.assertEquals(TimestampData.fromEpochMillis((Long) originalValues[1]), ((ArrayComparable) converted).getValues().get(1));
  }

  @Test
  public void testCreateConverters() {
    HoodieSchema schema = HoodieSchema.createRecord("test", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
        HoodieSchemaField.of("age", HoodieSchema.create(HoodieSchemaType.INT), null, null)));

    List<String> orderingFieldNames = Arrays.asList("id", "age");
    boolean utcTimezone = true;

    List<java.util.function.Function<Comparable, Comparable>> converters =
        OrderingValueEngineTypeConverter.createConverters(schema, orderingFieldNames, utcTimezone);

    Assertions.assertEquals(2, converters.size());

    String inputString = "test";
    Comparable convertedString = converters.get(0).apply(inputString);
    Assertions.assertNotNull(convertedString);

    Integer inputInt = 25;
    Comparable convertedInt = converters.get(1).apply(inputInt);

    Assertions.assertEquals(inputInt, convertedInt);
  }

  @Test
  public void testConvertWithDecimalValue() {
    HoodieSchema schema = HoodieSchema.createRecord("test", null, null, Collections.singletonList(
        HoodieSchemaField.of("price", HoodieSchema.createDecimal(10, 2), null, null)));

    List<String> orderingFieldNames = Collections.singletonList("price");
    boolean utcTimezone = true;

    OrderingValueEngineTypeConverter converter =
        OrderingValueEngineTypeConverter.create(schema, orderingFieldNames, utcTimezone);

    BigDecimal originalValue = new BigDecimal("123.45");
    Comparable converted = converter.convert(originalValue);

    Assertions.assertEquals(DecimalData.fromBigDecimal(originalValue, 10, 2), converted);
  }

  @Test
  public void testConvertWithDateValue() {
    HoodieSchema schema = HoodieSchema.createRecord("test", null, null, Collections.singletonList(
        HoodieSchemaField.of("birth_date", HoodieSchema.createDate())));

    List<String> orderingFieldNames = Collections.singletonList("birth_date");

    OrderingValueEngineTypeConverter converter =
        OrderingValueEngineTypeConverter.create(schema, orderingFieldNames, true);

    LocalDate originalValue = LocalDate.of(2020, 1, 1); // 2020-01-01
    Comparable converted = converter.convert(originalValue);

    Assertions.assertNotNull(converted);
    Assertions.assertEquals(0, converted.compareTo(18262));
  }

  @Test
  public void testConvertWithNullValue() {
    HoodieSchema schema = HoodieSchema.createRecord("test", null, null, Collections.singletonList(
        HoodieSchemaField.of("optional_field", HoodieSchema.createNullable(HoodieSchemaType.STRING))));

    List<String> orderingFieldNames = Collections.singletonList("optional_field");
    boolean utcTimezone = true;

    OrderingValueEngineTypeConverter converter =
        OrderingValueEngineTypeConverter.create(schema, orderingFieldNames, utcTimezone);

    Comparable converted = converter.convert(null);
    Assertions.assertNull(converted);
  }
}
