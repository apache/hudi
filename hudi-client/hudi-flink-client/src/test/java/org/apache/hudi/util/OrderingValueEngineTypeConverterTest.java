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

import org.apache.hudi.common.util.collection.ArrayComparable;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
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
public class OrderingValueEngineTypeConverterTest {

  @Test
  public void testCreateWithMultipleOrderingFields() {
    Schema schema = SchemaBuilder
        .record("test").fields()
        .name("id").type().stringType().noDefault()
        .name("timestamp").type().longType().noDefault()
        .name("amount").type().doubleType().noDefault()
        .endRecord();

    List<String> orderingFieldNames = Arrays.asList("id", "timestamp");
    boolean utcTimezone = true;

    OrderingValueEngineTypeConverter converter =
        OrderingValueEngineTypeConverter.create(schema, orderingFieldNames, utcTimezone);

    Assertions.assertEquals(orderingFieldNames.size(), converter.getConverters().size());
  }

  @Test
  public void testConvertWithSingleValue() {
    Schema schema = SchemaBuilder
        .record("test").fields()
        .name("name").type().stringType().noDefault()
        .endRecord();

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
    Schema tsSchema = LogicalTypes.timestampMillis()
        .addToSchema(Schema.create(Schema.Type.LONG));

    Schema schema = SchemaBuilder
        .record("test").fields()
        .name("timestamp").type(tsSchema).noDefault()
        .endRecord();

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
    Schema tsSchema = LogicalTypes.timestampMillis()
        .addToSchema(Schema.create(Schema.Type.LONG));

    Schema schema = SchemaBuilder
        .record("test").fields()
        .name("name").type().stringType().noDefault()
        .name("timestamp").type(tsSchema).noDefault()
        .endRecord();

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
    Schema schema = SchemaBuilder
        .record("test").fields()
        .name("id").type().stringType().noDefault()
        .name("age").type().intType().noDefault()
        .endRecord();

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
    Schema decimalSchema = LogicalTypes.decimal(10, 2)
        .addToSchema(Schema.create(Schema.Type.BYTES));

    Schema schema = SchemaBuilder
        .record("test").fields()
        .name("price").type(decimalSchema).noDefault()
        .endRecord();

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
    Schema dateSchema = LogicalTypes.date()
        .addToSchema(Schema.create(Schema.Type.INT));

    Schema schema = SchemaBuilder
        .record("test").fields()
        .name("birth_date").type(dateSchema).noDefault()
        .endRecord();

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
    Schema schema = SchemaBuilder
        .record("test").fields()
        .name("optional_field").type(
            Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))
        ).noDefault()
        .endRecord();

    List<String> orderingFieldNames = Collections.singletonList("optional_field");
    boolean utcTimezone = true;

    OrderingValueEngineTypeConverter converter =
        OrderingValueEngineTypeConverter.create(schema, orderingFieldNames, utcTimezone);

    Comparable converted = converter.convert(null);
    Assertions.assertNull(converted);
  }
}
