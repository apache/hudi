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

package org.apache.hudi.source;

import org.apache.hudi.source.ExpressionPredicates.And;
import org.apache.hudi.source.ExpressionPredicates.Equals;
import org.apache.hudi.source.ExpressionPredicates.GreaterThan;
import org.apache.hudi.source.ExpressionPredicates.GreaterThanOrEqual;
import org.apache.hudi.source.ExpressionPredicates.In;
import org.apache.hudi.source.ExpressionPredicates.LessThan;
import org.apache.hudi.source.ExpressionPredicates.LessThanOrEqual;
import org.apache.hudi.source.ExpressionPredicates.Not;
import org.apache.hudi.source.ExpressionPredicates.NotEquals;
import org.apache.hudi.source.ExpressionPredicates.Or;
import org.apache.hudi.source.ExpressionPredicates.Predicate;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators.Eq;
import org.apache.parquet.filter2.predicate.Operators.Gt;
import org.apache.parquet.filter2.predicate.Operators.IntColumn;
import org.apache.parquet.filter2.predicate.Operators.Lt;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.hudi.source.ExpressionPredicates.fromExpression;
import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.gt;
import static org.apache.parquet.filter2.predicate.FilterApi.gtEq;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.lt;
import static org.apache.parquet.filter2.predicate.FilterApi.ltEq;
import static org.apache.parquet.filter2.predicate.FilterApi.not;
import static org.apache.parquet.filter2.predicate.FilterApi.notEq;
import static org.apache.parquet.filter2.predicate.FilterApi.or;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link ExpressionPredicates}.
 */
public class TestExpressionPredicates {

  private static final String TEST_NAME_WITH_PARAMS = "[{index}] Test with fieldName={0}, dataType={1}, literalValue={2}";

  @Test
  public void testFilterPredicateFromExpression() {
    FieldReferenceExpression fieldReference = new FieldReferenceExpression("f_int", DataTypes.INT(), 0, 0);
    ValueLiteralExpression valueLiteral = new ValueLiteralExpression(10);
    List<ResolvedExpression> expressions = Arrays.asList(fieldReference, valueLiteral);
    IntColumn intColumn = intColumn("f_int");

    // equals
    CallExpression equalsExpression = CallExpression.permanent(
        BuiltInFunctionDefinitions.EQUALS, expressions, DataTypes.BOOLEAN());
    Predicate predicate1 = Equals.getInstance().bindValueLiteral(valueLiteral).bindFieldReference(fieldReference);
    Eq<Integer> eq = eq(intColumn, 10);
    Predicate predicate2 = fromExpression(equalsExpression);
    assertEquals(predicate1.toString(), predicate2.toString());
    assertEquals(eq, predicate2.filter());

    // not equals
    CallExpression notEqualsExpression = CallExpression.permanent(
        BuiltInFunctionDefinitions.NOT_EQUALS, expressions, DataTypes.BOOLEAN());
    Predicate predicate3 = NotEquals.getInstance().bindValueLiteral(valueLiteral).bindFieldReference(fieldReference);
    Predicate predicate4 = fromExpression(notEqualsExpression);
    assertEquals(predicate3.toString(), predicate4.toString());
    assertEquals(notEq(intColumn, 10), predicate4.filter());

    // less than
    CallExpression lessThanExpression = CallExpression.permanent(
        BuiltInFunctionDefinitions.LESS_THAN, expressions, DataTypes.BOOLEAN());
    Predicate predicate5 = LessThan.getInstance().bindValueLiteral(valueLiteral).bindFieldReference(fieldReference);
    Lt<Integer> lt = lt(intColumn, 10);
    Predicate predicate6 = fromExpression(lessThanExpression);
    assertEquals(predicate5.toString(), predicate6.toString());
    assertEquals(lt, predicate6.filter());

    // greater than
    CallExpression greaterThanExpression = CallExpression.permanent(
        BuiltInFunctionDefinitions.GREATER_THAN, expressions, DataTypes.BOOLEAN());
    Predicate predicate7 = GreaterThan.getInstance().bindValueLiteral(valueLiteral).bindFieldReference(fieldReference);
    Gt<Integer> gt = gt(intColumn, 10);
    Predicate predicate8 = fromExpression(greaterThanExpression);
    assertEquals(predicate7.toString(), predicate8.toString());
    assertEquals(gt, predicate8.filter());

    // less than or equal
    CallExpression lessThanOrEqualExpression = CallExpression.permanent(
        BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL, expressions, DataTypes.BOOLEAN());
    Predicate predicate9 = LessThanOrEqual.getInstance().bindValueLiteral(valueLiteral).bindFieldReference(fieldReference);
    Predicate predicate10 = fromExpression(lessThanOrEqualExpression);
    assertEquals(predicate9.toString(), predicate10.toString());
    assertEquals(ltEq(intColumn, 10), predicate10.filter());

    // greater than or equal
    CallExpression greaterThanOrEqualExpression = CallExpression.permanent(
        BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL, expressions, DataTypes.BOOLEAN());
    Predicate predicate11 = GreaterThanOrEqual.getInstance().bindValueLiteral(valueLiteral).bindFieldReference(fieldReference);
    Predicate predicate12 = fromExpression(greaterThanOrEqualExpression);
    assertEquals(predicate11.toString(), predicate12.toString());
    assertEquals(gtEq(intColumn, 10), predicate12.filter());

    // in
    ValueLiteralExpression valueLiteral1 = new ValueLiteralExpression(11);
    ValueLiteralExpression valueLiteral2 = new ValueLiteralExpression(12);
    CallExpression inExpression = CallExpression.permanent(
        BuiltInFunctionDefinitions.IN,
        Arrays.asList(fieldReference, valueLiteral1, valueLiteral2),
        DataTypes.BOOLEAN());
    Predicate predicate13 = In.getInstance().bindValueLiterals(Arrays.asList(valueLiteral1, valueLiteral2)).bindFieldReference(fieldReference);
    Predicate predicate14 = fromExpression(inExpression);
    assertEquals(predicate13.toString(), predicate14.toString());
    assertEquals(or(eq(intColumn, 11), eq(intColumn, 12)), predicate14.filter());

    // not
    CallExpression notExpression = CallExpression.permanent(
        BuiltInFunctionDefinitions.NOT,
        Collections.singletonList(equalsExpression),
        DataTypes.BOOLEAN());
    Predicate predicate15 = Not.getInstance().bindPredicate(predicate2);
    Predicate predicate16 = fromExpression(notExpression);
    assertEquals(predicate15.toString(), predicate16.toString());
    assertEquals(not(eq), predicate16.filter());

    // and
    CallExpression andExpression = CallExpression.permanent(
        BuiltInFunctionDefinitions.AND,
        Arrays.asList(lessThanExpression, greaterThanExpression),
        DataTypes.BOOLEAN());
    Predicate predicate17 = And.getInstance().bindPredicates(predicate6, predicate8);
    Predicate predicate18 = fromExpression(andExpression);
    assertEquals(predicate17.toString(), predicate18.toString());
    assertEquals(and(lt, gt), predicate18.filter());

    // or
    CallExpression orExpression = CallExpression.permanent(
        BuiltInFunctionDefinitions.OR,
        Arrays.asList(lessThanExpression, greaterThanExpression),
        DataTypes.BOOLEAN());
    Predicate predicate19 = Or.getInstance().bindPredicates(predicate6, predicate8);
    Predicate predicate20 = fromExpression(orExpression);
    assertEquals(predicate19.toString(), predicate20.toString());
    assertEquals(or(lt, gt), predicate20.filter());
  }

  @Test
  public void testDisablePredicatesPushDownForUnsupportedType() {
    FieldReferenceExpression fieldReference = new FieldReferenceExpression("f_decimal", DataTypes.DECIMAL(7, 2), 0, 0);
    ValueLiteralExpression valueLiteral = new ValueLiteralExpression(BigDecimal.valueOf(100.00));
    List<ResolvedExpression> expressions = Arrays.asList(fieldReference, valueLiteral);

    CallExpression greaterThanExpression = CallExpression.permanent(BuiltInFunctionDefinitions.GREATER_THAN, expressions, DataTypes.DECIMAL(7, 2));
    Predicate greaterThanPredicate = fromExpression(greaterThanExpression);
    CallExpression lessThanExpression = CallExpression.permanent(BuiltInFunctionDefinitions.LESS_THAN, expressions, DataTypes.DECIMAL(7, 2));
    Predicate lessThanPredicate = fromExpression(lessThanExpression);

    assertNull(And.getInstance().bindPredicates(greaterThanPredicate, lessThanPredicate).filter(), "Decimal type push down is unsupported, so we expect null");
    assertNull(Or.getInstance().bindPredicates(greaterThanPredicate, lessThanPredicate).filter(), "Decimal type push down is unsupported, so we expect null");
    assertNull(Not.getInstance().bindPredicate(greaterThanPredicate).filter(), "Decimal type push down is unsupported, so we expect null");
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 4, 5, 7, 8, 9})
  void testDisableTimestampPredicatePushDown(int precision) {
    FieldReferenceExpression field = new FieldReferenceExpression("ts", DataTypes.TIMESTAMP(precision), 0, 0);
    ValueLiteralExpression literal = new ValueLiteralExpression(LocalDateTime.parse("1970-01-01T00:00:01.000001"));
    BuiltInFunctionDefinition[] functions = {
        BuiltInFunctionDefinitions.EQUALS, BuiltInFunctionDefinitions.NOT_EQUALS,
        BuiltInFunctionDefinitions.LESS_THAN, BuiltInFunctionDefinitions.GREATER_THAN,
        BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL, BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL
    };
    for (BuiltInFunctionDefinition function : functions) {
      assertNull(fromExpression(CallExpression.permanent(function, Arrays.asList(field, literal), DataTypes.BOOLEAN())).filter());
      assertNull(fromExpression(CallExpression.permanent(function, Arrays.asList(literal, field), DataTypes.BOOLEAN())).filter());
    }
    Predicate timestamp = Equals.getInstance().bindFieldReference(field).bindValueLiteral(literal);
    assertNull(In.getInstance().bindValueLiterals(Arrays.asList(literal, literal)).bindFieldReference(field).filter());
    Predicate integer = Equals.getInstance()
        .bindFieldReference(new FieldReferenceExpression("id", DataTypes.INT(), 0, 1))
        .bindValueLiteral(new ValueLiteralExpression(1));
    assertEquals(eq(intColumn("id"), 1), integer.filter());
    assertNull(And.getInstance().bindPredicates(timestamp, integer).filter());
    assertNull(Or.getInstance().bindPredicates(integer, timestamp).filter());
    assertNull(Not.getInstance().bindPredicate(timestamp).filter());
    assertNull(Not.getInstance().bindPredicate(And.getInstance().bindPredicates(timestamp, integer)).filter());
  }

  @ParameterizedTest
  @MethodSource("timestampLiterals")
  void testTimestampPredicatePushDown(int precision, String text, long expected) {
    FieldReferenceExpression field = new FieldReferenceExpression("ts", DataTypes.TIMESTAMP(precision), 0, 0);
    ValueLiteralExpression literal = new ValueLiteralExpression(LocalDateTime.parse(text));
    BuiltInFunctionDefinition[] functions = {
        BuiltInFunctionDefinitions.EQUALS, BuiltInFunctionDefinitions.NOT_EQUALS,
        BuiltInFunctionDefinitions.LESS_THAN, BuiltInFunctionDefinitions.GREATER_THAN,
        BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL, BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL
    };
    FilterPredicate[] filters = {
        eq(longColumn("ts"), expected), notEq(longColumn("ts"), expected),
        lt(longColumn("ts"), expected), gt(longColumn("ts"), expected),
        ltEq(longColumn("ts"), expected), gtEq(longColumn("ts"), expected)
    };
    for (int i = 0; i < functions.length; i++) {
      assertEquals(filters[i], fromExpression(CallExpression.permanent(
          functions[i], Arrays.asList(field, literal), DataTypes.BOOLEAN())).filter());
    }
    assertEquals(gt(longColumn("ts"), expected), fromExpression(CallExpression.permanent(
        BuiltInFunctionDefinitions.LESS_THAN, Arrays.asList(literal, field), DataTypes.BOOLEAN())).filter());
    Predicate equals = Equals.getInstance().bindFieldReference(field).bindValueLiteral(literal);
    assertEquals(or(filters[0], filters[0]), In.getInstance()
        .bindValueLiterals(Arrays.asList(literal, literal)).bindFieldReference(field).filter());
    assertEquals(not(filters[0]), Not.getInstance().bindPredicate(equals).filter());
    assertEquals(and(filters[0], filters[0]), And.getInstance().bindPredicates(equals, equals).filter());
    assertEquals(or(filters[0], filters[0]), Or.getInstance().bindPredicates(equals, equals).filter());
  }

  static Stream<Arguments> timestampLiterals() {
    return Stream.of(
        Arguments.of(3, "1970-01-01T00:00:01", 1000L),
        Arguments.of(3, "1970-01-01T00:00:01.001", 1001L),
        Arguments.of(6, "1970-01-01T00:00:01", 1000000L),
        Arguments.of(6, "1970-01-01T00:00:01.000001", 1000001L),
        Arguments.of(6, "1969-12-31T23:59:59.999999", -1L));
  }

  @ParameterizedTest
  @ValueSource(ints = {3, 6})
  void testTimestampPushDownRejectsInexactOrOverflowingLiterals(int precision) {
    FieldReferenceExpression field = new FieldReferenceExpression("ts", DataTypes.TIMESTAMP(precision), 0, 0);
    ValueLiteralExpression literal = new ValueLiteralExpression(LocalDateTime.parse("1970-01-01T00:00:01"));
    ValueLiteralExpression finer = new ValueLiteralExpression(LocalDateTime.parse(precision == 3
        ? "1970-01-01T00:00:01.000001" : "1970-01-01T00:00:01.000000001"));
    Predicate unsupported = LessThan.getInstance().bindFieldReference(field).bindValueLiteral(finer);
    assertNull(unsupported.filter());
    assertNull(Not.getInstance().bindPredicate(unsupported).filter());
    assertNull(In.getInstance().bindValueLiterals(Arrays.asList(literal, finer)).bindFieldReference(field).filter());
    assertNull(Equals.getInstance().bindFieldReference(field)
        .bindValueLiteral(new ValueLiteralExpression(LocalDateTime.MAX.withNano(0))).filter());
  }

  public static Stream<Arguments> testColumnPredicateLiteralTypeConversionParams() {
    return Stream.of(
        Arguments.of("f_boolean", DataTypes.BOOLEAN(), Boolean.TRUE),
        Arguments.of("f_boolean", DataTypes.BOOLEAN(), "true"),
        Arguments.of("f_tinyint", DataTypes.TINYINT(), 12345),
        Arguments.of("f_tinyint", DataTypes.TINYINT(), "12345"),
        Arguments.of("f_smallint", DataTypes.SMALLINT(), 12345),
        Arguments.of("f_smallint", DataTypes.SMALLINT(), "12345"),
        Arguments.of("f_integer", DataTypes.INT(), 12345),
        Arguments.of("f_integer", DataTypes.INT(), "12345"),
        Arguments.of("f_bigint", DataTypes.BIGINT(), 12345L),
        Arguments.of("f_bigint", DataTypes.BIGINT(), 12345),
        Arguments.of("f_bigint", DataTypes.BIGINT(), "12345"),
        Arguments.of("f_float", DataTypes.FLOAT(), 123.45f),
        Arguments.of("f_float", DataTypes.FLOAT(), "123.45f"),
        Arguments.of("f_double", DataTypes.DOUBLE(), 123.45),
        Arguments.of("f_double", DataTypes.DOUBLE(), "123.45"),
        Arguments.of("f_varbinary", DataTypes.VARBINARY(10), "a".getBytes()),
        Arguments.of("f_varbinary", DataTypes.VARBINARY(10), "a"),
        Arguments.of("f_binary", DataTypes.BINARY(10), "a".getBytes()),
        Arguments.of("f_binary", DataTypes.BINARY(10), "a"),
        Arguments.of("f_date", DataTypes.DATE(), LocalDate.now()),
        Arguments.of("f_date", DataTypes.DATE(), 19740),
        Arguments.of("f_date", DataTypes.DATE(), 19740L),
        Arguments.of("f_date", DataTypes.DATE(), "2024-01-18"),
        Arguments.of("f_char", DataTypes.CHAR(1), "a"),
        Arguments.of("f_char", DataTypes.CHAR(1), 1),
        Arguments.of("f_varchar", DataTypes.VARCHAR(1), "a"),
        Arguments.of("f_varchar", DataTypes.VARCHAR(1), 1),
        Arguments.of("f_time", DataTypes.TIME(), LocalTime.now()),
        Arguments.of("f_time", DataTypes.TIME(), 12345),
        Arguments.of("f_time", DataTypes.TIME(), 60981896000L),
        Arguments.of("f_time", DataTypes.TIME(), "20:00:00"),
        Arguments.of("f_timestamp", DataTypes.TIMESTAMP(), LocalDateTime.now()),
        Arguments.of("f_timestamp", DataTypes.TIMESTAMP(), 12345),
        Arguments.of("f_timestamp", DataTypes.TIMESTAMP(), 1705568913701L),
        Arguments.of("f_timestamp", DataTypes.TIMESTAMP(), "2024-01-18T15:00:00")
    );
  }

  @ParameterizedTest(name = TEST_NAME_WITH_PARAMS)
  @MethodSource("testColumnPredicateLiteralTypeConversionParams")
  public void testColumnPredicateLiteralTypeConversion(String fieldName, DataType dataType, Object literalValue) {
    FieldReferenceExpression fieldReference = new FieldReferenceExpression(fieldName, dataType, 0, 0);
    ValueLiteralExpression valueLiteral = new ValueLiteralExpression(literalValue);

    ExpressionPredicates.ColumnPredicate predicate = Equals.getInstance().bindFieldReference(fieldReference).bindValueLiteral(valueLiteral);
    assertDoesNotThrow(predicate::filter, () -> String.format("Convert from %s to %s failed", literalValue.getClass().getName(), dataType));
  }

  @Test
  public void testUnaryPredicateReferences() {
    // Test Equals predicate references method
    FieldReferenceExpression fieldReference = new FieldReferenceExpression("field1", DataTypes.STRING(), 0, 0);
    ValueLiteralExpression valueLiteral = new ValueLiteralExpression("value1");
    
    ExpressionPredicates.ColumnPredicate equalsPredicate = Equals.getInstance()
        .bindFieldReference(fieldReference)
        .bindValueLiteral(valueLiteral);
    
    assertEquals(Collections.singletonList("field1"), equalsPredicate.references());

    // Test GreaterThan predicate references method
    valueLiteral = new ValueLiteralExpression(10);
    ExpressionPredicates.ColumnPredicate gtPredicate = GreaterThan.getInstance()
        .bindFieldReference(fieldReference)
        .bindValueLiteral(valueLiteral);
    assertEquals(Collections.singletonList("field1"), gtPredicate.references());

    ValueLiteralExpression valueLiteral1 = new ValueLiteralExpression("value1");
    ValueLiteralExpression valueLiteral2 = new ValueLiteralExpression("value2");

    ExpressionPredicates.ColumnPredicate inPredicate = In.getInstance()
        .bindValueLiterals(Arrays.asList(valueLiteral1, valueLiteral2))
        .bindFieldReference(fieldReference);

    assertEquals(Collections.singletonList("field1"), inPredicate.references());
  }

  @Test
  public void testNotPredicateReferences() {
    // Test Not predicate references method - should delegate to underlying predicate
    FieldReferenceExpression fieldReference = new FieldReferenceExpression("field4", DataTypes.STRING(), 0, 0);
    ValueLiteralExpression valueLiteral = new ValueLiteralExpression("value1");
    
    ExpressionPredicates.ColumnPredicate innerPredicate = Equals.getInstance()
        .bindFieldReference(fieldReference)
        .bindValueLiteral(valueLiteral);
    
    Predicate notPredicate = Not.getInstance().bindPredicate(innerPredicate);
    
    List<String> references = notPredicate.references();
    assertEquals(1, references.size());
    assertEquals("field4", references.get(0));
  }

  @Test
  public void testAndPredicateReferencesWithSameColumn() {
    // Test And predicate references method with same column
    FieldReferenceExpression fieldReference1 = new FieldReferenceExpression("field1", DataTypes.INT(), 0, 0);
    FieldReferenceExpression fieldReference2 = new FieldReferenceExpression("field1", DataTypes.INT(), 0, 0);
    ValueLiteralExpression valueLiteral1 = new ValueLiteralExpression(10);
    ValueLiteralExpression valueLiteral2 = new ValueLiteralExpression(20);
    
    ExpressionPredicates.ColumnPredicate predicate1 = GreaterThan.getInstance()
        .bindFieldReference(fieldReference1)
        .bindValueLiteral(valueLiteral1);
    
    ExpressionPredicates.ColumnPredicate predicate2 = Equals.getInstance()
        .bindFieldReference(fieldReference2)
        .bindValueLiteral(valueLiteral2);
    
    Predicate andPredicate = And.getInstance().bindPredicates(predicate1, predicate2);
    
    List<String> references = andPredicate.references();
    assertEquals(1, references.size());
    assertEquals("field1", references.get(0));
  }

  @Test
  public void testAndPredicateReferencesWithDifferentColumns() {
    // Test And predicate references method with different columns
    FieldReferenceExpression fieldReference1 = new FieldReferenceExpression("field1", DataTypes.INT(), 0, 0);
    FieldReferenceExpression fieldReference2 = new FieldReferenceExpression("field2", DataTypes.STRING(), 0, 0);
    ValueLiteralExpression valueLiteral1 = new ValueLiteralExpression(10);
    ValueLiteralExpression valueLiteral2 = new ValueLiteralExpression("test");
    
    ExpressionPredicates.ColumnPredicate predicate1 = GreaterThan.getInstance()
        .bindFieldReference(fieldReference1)
        .bindValueLiteral(valueLiteral1);
    
    ExpressionPredicates.ColumnPredicate predicate2 = Equals.getInstance()
        .bindFieldReference(fieldReference2)
        .bindValueLiteral(valueLiteral2);
    
    Predicate andPredicate = And.getInstance().bindPredicates(predicate1, predicate2);
    
    assertEquals(Arrays.asList("field1", "field2"), andPredicate.references());
  }

  @Test
  public void testOrPredicateReferencesWithSameColumn() {
    // Test Or predicate references method with same column
    FieldReferenceExpression fieldReference1 = new FieldReferenceExpression("field1", DataTypes.INT(), 0, 0);
    FieldReferenceExpression fieldReference2 = new FieldReferenceExpression("field1", DataTypes.INT(), 0, 0);
    ValueLiteralExpression valueLiteral1 = new ValueLiteralExpression(10);
    ValueLiteralExpression valueLiteral2 = new ValueLiteralExpression(20);
    
    ExpressionPredicates.ColumnPredicate predicate1 = GreaterThan.getInstance()
        .bindFieldReference(fieldReference1)
        .bindValueLiteral(valueLiteral1);
    
    ExpressionPredicates.ColumnPredicate predicate2 = Equals.getInstance()
        .bindFieldReference(fieldReference2)
        .bindValueLiteral(valueLiteral2);
    
    Predicate orPredicate = Or.getInstance().bindPredicates(predicate1, predicate2);
    
    List<String> references = orPredicate.references();
    assertEquals(1, references.size());
    assertEquals("field1", references.get(0));
  }

  @Test
  public void testOrPredicateReferencesWithDifferentColumns() {
    // Test Or predicate references method with different columns
    FieldReferenceExpression fieldReference1 = new FieldReferenceExpression("field1", DataTypes.INT(), 0, 0);
    FieldReferenceExpression fieldReference2 = new FieldReferenceExpression("field2", DataTypes.STRING(), 0, 0);
    ValueLiteralExpression valueLiteral1 = new ValueLiteralExpression(10);
    ValueLiteralExpression valueLiteral2 = new ValueLiteralExpression("test");
    
    ExpressionPredicates.ColumnPredicate predicate1 = GreaterThan.getInstance()
        .bindFieldReference(fieldReference1)
        .bindValueLiteral(valueLiteral1);
    
    ExpressionPredicates.ColumnPredicate predicate2 = Equals.getInstance()
        .bindFieldReference(fieldReference2)
        .bindValueLiteral(valueLiteral2);
    
    Predicate orPredicate = Or.getInstance().bindPredicates(predicate1, predicate2);
    assertEquals(Arrays.asList("field1", "field2"), orPredicate.references());
  }

  @Test
  public void testAlwaysNullPredicateReferences() {
    // Test AlwaysNull predicate references method - should return empty list
    Predicate alwaysNullPredicate = ExpressionPredicates.AlwaysNull.getInstance();
    
    List<String> references = alwaysNullPredicate.references();
    assertEquals(0, references.size());
    assertTrue(references.isEmpty());
  }

  @Test
  public void testNestedPredicateReferences() {
    // Test nested predicates (AND inside NOT)
    FieldReferenceExpression fieldReference1 = new FieldReferenceExpression("field1", DataTypes.INT(), 0, 0);
    FieldReferenceExpression fieldReference2 = new FieldReferenceExpression("field2", DataTypes.STRING(), 0, 0);
    ValueLiteralExpression valueLiteral1 = new ValueLiteralExpression(10);
    ValueLiteralExpression valueLiteral2 = new ValueLiteralExpression("test");
    
    ExpressionPredicates.ColumnPredicate predicate1 = GreaterThan.getInstance()
        .bindFieldReference(fieldReference1)
        .bindValueLiteral(valueLiteral1);
    
    ExpressionPredicates.ColumnPredicate predicate2 = Equals.getInstance()
        .bindFieldReference(fieldReference2)
        .bindValueLiteral(valueLiteral2);
    
    Predicate andPredicate = And.getInstance().bindPredicates(predicate1, predicate2);
    Predicate notPredicate = Not.getInstance().bindPredicate(andPredicate);
    assertEquals(Arrays.asList("field1", "field2"), notPredicate.references());
  }
}
