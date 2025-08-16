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
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;
import org.apache.parquet.filter2.predicate.Operators.Eq;
import org.apache.parquet.filter2.predicate.Operators.Gt;
import org.apache.parquet.filter2.predicate.Operators.IntColumn;
import org.apache.parquet.filter2.predicate.Operators.Lt;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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
import static org.apache.parquet.filter2.predicate.FilterApi.lt;
import static org.apache.parquet.filter2.predicate.FilterApi.ltEq;
import static org.apache.parquet.filter2.predicate.FilterApi.not;
import static org.apache.parquet.filter2.predicate.FilterApi.notEq;
import static org.apache.parquet.filter2.predicate.FilterApi.or;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

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
}
