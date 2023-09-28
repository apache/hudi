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
import org.apache.parquet.filter2.predicate.Operators.Eq;
import org.apache.parquet.filter2.predicate.Operators.Gt;
import org.apache.parquet.filter2.predicate.Operators.IntColumn;
import org.apache.parquet.filter2.predicate.Operators.Lt;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test cases for {@link ExpressionPredicates}.
 */
public class TestExpressionPredicates {

  @Test
  public void testFilterPredicateFromExpression() {
    FieldReferenceExpression fieldReference = new FieldReferenceExpression("f_int", DataTypes.INT(), 0, 0);
    ValueLiteralExpression valueLiteral = new ValueLiteralExpression(10);
    List<ResolvedExpression> expressions = Arrays.asList(fieldReference, valueLiteral);
    IntColumn intColumn = intColumn("f_int");

    // equals
    CallExpression equalsExpression = new CallExpression(
        BuiltInFunctionDefinitions.EQUALS, expressions, DataTypes.BOOLEAN());
    Predicate predicate1 = Equals.getInstance().bindValueLiteral(valueLiteral).bindFieldReference(fieldReference);
    Eq<Integer> eq = eq(intColumn, 10);
    Predicate predicate2 = fromExpression(equalsExpression);
    assertEquals(predicate1.toString(), predicate2.toString());
    assertEquals(eq, predicate2.filter());

    // not equals
    CallExpression notEqualsExpression = new CallExpression(
        BuiltInFunctionDefinitions.NOT_EQUALS, expressions, DataTypes.BOOLEAN());
    Predicate predicate3 = NotEquals.getInstance().bindValueLiteral(valueLiteral).bindFieldReference(fieldReference);
    Predicate predicate4 = fromExpression(notEqualsExpression);
    assertEquals(predicate3.toString(), predicate4.toString());
    assertEquals(notEq(intColumn, 10), predicate4.filter());

    // less than
    CallExpression lessThanExpression = new CallExpression(
        BuiltInFunctionDefinitions.LESS_THAN, expressions, DataTypes.BOOLEAN());
    Predicate predicate5 = LessThan.getInstance().bindValueLiteral(valueLiteral).bindFieldReference(fieldReference);
    Lt<Integer> lt = lt(intColumn, 10);
    Predicate predicate6 = fromExpression(lessThanExpression);
    assertEquals(predicate5.toString(), predicate6.toString());
    assertEquals(lt, predicate6.filter());

    // greater than
    CallExpression greaterThanExpression = new CallExpression(
        BuiltInFunctionDefinitions.GREATER_THAN, expressions, DataTypes.BOOLEAN());
    Predicate predicate7 = GreaterThan.getInstance().bindValueLiteral(valueLiteral).bindFieldReference(fieldReference);
    Gt<Integer> gt = gt(intColumn, 10);
    Predicate predicate8 = fromExpression(greaterThanExpression);
    assertEquals(predicate7.toString(), predicate8.toString());
    assertEquals(gt, predicate8.filter());

    // less than or equal
    CallExpression lessThanOrEqualExpression = new CallExpression(
        BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL, expressions, DataTypes.BOOLEAN());
    Predicate predicate9 = LessThanOrEqual.getInstance().bindValueLiteral(valueLiteral).bindFieldReference(fieldReference);
    Predicate predicate10 = fromExpression(lessThanOrEqualExpression);
    assertEquals(predicate9.toString(), predicate10.toString());
    assertEquals(ltEq(intColumn, 10), predicate10.filter());

    // greater than or equal
    CallExpression greaterThanOrEqualExpression = new CallExpression(
        BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL, expressions, DataTypes.BOOLEAN());
    Predicate predicate11 = GreaterThanOrEqual.getInstance().bindValueLiteral(valueLiteral).bindFieldReference(fieldReference);
    Predicate predicate12 = fromExpression(greaterThanOrEqualExpression);
    assertEquals(predicate11.toString(), predicate12.toString());
    assertEquals(gtEq(intColumn, 10), predicate12.filter());

    // in
    ValueLiteralExpression valueLiteral1 = new ValueLiteralExpression(11);
    ValueLiteralExpression valueLiteral2 = new ValueLiteralExpression(12);
    CallExpression inExpression = new CallExpression(
        BuiltInFunctionDefinitions.IN,
        Arrays.asList(fieldReference, valueLiteral1, valueLiteral2),
        DataTypes.BOOLEAN());
    Predicate predicate13 = In.getInstance().bindValueLiterals(Arrays.asList(valueLiteral1, valueLiteral2)).bindFieldReference(fieldReference);
    Predicate predicate14 = fromExpression(inExpression);
    assertEquals(predicate13.toString(), predicate14.toString());
    assertEquals(or(eq(intColumn, 11), eq(intColumn, 12)), predicate14.filter());

    // not
    CallExpression notExpression = new CallExpression(
        BuiltInFunctionDefinitions.NOT,
        Collections.singletonList(equalsExpression),
        DataTypes.BOOLEAN());
    Predicate predicate15 = Not.getInstance().bindPredicate(predicate2);
    Predicate predicate16 = fromExpression(notExpression);
    assertEquals(predicate15.toString(), predicate16.toString());
    assertEquals(not(eq), predicate16.filter());

    // and
    CallExpression andExpression = new CallExpression(
        BuiltInFunctionDefinitions.AND,
        Arrays.asList(lessThanExpression, greaterThanExpression),
        DataTypes.BOOLEAN());
    Predicate predicate17 = And.getInstance().bindPredicates(predicate6, predicate8);
    Predicate predicate18 = fromExpression(andExpression);
    assertEquals(predicate17.toString(), predicate18.toString());
    assertEquals(and(lt, gt), predicate18.filter());

    // or
    CallExpression orExpression = new CallExpression(
        BuiltInFunctionDefinitions.OR,
        Arrays.asList(lessThanExpression, greaterThanExpression),
        DataTypes.BOOLEAN());
    Predicate predicate19 = Or.getInstance().bindPredicates(predicate6, predicate8);
    Predicate predicate20 = fromExpression(orExpression);
    assertEquals(predicate19.toString(), predicate20.toString());
    assertEquals(or(lt, gt), predicate20.filter());
  }
}
