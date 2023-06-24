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

package org.apache.hudi.hive.expression;

import java.util.Arrays;

/**
 * The expression that accept two child expressions.
 */
public class BinaryOperator extends Expression {

  private final Operator operator;
  private final Expression left;
  private final Expression right;

  private BinaryOperator(Expression left, Operator operator, Expression right) {
    super(Arrays.asList(left, right));
    this.left = left;
    this.operator = operator;
    this.right = right;
  }

  public static BinaryOperator and(Expression left, Expression right) {
    return new BinaryOperator(left, Operator.AND, right);
  }

  public static BinaryOperator or(Expression left, Expression right) {
    return new BinaryOperator(left, Operator.OR, right);
  }

  public static BinaryOperator eq(Expression left, Expression right) {
    return new BinaryOperator(left, Operator.EQ, right);
  }

  public static BinaryOperator gteq(Expression left, Expression right) {
    return new BinaryOperator(left, Operator.GT_EQ, right);
  }

  public static BinaryOperator lteq(Expression left, Expression right) {
    return new BinaryOperator(left, Operator.LT_EQ, right);
  }

  public Operator getOperator() {
    return operator;
  }

  public Expression getLeft() {
    return left;
  }

  public Expression getRight() {
    return right;
  }

  @Override
  public <T> T accept(ExpressionVisitor<T> exprVisitor) {
    return exprVisitor.visitBinaryOperator(this);
  }
}
