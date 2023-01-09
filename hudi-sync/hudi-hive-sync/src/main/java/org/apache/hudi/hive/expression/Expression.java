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

import java.util.List;

public abstract class Expression {

  public enum Operator {
    AND("AND", "&&"),
    OR("OR", "||"),
    GT(">", ">"),
    LT("<", "<"),
    EQ("=", "="),
    GT_EQ(">=", ">="),
    LT_EQ("<=", "<=");

    public final String sqlOperator;
    public final String symbol;

    Operator(String sqlOperator, String symbol) {
      this.sqlOperator = sqlOperator;
      this.symbol = symbol;
    }
  }

  private final List<Expression> children;

  public Expression(List<Expression> children) {
    this.children = children;
  }

  /**
   * Traverses the expression with the provided {@link ExpressionVisitor}
   */
  public abstract <T> T accept(ExpressionVisitor<T> exprVisitor);
}
