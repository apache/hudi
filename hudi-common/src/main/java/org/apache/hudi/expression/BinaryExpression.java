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

package org.apache.hudi.expression;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;
import java.util.List;

/**
 * The expression that accept two child expressions.
 */
@AllArgsConstructor(access = AccessLevel.PACKAGE)
@Getter
public abstract class BinaryExpression implements Expression {

  private final Expression left;
  private final Operator operator;
  private final Expression right;

  @Override
  public List<Expression> getChildren() {
    return Arrays.asList(left, right);
  }

  @Override
  public String toString() {
    return left.toString() + " " + operator.symbol + " " + right.toString();
  }
}
