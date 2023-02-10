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

package org.apache.hudi.source.evaluator;

import org.apache.hudi.source.stats.ColumnStats;
import org.apache.hudi.util.ExpressionUtils;

import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.types.logical.LogicalType;

import javax.validation.constraints.NotNull;

import java.util.Map;

/**
 * Leaf evaluator which compares the field value with literal values.
 */
public abstract class NullFalseEvaluator extends LeafEvaluator {

  // the constant literal value
  protected Object val;

  public NullFalseEvaluator bindVal(ValueLiteralExpression vExpr) {
    this.val = ExpressionUtils.getValueFromLiteral(vExpr);
    return this;
  }

  @Override
  public final boolean eval(Map<String, ColumnStats> columnStatsMap) {
    if (this.val == null) {
      return false;
    } else {
      return eval(this.val, getColumnStats(columnStatsMap), this.type);
    }
  }

  protected abstract boolean eval(@NotNull Object val, ColumnStats columnStats, LogicalType type);

  protected abstract boolean eval(@NotNull Object val, @NotNull Object columnValue, LogicalType type);
}
