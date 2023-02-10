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

package org.apache.hudi.source.evaluator;

import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.source.stats.ColumnStats;

import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.Map;

/**
 * Leaf evaluator which depends on the given field.
 */
public abstract class LeafEvaluator implements Evaluator {

  // referenced field type
  protected LogicalType type;

  // referenced field name
  protected String name;

  // referenced field index
  protected int index;

  public LeafEvaluator bindFieldReference(FieldReferenceExpression expr) {
    this.type = expr.getOutputDataType().getLogicalType();
    this.name = expr.getName();
    this.index = expr.getFieldIndex();
    return this;
  }

  protected ColumnStats getColumnStats(Map<String, ColumnStats> columnStatsMap) {
    ColumnStats columnStats = columnStatsMap.get(this.name);
    ValidationUtils.checkState(
        columnStats != null,
        "Can not find column " + this.name);
    return columnStats;
  }
}
