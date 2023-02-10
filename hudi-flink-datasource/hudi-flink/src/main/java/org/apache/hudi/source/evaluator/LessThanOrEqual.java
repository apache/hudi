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

import org.apache.hudi.source.stats.ColumnStats;

import org.apache.flink.table.types.logical.LogicalType;

import javax.validation.constraints.NotNull;

import static org.apache.hudi.util.EvaluatorUtils.compare;

/**
 * To evaluate <= expr.
 */
public class LessThanOrEqual extends NullFalseEvaluator {
  private static final long serialVersionUID = 1L;

  public static LessThanOrEqual getInstance() {
    return new LessThanOrEqual();
  }

  @Override
  protected boolean eval(@NotNull Object val, ColumnStats columnStats, LogicalType type) {
    Object minVal = columnStats.getMinVal();
    if (minVal == null) {
      return false;
    }
    return compare(minVal, val, type) <= 0;
  }

  @Override
  protected boolean eval(@NotNull Object val, @NotNull Object columnValue, LogicalType type) {
    return compare(columnValue, val, type) <= 0;
  }
}
