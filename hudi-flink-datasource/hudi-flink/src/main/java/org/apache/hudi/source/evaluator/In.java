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

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import static org.apache.hudi.util.EvaluatorUtils.compare;

/**
 * To evaluate IN expr.
 */
public class In extends LeafEvaluator {
  private static final long serialVersionUID = 1L;

  private static final int IN_PREDICATE_LIMIT = 200;

  public static In getInstance() {
    return new In();
  }

  private Object[] vals;

  @Override
  public boolean eval(Map<String, ColumnStats> columnStatsMap) {
    if (Arrays.stream(vals).anyMatch(Objects::isNull)) {
      return false;
    }
    ColumnStats columnStats = getColumnStats(columnStatsMap);
    Object minVal = columnStats.getMinVal();
    Object maxVal = columnStats.getMaxVal();
    if (minVal == null) {
      return false; // values are all null and literalSet cannot contain null.
    }

    if (vals.length > IN_PREDICATE_LIMIT) {
      // skip evaluating the predicate if the number of values is too big
      return true;
    }

    return Arrays.stream(vals).anyMatch(v ->
        compare(minVal, v, this.type) <= 0 && compare(maxVal, v, this.type) >= 0);
  }

  public void bindVals(Object... vals) {
    this.vals = vals;
  }
}
