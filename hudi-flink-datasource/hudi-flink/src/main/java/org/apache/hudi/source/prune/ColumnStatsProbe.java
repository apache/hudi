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

package org.apache.hudi.source.prune;

import org.apache.hudi.source.ExpressionEvaluators;
import org.apache.hudi.source.stats.ColumnStats;
import org.apache.hudi.util.ExpressionUtils;

import lombok.Getter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.source.ExpressionEvaluators.fromExpression;

/**
 * Utility for filtering the column stats metadata payloads.
 */
public class ColumnStatsProbe implements Serializable {
  private static final long serialVersionUID = 1L;

  @Getter
  private final String[] referencedCols;
  private final List<ExpressionEvaluators.Evaluator> evaluators;

  private ColumnStatsProbe(String[] referencedCols, List<ExpressionEvaluators.Evaluator> evaluators) {
    this.referencedCols = referencedCols;
    this.evaluators = evaluators;
  }

  /**
   * Filters the index row with specific data filters and query fields.
   *
   * @param indexRow    The index row
   * @param queryFields The query fields referenced by the filters
   * @return true if the index row should be considered as a candidate
   */
  public boolean test(RowData indexRow, RowType.RowField[] queryFields) {
    Map<String, ColumnStats> columnStatsMap = convertColumnStats(indexRow, queryFields);
    for (ExpressionEvaluators.Evaluator evaluator : evaluators) {
      if (!evaluator.eval(columnStatsMap)) {
        return false;
      }
    }
    return true;
  }

  @Nullable
  public static ColumnStatsProbe newInstance(List<ResolvedExpression> filters) {
    if (filters.isEmpty()) {
      return null;
    }
    String[] referencedCols = ExpressionUtils.referencedColumns(filters);
    if (referencedCols.length == 0) {
      return null;
    }
    List<ExpressionEvaluators.Evaluator> evaluators = fromExpression(filters);
    return new ColumnStatsProbe(referencedCols, evaluators);
  }

  public static Map<String, ColumnStats> convertColumnStats(RowData indexRow, RowType.RowField[] queryFields) {
    if (indexRow == null || queryFields == null) {
      throw new IllegalArgumentException("Index Row and query fields could not be null.");
    }
    Map<String, ColumnStats> mapping = new LinkedHashMap<>();
    for (int i = 0; i < queryFields.length; i++) {
      String name = queryFields[i].getName();
      int startPos = 2 + i * 3;
      LogicalType colType = queryFields[i].getType();
      Object minVal = indexRow.isNullAt(startPos) ? null : getValAsJavaObj(indexRow, startPos, colType);
      Object maxVal = indexRow.isNullAt(startPos + 1) ? null : getValAsJavaObj(indexRow, startPos + 1, colType);
      long nullCnt = indexRow.getLong(startPos + 2);
      mapping.put(name, new ColumnStats(minVal, maxVal, nullCnt));
    }
    return mapping;
  }

  /**
   * Returns the value as Java object at position {@code pos} of row {@code indexRow}.
   */
  private static Object getValAsJavaObj(RowData indexRow, int pos, LogicalType colType) {
    switch (colType.getTypeRoot()) {
      // NOTE: Since we can't rely on Avro's "date", and "timestamp-micros" logical-types, we're
      //       manually encoding corresponding values as int and long w/in the Column Stats Index and
      //       here we have to decode those back into corresponding logical representation.
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        TimestampType tsType = (TimestampType) colType;
        return indexRow.getTimestamp(pos, tsType.getPrecision()).getMillisecond();
      case TIME_WITHOUT_TIME_ZONE:
      case DATE:
      case BIGINT:
        return indexRow.getLong(pos);
      // NOTE: All integral types of size less than Int are encoded as Ints in MT
      case BOOLEAN:
        return indexRow.getBoolean(pos);
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return indexRow.getInt(pos);
      case FLOAT:
        return indexRow.getFloat(pos);
      case DOUBLE:
        return indexRow.getDouble(pos);
      case BINARY:
      case VARBINARY:
        return indexRow.getBinary(pos);
      case CHAR:
      case VARCHAR:
        return indexRow.getString(pos).toString();
      case DECIMAL:
        DecimalType decimalType = (DecimalType) colType;
        return indexRow.getDecimal(pos, decimalType.getPrecision(), decimalType.getScale()).toBigDecimal();
      default:
        throw new UnsupportedOperationException("Unsupported type: " + colType);
    }
  }
}
