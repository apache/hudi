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

package org.apache.hudi.source;

import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.source.stats.ColumnStats;
import org.apache.hudi.util.ExpressionUtils;

import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.logical.LogicalType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Tool to evaluate the {@link org.apache.flink.table.expressions.ResolvedExpression}s.
 */
public class ExpressionEvaluators {
  
  /**
   * Converts specific call expression list to the evaluator list.
   */
  public static List<Evaluator> fromExpression(List<ResolvedExpression> exprs) {
    return exprs.stream()
        .map(e -> fromExpression((CallExpression) e))
        .collect(Collectors.toList());
  }

  /**
   * Converts specific call expression to the evaluator.
   * <p>Two steps to bind the call:
   * 1. map the evaluator instance;
   * 2. bind the field reference;
   *
   * <p>Normalize the expression to simplify the subsequent decision logic:
   * always put the literal expression in the RHS.
   */
  public static Evaluator fromExpression(CallExpression expr) {
    FunctionDefinition funDef = expr.getFunctionDefinition();
    List<Expression> childExprs = expr.getChildren();

    boolean normalized = childExprs.get(0) instanceof FieldReferenceExpression;

    if (BuiltInFunctionDefinitions.NOT.equals(funDef)) {
      Not evaluator = Not.getInstance();
      Evaluator childEvaluator = fromExpression((CallExpression) childExprs.get(0));
      return evaluator.bindEvaluator(childEvaluator);
    }

    if (BuiltInFunctionDefinitions.AND.equals(funDef)) {
      And evaluator = And.getInstance();
      Evaluator evaluator1 = fromExpression((CallExpression) childExprs.get(0));
      Evaluator evaluator2 = fromExpression((CallExpression) childExprs.get(1));
      return evaluator.bindEvaluator(evaluator1, evaluator2);
    }

    if (BuiltInFunctionDefinitions.OR.equals(funDef)) {
      Or evaluator = Or.getInstance();
      Evaluator evaluator1 = fromExpression((CallExpression) childExprs.get(0));
      Evaluator evaluator2 = fromExpression((CallExpression) childExprs.get(1));
      return evaluator.bindEvaluator(evaluator1, evaluator2);
    }

    // handle unary operators
    if (BuiltInFunctionDefinitions.IS_NULL.equals(funDef)) {
      FieldReferenceExpression rExpr = (FieldReferenceExpression) childExprs.get(0);
      return IsNull.getInstance()
          .bindFieldReference(rExpr);
    } else if (BuiltInFunctionDefinitions.IS_NOT_NULL.equals(funDef)) {
      FieldReferenceExpression rExpr = (FieldReferenceExpression) childExprs.get(0);
      return IsNotNull.getInstance()
          .bindFieldReference(rExpr);
    }

    boolean hasNullLiteral =
        childExprs.stream().anyMatch(e ->
            e instanceof ValueLiteralExpression
                && ExpressionUtils.getValueFromLiteral((ValueLiteralExpression) e) == null);
    if (hasNullLiteral) {
      return AlwaysFalse.getInstance();
    }

    // handle IN specifically
    if (BuiltInFunctionDefinitions.IN.equals(funDef)) {
      ValidationUtils.checkState(normalized, "The IN expression expects to be normalized");
      In in = In.getInstance();
      FieldReferenceExpression rExpr = (FieldReferenceExpression) childExprs.get(0);
      in.bindFieldReference(rExpr);
      in.bindVals(getInLiteralVals(childExprs));
      return in;
    }

    NullFalseEvaluator evaluator;
    // handle binary operators
    if (BuiltInFunctionDefinitions.EQUALS.equals(funDef)) {
      evaluator = EqualTo.getInstance();
    } else if (BuiltInFunctionDefinitions.NOT_EQUALS.equals(funDef)) {
      evaluator = NotEqualTo.getInstance();
    } else if (BuiltInFunctionDefinitions.LESS_THAN.equals(funDef)) {
      evaluator = normalized ? LessThan.getInstance() : GreaterThan.getInstance();
    } else if (BuiltInFunctionDefinitions.GREATER_THAN.equals(funDef)) {
      evaluator = normalized ? GreaterThan.getInstance() : LessThan.getInstance();
    } else if (BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL.equals(funDef)) {
      evaluator = normalized ? LessThanOrEqual.getInstance() : GreaterThanOrEqual.getInstance();
    } else if (BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL.equals(funDef)) {
      evaluator = normalized ? GreaterThanOrEqual.getInstance() : LessThanOrEqual.getInstance();
    } else {
      throw new AssertionError("Unexpected function definition " + funDef);
    }
    FieldReferenceExpression rExpr = normalized
        ? (FieldReferenceExpression) childExprs.get(0)
        : (FieldReferenceExpression) childExprs.get(1);
    ValueLiteralExpression vExpr = normalized
        ? (ValueLiteralExpression) childExprs.get(1)
        : (ValueLiteralExpression) childExprs.get(0);
    evaluator
        .bindVal(vExpr)
        .bindFieldReference(rExpr);
    return evaluator;
  }

  /**
   * Decides whether it's possible to match based on the column values and column stats.
   * The evaluator can be nested.
   */
  public interface Evaluator extends Serializable {

    /**
     * Evaluates whether it's possible to match based on the column stats.
     *
     * @param columnStatsMap column statistics
     * @return false if there is no any possible to match, true otherwise.
     */
    boolean eval(Map<String, ColumnStats> columnStatsMap);
  }

  /**
   * Leaf evaluator which depends on the given field.
   */
  public abstract static class LeafEvaluator implements Evaluator {

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

  /**
   * Leaf evaluator which compares the field value with literal values.
   */
  public abstract static class NullFalseEvaluator extends LeafEvaluator {

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
  }

  /**
   * To evaluate = expr.
   */
  public static class EqualTo extends NullFalseEvaluator {
    private static final long serialVersionUID = 1L;

    public static EqualTo getInstance() {
      return new EqualTo();
    }

    @Override
    protected boolean eval(@NotNull Object val, ColumnStats columnStats, LogicalType type) {
      Object minVal = columnStats.getMinVal();
      Object maxVal = columnStats.getMaxVal();
      if (minVal == null || maxVal == null) {
        return false;
      }
      if (compare(minVal, val, type) > 0) {
        return false;
      }
      return compare(maxVal, val, type) >= 0;
    }
  }

  /**
   * To evaluate <> expr.
   */
  public static class NotEqualTo extends NullFalseEvaluator {
    private static final long serialVersionUID = 1L;

    public static NotEqualTo getInstance() {
      return new NotEqualTo();
    }

    @Override
    protected boolean eval(@NotNull Object val, ColumnStats columnStats, LogicalType type) {
      Object minVal = columnStats.getMinVal();
      Object maxVal = columnStats.getMaxVal();
      if (minVal == null || maxVal == null) {
        return false;
      }
      // return false if min == max == val, otherwise return true.
      // because the bounds are not necessarily a min or max value, this cannot be answered using them.
      // notEq(col, X) with (X, Y) doesn't guarantee that X is a value in col.
      return compare(minVal, val, type) != 0 || compare(maxVal, val, type) != 0;
    }
  }

  /**
   * To evaluate IS NULL expr.
   */
  public static class IsNull extends LeafEvaluator {
    private static final long serialVersionUID = 1L;

    public static IsNull getInstance() {
      return new IsNull();
    }

    @Override
    public boolean eval(Map<String, ColumnStats> columnStatsMap) {
      ColumnStats columnStats = getColumnStats(columnStatsMap);
      return columnStats.getNullCnt() > 0;
    }
  }

  /**
   * To evaluate IS NOT NULL expr.
   */
  public static class IsNotNull extends LeafEvaluator {
    private static final long serialVersionUID = 1L;

    public static IsNotNull getInstance() {
      return new IsNotNull();
    }

    @Override
    public boolean eval(Map<String, ColumnStats> columnStatsMap) {
      ColumnStats columnStats = getColumnStats(columnStatsMap);
      // should consider FLOAT/DOUBLE & NAN
      return columnStats.getMinVal() != null || columnStats.getNullCnt() <= 0;
    }
  }

  /**
   * To evaluate < expr.
   */
  public static class LessThan extends NullFalseEvaluator {
    private static final long serialVersionUID = 1L;

    public static LessThan getInstance() {
      return new LessThan();
    }

    @Override
    public boolean eval(@NotNull Object val, ColumnStats columnStats, LogicalType type) {
      Object minVal = columnStats.getMinVal();
      if (minVal == null) {
        return false;
      }
      return compare(minVal, val, type) < 0;
    }
  }

  /**
   * To evaluate > expr.
   */
  public static class GreaterThan extends NullFalseEvaluator {
    private static final long serialVersionUID = 1L;

    public static GreaterThan getInstance() {
      return new GreaterThan();
    }

    @Override
    protected boolean eval(@NotNull Object val, ColumnStats columnStats, LogicalType type) {
      Object maxVal = columnStats.getMaxVal();
      if (maxVal == null) {
        return false;
      }
      return compare(maxVal, val, type) > 0;
    }
  }

  /**
   * To evaluate <= expr.
   */
  public static class LessThanOrEqual extends NullFalseEvaluator {
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
  }

  /**
   * To evaluate >= expr.
   */
  public static class GreaterThanOrEqual extends NullFalseEvaluator {
    private static final long serialVersionUID = 1L;

    public static GreaterThanOrEqual getInstance() {
      return new GreaterThanOrEqual();
    }

    @Override
    protected boolean eval(@NotNull Object val, ColumnStats columnStats, LogicalType type) {
      Object maxVal = columnStats.getMaxVal();
      if (maxVal == null) {
        return false;
      }
      return compare(maxVal, val, type) >= 0;
    }
  }

  /**
   * To evaluate IN expr.
   */
  public static class In extends LeafEvaluator {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(In.class);

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
        LOGGER.warn("Skip evaluating in predicate because the number of values is too big!");
        return true;
      }

      return Arrays.stream(vals).anyMatch(v ->
          compare(minVal, v, this.type) <= 0 && compare(maxVal, v, this.type) >= 0);
    }

    public void bindVals(Object... vals) {
      this.vals = vals;
    }
  }

  /**
   * A special evaluator which is not possible to match any condition.
   */
  public static class AlwaysFalse implements Evaluator {
    private static final long serialVersionUID = 1L;

    public static AlwaysFalse getInstance() {
      return new AlwaysFalse();
    }

    @Override
    public boolean eval(Map<String, ColumnStats> columnStatsMap) {
      return false;
    }
  }

  // component predicate

  /**
   * To evaluate NOT expr.
   */
  public static class Not implements Evaluator {
    private static final long serialVersionUID = 1L;

    public static Not getInstance() {
      return new Not();
    }

    private Evaluator evaluator;

    @Override
    public boolean eval(Map<String, ColumnStats> columnStatsMap) {
      return !this.evaluator.eval(columnStatsMap);
    }

    public Evaluator bindEvaluator(Evaluator evaluator) {
      this.evaluator = evaluator;
      return this;
    }
  }

  /**
   * To evaluate AND expr.
   */
  public static class And implements Evaluator {
    private static final long serialVersionUID = 1L;

    public static And getInstance() {
      return new And();
    }

    private Evaluator[] evaluators;

    @Override
    public boolean eval(Map<String, ColumnStats> columnStatsMap) {
      for (Evaluator evaluator : evaluators) {
        if (!evaluator.eval(columnStatsMap)) {
          return false;
        }
      }
      return true;
    }

    public Evaluator bindEvaluator(Evaluator... evaluators) {
      this.evaluators = evaluators;
      return this;
    }
  }

  /**
   * To evaluate OR expr.
   */
  public static class Or implements Evaluator {
    private static final long serialVersionUID = 1L;

    public static Or getInstance() {
      return new Or();
    }

    private Evaluator[] evaluators;

    @Override
    public boolean eval(Map<String, ColumnStats> columnStatsMap) {
      for (Evaluator evaluator : evaluators) {
        if (evaluator.eval(columnStatsMap)) {
          return true;
        }
      }
      return false;
    }

    public Evaluator bindEvaluator(Evaluator... evaluators) {
      this.evaluators = evaluators;
      return this;
    }
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------
  
  /**
   * Returns the IN expression literal values.
   */
  private static Object[] getInLiteralVals(List<Expression> childExprs) {
    List<Object> vals = new ArrayList<>();
    for (int i = 1; i < childExprs.size(); i++) {
      vals.add(ExpressionUtils.getValueFromLiteral((ValueLiteralExpression) childExprs.get(i)));
    }
    return vals.toArray();
  }

  private static int compare(@NotNull Object val1, @NotNull Object val2, LogicalType logicalType) {
    switch (logicalType.getTypeRoot()) {
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIME_WITHOUT_TIME_ZONE:
      case DATE:
        return ((Long) val1).compareTo((Long) val2); 
      case BOOLEAN:
        return ((Boolean) val1).compareTo((Boolean) val2);
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
      case DECIMAL:
        return getBigDecimal(val1).compareTo(getBigDecimal(val2));
      case BINARY:
      case VARBINARY:
        return compareBytes((byte[]) val1, (byte[]) val2);
      case CHAR:
      case VARCHAR:
        return ((String) val1).compareTo((String) val2);
      default:
        throw new UnsupportedOperationException("Unsupported type: " + logicalType);
    }
  }

  private static BigDecimal getBigDecimal(@NotNull Object value) {
    if (value instanceof BigDecimal) {
      return (BigDecimal) value;
    } else if (value instanceof Double) {
      // new BigDecimal() are used instead of BigDecimal.valueOf() due to
      // receive exact decimal representation of the double's binary floating-point value
      return BigDecimal.valueOf((Double) value);
    } else if (value instanceof Float) {
      return BigDecimal.valueOf(((Float) value).doubleValue());
    } else if (value instanceof Long) {
      return new BigDecimal((Long) value);
    } else if (value instanceof Integer) {
      return new BigDecimal((Integer) value);
    } else if (value instanceof Short) {
      return new BigDecimal((Short) value);
    } else if (value instanceof Byte) {
      return new BigDecimal((Byte) value);
    } else {
      throw new UnsupportedOperationException("Unable convert to BigDecimal: " + value);
    }
  }
  
  private static int compareBytes(byte[] v1, byte[] v2) {
    int len1 = v1.length;
    int len2 = v2.length;
    int lim = Math.min(len1, len2);

    int k = 0;
    while (k < lim) {
      byte c1 = v1[k];
      byte c2 = v2[k];
      if (c1 != c2) {
        return c1 - c2;
      }
      k++;
    }
    return len1 - len2;
  }
}
