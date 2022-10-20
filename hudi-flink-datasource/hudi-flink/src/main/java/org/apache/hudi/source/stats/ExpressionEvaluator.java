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

package org.apache.hudi.source.stats;

import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.util.ExpressionUtils;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import javax.validation.constraints.NotNull;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tool to evaluate the {@link org.apache.flink.table.expressions.ResolvedExpression}s.
 */
public class ExpressionEvaluator {
  private static final int IN_PREDICATE_LIMIT = 200;

  /**
   * Filter the index row with specific data filters and query fields.
   *
   * @param filters     The pushed down data filters
   * @param indexRow    The index row
   * @param queryFields The query fields referenced by the filters
   * @return true if the index row should be considered as a candidate
   */
  public static boolean filterExprs(List<ResolvedExpression> filters, RowData indexRow, RowType.RowField[] queryFields) {
    for (ResolvedExpression filter : filters) {
      if (!Evaluator.bindCall((CallExpression) filter, indexRow, queryFields).eval()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Used for deciding whether the literal values match the column stats.
   * The evaluator can be nested.
   */
  public abstract static class Evaluator {
    // the constant literal value
    protected Object val;

    // column stats
    protected Object minVal;
    protected Object maxVal;
    protected long nullCnt = 0;

    // referenced field type
    protected LogicalType type;

    /**
     * Binds the evaluator with specific call expression.
     *
     * <p>Three steps to bind the call:
     * 1. map the evaluator instance;
     * 2. bind the field reference;
     * 3. bind the column stats.
     *
     * <p>Normalize the expression to simplify the subsequent decision logic:
     * always put the literal expression in the RHS.
     */
    public static Evaluator bindCall(CallExpression call, RowData indexRow, RowType.RowField[] queryFields) {
      FunctionDefinition funDef = call.getFunctionDefinition();
      List<Expression> childExprs = call.getChildren();

      boolean normalized = childExprs.get(0) instanceof FieldReferenceExpression;
      final Evaluator evaluator;

      if (BuiltInFunctionDefinitions.NOT.equals(funDef)) {
        evaluator = Not.getInstance();
        Evaluator childEvaluator = bindCall((CallExpression) childExprs.get(0), indexRow, queryFields);
        return ((Not) evaluator).bindEvaluator(childEvaluator);
      }

      if (BuiltInFunctionDefinitions.AND.equals(funDef)) {
        evaluator = And.getInstance();
        Evaluator evaluator1 = bindCall((CallExpression) childExprs.get(0), indexRow, queryFields);
        Evaluator evaluator2 = bindCall((CallExpression) childExprs.get(1), indexRow, queryFields);
        return ((And) evaluator).bindEvaluator(evaluator1, evaluator2);
      }

      if (BuiltInFunctionDefinitions.OR.equals(funDef)) {
        evaluator = Or.getInstance();
        Evaluator evaluator1 = bindCall((CallExpression) childExprs.get(0), indexRow, queryFields);
        Evaluator evaluator2 = bindCall((CallExpression) childExprs.get(1), indexRow, queryFields);
        return ((Or) evaluator).bindEvaluator(evaluator1, evaluator2);
      }

      // handle IN specifically
      if (BuiltInFunctionDefinitions.IN.equals(funDef)) {
        ValidationUtils.checkState(normalized, "The IN expression expects to be normalized");
        evaluator = In.getInstance();
        FieldReferenceExpression rExpr = (FieldReferenceExpression) childExprs.get(0);
        evaluator.bindFieldReference(rExpr);
        ((In) evaluator).bindVals(getInLiteralVals(childExprs));
        return evaluator.bindColStats(indexRow, queryFields, rExpr);
      }

      // handle unary operators
      if (BuiltInFunctionDefinitions.IS_NULL.equals(funDef)) {
        FieldReferenceExpression rExpr = (FieldReferenceExpression) childExprs.get(0);
        return IsNull.getInstance()
            .bindFieldReference(rExpr)
            .bindColStats(indexRow, queryFields, rExpr);
      } else if (BuiltInFunctionDefinitions.IS_NOT_NULL.equals(funDef)) {
        FieldReferenceExpression rExpr = (FieldReferenceExpression) childExprs.get(0);
        return IsNotNull.getInstance()
            .bindFieldReference(rExpr)
            .bindColStats(indexRow, queryFields, rExpr);
      }

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
          .bindFieldReference(rExpr)
          .bindVal(vExpr)
          .bindColStats(indexRow, queryFields, rExpr);
      return evaluator;
    }

    public Evaluator bindColStats(
        RowData indexRow,
        RowType.RowField[] queryFields,
        FieldReferenceExpression expr) {
      int colPos = -1;
      for (int i = 0; i < queryFields.length; i++) {
        if (expr.getName().equals(queryFields[i].getName())) {
          colPos = i;
        }
      }
      ValidationUtils.checkState(colPos != -1, "Can not find column " + expr.getName());
      int startPos = 2 + colPos * 3;
      LogicalType colType = queryFields[colPos].getType();
      Object minVal = indexRow.isNullAt(startPos) ? null : getValAsJavaObj(indexRow, startPos, colType);
      Object maxVal = indexRow.isNullAt(startPos + 1) ? null : getValAsJavaObj(indexRow, startPos + 1, colType);
      long nullCnt = indexRow.getLong(startPos + 2);

      this.minVal = minVal;
      this.maxVal = maxVal;
      this.nullCnt = nullCnt;
      return this;
    }

    public Evaluator bindVal(ValueLiteralExpression vExpr) {
      this.val = ExpressionUtils.getValueFromLiteral(vExpr);
      return this;
    }

    public Evaluator bindFieldReference(FieldReferenceExpression expr) {
      this.type = expr.getOutputDataType().getLogicalType();
      return this;
    }

    public abstract boolean eval();
  }

  /**
   * To evaluate = expr.
   */
  public static class EqualTo extends Evaluator {

    public static EqualTo getInstance() {
      return new EqualTo();
    }

    @Override
    public boolean eval() {
      if (this.minVal == null || this.maxVal == null || this.val == null) {
        return false;
      }
      if (compare(this.minVal, this.val, this.type) > 0) {
        return false;
      }
      return compare(this.maxVal, this.val, this.type) >= 0;
    }
  }

  /**
   * To evaluate <> expr.
   */
  public static class NotEqualTo extends Evaluator {
    public static NotEqualTo getInstance() {
      return new NotEqualTo();
    }

    @Override
    public boolean eval() {
      // because the bounds are not necessarily a min or max value, this cannot be answered using
      // them. notEq(col, X) with (X, Y) doesn't guarantee that X is a value in col.
      return true;
    }
  }

  /**
   * To evaluate IS NULL expr.
   */
  public static class IsNull extends Evaluator {
    public static IsNull getInstance() {
      return new IsNull();
    }

    @Override
    public boolean eval() {
      return this.nullCnt > 0;
    }
  }

  /**
   * To evaluate IS NOT NULL expr.
   */
  public static class IsNotNull extends Evaluator {
    public static IsNotNull getInstance() {
      return new IsNotNull();
    }

    @Override
    public boolean eval() {
      // should consider FLOAT/DOUBLE & NAN
      return this.minVal != null || this.nullCnt <= 0;
    }
  }

  /**
   * To evaluate < expr.
   */
  public static class LessThan extends Evaluator {
    public static LessThan getInstance() {
      return new LessThan();
    }

    @Override
    public boolean eval() {
      if (this.minVal == null) {
        return false;
      }
      return compare(this.minVal, this.val, this.type) < 0;
    }
  }

  /**
   * To evaluate > expr.
   */
  public static class GreaterThan extends Evaluator {
    public static GreaterThan getInstance() {
      return new GreaterThan();
    }

    @Override
    public boolean eval() {
      if (this.maxVal == null) {
        return false;
      }
      return compare(this.maxVal, this.val, this.type) > 0;
    }
  }

  /**
   * To evaluate <= expr.
   */
  public static class LessThanOrEqual extends Evaluator {
    public static LessThanOrEqual getInstance() {
      return new LessThanOrEqual();
    }

    @Override
    public boolean eval() {
      if (this.minVal == null) {
        return false;
      }
      return compare(this.minVal, this.val, this.type) <= 0;
    }
  }

  /**
   * To evaluate >= expr.
   */
  public static class GreaterThanOrEqual extends Evaluator {
    public static GreaterThanOrEqual getInstance() {
      return new GreaterThanOrEqual();
    }

    @Override
    public boolean eval() {
      if (this.maxVal == null) {
        return false;
      }
      return compare(this.maxVal, this.val, this.type) >= 0;
    }
  }

  /**
   * To evaluate IN expr.
   */
  public static class In extends Evaluator {
    public static In getInstance() {
      return new In();
    }

    private Object[] vals;

    @Override
    public boolean eval() {
      if (this.minVal == null) {
        return false; // values are all null and literalSet cannot contain null.
      }

      if (vals.length > IN_PREDICATE_LIMIT) {
        // skip evaluating the predicate if the number of values is too big
        return true;
      }

      vals = Arrays.stream(vals).filter(v -> compare(this.minVal, v, this.type) <= 0).toArray();
      if (vals.length == 0) { // if all values are less than lower bound, rows cannot match.
        return false;
      }

      vals = Arrays.stream(vals).filter(v -> compare(this.maxVal, v, this.type) >= 0).toArray();
      if (vals.length == 0) { // if all remaining values are greater than upper bound, rows cannot match.
        return false;
      }

      return true;
    }

    public void bindVals(Object... vals) {
      this.vals = vals;
    }
  }

  // component predicate

  /**
   * To evaluate NOT expr.
   */
  public static class Not extends Evaluator {
    public static Not getInstance() {
      return new Not();
    }

    private Evaluator evaluator;

    @Override
    public boolean eval() {
      return !this.evaluator.eval();
    }

    public Evaluator bindEvaluator(Evaluator evaluator) {
      this.evaluator = evaluator;
      return this;
    }
  }

  /**
   * To evaluate AND expr.
   */
  public static class And extends Evaluator {
    public static And getInstance() {
      return new And();
    }

    private Evaluator[] evaluators;

    @Override
    public boolean eval() {
      for (Evaluator evaluator : evaluators) {
        if (!evaluator.eval()) {
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
  public static class Or extends Evaluator {
    public static Or getInstance() {
      return new Or();
    }

    private Evaluator[] evaluators;

    @Override
    public boolean eval() {
      for (Evaluator evaluator : evaluators) {
        if (evaluator.eval()) {
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
        return ((Integer) val1).compareTo((Integer) val2);
      case FLOAT:
        return ((Float) val1).compareTo((Float) val2);
      case DOUBLE:
        return ((Double) val1).compareTo((Double) val2);
      case BINARY:
      case VARBINARY:
        return compareBytes((byte[]) val1, (byte[]) val2);
      case CHAR:
      case VARCHAR:
        return ((String) val1).compareTo((String) val2);
      case DECIMAL:
        return ((BigDecimal) val1).compareTo((BigDecimal) val2);
      default:
        throw new UnsupportedOperationException("Unsupported type: " + logicalType);
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
