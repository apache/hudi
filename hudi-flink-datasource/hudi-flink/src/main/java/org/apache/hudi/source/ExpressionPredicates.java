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

package org.apache.hudi.source;

import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.hudi.util.ImplicitTypeConverter;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.common.util.ValidationUtils.checkState;
import static org.apache.hudi.util.ExpressionUtils.getValueFromLiteral;
import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.booleanColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.doubleColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.floatColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.gt;
import static org.apache.parquet.filter2.predicate.FilterApi.gtEq;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.lt;
import static org.apache.parquet.filter2.predicate.FilterApi.ltEq;
import static org.apache.parquet.filter2.predicate.FilterApi.not;
import static org.apache.parquet.filter2.predicate.FilterApi.notEq;
import static org.apache.parquet.filter2.predicate.FilterApi.or;
import static org.apache.parquet.io.api.Binary.fromConstantByteArray;
import static org.apache.parquet.io.api.Binary.fromString;

/**
 * Tool to predicate the {@link org.apache.flink.table.expressions.ResolvedExpression}s.
 */
public class ExpressionPredicates {

  private static final Logger LOG = LoggerFactory.getLogger(ExpressionPredicates.class);

  /**
   * Converts specific call expression list to the predicate list.
   *
   * @param resolvedExpressions The resolved expressions to convert.
   * @return The converted predicates.
   */
  public static List<Predicate> fromExpression(List<ResolvedExpression> resolvedExpressions) {
    return resolvedExpressions.stream()
        .map(e -> fromExpression((CallExpression) e))
        .collect(Collectors.toList());
  }

  /**
   * Converts specific call expression to the predicate.
   *
   * <p>Two steps to bind the call:
   * 1. map the predicate instance;
   * 2. bind the field reference;
   *
   * <p>Normalize the expression to simplify the subsequent decision logic:
   * always put the literal expression in the RHS.
   *
   * @param callExpression The call expression to convert.
   * @return The converted predicate.
   */
  public static Predicate fromExpression(CallExpression callExpression) {
    FunctionDefinition functionDefinition = callExpression.getFunctionDefinition();
    List<Expression> childExpressions = callExpression.getChildren();

    boolean normalized = childExpressions.get(0) instanceof FieldReferenceExpression;

    if (BuiltInFunctionDefinitions.NOT.equals(functionDefinition)) {
      Not predicate = Not.getInstance();
      Predicate childPredicate = fromExpression((CallExpression) childExpressions.get(0));
      return predicate.bindPredicate(childPredicate);
    }

    if (BuiltInFunctionDefinitions.AND.equals(functionDefinition)) {
      And predicate = And.getInstance();
      Predicate predicate1 = fromExpression((CallExpression) childExpressions.get(0));
      Predicate predicate2 = fromExpression((CallExpression) childExpressions.get(1));
      return predicate.bindPredicates(predicate1, predicate2);
    }

    if (BuiltInFunctionDefinitions.OR.equals(functionDefinition)) {
      Or predicate = Or.getInstance();
      Predicate predicate1 = fromExpression((CallExpression) childExpressions.get(0));
      Predicate predicate2 = fromExpression((CallExpression) childExpressions.get(1));
      return predicate.bindPredicates(predicate1, predicate2);
    }

    if (BuiltInFunctionDefinitions.IS_NULL.equals(functionDefinition)
        || BuiltInFunctionDefinitions.IS_NOT_NULL.equals(functionDefinition)
        || childExpressions.stream().anyMatch(e -> e instanceof ValueLiteralExpression
        && getValueFromLiteral((ValueLiteralExpression) e) == null)) {
      return AlwaysNull.getInstance();
    }

    // handle IN specifically
    if (BuiltInFunctionDefinitions.IN.equals(functionDefinition)) {
      checkState(normalized, "The IN expression expects to be normalized");
      In in = In.getInstance();
      FieldReferenceExpression fieldReference = (FieldReferenceExpression) childExpressions.get(0);
      List<ValueLiteralExpression> valueLiterals = IntStream.range(1, childExpressions.size())
          .mapToObj(index -> (ValueLiteralExpression) childExpressions.get(index))
          .collect(Collectors.toList());
      return in.bindValueLiterals(valueLiterals).bindFieldReference(fieldReference);
    }

    ColumnPredicate predicate;
    // handle binary operators
    if (BuiltInFunctionDefinitions.EQUALS.equals(functionDefinition)) {
      predicate = Equals.getInstance();
    } else if (BuiltInFunctionDefinitions.NOT_EQUALS.equals(functionDefinition)) {
      predicate = NotEquals.getInstance();
    } else if (BuiltInFunctionDefinitions.LESS_THAN.equals(functionDefinition)) {
      predicate = normalized ? LessThan.getInstance() : GreaterThan.getInstance();
    } else if (BuiltInFunctionDefinitions.GREATER_THAN.equals(functionDefinition)) {
      predicate = normalized ? GreaterThan.getInstance() : LessThan.getInstance();
    } else if (BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL.equals(functionDefinition)) {
      predicate = normalized ? LessThanOrEqual.getInstance() : GreaterThanOrEqual.getInstance();
    } else if (BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL.equals(functionDefinition)) {
      predicate = normalized ? GreaterThanOrEqual.getInstance() : LessThanOrEqual.getInstance();
    } else {
      throw new AssertionError("Unexpected function definition " + functionDefinition);
    }
    FieldReferenceExpression fieldReference = normalized
        ? (FieldReferenceExpression) childExpressions.get(0)
        : (FieldReferenceExpression) childExpressions.get(1);
    ValueLiteralExpression valueLiteral = normalized
        ? (ValueLiteralExpression) childExpressions.get(1)
        : (ValueLiteralExpression) childExpressions.get(0);
    return predicate.bindValueLiteral(valueLiteral).bindFieldReference(fieldReference);
  }

  // --------------------------------------------------------------------------------------------
  //  Classes to define predicates
  // --------------------------------------------------------------------------------------------

  /**
   * A filter predicate that can be evaluated by the FileInputFormat.
   */
  public interface Predicate extends Serializable {

    /**
     * Predicates the criteria for which records to keep when loading data from a parquet file.
     *
     * @return A filter predicate of parquet file.
     */
    FilterPredicate filter();
  }

  /**
   * Column predicate which depends on the given field.
   */
  public abstract static class ColumnPredicate implements Predicate {

    // referenced field type
    protected LogicalType literalType;

    // referenced field name
    protected String columnName;

    // the constant literal value
    protected Serializable literal;

    /**
     * Binds field reference to create a column predicate.
     *
     * @param fieldReference The field reference to negate.
     * @return A column predicate.
     */
    public ColumnPredicate bindFieldReference(FieldReferenceExpression fieldReference) {
      this.literalType = fieldReference.getOutputDataType().getLogicalType();
      this.columnName = fieldReference.getName();
      return this;
    }

    /**
     * Binds value literal to create a column predicate.
     *
     * @param valueLiteral The value literal to negate.
     * @return A column predicate.
     */
    public ColumnPredicate bindValueLiteral(ValueLiteralExpression valueLiteral) {
      Object literalObject = getValueFromLiteral(valueLiteral);
      // validate that literal is serializable
      if (literalObject instanceof Serializable) {
        this.literal = (Serializable) literalObject;
      } else {
        LOG.warn("Encountered a non-serializable literal. " + "Cannot push predicate with value literal [{}] into FileInputFormat. " + "This is a bug and should be reported.", valueLiteral);
        this.literal = null;
      }
      return this;
    }

    @Override
    public FilterPredicate filter() {
      Serializable convertedLiteral = ImplicitTypeConverter.convertImplicitly(literalType, literal);
      return toParquetPredicate(getFunctionDefinition(), literalType, columnName, convertedLiteral);
    }

    /**
     * Returns function definition of predicate.
     *
     * @return A function definition of predicate.
     */
    public FunctionDefinition getFunctionDefinition() {
      return null;
    }
  }

  /**
   * An EQUALS predicate that can be evaluated by the FileInputFormat.
   */
  public static class Equals extends ColumnPredicate {

    private static final long serialVersionUID = 1L;

    /**
     * Returns a EQUALS predicate.
     *
     * @return A EQUALS predicate instance.
     */
    public static Equals getInstance() {
      return new Equals();
    }

    @Override
    public FunctionDefinition getFunctionDefinition() {
      return BuiltInFunctionDefinitions.EQUALS;
    }

    @Override
    public String toString() {
      return columnName + " = " + literal;
    }
  }

  /**
   * A NOT_EQUALS predicate that can be evaluated by the FileInputFormat.
   */
  public static class NotEquals extends ColumnPredicate {

    private static final long serialVersionUID = 1L;

    /**
     * Returns a NOT_EQUALS predicate.
     *
     * @return A NOT_EQUALS predicate instance.
     */
    public static NotEquals getInstance() {
      return new NotEquals();
    }

    @Override
    public FunctionDefinition getFunctionDefinition() {
      return BuiltInFunctionDefinitions.NOT_EQUALS;
    }

    @Override
    public String toString() {
      return columnName + " != " + literal;
    }
  }

  /**
   * A LESS_THAN predicate that can be evaluated by the FileInputFormat.
   */
  public static class LessThan extends ColumnPredicate {

    private static final long serialVersionUID = 1L;

    /**
     * Returns a LESS_THAN predicate.
     *
     * @return A LESS_THAN predicate instance.
     */
    public static LessThan getInstance() {
      return new LessThan();
    }

    @Override
    public FunctionDefinition getFunctionDefinition() {
      return BuiltInFunctionDefinitions.LESS_THAN;
    }

    @Override
    public String toString() {
      return columnName + " < " + literal;
    }
  }

  /**
   * A GREATER_THAN predicate that can be evaluated by the FileInputFormat.
   */
  public static class GreaterThan extends ColumnPredicate {

    private static final long serialVersionUID = 1L;

    /**
     * Returns a GREATER_THAN predicate.
     *
     * @return A GREATER_THAN predicate instance.
     */
    public static GreaterThan getInstance() {
      return new GreaterThan();
    }

    @Override
    public FunctionDefinition getFunctionDefinition() {
      return BuiltInFunctionDefinitions.GREATER_THAN;
    }

    @Override
    public String toString() {
      return columnName + " > " + literal;
    }
  }

  /**
   * A LESS_THAN_OR_EQUAL predicate that can be evaluated by the FileInputFormat.
   */
  public static class LessThanOrEqual extends ColumnPredicate {

    private static final long serialVersionUID = 1L;

    /**
     * Returns a LESS_THAN_OR_EQUAL predicate.
     *
     * @return A LESS_THAN_OR_EQUAL predicate instance.
     */
    public static LessThanOrEqual getInstance() {
      return new LessThanOrEqual();
    }

    @Override
    public FunctionDefinition getFunctionDefinition() {
      return BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL;
    }

    @Override
    public String toString() {
      return columnName + " <= " + literal;
    }
  }

  /**
   * A GREATER_THAN_OR_EQUAL predicate that can be evaluated by the FileInputFormat.
   */
  public static class GreaterThanOrEqual extends ColumnPredicate {

    private static final long serialVersionUID = 1L;

    /**
     * Returns a GREATER_THAN_OR_EQUAL predicate.
     *
     * @return A GREATER_THAN_OR_EQUAL predicate instance.
     */
    public static GreaterThanOrEqual getInstance() {
      return new GreaterThanOrEqual();
    }

    @Override
    public FunctionDefinition getFunctionDefinition() {
      return BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL;
    }

    @Override
    public String toString() {
      return columnName + " >= " + literal;
    }
  }

  /**
   * An IN predicate that can be evaluated by the FileInputFormat.
   */
  public static class In extends ColumnPredicate {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ExpressionEvaluators.In.class);


    private static final int IN_PREDICATE_LIMIT = 200;

    // the constant literal values
    protected List<Serializable> literals;

    /**
     * Returns an IN predicate.
     *
     * @return An IN predicate instance.
     */
    public static In getInstance() {
      return new In();
    }

    /**
     * Binds value literals to create an IN predicate.
     *
     * @param valueLiterals The value literals to negate.
     * @return An IN predicate.
     */
    public ColumnPredicate bindValueLiterals(List<ValueLiteralExpression> valueLiterals) {
      this.literals = valueLiterals.stream().map(valueLiteral -> {
        Object literalObject = getValueFromLiteral(valueLiteral);
        // validate that literal is serializable
        if (literalObject instanceof Serializable) {
          return (Serializable) literalObject;
        } else {
          LOG.warn("Encountered a non-serializable literal. " + "Cannot push predicate with value literal [{}] into FileInputFormat. " + "This is a bug and should be reported.", valueLiteral);
          return null;
        }
      }).collect(Collectors.toList());
      return this;
    }

    @Override
    public FilterPredicate filter() {
      if (literals.stream().anyMatch(Objects::isNull) || literals.size() > IN_PREDICATE_LIMIT) {
        return null;
      }

      FilterPredicate filterPredicate = null;
      for (Serializable literal : literals) {
        FilterPredicate predicate = toParquetPredicate(BuiltInFunctionDefinitions.EQUALS, literalType, columnName, literal);
        if (predicate != null) {
          filterPredicate = filterPredicate == null ? predicate : or(filterPredicate, predicate);
        }
      }
      return filterPredicate;
    }

    @Override
    public String toString() {
      return columnName + " IN(" + Arrays.toString(literals.toArray()) + ")";
    }
  }

  /**
   * A special predicate which is not possible to match any condition.
   */
  public static class AlwaysNull implements Predicate {

    private static final long serialVersionUID = 1L;

    public static AlwaysNull getInstance() {
      return new AlwaysNull();
    }

    @Override
    public FilterPredicate filter() {
      return null;
    }
  }

  /**
   * A NOT predicate to negate a predicate that can be evaluated by the FileInputFormat.
   */
  public static class Not implements Predicate {

    private static final long serialVersionUID = 1L;

    private Predicate predicate;

    /**
     * Returns a NOT predicate.
     */
    public static Not getInstance() {
      return new Not();
    }

    /**
     * Binds predicate to create a NOT predicate.
     *
     * @param predicate The predicate to negate.
     * @return A NOT predicate.
     */
    public Predicate bindPredicate(Predicate predicate) {
      this.predicate = predicate;
      return this;
    }

    @Override
    public FilterPredicate filter() {
      FilterPredicate filterPredicate = predicate.filter();
      if (null == filterPredicate) {
        return null;
      }
      return not(filterPredicate);
    }

    @Override
    public String toString() {
      return "NOT(" + predicate.toString() + ")";
    }
  }

  /**
   * An AND predicate that can be evaluated by the FileInputFormat.
   */
  public static class And implements Predicate {

    private static final long serialVersionUID = 1L;

    private Predicate[] predicates;

    /**
     * Returns an AND predicate.
     */
    public static And getInstance() {
      return new And();
    }

    /**
     * Binds predicates to create an AND predicate.
     *
     * @param predicates The disjunctive predicates.
     * @return An AND predicate.
     */
    public Predicate bindPredicates(Predicate... predicates) {
      this.predicates = predicates;
      return this;
    }

    @Override
    public FilterPredicate filter() {
      FilterPredicate filterPredicate0 = predicates[0].filter();
      FilterPredicate filterPredicate1 = predicates[1].filter();
      if (null == filterPredicate0 || null == filterPredicate1) {
        return null;
      }
      return and(filterPredicate0, filterPredicate1);
    }

    @Override
    public String toString() {
      return "AND(" + Arrays.toString(predicates) + ")";
    }
  }

  /**
   * An OR predicate that can be evaluated by the FileInputFormat.
   */
  public static class Or implements Predicate {

    private static final long serialVersionUID = 1L;

    private Predicate[] predicates;

    /**
     * Returns an OR predicate.
     */
    public static Or getInstance() {
      return new Or();
    }

    /**
     * Binds predicates to create an OR predicate.
     *
     * @param predicates The disjunctive predicates.
     * @return An OR predicate.
     */
    public Predicate bindPredicates(Predicate... predicates) {
      this.predicates = predicates;
      return this;
    }

    @Override
    public FilterPredicate filter() {
      FilterPredicate filterPredicate0 = predicates[0].filter();
      FilterPredicate filterPredicate1 = predicates[1].filter();
      if (null == filterPredicate0 || null == filterPredicate1) {
        return null;
      }
      return or(filterPredicate0, filterPredicate1);
    }

    @Override
    public String toString() {
      return "OR(" + Arrays.toString(predicates) + ")";
    }
  }

  private static FilterPredicate toParquetPredicate(FunctionDefinition functionDefinition, LogicalType literalType, String columnName, Serializable literal) {
    switch (literalType.getTypeRoot()) {
      case BOOLEAN:
        return predicateSupportsEqNotEq(functionDefinition, booleanColumn(columnName), (Boolean) literal);
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case DATE:
      case TIME_WITHOUT_TIME_ZONE:
        return predicateSupportsLtGt(functionDefinition, intColumn(columnName), (Integer) literal);
      case BIGINT:
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        return predicateSupportsLtGt(functionDefinition, longColumn(columnName), (Long) literal);
      case FLOAT:
        return predicateSupportsLtGt(functionDefinition, floatColumn(columnName), (Float) literal);
      case DOUBLE:
        return predicateSupportsLtGt(functionDefinition, doubleColumn(columnName), (Double) literal);
      case BINARY:
      case VARBINARY:
        return predicateSupportsLtGt(functionDefinition, binaryColumn(columnName), fromConstantByteArray((byte[]) literal));
      case CHAR:
      case VARCHAR:
        return predicateSupportsLtGt(functionDefinition, binaryColumn(columnName), fromString((String) literal));
      default:
        return null;
    }
  }

  private static <T extends Comparable<T>, C extends Operators.Column<T> & Operators.SupportsEqNotEq> FilterPredicate predicateSupportsEqNotEq(
      FunctionDefinition functionDefinition, C column, T value) {
    if (BuiltInFunctionDefinitions.EQUALS.equals(functionDefinition)) {
      return eq(column, value);
    } else if (BuiltInFunctionDefinitions.NOT_EQUALS.equals(functionDefinition)) {
      return notEq(column, value);
    } else {
      throw new AssertionError("Unexpected function definition " + functionDefinition);
    }
  }

  private static <T extends Comparable<T>, C extends Operators.Column<T> & Operators.SupportsLtGt> FilterPredicate predicateSupportsLtGt(FunctionDefinition functionDefinition, C column, T value) {
    if (BuiltInFunctionDefinitions.EQUALS.equals(functionDefinition)) {
      return eq(column, value);
    } else if (BuiltInFunctionDefinitions.NOT_EQUALS.equals(functionDefinition)) {
      return notEq(column, value);
    } else if (BuiltInFunctionDefinitions.LESS_THAN.equals(functionDefinition)) {
      return lt(column, value);
    } else if (BuiltInFunctionDefinitions.GREATER_THAN.equals(functionDefinition)) {
      return gt(column, value);
    } else if (BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL.equals(functionDefinition)) {
      return ltEq(column, value);
    } else if (BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL.equals(functionDefinition)) {
      return gtEq(column, value);
    } else {
      throw new AssertionError("Unexpected function definition " + functionDefinition);
    }
  }
}
