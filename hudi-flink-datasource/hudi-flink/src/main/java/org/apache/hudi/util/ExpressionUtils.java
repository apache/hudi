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

package org.apache.hudi.util;

import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;

/**
 * Utilities for expression resolving.
 */
public class ExpressionUtils {

  /**
   * Collect the referenced columns with given expressions,
   * only simple call expression is supported.
   */
  public static String[] referencedColumns(List<ResolvedExpression> exprs) {
    return exprs.stream()
        .map(ExpressionUtils::getReferencedColumns)
        .filter(columns -> columns.length > 0)
        .flatMap(Arrays::stream)
        .distinct() // deduplication
        .toArray(String[]::new);
  }

  /**
   * Returns whether the given expression is simple call expression:
   * a binary call with one operand as field reference and another operand
   * as literal.
   */
  public static boolean isSimpleCallExpression(Expression expr) {
    if (!(expr instanceof CallExpression)) {
      return false;
    }
    CallExpression callExpression = (CallExpression) expr;
    FunctionDefinition funcDef = callExpression.getFunctionDefinition();
    // simple call list:
    // NOT AND OR IN EQUALS NOT_EQUALS IS_NULL IS_NOT_NULL LESS_THAN GREATER_THAN
    // LESS_THAN_OR_EQUAL GREATER_THAN_OR_EQUAL

    if (funcDef == BuiltInFunctionDefinitions.NOT
        || funcDef == BuiltInFunctionDefinitions.AND
        || funcDef == BuiltInFunctionDefinitions.OR) {
      return callExpression.getChildren().stream()
          .allMatch(ExpressionUtils::isSimpleCallExpression);
    }
    if (!(funcDef == BuiltInFunctionDefinitions.IN
        || funcDef == BuiltInFunctionDefinitions.EQUALS
        || funcDef == BuiltInFunctionDefinitions.NOT_EQUALS
        || funcDef == BuiltInFunctionDefinitions.IS_NULL
        || funcDef == BuiltInFunctionDefinitions.IS_NOT_NULL
        || funcDef == BuiltInFunctionDefinitions.LESS_THAN
        || funcDef == BuiltInFunctionDefinitions.GREATER_THAN
        || funcDef == BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL
        || funcDef == BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL)) {
      return false;
    }
    // handle IN
    if (funcDef == BuiltInFunctionDefinitions.IN) {
      // In expression RHS operands are always literals
      return true;
    }
    // handle unary operator
    if (funcDef == BuiltInFunctionDefinitions.IS_NULL
        || funcDef == BuiltInFunctionDefinitions.IS_NOT_NULL) {
      return callExpression.getChildren().stream()
          .allMatch(e -> e instanceof FieldReferenceExpression);
    }
    // handle binary operator
    return isFieldReferenceAndLiteral(callExpression.getChildren());
  }

  private static boolean isFieldReferenceAndLiteral(List<Expression> exprs) {
    if (exprs.size() != 2) {
      return false;
    }
    final Expression expr0 = exprs.get(0);
    final Expression expr1 = exprs.get(1);
    return expr0 instanceof FieldReferenceExpression && expr1 instanceof ValueLiteralExpression
        || expr0 instanceof ValueLiteralExpression && expr1 instanceof FieldReferenceExpression;
  }

  private static String[] getReferencedColumns(ResolvedExpression expression) {
    CallExpression callExpr = (CallExpression) expression;
    FunctionDefinition funcDef = callExpr.getFunctionDefinition();
    if (funcDef == BuiltInFunctionDefinitions.NOT
        || funcDef == BuiltInFunctionDefinitions.AND
        || funcDef == BuiltInFunctionDefinitions.OR) {
      return callExpr.getChildren().stream()
          .map(e -> getReferencedColumns((ResolvedExpression) e))
          .flatMap(Arrays::stream)
          .toArray(String[]::new);
    }

    return expression.getChildren().stream()
        .filter(expr -> expr instanceof FieldReferenceExpression)
        .map(expr -> ((FieldReferenceExpression) expr).getName())
        .toArray(String[]::new);
  }

  /**
   * Returns the value with given value literal expression.
   *
   * <p>Returns null if the value can not parse as the output data type correctly,
   * should call {@code ValueLiteralExpression.isNull} first to decide whether
   * the literal is NULL.
   */
  @Nullable
  public static Object getValueFromLiteral(ValueLiteralExpression expr) {
    LogicalType logicalType = expr.getOutputDataType().getLogicalType();
    switch (logicalType.getTypeRoot()) {
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        return expr.getValueAs(LocalDateTime.class)
            .map(ldt -> ldt.toInstant(ZoneOffset.UTC).toEpochMilli())
            .orElse(null);
      case TIME_WITHOUT_TIME_ZONE:
        return expr.getValueAs(LocalTime.class)
            .map(lt -> lt.get(ChronoField.MILLI_OF_DAY))
            .orElse(null);
      case DATE:
        return expr.getValueAs(LocalDate.class)
            .map(date -> (int) date.toEpochDay())
            .orElse(null);
      // NOTE: All integral types of size less than Int are encoded as Ints in MT
      case BOOLEAN:
        return expr.getValueAs(Boolean.class).orElse(null);
      case TINYINT:
        return expr.getValueAs(Byte.class).orElse(null);
      case SMALLINT:
        return expr.getValueAs(Short.class).orElse(null);
      case INTEGER:
        return expr.getValueAs(Integer.class).orElse(null);
      case BIGINT:
        return expr.getValueAs(Long.class).orElse(null);
      case FLOAT:
        return expr.getValueAs(Float.class).orElse(null);
      case DOUBLE:
        return expr.getValueAs(Double.class).orElse(null);
      case BINARY:
      case VARBINARY:
        return expr.getValueAs(byte[].class).orElse(null);
      case CHAR:
      case VARCHAR:
        return expr.getValueAs(String.class).orElse(null);
      case DECIMAL:
        return expr.getValueAs(BigDecimal.class).orElse(null);
      default:
        throw new UnsupportedOperationException("Unsupported type: " + logicalType);
    }
  }

  /**
   * Returns the field as part of a hoodie key with given value literal expression.
   *
   * <p>CAUTION: the data type and value parsing should follow the impl of {@link #getValueFromLiteral(ValueLiteralExpression)}.
   *
   * <p>CAUTION: the data and timestamp conversion should follow the impl if {@code HoodieAvroUtils.convertValueForAvroLogicalTypes}.
   *
   * <p>Returns null if the value can not parse as the output data type correctly,
   * should call {@code ValueLiteralExpression.isNull} first to decide whether
   * the literal is NULL.
   */
  @Nullable
  public static Object getKeyFromLiteral(ValueLiteralExpression expr, boolean logicalTimestamp) {
    Object val = getValueFromLiteral(expr);
    if (val == null) {
      return null;
    }
    LogicalType logicalType = expr.getOutputDataType().getLogicalType();
    switch (logicalType.getTypeRoot()) {
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        return logicalTimestamp ? new Timestamp((long) val) : val;
      case DATE:
        return LocalDate.ofEpochDay((int) val);
      default:
        return val;
    }
  }

  /**
   * Returns whether all the fields {@code fields} are involved in the filtering predicates.
   *
   * @param exprs  The filters
   * @param fields The field set
   */
  public static boolean isFilteringByAllFields(List<ResolvedExpression> exprs, Set<String> fields) {
    if (exprs.size() != fields.size()) {
      return false;
    }
    Set<String> referencedPks = exprs.stream()
        .map(ResolvedExpression::getChildren)
        .flatMap(Collection::stream)
        .filter(expr -> expr instanceof FieldReferenceExpression)
        .map(rExpr -> ((FieldReferenceExpression) rExpr).getName())
        .collect(Collectors.toSet());
    return referencedPks.size() == fields.size();
  }

  /**
   * Returns whether the given expression {@code resolvedExpr} is a
   * literal equivalence predicate within the fields {@code fields}.
   */
  public static boolean isEqualsLitExpr(ResolvedExpression resolvedExpr, Set<String> fields) {
    CallExpression callExpr = (CallExpression) resolvedExpr;
    FunctionDefinition funcDef = callExpr.getFunctionDefinition();
    if (funcDef != BuiltInFunctionDefinitions.EQUALS) {
      return false;
    }

    if (!isFieldReferenceAndLiteral(callExpr.getChildren())) {
      return false;
    }

    return callExpr.getChildren().stream()
        .filter(expr -> expr instanceof FieldReferenceExpression)
        .anyMatch(expr -> fields.contains(((FieldReferenceExpression) expr).getName()));
  }

  public static List<ResolvedExpression> filterSimpleCallExpression(List<ResolvedExpression> exprs) {
    return exprs.stream()
        .filter(ExpressionUtils::isSimpleCallExpression)
        .collect(Collectors.toList());
  }

  /**
   * Extracts partition predicate from filter condition.
   *
   * <p>NOTE: the {@code expressions} should be simple call expressions.
   *
   * @return A tuple of partition predicates and non-partition predicates.
   */
  public static Tuple2<List<ResolvedExpression>, List<ResolvedExpression>> splitExprByPartitionCall(
      List<ResolvedExpression> expressions,
      List<String> partitionKeys,
      RowType tableRowType) {
    if (partitionKeys.isEmpty()) {
      return Tuple2.of(expressions, Collections.emptyList());
    } else {
      List<ResolvedExpression> partitionFilters = new ArrayList<>();
      List<ResolvedExpression> nonPartitionFilters = new ArrayList<>();
      final List<String> fieldNames = tableRowType.getFieldNames();
      Set<Integer> parFieldPos = partitionKeys.stream().map(fieldNames::indexOf).collect(Collectors.toSet());
      for (ResolvedExpression expr : expressions) {
        for (CallExpression e : splitByAnd(expr)) {
          if (isPartitionCallExpr(e, parFieldPos)) {
            partitionFilters.add(expr);
          } else {
            nonPartitionFilters.add(e);
          }
        }
      }
      return Tuple2.of(nonPartitionFilters, partitionFilters);
    }
  }

  /**
   * Filter the filter expressions that only contain indexed columns.
   */
  public static List<ResolvedExpression> filterExpressionWithIndexedCols(List<ResolvedExpression> expressions, HoodieTableMetaClient metaClient, HoodieSchema tableSchema) {
    if (metaClient == null) {
      return Collections.emptyList();
    }
    HoodieIndexDefinition indexDefinition = metaClient.getIndexMetadata().map(indexMeta -> indexMeta.getIndexDefinitions().get(PARTITION_NAME_COLUMN_STATS)).orElse(null);
    if (indexDefinition == null) {
      return Collections.emptyList();
    }
    List<String> indexedCols = HoodieTableMetadataUtil.getValidIndexedColumns(indexDefinition, tableSchema, metaClient.getTableConfig());
    return expressions.stream().filter(expr -> {
      String[] refs = referencedColumns(Collections.singletonList(expr));
      return Arrays.stream(refs).allMatch(indexedCols::contains);
    }).collect(Collectors.toList());
  }

  private static List<CallExpression> splitByAnd(ResolvedExpression expr) {
    List<CallExpression> result = new ArrayList<>();
    splitByAnd(expr, result);
    return result;
  }

  private static void splitByAnd(
      ResolvedExpression expr,
      List<CallExpression> result) {
    if (!(expr instanceof CallExpression)) {
      return;
    }
    CallExpression callExpr = (CallExpression) expr;
    FunctionDefinition funcDef = callExpr.getFunctionDefinition();

    if (funcDef == BuiltInFunctionDefinitions.AND) {
      callExpr.getChildren().stream()
          .filter(child -> child instanceof CallExpression)
          .forEach(child -> splitByAnd((CallExpression) child, result));
    } else {
      result.add(callExpr);
    }
  }

  /**
   * Returns whether the {@code expr} is a partition call expression.
   *
   * @param expr        The expression
   * @param parFieldPos The partition field positions within the table schema
   */
  private static boolean isPartitionCallExpr(CallExpression expr, Set<Integer> parFieldPos) {
    List<Expression> children = expr.getChildren();
    // if any child expr reference a non-partition field, returns false.
    return children.stream()
        .allMatch(
            child -> {
              if (child instanceof FieldReferenceExpression) {
                FieldReferenceExpression refExpr = (FieldReferenceExpression) child;
                return parFieldPos.contains(refExpr.getFieldIndex());
              } else if (child instanceof CallExpression) {
                return isPartitionCallExpr((CallExpression) child, parFieldPos);
              } else {
                return true;
              }
            });
  }
}
