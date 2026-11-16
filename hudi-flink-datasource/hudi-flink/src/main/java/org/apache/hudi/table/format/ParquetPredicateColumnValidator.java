/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.table.format;

import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.schema.MessageType;

/** Checks physical column availability without evaluating or rewriting a Parquet predicate. */
final class ParquetPredicateColumnValidator implements FilterPredicate.Visitor<Boolean> {
  private final MessageType fileSchema;

  private ParquetPredicateColumnValidator(MessageType fileSchema) {
    this.fileSchema = fileSchema;
  }

  static boolean hasAllColumns(FilterPredicate predicate, MessageType fileSchema) {
    try {
      return predicate.accept(new ParquetPredicateColumnValidator(fileSchema));
    } catch (UnsupportedOperationException e) {
      // Newer Parquet versions can add operators absent from the minimum supported API.
      // Omit the whole predicate if its column references cannot be checked.
      return false;
    }
  }

  private boolean contains(Operators.Column<?> column) {
    return fileSchema.containsPath(column.getColumnPath().toArray());
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(Operators.Eq<T> predicate) {
    return contains(predicate.getColumn());
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(Operators.NotEq<T> predicate) {
    return contains(predicate.getColumn());
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(Operators.Lt<T> predicate) {
    return contains(predicate.getColumn());
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(Operators.LtEq<T> predicate) {
    return contains(predicate.getColumn());
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(Operators.Gt<T> predicate) {
    return contains(predicate.getColumn());
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(Operators.GtEq<T> predicate) {
    return contains(predicate.getColumn());
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(Operators.In<T> predicate) {
    return contains(predicate.getColumn());
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(Operators.NotIn<T> predicate) {
    return contains(predicate.getColumn());
  }

  @Override
  public Boolean visit(Operators.And predicate) {
    return predicate.getLeft().accept(this) && predicate.getRight().accept(this);
  }

  @Override
  public Boolean visit(Operators.Or predicate) {
    return predicate.getLeft().accept(this) && predicate.getRight().accept(this);
  }

  @Override
  public Boolean visit(Operators.Not predicate) {
    return predicate.getPredicate().accept(this);
  }

  @Override
  public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Boolean visit(Operators.UserDefined<T, U> predicate) {
    return contains(predicate.getColumn());
  }

  @Override
  public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Boolean visit(Operators.LogicalNotUserDefined<T, U> predicate) {
    return predicate.getUserDefined().accept(this);
  }
}
