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

package org.apache.parquet.internal.filter2.columnindex;

import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.FilterPredicateCompat;
import org.apache.parquet.filter2.compat.FilterCompat.NoOpFilter;
import org.apache.parquet.filter2.compat.FilterCompat.UnboundRecordFilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate.Visitor;
import org.apache.parquet.filter2.predicate.Operators.And;
import org.apache.parquet.filter2.predicate.Operators.Column;
import org.apache.parquet.filter2.predicate.Operators.Eq;
import org.apache.parquet.filter2.predicate.Operators.Gt;
import org.apache.parquet.filter2.predicate.Operators.GtEq;
import org.apache.parquet.filter2.predicate.Operators.LogicalNotUserDefined;
import org.apache.parquet.filter2.predicate.Operators.Lt;
import org.apache.parquet.filter2.predicate.Operators.LtEq;
import org.apache.parquet.filter2.predicate.Operators.Not;
import org.apache.parquet.filter2.predicate.Operators.NotEq;
import org.apache.parquet.filter2.predicate.Operators.Or;
import org.apache.parquet.filter2.predicate.Operators.UserDefined;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore.MissingOffsetIndexException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.function.Function;

/**
 * Filter implementation based on column indexes.
 * No filtering will be applied for columns where no column index is available.
 * Offset index is required for all the columns in the projection, therefore a {@link MissingOffsetIndexException} will
 * be thrown from any {@code visit} methods if any of the required offset indexes is missing.
 */
public class HoodieColumnIndexFilter implements Visitor<HoodieRowRanges> {

  private static final Logger LOGGER = LoggerFactory.getLogger(HoodieColumnIndexFilter.class);
  private final ColumnIndexStore columnIndexStore;
  private final Set<ColumnPath> columns;
  private final long rowCount;
  private HoodieRowRanges allRows;

  /**
   * Calculates the row ranges containing the indexes of the rows might match the specified filter.
   *
   * @param filter
   *          to be used for filtering the rows
   * @param columnIndexStore
   *          the store for providing column/offset indexes
   * @param paths
   *          the paths of the columns used in the actual projection; a column not being part of the projection will be
   *          handled as containing {@code null} values only even if the column has values written in the file
   * @param rowCount
   *          the total number of rows in the row-group
   * @return the ranges of the possible matching row indexes; the returned ranges will contain all the rows if any of
   *         the required offset index is missing
   */
  public static HoodieRowRanges calculateHoodieRowRanges(FilterCompat.Filter filter, ColumnIndexStore columnIndexStore,
                                             Set<ColumnPath> paths, long rowCount) {
    return filter.accept(new FilterCompat.Visitor<HoodieRowRanges>() {
      @Override
      public HoodieRowRanges visit(FilterPredicateCompat filterPredicateCompat) {
        try {
          return filterPredicateCompat.getFilterPredicate()
              .accept(new HoodieColumnIndexFilter(columnIndexStore, paths, rowCount));
        } catch (MissingOffsetIndexException e) {
          LOGGER.info(e.getMessage());
          return HoodieRowRanges.createSingle(rowCount);
        }
      }

      @Override
      public HoodieRowRanges visit(UnboundRecordFilterCompat unboundRecordFilterCompat) {
        return HoodieRowRanges.createSingle(rowCount);
      }

      @Override
      public HoodieRowRanges visit(NoOpFilter noOpFilter) {
        return HoodieRowRanges.createSingle(rowCount);
      }
    });
  }

  private HoodieColumnIndexFilter(ColumnIndexStore columnIndexStore, Set<ColumnPath> paths, long rowCount) {
    this.columnIndexStore = columnIndexStore;
    this.columns = paths;
    this.rowCount = rowCount;
  }

  private HoodieRowRanges allRows() {
    if (allRows == null) {
      allRows = HoodieRowRanges.createSingle(rowCount);
    }
    return allRows;
  }

  @Override
  public <T extends Comparable<T>> HoodieRowRanges visit(Eq<T> eq) {
    return applyPredicate(eq.getColumn(), ci -> ci.visit(eq), eq.getValue() == null ? allRows() : HoodieRowRanges.EMPTY);
  }

  @Override
  public <T extends Comparable<T>> HoodieRowRanges visit(NotEq<T> notEq) {
    return applyPredicate(notEq.getColumn(), ci -> ci.visit(notEq),
        notEq.getValue() == null ? HoodieRowRanges.EMPTY : allRows());
  }

  @Override
  public <T extends Comparable<T>> HoodieRowRanges visit(Lt<T> lt) {
    return applyPredicate(lt.getColumn(), ci -> ci.visit(lt), HoodieRowRanges.EMPTY);
  }

  @Override
  public <T extends Comparable<T>> HoodieRowRanges visit(LtEq<T> ltEq) {
    return applyPredicate(ltEq.getColumn(), ci -> ci.visit(ltEq), HoodieRowRanges.EMPTY);
  }

  @Override
  public <T extends Comparable<T>> HoodieRowRanges visit(Gt<T> gt) {
    return applyPredicate(gt.getColumn(), ci -> ci.visit(gt), HoodieRowRanges.EMPTY);
  }

  @Override
  public <T extends Comparable<T>> HoodieRowRanges visit(GtEq<T> gtEq) {
    return applyPredicate(gtEq.getColumn(), ci -> ci.visit(gtEq), HoodieRowRanges.EMPTY);
  }

  @Override
  public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> HoodieRowRanges visit(UserDefined<T, U> udp) {
    return applyPredicate(udp.getColumn(), ci -> ci.visit(udp),
        udp.getUserDefinedPredicate().acceptsNullValue() ? allRows() : HoodieRowRanges.EMPTY);
  }

  @Override
  public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> HoodieRowRanges visit(
      LogicalNotUserDefined<T, U> udp) {
    return applyPredicate(udp.getUserDefined().getColumn(), ci -> ci.visit(udp),
        udp.getUserDefined().getUserDefinedPredicate().acceptsNullValue() ? HoodieRowRanges.EMPTY : allRows());
  }

  private HoodieRowRanges applyPredicate(Column<?> column, Function<ColumnIndex, PrimitiveIterator.OfInt> func,
                                   HoodieRowRanges rangesForMissingColumns) {
    ColumnPath columnPath = column.getColumnPath();
    if (!columns.contains(columnPath)) {
      return rangesForMissingColumns;
    }

    OffsetIndex oi = columnIndexStore.getOffsetIndex(columnPath);
    ColumnIndex ci = columnIndexStore.getColumnIndex(columnPath);
    if (ci == null) {
      LOGGER.info("No column index for column {} is available; Unable to filter on this column", columnPath);
      return allRows();
    }

    return HoodieRowRanges.create(rowCount, func.apply(ci), oi);
  }

  @Override
  public HoodieRowRanges visit(And and) {
    return HoodieRowRanges.intersection(and.getLeft().accept(this), and.getRight().accept(this));
  }

  @Override
  public HoodieRowRanges visit(Or or) {
    return HoodieRowRanges.union(or.getLeft().accept(this), or.getRight().accept(this));
  }

  @Override
  public HoodieRowRanges visit(Not not) {
    throw new IllegalArgumentException(
        "Predicates containing a NOT must be run through LogicalInverseRewriter. " + not);
  }
}
