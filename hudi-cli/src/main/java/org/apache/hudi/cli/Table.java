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

package org.apache.hudi.cli;

import org.apache.hudi.common.util.Option;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Table to be rendered. This class takes care of ordering rows and limiting before renderer renders it.
 */
public class Table implements Iterable<List<String>> {

  // Header for this table
  private final TableHeader rowHeader;
  // User-specified conversions before rendering
  private final Map<String, Function<Object, String>> fieldNameToConverterMap;
  // Option attribute to track sorting field
  private final Option<String> orderingFieldNameOptional;
  // Whether sorting has to be in descending order (by default : optional)
  private final Option<Boolean> isDescendingOptional;
  // Limit the number of entries rendered
  private final Option<Integer> limitOptional;
  // Raw list of rows
  private final List<List<Comparable>> rawRows;
  // Flag to determine if all the rows have been added
  private boolean finishedAdding = false;
  // Rows ready for Rendering
  private List<List<String>> renderRows;

  public Table(TableHeader rowHeader, Map<String, Function<Object, String>> fieldNameToConverterMap,
      Option<String> orderingFieldNameOptional, Option<Boolean> isDescendingOptional, Option<Integer> limitOptional) {
    this.rowHeader = rowHeader;
    this.fieldNameToConverterMap = fieldNameToConverterMap;
    this.orderingFieldNameOptional = orderingFieldNameOptional;
    this.isDescendingOptional = isDescendingOptional;
    this.limitOptional = limitOptional;
    this.rawRows = new ArrayList<>();
  }

  /**
   * Main API to add row to the table.
   * 
   * @param row Row
   */
  public Table add(List<Comparable> row) {
    if (finishedAdding) {
      throw new IllegalStateException("Container already marked done for adding. No more entries can be added.");
    }

    if (rowHeader.getFieldNames().size() != row.size()) {
      throw new IllegalArgumentException("Incorrect number of fields in row. Expected: "
          + rowHeader.getFieldNames().size() + ", Got: " + row.size() + ", Row: " + row);
    }

    this.rawRows.add(new ArrayList<>(row));
    return this;
  }

  /**
   * Add all rows.
   * 
   * @param rows Rows to be added
   * @return
   */
  public Table addAll(List<List<Comparable>> rows) {
    rows.forEach(this::add);
    return this;
  }

  /**
   * Add all rows.
   * 
   * @param rows Rows to be added
   * @return
   */
  public Table addAllRows(List<Comparable[]> rows) {
    rows.forEach(r -> add(Arrays.asList(r)));
    return this;
  }

  /**
   * API to let the table know writing is over and reading is going to start.
   */
  public Table flip() {
    this.finishedAdding = true;
    sortAndLimit();
    return this;
  }

  /**
   * Sorting of rows by a specified field.
   * 
   * @return
   */
  private List<List<Comparable>> orderRows() {
    return orderingFieldNameOptional.map(orderingColumnName -> {
      return rawRows.stream().sorted((row1, row2) -> {
        Comparable fieldForRow1 = row1.get(rowHeader.indexOf(orderingColumnName));
        Comparable fieldForRow2 = row2.get(rowHeader.indexOf(orderingColumnName));
        int cmpRawResult = fieldForRow1.compareTo(fieldForRow2);
        return isDescendingOptional.map(isDescending -> isDescending ? -1 * cmpRawResult : cmpRawResult).orElse(cmpRawResult);
      }).collect(Collectors.toList());
    }).orElse(rawRows);
  }

  /**
   * Prepares for rendering. Rows are sorted and limited
   */
  private void sortAndLimit() {
    this.renderRows = new ArrayList<>();
    final int limit = this.limitOptional.orElse(rawRows.size());
    final List<List<Comparable>> orderedRows = orderRows();
    renderRows = orderedRows.stream().limit(limit).map(row -> IntStream.range(0, rowHeader.getNumFields()).mapToObj(idx -> {
      String fieldName = rowHeader.get(idx);
      if (fieldNameToConverterMap.containsKey(fieldName)) {
        return fieldNameToConverterMap.get(fieldName).apply(row.get(idx));
      }
      Object v = row.get(idx);
      return v == null ? "null" : v.toString();
    }).collect(Collectors.toList())).collect(Collectors.toList());
  }

  @Override
  public Iterator<List<String>> iterator() {
    if (!finishedAdding) {
      throw new IllegalStateException("Container must be flipped before reading the data");
    }
    return renderRows.iterator();
  }

  @Override
  public void forEach(Consumer<? super List<String>> action) {
    if (!finishedAdding) {
      throw new IllegalStateException("Container must be flipped before reading the data");
    }
    renderRows.forEach(action);
  }

  public List<String> getFieldNames() {
    return rowHeader.getFieldNames();
  }

  public List<List<String>> getRenderRows() {
    return renderRows;
  }
}
