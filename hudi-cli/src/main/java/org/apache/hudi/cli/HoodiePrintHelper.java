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

import com.jakewharton.fliptables.FlipTable;

import java.util.List;
import java.util.Map;
import java.util.function.Function;


/**
 * Helper class to render table for hoodie-cli.
 */
public class HoodiePrintHelper {

  /**
   * Print header and raw rows.
   *
   * @param header Header
   * @param rows Raw Rows
   * @return output
   */
  public static String print(String[] header, String[][] rows) {
    return printTextTable(header, rows);
  }

  /**
   * Serialize Table to printable string.
   *
   * @param rowHeader Row Header
   * @param fieldNameToConverterMap Field Specific Converters
   * @param sortByField Sorting field
   * @param isDescending Order
   * @param limit Limit
   * @param headerOnly Headers only
   * @param rows List of rows
   * @return Serialized form for printing
   */
  public static String print(TableHeader rowHeader, Map<String, Function<Object, String>> fieldNameToConverterMap,
      String sortByField, boolean isDescending, Integer limit, boolean headerOnly, List<Comparable[]> rows) {

    if (headerOnly) {
      return HoodiePrintHelper.print(rowHeader);
    }

    if (!sortByField.isEmpty() && !rowHeader.containsField(sortByField)) {
      return String.format("Field[%s] is not in table, given columns[%s]", sortByField, rowHeader.getFieldNames());
    }

    Table table =
        new Table(rowHeader, fieldNameToConverterMap, Option.ofNullable(sortByField.isEmpty() ? null : sortByField),
            Option.ofNullable(isDescending), Option.ofNullable(limit <= 0 ? null : limit)).addAllRows(rows).flip();

    return HoodiePrintHelper.print(table);
  }

  /**
   * Render rows in Table.
   *
   * @param buffer Table
   * @return output
   */
  private static String print(Table buffer) {
    String[] header = new String[buffer.getFieldNames().size()];
    buffer.getFieldNames().toArray(header);

    String[][] rows =
        buffer.getRenderRows().stream().map(l -> l.stream().toArray(String[]::new)).toArray(String[][]::new);
    return printTextTable(header, rows);
  }

  /**
   * Render only header of the table.
   *
   * @param header Table Header
   * @return output
   */
  private static String print(TableHeader header) {
    String[] head = new String[header.getFieldNames().size()];
    header.getFieldNames().toArray(head);
    return printTextTable(head, new String[][] {});
  }

  /**
   * Print Text table.
   *
   * @param headers Headers
   * @param data Table
   */
  private static String printTextTable(String[] headers, String[][] data) {
    return FlipTable.of(headers, data);
  }
}
