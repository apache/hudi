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

package org.apache.hudi.io.storage;

import dev.vortex.api.Expression;
import dev.vortex.spark.read.SparkFilterToVortexExpression;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.hudi.common.table.read.buffer.PositionBasedFileGroupRecordBuffer.ROW_INDEX_TEMPORARY_COLUMN_NAME;

/**
 * Builds the parts of a Vortex scan: the projection, the filter and the row range of a split.
 */
public final class VortexScans {

  /**
   * The Vortex writer makes chunks from blocks of this many rows. A split boundary on a block
   * boundary does not cut a chunk.
   */
  static final long ROW_BLOCK = 8192;

  private VortexScans() {
  }

  /**
   * Returns the projection that reads the fields of {@code requestSchema} from the file.
   *
   * <p>Each field is a top-level column of the file, or the row index column of Hudi. The row
   * index column gets the position of the row in the file, before filtering.
   */
  public static Expression projection(StructType requestSchema) {
    String[] names = requestSchema.fieldNames();
    boolean hasRowIndex = false;
    for (String name : names) {
      hasRowIndex |= isRowIndexColumn(name);
    }

    if (!hasRowIndex) {
      return Expression.select(names, Expression.root());
    }

    Expression[] values = new Expression[names.length];
    for (int i = 0; i < names.length; i++) {
      values[i] = isRowIndexColumn(names[i]) ? Expression.rowIdx() : Expression.column(names[i]);
    }
    return Expression.pack(names, values, false);
  }

  /**
   * Returns the projection that reads the fields of {@code requestSchema} from a file with the
   * schema {@code fileSchema}.
   *
   * <p>Spark can request a struct without the fields that a query does not read. Then the
   * projection reads only the requested fields of the struct, also in nested structs, and Vortex
   * does not decode the other fields. The requested fields that the struct does not have are not
   * in the result, and the reader gives null for them. Arrays and maps are read whole.
   */
  public static Expression projection(StructType requestSchema, StructType fileSchema) {
    String[] names = requestSchema.fieldNames();
    Expression[] values = new Expression[names.length];
    boolean pruned = false;
    for (int i = 0; i < names.length; i++) {
      StructField requested = requestSchema.fields()[i];
      int fileIndex = isRowIndexColumn(names[i]) ? -1 : fieldIndex(fileSchema, names[i]);
      if (fileIndex < 0) {
        values[i] = isRowIndexColumn(names[i]) ? Expression.rowIdx() : Expression.column(names[i]);
        pruned |= isRowIndexColumn(names[i]);
        continue;
      }

      StructField file = fileSchema.fields()[fileIndex];
      Expression column = Expression.column(file.name());
      values[i] = project(column, file.dataType(), requested.dataType());
      pruned |= values[i] != column || !file.name().equals(names[i]);
    }

    return pruned ? Expression.pack(names, values, false) : Expression.select(names, Expression.root());
  }

  /**
   * Returns the expression that reads {@code value} of type {@code fileType} with the struct fields
   * of {@code requestedType}. Returns {@code value} itself when no field is pruned.
   */
  private static Expression project(Expression value, DataType fileType, DataType requestedType) {
    if (!(fileType instanceof StructType) || !(requestedType instanceof StructType)
        || VortexArrowColumnVector.hasSameShape(fileType, requestedType)) {
      return value;
    }

    StructType fileStruct = (StructType) fileType;
    List<String> names = new ArrayList<>();
    List<Expression> fields = new ArrayList<>();
    for (StructField requested : ((StructType) requestedType).fields()) {
      int index = fieldIndex(fileStruct, requested.name());
      if (index >= 0) {
        StructField file = fileStruct.fields()[index];
        names.add(file.name());
        fields.add(project(Expression.getItem(file.name(), value), file.dataType(), requested.dataType()));
      }
    }

    // A struct that is null stays null.
    Expression packed = Expression.pack(names.toArray(new String[0]), fields.toArray(new Expression[0]), true);
    return Expression.mask(packed, Expression.isNotNull(value));
  }

  /**
   * Returns the index of the field with the name, or of the only field with the name in another
   * case. Returns -1 if there is no such field.
   */
  static int fieldIndex(StructType struct, String name) {
    StructField[] fields = struct.fields();
    int caseInsensitiveMatch = -1;
    for (int i = 0; i < fields.length; i++) {
      if (fields[i].name().equals(name)) {
        return i;
      }
      if (fields[i].name().equalsIgnoreCase(name)) {
        caseInsensitiveMatch = caseInsensitiveMatch == -1 ? i : -2;
      }
    }
    return caseInsensitiveMatch >= 0 ? caseInsensitiveMatch : -1;
  }

  /**
   * Returns true if the field is the row index column, which the file does not store.
   */
  public static boolean isRowIndexColumn(String name) {
    return ROW_INDEX_TEMPORARY_COLUMN_NAME.equals(name);
  }

  /**
   * Returns the conjunction of the filters that Vortex can evaluate, if there is one.
   *
   * <p>Vortex evaluates a filter on each row and drops the rows that do not match. A filter is
   * pushed only if each column that it reads is in {@code fileSchema} with the same type as in
   * {@code readSchema}. A filter on a column that has a different type in the file, for example
   * after a schema change, stays in Spark.
   *
   * @param filters    the filters that Spark gave to the reader
   * @param readSchema the columns that the query reads, with the types that the query uses
   * @param fileSchema the top-level columns of the file
   */
  public static Optional<Expression> filter(Collection<Filter> filters, StructType readSchema, StructType fileSchema) {
    if (filters.isEmpty()) {
      return Optional.empty();
    }

    StructType filterable = filterableColumns(readSchema, fileSchema);
    if (filterable.isEmpty()) {
      return Optional.empty();
    }

    List<Expression> converted = new ArrayList<>();
    for (Filter filter : filters) {
      SparkFilterToVortexExpression.convert(filter, filterable).ifPresent(converted::add);
    }

    if (converted.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(converted.size() == 1
        ? converted.get(0)
        : Expression.and(converted.toArray(new Expression[0])));
  }

  private static StructType filterableColumns(StructType readSchema, StructType fileSchema) {
    Map<String, DataType> fileTypes = new HashMap<>();
    for (StructField field : fileSchema.fields()) {
      fileTypes.put(field.name(), field.dataType());
    }

    List<StructField> filterable = new ArrayList<>();
    for (StructField field : readSchema.fields()) {
      DataType fileType = fileTypes.get(field.name());
      if (fileType != null && fileType.sameType(field.dataType())) {
        filterable.add(field);
      }
    }
    return new StructType(filterable.toArray(new StructField[0]));
  }

  /**
   * Returns the rows that a split of a file reads, as {@code [begin, end)}.
   *
   * <p>Spark splits a file by bytes. Each byte offset is mapped to a row in proportion to the file
   * length, and then rounded down to a block of {@link #ROW_BLOCK} rows. The splits of one file
   * thus read each row exactly once.
   *
   * @param start    the first byte of the split
   * @param length   the number of bytes in the split
   * @param fileSize the length of the file in bytes
   * @param rowCount the number of rows in the file
   */
  public static long[] splitRowRange(long start, long length, long fileSize, long rowCount) {
    return new long[] {
        rowAt(start, fileSize, rowCount),
        rowAt(start + length, fileSize, rowCount)};
  }

  /**
   * Returns true if a split covers the whole file.
   */
  public static boolean isWholeFile(long start, long length, long fileSize) {
    return start <= 0 && length >= fileSize;
  }

  private static long rowAt(long offset, long fileSize, long rowCount) {
    if (offset >= fileSize) {
      return rowCount;
    }
    if (offset <= 0) {
      return 0;
    }

    long row = BigInteger.valueOf(offset)
        .multiply(BigInteger.valueOf(rowCount))
        .divide(BigInteger.valueOf(fileSize))
        .longValueExact();
    return row / ROW_BLOCK * ROW_BLOCK;
  }
}
