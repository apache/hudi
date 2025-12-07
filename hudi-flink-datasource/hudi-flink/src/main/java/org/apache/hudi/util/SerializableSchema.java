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

package org.apache.hudi.util;

import org.apache.hudi.common.util.Option;

import lombok.Getter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;

import java.io.Serializable;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.DataTypes.ROW;

/**
 * A serializable substitute for {@code ResolvedSchema}.
 */
public class SerializableSchema implements Serializable {
  private static final long serialVersionUID = 1L;
  private final List<Column> columns;

  private SerializableSchema(List<Column> columns) {
    this.columns = columns;
  }

  public static SerializableSchema create(ResolvedSchema resolvedSchema) {
    List<Column> columns = resolvedSchema.getColumns().stream()
        .filter(c -> c.isPhysical() || c instanceof org.apache.flink.table.catalog.Column.MetadataColumn)
        .map(column -> Column.create(column.getName(), column.getDataType()))
        .collect(Collectors.toList());
    return new SerializableSchema(columns);
  }

  public List<String> getColumnNames() {
    return this.columns.stream().map(Column::getName).collect(Collectors.toList());
  }

  public List<DataType> getColumnDataTypes() {
    return this.columns.stream().map(Column::getDataType).collect(Collectors.toList());
  }

  public Option<Column> getColumn(String columnName) {
    return Option.fromJavaOptional(this.columns.stream().filter((col) -> col.getName().equals(columnName)).findFirst());
  }

  public DataType toSourceRowDataType() {
    return this.toRowDataType((c) -> true);
  }

  private DataType toRowDataType(Predicate<Column> predicate) {
    final DataTypes.Field[] fieldsArray = columns.stream().filter(predicate)
        .map(SerializableSchema::columnToField)
        .toArray(DataTypes.Field[]::new);
    // the row should never be null
    return ROW(fieldsArray).notNull();
  }

  private static DataTypes.Field columnToField(Column column) {
    return DataTypes.FIELD(column.getName(), DataTypeUtils.removeTimeAttribute(column.getDataType()));
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------
  @Getter
  public static class Column implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String name;
    private final DataType dataType;

    private Column(String name, DataType dataType) {
      this.name = name;
      this.dataType = dataType;
    }

    public static Column create(String name, DataType type) {
      return new Column(name, type);
    }
  }
}
