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

package org.apache.hudi.hive;

import org.apache.parquet.schema.MessageType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Represents the schema difference between the storage schema and hive table schema.
 */
public class SchemaDifference {

  private final MessageType storageSchema;
  private final Map<String, String> tableSchema;
  private final List<String> deleteColumns;
  private final Map<String, String> updateColumnTypes;
  private final Map<String, String> addColumnTypes;

  private SchemaDifference(MessageType storageSchema, Map<String, String> tableSchema, List<String> deleteColumns,
      Map<String, String> updateColumnTypes, Map<String, String> addColumnTypes) {
    this.storageSchema = storageSchema;
    this.tableSchema = tableSchema;
    this.deleteColumns = Collections.unmodifiableList(deleteColumns);
    this.updateColumnTypes = Collections.unmodifiableMap(updateColumnTypes);
    this.addColumnTypes = Collections.unmodifiableMap(addColumnTypes);
  }

  public List<String> getDeleteColumns() {
    return deleteColumns;
  }

  public Map<String, String> getUpdateColumnTypes() {
    return updateColumnTypes;
  }

  public Map<String, String> getAddColumnTypes() {
    return addColumnTypes;
  }

  public static Builder newBuilder(MessageType storageSchema, Map<String, String> tableSchema) {
    return new Builder(storageSchema, tableSchema);
  }

  public boolean isEmpty() {
    return deleteColumns.isEmpty() && updateColumnTypes.isEmpty() && addColumnTypes.isEmpty();
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", SchemaDifference.class.getSimpleName() + "[", "]")
           .add("storageSchema=" + storageSchema)
           .add("tableSchema=" + tableSchema)
           .add("deleteColumns=" + deleteColumns)
           .add("updateColumnTypes=" + updateColumnTypes)
           .add("addColumnTypes=" + addColumnTypes)
           .toString();
  }

  public static class Builder {

    private final MessageType storageSchema;
    private final Map<String, String> tableSchema;
    private List<String> deleteColumns;
    private Map<String, String> updateColumnTypes;
    private Map<String, String> addColumnTypes;

    public Builder(MessageType storageSchema, Map<String, String> tableSchema) {
      this.storageSchema = storageSchema;
      this.tableSchema = tableSchema;
      deleteColumns = new ArrayList<>();
      updateColumnTypes = new HashMap<>();
      addColumnTypes = new LinkedHashMap<>();
    }

    public Builder deleteTableColumn(String column) {
      deleteColumns.add(column);
      return this;
    }

    public Builder updateTableColumn(String column, String storageColumnType) {
      updateColumnTypes.put(column, storageColumnType);
      return this;
    }

    public Builder addTableColumn(String name, String type) {
      addColumnTypes.put(name, type);
      return this;
    }

    public SchemaDifference build() {
      return new SchemaDifference(storageSchema, tableSchema, deleteColumns, updateColumnTypes, addColumnTypes);
    }
  }
}
