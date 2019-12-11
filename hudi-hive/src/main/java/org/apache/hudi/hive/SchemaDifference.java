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

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.parquet.schema.MessageType;

import java.util.List;
import java.util.Map;

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
    this.deleteColumns = ImmutableList.copyOf(deleteColumns);
    this.updateColumnTypes = ImmutableMap.copyOf(updateColumnTypes);
    this.addColumnTypes = ImmutableMap.copyOf(addColumnTypes);
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

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("deleteColumns", deleteColumns).add("updateColumnTypes", updateColumnTypes)
        .add("addColumnTypes", addColumnTypes).toString();
  }

  public static Builder newBuilder(MessageType storageSchema, Map<String, String> tableSchema) {
    return new Builder(storageSchema, tableSchema);
  }

  public boolean isEmpty() {
    return deleteColumns.isEmpty() && updateColumnTypes.isEmpty() && addColumnTypes.isEmpty();
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
      deleteColumns = Lists.newArrayList();
      updateColumnTypes = Maps.newHashMap();
      addColumnTypes = Maps.newHashMap();
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
