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

package org.apache.hudi.internal.schema;

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.internal.schema.Types.Field;
import org.apache.hudi.internal.schema.Types.RecordType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Internal schema for hudi table.
 * used to support schema evolution.
 */
public class InternalSchema implements Serializable {

  private static final long DEFAULT_VERSION_ID = 0;

  private final RecordType record;

  private int maxColumnId;
  private long versionId;

  private transient Map<Integer, Field> idToField = null;
  private transient Map<String, Integer> nameToId = null;
  private transient Map<Integer, String> idToName = null;

  public static InternalSchema getEmptyInternalSchema() {
    return new InternalSchema(-1L, new ArrayList<>());
  }

  public boolean isEmptySchema() {
    return versionId < 0;
  }

  public InternalSchema(List<Field> columns) {
    this(DEFAULT_VERSION_ID, columns);
  }

  public InternalSchema(Field... columns) {
    this(DEFAULT_VERSION_ID, Arrays.asList(columns));
  }

  public InternalSchema(long versionId, List<Field> cols) {
    this.versionId = versionId;
    this.record = RecordType.get(cols);
    idToName = cols.isEmpty() ? new HashMap<>() : InternalSchemaBuilder.getBuilder().buildIdToName(record);
    nameToId = cols.isEmpty() ? new HashMap<>() : idToName.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    maxColumnId = idToName.isEmpty() ? -1 : idToName.keySet().stream().max(Comparator.comparing(Integer::valueOf)).get();
  }

  public InternalSchema(long versionId, int maxColumnId, List<Field> cols) {
    this.maxColumnId = maxColumnId;
    this.versionId = versionId;
    this.record = RecordType.get(cols);
    buildIdToName();
  }

  public InternalSchema(long versionId, int maxColumnId, Field... cols) {
    this(versionId, maxColumnId, Arrays.asList(cols));
  }

  public RecordType getRecord() {
    return record;
  }

  private Map<Integer, String> buildIdToName() {
    if (idToName == null) {
      idToName = InternalSchemaBuilder.getBuilder().buildIdToName(record);
    }
    return idToName;
  }

  private Map<String, Integer> buildNameToId() {
    if (nameToId == null) {
      if (idToName != null && !idToName.isEmpty()) {
        nameToId = idToName.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        return nameToId;
      }
      nameToId = InternalSchemaBuilder.getBuilder().buildNameToId(record);
    }
    return nameToId;
  }

  private Map<Integer, Field> buildIdToField() {
    if (idToField == null) {
      idToField = InternalSchemaBuilder.getBuilder().buildIdToField(record);
    }
    return idToField;
  }

  /**
   * Get all columns full name.
   */
  public List<String> getAllColsFullName() {
    if (nameToId == null) {
      nameToId = InternalSchemaBuilder.getBuilder().buildNameToId(record);
    }
    return Arrays.asList(nameToId.keySet().toArray(new String[0]));
  }

  /**
   * Set the version ID for this schema.
   */
  public InternalSchema setSchemaId(long versionId) {
    this.versionId = versionId;
    return this;
  }

  /**
   * Returns the version ID for this schema.
   */
  public long schemaId() {
    return this.versionId;
  }

  /**
   * Set the version ID for this schema.
   */
  public void setMaxColumnId(int maxColumnId) {
    this.maxColumnId = maxColumnId;
  }

  /**
   * Returns the max column id for this schema.
   */
  public int getMaxColumnId() {
    return this.maxColumnId;
  }

  /**
   * Returns a List of the {@link Field columns} in this Schema.
   */
  public List<Field> columns() {
    return record.fields();
  }

  /**
   * Returns the {@link Type} of a sub-field identified by the field name.
   *
   * @param id a field id
   * @return fullName of field of
   */
  public String findfullName(int id) {
    if (idToName == null) {
      buildIdToName();
    }
    String result = idToName.get(id);
    return result == null ? "" : result;
  }

  /**
   * Returns the {@link Type} of a sub-field identified by the field name.
   *
   * @param name a field name
   * @return a Type for the sub-field or null if it is not found
   */
  public Type findType(String name) {
    if (name == null || name.isEmpty()) {
      return null;
    }
    Integer id = buildNameToId().get(name);
    if (id != null) { // name is found
      return findType(id);
    }
    return null;
  }

  /**
   * Returns the {@link Type} of a sub-field identified by the field id.
   *
   * @param id a field id
   * @return a Type for the sub-field or null if it is not found
   */
  public Type findType(int id) {
    Field field = buildIdToField().get(id);
    if (field != null) {
      return field.type();
    }
    return null;
  }

  /**
   * Returns all field ids
   */
  public Set<Integer> getAllIds() {
    if (idToName == null) {
      buildIdToName();
    }
    return idToName.keySet();
  }

  /**
   * Returns the sub-field identified by the field id.
   *
   * @param id a field id
   * @return the sub-field or null if it is not found
   */
  public Field findField(int id) {
    return buildIdToField().get(id);
  }

  /**
   * Returns a sub-field by name as a {@link Field}.
   * The result may be a top-level or a nested field.
   *
   * @param name a String name
   * @return a Type for the sub-field or null if it is not found
   */
  public Field findField(String name) {
    if (name == null || name.isEmpty()) {
      return null;
    }
    Integer id = buildNameToId().get(name);
    if (id != null) {
      return buildIdToField().get(id);
    }
    return null;
  }

  /**
   * Whether colName exists in current Schema.
   * Case insensitive.
   *
   * @param colName a colName
   * @return Whether colName exists in current Schema
   */
  public boolean findDuplicateCol(String colName) {
    return idToName.entrySet().stream().map(e -> e.getValue().toLowerCase(Locale.ROOT))
        .collect(Collectors.toSet()).contains(colName);
  }

  public int findIdByName(String name) {
    if (name == null || name.isEmpty()) {
      return -1;
    }
    return buildNameToId().getOrDefault(name, -1);
  }

  @Override
  public String toString() {
    return String.format("table {\n%s\n}",
        StringUtils.join(record.fields().stream()
            .map(f -> " " + f)
            .collect(Collectors.toList()).toArray(new String[0]), "\n"));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (!(o instanceof InternalSchema)) {
      return false;
    }
    InternalSchema that = (InternalSchema) o;
    if (versionId != that.schemaId()) {
      return false;
    }
    return record.equals(that.record);
  }

  @Override
  public int hashCode() {
    return record.hashCode();
  }
}
