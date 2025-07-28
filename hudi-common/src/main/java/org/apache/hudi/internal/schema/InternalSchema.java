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
import java.util.Collections;
import java.util.Comparator;
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
  public static final String ARRAY_ELEMENT = "element";
  public static final String MAP_KEY = "key";
  public static final String MAP_VALUE = "value";

  private static final InternalSchema EMPTY_SCHEMA = new InternalSchema(-1L, RecordType.get());

  private static final long DEFAULT_VERSION_ID = 0;

  private final RecordType record;

  private int maxColumnId;
  private long versionId;

  private transient Map<Integer, Field> idToField = null;
  private transient Map<String, Integer> nameToId = null;
  private transient Map<Integer, String> idToName = null;
  private transient Map<String, Integer> nameToPosition = null;

  public static InternalSchema getEmptyInternalSchema() {
    return EMPTY_SCHEMA;
  }

  public boolean isEmptySchema() {
    return versionId < 0;
  }

  public InternalSchema(RecordType recordType) {
    this(DEFAULT_VERSION_ID, recordType);
  }

  public InternalSchema(long versionId, int maxColumnId, RecordType recordType) {
    this.maxColumnId = maxColumnId;
    this.versionId = versionId;
    this.record = recordType;
    getIdToName();
  }

  public InternalSchema(long versionId, RecordType recordType) {
    this.versionId = versionId;
    this.record = recordType;
    this.idToName = buildIdToName(record);
    this.nameToId = recordType.fields().isEmpty()
        ? Collections.emptyMap()
        : idToName.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    this.maxColumnId = idToName.isEmpty() ? -1 : idToName.keySet().stream().max(Comparator.comparing(Integer::valueOf)).get();
  }

  public RecordType getRecord() {
    return record;
  }

  private static Map<Integer, String> buildIdToName(RecordType record) {
    return record.fields().isEmpty()
        ? Collections.emptyMap()
        : InternalSchemaBuilder.getBuilder().buildIdToName(record);
  }

  private Map<Integer, String> getIdToName() {
    if (idToName == null) {
      idToName = buildIdToName(record);
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
    return new ArrayList<>(nameToId.keySet());
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
   * Returns the fully qualified name of the field corresponding to the given id.
   *
   * @param id a field id
   * @return full name of field corresponding to id
   */
  public String findFullName(int id) {
    String result = getIdToName().get(id);
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
    return getIdToName().keySet();
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
   * Whether {@code colName} exists in the current Schema
   *
   * @param colName a column name
   * @param caseSensitive whether columns names should be treated as case-sensitive
   * @return whether schema contains column identified by {@code colName}
   */
  public boolean hasColumn(String colName, boolean caseSensitive) {
    if (caseSensitive) {
      // In case we do a case-sensitive check we just need to validate whether
      // schema contains field-name as it is
      return getIdToName().containsValue(colName);
    } else {
      return getIdToName().values()
          .stream()
          .map(fieldName -> fieldName.toLowerCase(Locale.ROOT))
          .collect(Collectors.toSet())
          .contains(colName.toLowerCase(Locale.ROOT));
    }
  }

  public int findIdByName(String name) {
    if (name == null || name.isEmpty()) {
      return -1;
    }
    return buildNameToId().getOrDefault(name, -1);
  }

  /**
   * Returns the full name of the field and its position in the schema.
   * This differs from its ID in cases where new fields are not appended to the end of schemas.
   * The output is used when reconciling the order of fields while ingesting.
   * @return a mapping from full field name to a position
   */
  public Map<String, Integer> getNameToPosition() {
    if (nameToPosition == null) {
      nameToPosition = InternalSchemaBuilder.getBuilder().buildNameToPosition(record);
    }
    return nameToPosition;
  }

  @Override
  public String toString() {
    return String.format("table {\n%s\n}",
        StringUtils.join(record.fields().stream()
            .map(f -> " " + f).toArray(String[]::new), "\n"));
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

  public boolean equalsIgnoringVersion(Object o) {
    if (this == o) {
      return true;
    } else if (!(o instanceof InternalSchema)) {
      return false;
    }
    InternalSchema that = (InternalSchema) o;
    return record.equals(that.record);
  }

  @Override
  public int hashCode() {
    return record.hashCode();
  }
}
