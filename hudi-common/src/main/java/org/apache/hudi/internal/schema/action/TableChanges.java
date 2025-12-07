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

package org.apache.hudi.internal.schema.action;

import org.apache.hudi.exception.SchemaCompatibilityException;
import org.apache.hudi.internal.schema.HoodieSchemaException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.InternalSchemaBuilder;
import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.internal.schema.utils.SchemaChangeUtils;

import lombok.Getter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Operations of schema changes supported in schema evolution
 */
public class TableChanges {

  /** Deal with update columns changes for table. */
  @Getter
  public static class ColumnUpdateChange extends TableChange.BaseColumnChange {
    private final Map<Integer, Types.Field> updates = new HashMap<>();

    private ColumnUpdateChange(InternalSchema schema) {
      super(schema, false);
    }

    private ColumnUpdateChange(InternalSchema schema, boolean caseSensitive) {
      super(schema, caseSensitive);
    }

    @Override
    public boolean withPositionChange() {
      return true;
    }

    public Type applyUpdates(Types.Field oldField, Type type) {
      Types.Field update = updates.get(oldField.fieldId());
      if (update != null && update.type() != oldField.type()) {
        return update.type();
      }
      //
      ArrayList<ColumnPositionChange> pchanges = positionChangeMap.getOrDefault(oldField.fieldId(), new ArrayList<>());
      if (!pchanges.isEmpty()) {
        // when we build ColumnAddChange，we have already done some check, so it's safe to convert newType to RecordType
        List<Types.Field> newFields = TableChangesHelper.applyAddChange2Fields(((Types.RecordType) type).fields(), new ArrayList<>(), pchanges);
        return Types.RecordType.get(newFields);
      }
      return type;
    }

    /**
     * Update a column in the schema to a new type.
     * only support update primitive type.
     * Only updates that widen types are allowed.
     *
     * @param name name of the column to update
     * @param newType new type for the column
     * @return this
     * @throws SchemaCompatibilityException
     */
    public ColumnUpdateChange updateColumnType(String name, Type newType) {
      checkColModifyIsLegal(name);
      if (newType.isNestedType()) {
        throw new SchemaCompatibilityException(String.format("Cannot update column '%s' to nested type '%s'.", name, newType));
      }
      Types.Field field = internalSchema.findField(name);
      if (field == null) {
        throw new SchemaCompatibilityException(String.format("Cannot update type for column '%s' because it does not exist in the schema", name));
      }

      if (!SchemaChangeUtils.isTypeUpdateAllow(field.type(), newType)) {
        throw new SchemaCompatibilityException(String.format(
            "Cannot update column '%s' from type '%s' to incompatible type '%s'.", name, field.type(), newType));
      }

      if (field.type().equals(newType)) {
        // do nothings
        return this;
      }
      // save update info
      Types.Field update = updates.get(field.fieldId());
      if (update == null) {
        updates.put(field.fieldId(), Types.Field.get(field.fieldId(), field.isOptional(), field.name(), newType, field.doc()));
      } else {
        updates.put(field.fieldId(), Types.Field.get(field.fieldId(), update.isOptional(), update.name(), newType, update.doc()));
      }
      return this;
    }

    /**
     * Update a column doc in the schema to a new primitive type.
     *
     * @param name name of the column to update
     * @param newDoc new documentation for the column
     * @return this
     * @throws SchemaCompatibilityException
     */
    public ColumnUpdateChange updateColumnComment(String name, String newDoc) {
      checkColModifyIsLegal(name);
      Types.Field field = internalSchema.findField(name);
      if (field == null) {
        throw new SchemaCompatibilityException(String.format("Cannot update comment for column '%s' because it does not exist in the schema", name));
      }
      // consider null
      if (Objects.equals(field.doc(), newDoc)) {
        // do nothings
        return this;
      }
      // save update info
      Types.Field update = updates.get(field.fieldId());
      if (update == null) {
        updates.put(field.fieldId(), Types.Field.get(field.fieldId(), field.isOptional(), field.name(), field.type(), newDoc));
      } else {
        updates.put(field.fieldId(), Types.Field.get(field.fieldId(), update.isOptional(), update.name(), update.type(), newDoc));
      }
      return this;
    }

    /**
     * Rename a column in the schema.
     *
     * @param name name of the column to rename
     * @param newName new name for the column
     * @return this
     * @throws SchemaCompatibilityException
     */
    public ColumnUpdateChange renameColumn(String name, String newName) {
      checkColModifyIsLegal(name);
      Types.Field field = internalSchema.findField(name);
      if (field == null) {
        throw new SchemaCompatibilityException(String.format("Cannot rename column '%s' because it does not exist in the schema", name));
      }
      if (newName == null || newName.isEmpty()) {
        throw new SchemaCompatibilityException(String.format("Cannot rename column '%s' to empty or null name. New name must be non-empty", name));
      }
      if (internalSchema.hasColumn(newName, caseSensitive)) {
        throw new SchemaCompatibilityException(String.format("Cannot rename column '%s' to '%s' because a column with name '%s' already exists in the schema", name, newName, newName));
      }
      // save update info
      Types.Field update = updates.get(field.fieldId());
      if (update == null) {
        updates.put(field.fieldId(), Types.Field.get(field.fieldId(), field.isOptional(), newName, field.type(), field.doc()));
      } else {
        updates.put(field.fieldId(), Types.Field.get(field.fieldId(), update.isOptional(), newName, update.type(), update.doc()));
      }
      return this;
    }

    /**
     * Update nullable for column.
     * only support required type -> optional type
     *
     * @param name name of the column to update
     * @param nullable nullable for updated name
     * @return this
     * @throws SchemaCompatibilityException
     */
    public ColumnUpdateChange updateColumnNullability(String name, boolean nullable) {
      return updateColumnNullability(name, nullable, false);
    }

    public ColumnUpdateChange updateColumnNullability(String name, boolean nullable, boolean force) {
      checkColModifyIsLegal(name);
      Types.Field field = internalSchema.findField(name);
      if (field == null) {
        throw new SchemaCompatibilityException(String.format("Cannot update nullability for column '%s' because it does not exist in the schema", name));
      }
      if (field.isOptional() == nullable) {
        // do nothings
        return this;
      }
      if (field.isOptional() && !nullable && !force) {
        throw new SchemaCompatibilityException(String.format(
            "Cannot change column '%s' from optional to required. This would break compatibility with existing data that may contain null values", name));
      }
      // save update info
      Types.Field update = updates.get(field.fieldId());
      if (update == null) {
        updates.put(field.fieldId(), Types.Field.get(field.fieldId(), nullable, field.name(), field.type(), field.doc()));
      } else {
        updates.put(field.fieldId(), Types.Field.get(field.fieldId(), nullable, update.name(), update.type(), update.doc()));
      }

      return this;
    }

    public Map<Integer, ArrayList<ColumnPositionChange>> getPositionChangeMap() {
      return positionChangeMap;
    }

    @Override
    public ColumnChangeID columnChangeId() {
      return ColumnChangeID.UPDATE;
    }

    @Override
    protected Integer findIdByFullName(String fullName) {
      Types.Field field = internalSchema.findField(fullName);
      if (field != null) {
        return field.fieldId();
      } else {
        throw new SchemaCompatibilityException(String.format("Cannot find column ID for column path '%s'. The column may not exist in the schema", fullName));
      }
    }

    public static ColumnUpdateChange get(InternalSchema schema) {
      return new ColumnUpdateChange(schema);
    }

    public static ColumnUpdateChange get(InternalSchema schema, boolean caseSensitive) {
      return new ColumnUpdateChange(schema, caseSensitive);
    }
  }

  /** Deal with delete columns changes for table. */
  @Getter
  public static class ColumnDeleteChange extends TableChange.BaseColumnChange {
    private final Set<Integer> deletes = new HashSet<>();

    @Override
    public ColumnChangeID columnChangeId() {
      return ColumnChangeID.DELETE;
    }

    public static ColumnDeleteChange get(InternalSchema schema) {
      return new ColumnDeleteChange(schema);
    }

    private ColumnDeleteChange(InternalSchema schema) {
      super(schema);
    }

    @Override
    public boolean withPositionChange() {
      return false;
    }

    @Override
    public BaseColumnChange addPositionChange(String srcId, String dsrId, String orderType) {
      throw new UnsupportedOperationException("no support add position change for ColumnDeleteChange");
    }

    public ColumnDeleteChange deleteColumn(String name) {
      checkColModifyIsLegal(name);
      Types.Field field = internalSchema.findField(name);
      if (field == null) {
        throw new SchemaCompatibilityException(String.format("Cannot delete column '%s' because it does not exist in the schema", name));
      }
      deletes.add(field.fieldId());
      return this;
    }

    public Type applyDelete(int id, Type type) {
      if (deletes.contains(id)) {
        return null;
      }
      return type;
    }

    @Override
    protected Integer findIdByFullName(String fullName) {
      throw new UnsupportedOperationException("delete change cannot support this method");
    }
  }

  /**
   * Deal with add columns changes for table.
   */
  public static class ColumnAddChange extends TableChange.BaseColumnChange {
    // expose to test
    @Getter
    private final Map<String, Integer> fullColName2Id = new HashMap<>();
    @Getter
    private final Map<Integer, ArrayList<Types.Field>> parentId2AddCols = new HashMap<>();
    private int nextId;

    public static ColumnAddChange get(InternalSchema internalSchema) {
      return new ColumnAddChange(internalSchema);
    }

    public Type applyAdd(Types.Field originalField, Type type) {
      int fieldId = originalField.fieldId();
      ArrayList<Types.Field> addFields = parentId2AddCols.getOrDefault(fieldId, new ArrayList<>());
      ArrayList<ColumnPositionChange> pchanges = positionChangeMap.getOrDefault(fieldId, new ArrayList<>());

      if (!addFields.isEmpty() || !pchanges.isEmpty()) {
        // when we build ColumnAddChange，we have already done some check, so it's safe to convert newType to RecordType
        List<Types.Field> newFields = TableChangesHelper.applyAddChange2Fields(((Types.RecordType) type).fields(), addFields, pchanges);
        return Types.RecordType.get(newFields);
      }
      return type;
    }

    public ColumnAddChange addColumns(String name, Type type, String doc) {
      checkColModifyIsLegal(name);
      return addColumns("", name, type, doc);
    }

    public ColumnAddChange addColumns(String parent, String name, Type type, String doc) {
      checkColModifyIsLegal(name);
      addColumnsInternal(parent, name, type, doc);
      return this;
    }

    private void addColumnsInternal(String parent, String name, Type type, String doc) {
      // root record has no parent, so set parentId to -1 as default
      int parentId = -1;
      // do check
      String fullName = name;
      if (!parent.isEmpty()) {
        Types.Field parentField = internalSchema.findField(parent);
        if (parentField == null) {
          throw new HoodieSchemaException(String.format("Cannot add column '%s' because its parent column '%s' does not exist in the schema", name, parent));
        }
        if (!(parentField.type() instanceof Types.RecordType)) {
          throw new HoodieSchemaException(String.format(
              "Cannot add nested column '%s' to parent '%s' of type '%s'. Nested columns can only be added to struct/record types", name, parent, parentField.type()));
        }
        parentId = parentField.fieldId();
        Types.Field newParentField = internalSchema.findField(parent + "."  + name);
        if (newParentField != null) {
          throw new HoodieSchemaException(String.format("Cannot add column '%s' to parent '%s' because the column already exists at path '%s'", name, parent, parent + "." + name));
        }
        fullName = parent + "." + name;
      } else {
        if (internalSchema.hasColumn(name, caseSensitive)) {
          throw new HoodieSchemaException(String.format("Cannot add column '%s' because it already exists in the schema", name));
        }
      }
      if (fullColName2Id.containsKey(fullName)) {
        throw new HoodieSchemaException(String.format("Cannot add column '%s' multiple times. Column at path '%s' has already been added in this change set", name, fullName));
      }
      fullColName2Id.put(fullName, nextId);
      if (parentId != -1) {
        id2parent.put(nextId, parentId);
      }
      AtomicInteger assignNextId = new AtomicInteger(nextId + 1);
      Type typeWithNewId = InternalSchemaBuilder.getBuilder().refreshNewId(type, assignNextId);
      // only allow add optional columns.
      ArrayList<Types.Field> adds = parentId2AddCols.getOrDefault(parentId, new ArrayList<>());
      adds.add(Types.Field.get(nextId, true, name, typeWithNewId, doc));
      parentId2AddCols.put(parentId, adds);
      nextId = assignNextId.get();
    }

    private ColumnAddChange(InternalSchema internalSchema) {
      super(internalSchema);
      this.nextId = internalSchema.getMaxColumnId() + 1;
    }

    public Map<Integer, ArrayList<ColumnPositionChange>> getPositionChangeMap() {
      return positionChangeMap;
    }

    protected Integer findIdByFullName(String fullName) {
      Types.Field field = internalSchema.findField(fullName);
      if (field != null) {
        return field.fieldId();
      }
      return fullColName2Id.getOrDefault(fullName, -1);
    }

    @Override
    public ColumnChangeID columnChangeId() {
      return ColumnChangeID.ADD;
    }

    @Override
    public boolean withPositionChange() {
      return true;
    }
  }
}

