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

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.internal.schema.HoodieSchemaException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.InternalSchemaBuilder;
import org.apache.hudi.internal.schema.Types;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * TableChange subclasses represent requested changes to a table.
 * now only column changes support.
 * to do support partition changes
 */
public interface TableChange {
  /**
   * The action Type of schema change.
   */
  enum ColumnChangeID {
    ADD, UPDATE, DELETE, PROPERTY_CHANGE, REPLACE;
    private final String name;

    ColumnChangeID() {
      this.name = this.name().toLowerCase(Locale.ROOT);
    }

    public String getName() {
      return name;
    }
  }

  static ColumnChangeID fromValue(String value) {
    switch (value.toLowerCase(Locale.ROOT)) {
      case "add":
        return ColumnChangeID.ADD;
      case "change":
        return ColumnChangeID.UPDATE;
      case "delete":
        return ColumnChangeID.DELETE;
      case "property":
        return ColumnChangeID.PROPERTY_CHANGE;
      case "replace":
        return ColumnChangeID.REPLACE;
      default:
        throw new IllegalArgumentException("Invalid value of Type.");
    }
  }

  ColumnChangeID columnChangeId();

  default boolean withPositionChange() {
    return false;
  }

  /**
   * Information of base column changes
   */
  abstract class BaseColumnChange implements TableChange {
    protected final InternalSchema internalSchema;
    protected final Map<Integer, Integer> id2parent;
    protected final Map<Integer, ArrayList<ColumnPositionChange>> positionChangeMap = new HashMap<>();
    protected final boolean caseSensitive;

    BaseColumnChange(InternalSchema schema) {
      this(schema, false);
    }

    BaseColumnChange(InternalSchema schema, boolean caseSensitive) {
      this.internalSchema = schema;
      this.id2parent = InternalSchemaBuilder.getBuilder().index2Parents(schema.getRecord());
      this.caseSensitive = caseSensitive;
    }

    /**
     * Add position change.
     *
     * @param srcName column which need to be reordered
     * @param dsrName reference position
     * @param orderType change types
     * @return this
     */
    public BaseColumnChange addPositionChange(String srcName, String dsrName, ColumnPositionChange.ColumnPositionType orderType) {
      Integer srcId = findIdByFullName(srcName);
      Option<Integer> dsrIdOpt = dsrName.isEmpty() ? Option.empty() : Option.of(findIdByFullName(dsrName));
      Integer srcParentId = id2parent.get(srcId);
      Option<Integer>  dsrParentIdOpt = dsrIdOpt.map(id2parent::get);
      // forbid adjust hoodie metadata columns.
      switch (orderType) {
        case BEFORE:
          checkColModifyIsLegal(dsrName);
          break;
        case FIRST:
          if (srcId == null || srcId == -1 || srcParentId == null || srcParentId == -1) {
            throw new HoodieSchemaException("forbid adjust top-level columns position by using through first syntax");
          }
          break;
        case AFTER:
          List<String> checkColumns = HoodieRecord.HOODIE_META_COLUMNS.subList(0, HoodieRecord.HOODIE_META_COLUMNS.size() - 2);
          if (checkColumns.stream().anyMatch(f -> f.equalsIgnoreCase(dsrName))) {
            throw new HoodieSchemaException("forbid adjust the position of ordinary columns between meta columns");
          }
          break;
        case NO_OPERATION:
        default:
          break;
      }
      int parentId;
      if (srcParentId != null && dsrParentIdOpt.isPresent() && srcParentId.equals(dsrParentIdOpt.get())) {
        Types.Field parentField = internalSchema.findField(srcParentId);
        if (!(parentField.type() instanceof Types.RecordType)) {
          throw new HoodieSchemaException(String.format("only support reorder fields in struct type, but find: %s", parentField.type()));
        }
        parentId = parentField.fieldId();
      } else if (srcParentId == null &&  !dsrParentIdOpt.isPresent()) {
        parentId = -1;
      } else if (srcParentId != null && !dsrParentIdOpt.isPresent() && orderType.equals(ColumnPositionChange.ColumnPositionType.FIRST)) {
        parentId = srcParentId;
      } else {
        throw new HoodieSchemaException("cannot order position from different parent");
      }

      ArrayList<ColumnPositionChange> changes = positionChangeMap.getOrDefault(parentId, new ArrayList<>());
      changes.add(ColumnPositionChange.get(srcId, dsrIdOpt.orElse(-1), orderType));
      positionChangeMap.put(parentId, changes);
      return this;
    }

    public BaseColumnChange addPositionChange(String srcName, String dsrName, String orderType) {
      return addPositionChange(srcName, dsrName, ColumnPositionChange.fromTypeValue(orderType));
    }

    /**
     * Abstract method.
     * give a column fullName and return the field id
     *
     * @param fullName column fullName
     * @return field id of current column
     */
    protected abstract Integer findIdByFullName(String fullName);

    // Modify hudi meta columns is prohibited
    protected void checkColModifyIsLegal(String colNeedToModify) {
      if (HoodieRecord.HOODIE_META_COLUMNS.stream().anyMatch(f -> f.equalsIgnoreCase(colNeedToModify))) {
        throw new IllegalArgumentException(String.format("cannot modify hudi meta col: %s", colNeedToModify));
      }
    }

    @Override
    public boolean withPositionChange() {
      return false;
    }
  }

  /**
   * Column position change.
   * now support three change types: FIRST/AFTER/BEFORE
   * FIRST means the specified column should be the first column.
   * AFTER means the specified column should be put after the given column.
   * BEFORE means the specified column should be put before the given column.
   * Note that, the specified column may be a nested field:
   * AFTER/BEFORE means the given columns should in the same struct;
   * FIRST means this field should be the first one within the struct.
   */
  class ColumnPositionChange {
    public enum ColumnPositionType {
      FIRST,
      BEFORE,
      AFTER,
      // only expose to internal use.
      NO_OPERATION
    }

    static ColumnPositionType fromTypeValue(String value) {
      switch (value.toLowerCase(Locale.ROOT)) {
        case "first":
          return ColumnPositionType.FIRST;
        case "before":
          return ColumnPositionType.BEFORE;
        case "after":
          return ColumnPositionType.AFTER;
        case "no_operation":
          return ColumnPositionType.NO_OPERATION;
        default:
          throw new IllegalArgumentException(String.format("only support first/before/after but found: %s", value));
      }
    }

    private final int srcId;
    private final int dsrId;
    private final ColumnPositionType type;

    static ColumnPositionChange first(int srcId) {
      return new ColumnPositionChange(srcId, -1, ColumnPositionType.FIRST);
    }

    static ColumnPositionChange before(int srcId, int dsrId) {
      return new ColumnPositionChange(srcId, dsrId, ColumnPositionType.BEFORE);
    }

    static ColumnPositionChange after(int srcId, int dsrId) {
      return new ColumnPositionChange(srcId, dsrId, ColumnPositionType.AFTER);
    }

    static ColumnPositionChange get(int srcId, int dsrId, String type) {
      return get(srcId, dsrId, fromTypeValue(type));
    }

    static ColumnPositionChange get(int srcId, int dsrId, ColumnPositionType type) {
      switch (type) {
        case FIRST:
          return ColumnPositionChange.first(srcId);
        case BEFORE:
          return ColumnPositionChange.before(srcId, dsrId);
        case AFTER:
          return ColumnPositionChange.after(srcId, dsrId);
        default:
          throw new IllegalArgumentException(String.format("only support first/before/after but found: %s", type));
      }
    }

    private ColumnPositionChange(int srcId, int dsrId, ColumnPositionType type) {
      this.srcId = srcId;
      this.dsrId = dsrId;
      this.type = type;
    }

    public int getSrcId() {
      return srcId;
    }

    public int getDsrId() {
      return dsrId;
    }

    public ColumnPositionType type() {
      return type;
    }
  }
}
