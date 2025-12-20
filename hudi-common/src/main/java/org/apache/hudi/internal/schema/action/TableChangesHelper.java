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

import org.apache.hudi.internal.schema.Types;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Helper class to support Table schema changes.
 */
public class TableChangesHelper {

  /**
   * Apply add operation and column position change operation.
   *
   * @param fields origin column fields.
   * @param adds column fields to be added.
   * @param pchanges a wrapper class hold all the position change operations.
   * @return column fields after adjusting the position.
   */
  public static List<Types.Field> applyAddChange2Fields(List<Types.Field> fields, ArrayList<Types.Field> adds, ArrayList<TableChange.ColumnPositionChange> pchanges) {
    if (adds == null && pchanges == null) {
      return fields;
    }
    LinkedList<Types.Field> result = new LinkedList<>(fields);
    // apply add columns
    if (adds != null && !adds.isEmpty()) {
      result.addAll(adds);
    }
    // apply position change
    if (pchanges != null && !pchanges.isEmpty()) {
      for (TableChange.ColumnPositionChange pchange : pchanges) {
        Types.Field srcField = result.stream().filter(f -> f.fieldId() == pchange.getSrcId()).findFirst().get();
        Types.Field dsrField = result.stream().filter(f -> f.fieldId() == pchange.getDsrId()).findFirst().orElse(null);
        // we remove srcField first
        result.remove(srcField);
        switch (pchange.getType()) {
          case AFTER:
            // add srcField after dsrField
            result.add(result.indexOf(dsrField) + 1, srcField);
            break;
          case BEFORE:
            // add srcField before dsrField
            result.add(result.indexOf(dsrField), srcField);
            break;
          case FIRST:
            result.addFirst(srcField);
            break;
          default:
            // should not reach here
        }
      }
    }
    return result;
  }

  public static String getParentName(String fullColName) {
    int offset = fullColName.lastIndexOf(".");
    return offset > 0 ? fullColName.substring(0, offset) : "";
  }

  public static String getLeafName(String fullColName) {
    int offset = fullColName.lastIndexOf(".");
    return offset > 0 ? fullColName.substring(offset + 1) : fullColName;
  }
}
