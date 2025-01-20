/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.sync.datahub.util;

import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import io.datahubproject.models.util.FieldPath;

import java.util.ArrayList;
import java.util.List;

public class SchemaFieldsUtil {
  public static SchemaFieldArray reorderPrefixedFields(SchemaFieldArray fields, String prefix) {
    if (fields == null || fields.isEmpty()) {
      return fields;
    }

    // Split the list into underscore and non-underscore fields while preserving order
    List<SchemaField> prefixedFields = new ArrayList<>();
    List<SchemaField> normalFields = new ArrayList<>();

    for (SchemaField field : fields) {
      FieldPath fieldPath = new FieldPath(field.getFieldPath());

      if (fieldPath.isTopLevel() && fieldPath.leafFieldName().startsWith(prefix)) {
        prefixedFields.add(field);
      } else {
        normalFields.add(field);
      }
    }

    // Combine the lists with underscore fields at the end
    List<SchemaField> result = new ArrayList<>(normalFields.size() + prefixedFields.size());
    result.addAll(normalFields);
    result.addAll(prefixedFields);

    return new SchemaFieldArray(result);
  }
}
