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

package org.apache.hudi.schema;

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;

import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HoodieSparkSchemaUtils {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieSparkSchemaUtils.class);

  /**
   * Returns field name and the resp data type of the field. The data type will always refer to the leaf node.
   * for eg, for a.b.c, we turn Pair.of(a.b.c, DataType(c))
   * @param schema schema of table.
   * @param fieldName field name of interest.
   * @return
   */
  @VisibleForTesting
  public static Pair<String, StructField> getSchemaForField(StructType schema, String fieldName) {
    return getSchemaForField(schema, fieldName, StringUtils.EMPTY_STRING);
  }

  @VisibleForTesting
  public static Pair<String, StructField> getSchemaForField(StructType schema, String fieldName, String prefix) {
    if (!fieldName.contains(".")) {
      return Pair.of(prefix + schema.fields()[schema.fieldIndex(fieldName)].name(), schema.fields()[schema.fieldIndex(fieldName)]);
    } else {
      int rootFieldIndex = fieldName.indexOf(".");
      StructField rootField = schema.fields()[schema.fieldIndex(fieldName.substring(0, rootFieldIndex))];
      if (rootField == null) {
        throw new HoodieException("Failed to find " + fieldName + " in the table schema ");
      }
      return getSchemaForField((StructType) rootField.dataType(), fieldName.substring(rootFieldIndex + 1), prefix + fieldName.substring(0, rootFieldIndex + 1));
    }
  }
}
