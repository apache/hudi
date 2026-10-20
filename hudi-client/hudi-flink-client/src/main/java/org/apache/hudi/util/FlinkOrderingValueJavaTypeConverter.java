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

package org.apache.hudi.util;

import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.util.OrderingValueJavaTypeConverter;
import org.apache.hudi.common.util.OrderingValues;
import org.apache.hudi.common.util.collection.ArrayComparable;
import org.apache.hudi.common.util.collection.Pair;

import java.util.List;
import java.util.Properties;

/** Converts empty-record ordering values using the cached field converters, without retaining reader state. */
public enum FlinkOrderingValueJavaTypeConverter implements OrderingValueJavaTypeConverter {
  INSTANCE;

  @Override
  public Comparable<?> convert(Comparable<?> orderingValue, HoodieSchema schema, Properties props, String[] orderingFields) {
    if (orderingValue == null || OrderingValues.isDefault(orderingValue) || orderingFields == null || orderingFields.length == 0) {
      return orderingValue;
    }
    boolean utcTimezone = Boolean.parseBoolean(props.getProperty(
        HoodieStorageConfig.WRITE_UTC_TIMEZONE.key(), HoodieStorageConfig.WRITE_UTC_TIMEZONE.defaultValue().toString()));
    if (orderingValue instanceof ArrayComparable) {
      List<Comparable> values = ((ArrayComparable) orderingValue).getValues();
      Comparable[] converted = new Comparable[values.size()];
      for (int i = 0; i < values.size(); i++) {
        converted[i] = convertField(values.get(i), schema, orderingFields[i], utcTimezone);
      }
      return new ArrayComparable(converted);
    }
    return convertField(orderingValue, schema, orderingFields[0], utcTimezone);
  }

  private Comparable<?> convertField(Comparable<?> value, HoodieSchema schema, String fieldName, boolean utcTimezone) {
    // The default sentinel is an Integer even when the field is a decimal or timestamp.
    if (value == null || OrderingValues.isDefault(value)) {
      return value;
    }
    int separator = fieldName.lastIndexOf('.');
    HoodieSchema parentSchema = separator < 0 ? schema : schema.getNestedField(fieldName.substring(0, separator))
        .map(Pair::getRight).map(HoodieSchemaField::getNonNullSchema).orElse(null);
    if (parentSchema == null) {
      return value;
    }
    RowDataQueryContexts.FieldQueryContext fieldContext = RowDataQueryContexts.fromSchema(parentSchema, utcTimezone)
        .getFieldQueryContext(fieldName.substring(separator + 1));
    return fieldContext == null ? value : (Comparable<?>) fieldContext.convertToJavaType(value);
  }
}
