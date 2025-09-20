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

package org.apache.hudi.utilities.schema.postprocessor.add;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.utilities.exception.HoodieSchemaPostProcessException;
import org.apache.hudi.utilities.schema.SchemaPostProcessor;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.apache.hudi.avro.HoodieAvroUtils.createNewSchemaField;
import static org.apache.hudi.common.util.ConfigUtils.getBooleanWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getRawValueWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;
import static org.apache.hudi.utilities.config.SchemaProviderPostProcessorConfig.SCHEMA_POST_PROCESSOR_ADD_COLUMN_DEFAULT_PROP;
import static org.apache.hudi.utilities.config.SchemaProviderPostProcessorConfig.SCHEMA_POST_PROCESSOR_ADD_COLUMN_DOC_PROP;
import static org.apache.hudi.utilities.config.SchemaProviderPostProcessorConfig.SCHEMA_POST_PROCESSOR_ADD_COLUMN_NAME_PROP;
import static org.apache.hudi.utilities.config.SchemaProviderPostProcessorConfig.SCHEMA_POST_PROCESSOR_ADD_COLUMN_NULLABLE_PROP;
import static org.apache.hudi.utilities.config.SchemaProviderPostProcessorConfig.SCHEMA_POST_PROCESSOR_ADD_COLUMN_TYPE_PROP;

/**
 * A {@link SchemaPostProcessor} used to add a new column of primitive types to given schema. Only supports adding one
 * column at a time.
 * <p>
 * The new column will be appended to the end.
 * <p>
 * TODO support complex types.
 */
public class AddPrimitiveColumnSchemaPostProcessor extends SchemaPostProcessor {

  public AddPrimitiveColumnSchemaPostProcessor(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
  }

  @Override
  public Schema processSchema(Schema schema) {
    String newColumnName = getStringWithAltKeys(this.config, SCHEMA_POST_PROCESSOR_ADD_COLUMN_NAME_PROP);

    if (schema.getField(newColumnName) != null) {
      throw new HoodieSchemaPostProcessException(String.format("Column %s already exist!", newColumnName));
    }

    List<Schema.Field> sourceFields = schema.getFields();
    List<Schema.Field> targetFields = new ArrayList<>(sourceFields.size() + 1);


    for (Schema.Field sourceField : sourceFields) {
      targetFields.add(createNewSchemaField(sourceField));
    }

    // add new column to the end
    targetFields.add(buildNewColumn());

    return Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), false, targetFields);
  }

  private Schema.Field buildNewColumn() {

    String columnName = getStringWithAltKeys(this.config, SCHEMA_POST_PROCESSOR_ADD_COLUMN_NAME_PROP);
    String type = getStringWithAltKeys(this.config, SCHEMA_POST_PROCESSOR_ADD_COLUMN_TYPE_PROP).toUpperCase(Locale.ROOT);
    String doc = getStringWithAltKeys(this.config, SCHEMA_POST_PROCESSOR_ADD_COLUMN_DOC_PROP, true);
    Option<Object> defaultValue = getRawValueWithAltKeys(this.config, SCHEMA_POST_PROCESSOR_ADD_COLUMN_DEFAULT_PROP);

    boolean nullable = getBooleanWithAltKeys(this.config, SCHEMA_POST_PROCESSOR_ADD_COLUMN_NULLABLE_PROP);

    ValidationUtils.checkArgument(!StringUtils.isNullOrEmpty(columnName));
    ValidationUtils.checkArgument(!StringUtils.isNullOrEmpty(type));
    ValidationUtils.checkArgument(!Schema.Type.NULL.getName().equals(type));

    Schema newSchema = createSchema(type, nullable);

    return new Schema.Field(columnName, newSchema, doc, defaultValue.isPresent() ? defaultValue.get() : null);
  }

  private Schema createSchema(String type, boolean nullable) {
    Schema schema = Schema.create(Schema.Type.valueOf(type));
    if (nullable) {
      schema = Schema.createUnion(Schema.create(Schema.Type.NULL), schema);
    }
    return schema;
  }

}
