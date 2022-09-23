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

package org.apache.hudi.utilities.schema;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.utilities.exception.HoodieSchemaPostProcessException;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * A {@link SchemaPostProcessor} use to add column to given schema. Currently. only supports adding one column at a time.
 * Users can specify the position of new column by config {@link Config#SCHEMA_POST_PROCESSOR_ADD_COLUMN_NEXT_PROP},
 * the new column will be added before this column.
 * <p>
 * Currently supported types : bytes, string, int, long, float, double, boolean, decimal
 */
public class AddColumnSchemaPostProcessor extends SchemaPostProcessor {

  private static final Logger LOG = LogManager.getLogger(AddColumnSchemaPostProcessor.class);

  public AddColumnSchemaPostProcessor(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
  }

  /**
   * Configs supported.
   */
  public static class Config {
    public static final ConfigProperty<String> SCHEMA_POST_PROCESSOR_ADD_COLUMN_NAME_PROP = ConfigProperty
        .key("hoodie.deltastreamer.schemaprovider.schema_post_processor.add.column.name")
        .noDefaultValue()
        .withDocumentation("New column's name");

    public static final ConfigProperty<String> SCHEMA_POST_PROCESSOR_ADD_COLUMN_TYPE_PROP = ConfigProperty
        .key("hoodie.deltastreamer.schemaprovider.schema_post_processor.add.column.type")
        .noDefaultValue()
        .withDocumentation("New column's type");

    public static final ConfigProperty<String> SCHEMA_POST_PROCESSOR_ADD_COLUMN_DOC_PROP = ConfigProperty
        .key("hoodie.deltastreamer.schemaprovider.schema_post_processor.add.column.doc")
        .noDefaultValue()
        .withDocumentation("New column's doc");

    public static final ConfigProperty<String> SCHEMA_POST_PROCESSOR_ADD_COLUMN_DEFAULT_PROP = ConfigProperty
        .key("hoodie.deltastreamer.schemaprovider.schema_post_processor.add.column.default")
        .noDefaultValue()
        .withDocumentation("New column's default value");

    public static final ConfigProperty<String> SCHEMA_POST_PROCESSOR_ADD_COLUMN_SIZE_PROP = ConfigProperty
        .key("hoodie.deltastreamer.schemaprovider.schema_post_processor.add.column.size")
        .noDefaultValue()
        .withDocumentation("New column's size, used in decimal type");

    public static final ConfigProperty<String> SCHEMA_POST_PROCESSOR_ADD_COLUMN_PRECISION_PROP = ConfigProperty
        .key("hoodie.deltastreamer.schemaprovider.schema_post_processor.add.column.precision")
        .noDefaultValue()
        .withDocumentation("New column's precision, used in decimal type");

    public static final ConfigProperty<String> SCHEMA_POST_PROCESSOR_ADD_COLUMN_SCALE_PROP = ConfigProperty
        .key("hoodie.deltastreamer.schemaprovider.schema_post_processor.add.column.scale")
        .noDefaultValue()
        .withDocumentation("New column's precision, used in decimal type");

    public static final ConfigProperty<String> SCHEMA_POST_PROCESSOR_ADD_COLUMN_NEXT_PROP = ConfigProperty
        .key("hoodie.deltastreamer.schemaprovider.schema_post_processor.add.column.next")
        .defaultValue(HoodieRecord.HOODIE_IS_DELETED)
        .withDocumentation("Column name which locate next to new column, `_hoodie_is_deleted` by default.");
  }

  public static final String BYTES = "BYTES";
  public static final String STRING = "STRING";
  public static final String INT = "INT";
  public static final String LONG = "LONG";
  public static final String FLOAT = "FLOAT";
  public static final String DOUBLE = "DOUBLE";
  public static final String BOOLEAN = "BOOLEAN";
  public static final String DECIMAL = "DECIMAL";

  @Override
  public Schema processSchema(Schema schema) {
    String newColumnName = this.config.getString(Config.SCHEMA_POST_PROCESSOR_ADD_COLUMN_NAME_PROP.key());

    if (schema.getField(newColumnName) != null) {
      LOG.warn(String.format("Column %s already exist!", newColumnName));
      return schema;
    }

    List<Schema.Field> sourceFields = schema.getFields();
    List<Schema.Field> targetFields = new ArrayList<>(sourceFields.size() + 1);

    String nextColumnName = this.config.getString(Config.SCHEMA_POST_PROCESSOR_ADD_COLUMN_NEXT_PROP.key(),
        Config.SCHEMA_POST_PROCESSOR_ADD_COLUMN_NEXT_PROP.defaultValue());

    // mark whether the new column is added
    boolean isAdded = false;
    for (Schema.Field sourceField : sourceFields) {
      if (sourceField.name().equals(nextColumnName)) {
        targetFields.add(buildNewColumn());
        isAdded = true;
      }
      targetFields.add(new Schema.Field(sourceField.name(), sourceField.schema(), sourceField.doc(), sourceField.defaultVal()));
    }

    // this would happen when `nextColumn` does not exist. just append the new column to the end
    if (!isAdded) {
      targetFields.add(buildNewColumn());
    }

    return Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), false, targetFields);
  }

  private Schema.Field buildNewColumn() {
    Schema.Field result;

    String columnName = this.config.getString(Config.SCHEMA_POST_PROCESSOR_ADD_COLUMN_NAME_PROP.key());
    String type = this.config.getString(Config.SCHEMA_POST_PROCESSOR_ADD_COLUMN_TYPE_PROP.key()).toUpperCase(Locale.ROOT);
    String doc = this.config.getString(Config.SCHEMA_POST_PROCESSOR_ADD_COLUMN_DOC_PROP.key(), null);
    Object defaultValue = this.config.getOrDefault(Config.SCHEMA_POST_PROCESSOR_ADD_COLUMN_DEFAULT_PROP.key(),
        null);

    switch (type) {
      case STRING:
      case BYTES:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
        result = new Schema.Field(columnName, Schema.create(Schema.Type.valueOf(type)), doc, defaultValue);
        break;
      case DECIMAL:
        int size = this.config.getInteger(Config.SCHEMA_POST_PROCESSOR_ADD_COLUMN_SIZE_PROP.key(), 10);
        int precision = this.config.getInteger(Config.SCHEMA_POST_PROCESSOR_ADD_COLUMN_PRECISION_PROP.key());
        int scale = this.config.getInteger(Config.SCHEMA_POST_PROCESSOR_ADD_COLUMN_SCALE_PROP.key());

        Schema decimalSchema = Schema.createFixed(null, null, null, size);
        LogicalTypes.decimal(precision, scale).addToSchema(decimalSchema);

        result = new Schema.Field(columnName, decimalSchema, doc, defaultValue);
        break;
      default:
        throw new HoodieSchemaPostProcessException(String.format("Type %s is not supported", type));
    }
    return result;
  }
}
