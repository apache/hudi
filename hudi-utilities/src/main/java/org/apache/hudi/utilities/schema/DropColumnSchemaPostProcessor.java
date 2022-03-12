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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.utilities.exception.HoodieSchemaPostProcessException;

import org.apache.avro.Schema;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link SchemaPostProcessor} that support to delete column(s) from given schema.
 * <p>
 * Multiple columns are separated by commas.
 * For example:
 * <p>
 * properties.put("hoodie.deltastreamer.schemaprovider.schema_post_processor.delete.columns", "column1,column2").
 */
public class DropColumnSchemaPostProcessor extends SchemaPostProcessor {

  private static final Logger LOG = LogManager.getLogger(DropColumnSchemaPostProcessor.class);

  public DropColumnSchemaPostProcessor(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
  }

  public static class Config {
    public static final String DELETE_COLUMN_POST_PROCESSOR_COLUMN_PROP =
        "hoodie.deltastreamer.schemaprovider.schema_post_processor.delete.columns";
  }

  @Override
  public Schema processSchema(Schema schema) {

    String columnToDeleteStr = this.config.getString(Config.DELETE_COLUMN_POST_PROCESSOR_COLUMN_PROP);

    if (StringUtils.isNullOrEmpty(columnToDeleteStr)) {
      LOG.warn(String.format("Param %s is null or empty, return original schema", Config.DELETE_COLUMN_POST_PROCESSOR_COLUMN_PROP));
    }

    // convert field to lowerCase for compare purpose
    Set<String> columnsToDelete = Arrays.stream(columnToDeleteStr.split(","))
        .map(filed -> filed.toLowerCase(Locale.ROOT))
        .collect(Collectors.toSet());

    List<Schema.Field> sourceFields = schema.getFields();
    List<Schema.Field> targetFields = new LinkedList<>();

    for (Schema.Field sourceField : sourceFields) {
      if (!columnsToDelete.contains(sourceField.name().toLowerCase(Locale.ROOT))) {
        targetFields.add(new Schema.Field(sourceField.name(), sourceField.schema(), sourceField.doc(), sourceField.defaultVal()));
      }
    }

    if (targetFields.isEmpty()) {
      throw new HoodieSchemaPostProcessException("Target schema is empty, you can not remove all columns!");
    }

    return Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), false, targetFields);
  }

}
