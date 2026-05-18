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

package org.apache.hudi.utilities.schema.postprocessor;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.utilities.config.SchemaProviderPostProcessorConfig;
import org.apache.hudi.utilities.exception.HoodieSchemaPostProcessException;
import org.apache.hudi.utilities.schema.SchemaPostProcessor;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;

/**
 * A {@link SchemaPostProcessor} that support to delete column(s) from given schema.
 * <p>
 * Multiple columns are separated by commas.
 * For example:
 * <p>
 * properties.put("hoodie.streamer.schemaprovider.schema_post_processor.delete.columns", "column1,column2").
 */
public class DropColumnSchemaPostProcessor extends SchemaPostProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(DropColumnSchemaPostProcessor.class);

  public DropColumnSchemaPostProcessor(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
  }

  @Deprecated
  public static class Config {

    @Deprecated
    public static final String DELETE_COLUMN_POST_PROCESSOR_COLUMN_PROP =
        SchemaProviderPostProcessorConfig.DELETE_COLUMN_POST_PROCESSOR_COLUMN.key();
  }

  @Override
  @Deprecated
  public Schema processSchema(Schema schema) {
    return processSchema(HoodieSchema.fromAvroSchema(schema)).toAvroSchema();
  }

  @Override
  public HoodieSchema processSchema(HoodieSchema schema) {
    String columnToDeleteStr = getStringWithAltKeys(
        this.config, SchemaProviderPostProcessorConfig.DELETE_COLUMN_POST_PROCESSOR_COLUMN);

    if (StringUtils.isNullOrEmpty(columnToDeleteStr)) {
      LOG.warn("Param {} is null or empty, return original schema",
          SchemaProviderPostProcessorConfig.DELETE_COLUMN_POST_PROCESSOR_COLUMN.key());
    }

    // convert field to lowerCase for compare purpose
    Set<String> columnsToDelete = Arrays.stream(columnToDeleteStr.split(","))
        .map(filed -> filed.toLowerCase(Locale.ROOT))
        .collect(Collectors.toSet());

    List<HoodieSchemaField> sourceFields = schema.getFields();
    List<HoodieSchemaField> targetFields = new LinkedList<>();

    for (HoodieSchemaField sourceField : sourceFields) {
      if (!columnsToDelete.contains(sourceField.name().toLowerCase(Locale.ROOT))) {
        targetFields.add(HoodieSchemaUtils.createNewSchemaField(sourceField));
      }
    }

    if (targetFields.isEmpty()) {
      throw new HoodieSchemaPostProcessException("Target schema is empty, you can not remove all columns!");
    }

    return HoodieSchema.createRecord(schema.getName(), schema.getDoc().orElse(null), schema.getNamespace().orElse(null), false, targetFields);
  }
}
