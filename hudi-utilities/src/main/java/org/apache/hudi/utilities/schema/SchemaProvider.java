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

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIClass;
import org.apache.hudi.PublicAPIMethod;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.schema.postprocessor.add.BaseSchemaPostProcessorConfig;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.List;

/**
 * Class to provide schema for reading data and also writing into a Hoodie table,
 * used by deltastreamer (runs over Spark).
 */
@PublicAPIClass(maturity = ApiMaturityLevel.STABLE)
public abstract class SchemaProvider implements Serializable {

  protected TypedProperties config;

  protected JavaSparkContext jssc;

  protected SchemaPostProcessor schemaPostProcessor;

  public SchemaProvider(TypedProperties props) {
    this(props, null);
  }

  protected SchemaProvider(TypedProperties props, JavaSparkContext jssc) {
    this.config = props;
    this.jssc = jssc;
  }

  @PublicAPIMethod(maturity = ApiMaturityLevel.STABLE)
  public final Schema getSourceSchema() {
    if (schemaPostProcessor != null
        && !config.getBoolean(BaseSchemaPostProcessorConfig.SCHEMA_PROVIDER_SOURCE_DISABLE.key(),
        Boolean.parseBoolean(BaseSchemaPostProcessorConfig.SCHEMA_PROVIDER_SOURCE_DISABLE.defaultValue()))) {
      return schemaPostProcessor.processSchema(getUnprocessedSourceSchema());
    }
    return getUnprocessedSourceSchema();
  }

  public abstract Schema getUnprocessedSourceSchema();

  public void addPostProcessor(List<String> transformerClassNames) {
    if (schemaPostProcessor != null) {
      return;
    }
    String schemaPostProcessorClass = config.getString(SchemaPostProcessor.Config.SCHEMA_POST_PROCESSOR_PROP, null);
    boolean enableSparkAvroPostProcessor = Boolean.parseBoolean(config.getString(SparkAvroPostProcessor.Config.SPARK_AVRO_POST_PROCESSOR_PROP_ENABLE, "true"));

    if (transformerClassNames != null && !transformerClassNames.isEmpty() && enableSparkAvroPostProcessor) {
      if (!StringUtils.isNullOrEmpty(schemaPostProcessorClass)) {
        schemaPostProcessorClass = schemaPostProcessorClass + "," + SparkAvroPostProcessor.class.getName();
      } else {
        schemaPostProcessorClass = SparkAvroPostProcessor.class.getName();
      }
    }

    schemaPostProcessor = UtilHelpers.createSchemaPostProcessor(schemaPostProcessorClass, config, jssc);
  }

  public void removePostProcessor() {
    schemaPostProcessor = null;
  }

  @PublicAPIMethod(maturity = ApiMaturityLevel.STABLE)
  public final Schema getTargetSchema() {
    if (schemaPostProcessor != null
        && !config.getBoolean(BaseSchemaPostProcessorConfig.SCHEMA_PROVIDER_TARGET_DISABLE.key(),
        Boolean.parseBoolean(BaseSchemaPostProcessorConfig.SCHEMA_PROVIDER_TARGET_DISABLE.defaultValue()))) {
      return schemaPostProcessor.processSchema(getUnprocessedTargetSchema());
    }
    return getUnprocessedTargetSchema();
  }

  public Schema getUnprocessedTargetSchema() {
    // by default, use source schema as target for hoodie table as well
    return getUnprocessedSourceSchema();
  }
}
