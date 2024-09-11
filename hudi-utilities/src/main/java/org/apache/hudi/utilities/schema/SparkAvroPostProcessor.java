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

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.config.HoodieSchemaProviderConfig;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.types.StructType;

/**
 * HUDI-1343:Add standard schema postprocessor which would rewrite the schema using spark-avro conversion.
 */
public class SparkAvroPostProcessor extends SchemaPostProcessor {

  @Deprecated
  public static class Config {
    @Deprecated
    public static final String SPARK_AVRO_POST_PROCESSOR_PROP_ENABLE =
        HoodieSchemaProviderConfig.SPARK_AVRO_POST_PROCESSOR_ENABLE.key();
  }

  public SparkAvroPostProcessor(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
  }

  @Override
  public Schema processSchema(Schema schema) {
    if (schema == null) {
      return null;
    }

    StructType structType = AvroConversionUtils.convertAvroSchemaToStructType(schema);
    // NOTE: It's critical that we preserve incoming schema's qualified record-name to make
    //       sure we maintain schema's compatibility (as defined by [[AvroSchemaCompatibility]])
    return AvroConversionUtils.convertStructTypeToAvroSchema(structType, schema.getFullName());
  }
}