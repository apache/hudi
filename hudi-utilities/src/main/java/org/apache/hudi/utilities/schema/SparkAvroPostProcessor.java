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

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * HUDI-1343:Add standard schema postprocessor which would rewrite the schema using spark-avro conversion.
 */
public class SparkAvroPostProcessor extends SchemaPostProcessor {

  public static class Config {
    public static final String SPARK_AVRO_POST_PROCESSOR_PROP_ENABLE =
            "hoodie.deltastreamer.schemaprovider.spark_avro_post_processor.enable";
  }

  public SparkAvroPostProcessor(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
  }

  @Override
  public Schema processSchema(Schema schema) {
    return schema != null ? AvroConversionUtils.convertStructTypeToAvroSchema(
        AvroConversionUtils.convertAvroSchemaToStructType(schema), RowBasedSchemaProvider.HOODIE_RECORD_STRUCT_NAME,
        RowBasedSchemaProvider.HOODIE_RECORD_NAMESPACE) : null;
  }
}