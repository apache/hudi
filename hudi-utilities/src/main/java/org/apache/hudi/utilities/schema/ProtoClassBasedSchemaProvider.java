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
 *
 */

package org.apache.hudi.utilities.schema;

import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.sources.helpers.ProtoConversionUtil;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Collections;

/**
 * A schema provider that takes in a class name for a generated protobuf class that is on the classpath.
 */
public class ProtoClassBasedSchemaProvider extends SchemaProvider {
  /**
   * Configs supported.
   */
  public static class Config {
    public static final String PROTO_SCHEMA_CLASS_NAME = "hoodie.deltastreamer.schemaprovider.proto.className";
    public static final String PROTO_SCHEMA_FLATTEN_WRAPPED_PRIMITIVES = "hoodie.deltastreamer.schemaprovider.proto.flattenWrappers";
  }

  private final String schemaString;

  /**
   * To be lazily inited on executors.
   */
  private transient Schema schema;

  public ProtoClassBasedSchemaProvider(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
    DataSourceUtils.checkRequiredProperties(props, Collections.singletonList(
        Config.PROTO_SCHEMA_CLASS_NAME));
    String className = config.getString(Config.PROTO_SCHEMA_CLASS_NAME);
    boolean flattenWrappedPrimitives = props.getBoolean(ProtoClassBasedSchemaProvider.Config.PROTO_SCHEMA_FLATTEN_WRAPPED_PRIMITIVES, false);
    try {
      schemaString = ProtoConversionUtil.getAvroSchemaForMessageClass(ReflectionUtils.getClass(className), flattenWrappedPrimitives).toString();
    } catch (Exception e) {
      throw new HoodieException(String.format("Error reading proto source schema for class: %s", className), e);
    }
  }

  @Override
  public Schema getSourceSchema() {
    if (schema == null) {
      Schema.Parser parser = new Schema.Parser();
      schema = parser.parse(schemaString);
    }
    return schema;
  }

  @Override
  public Schema getTargetSchema() {
    return getSourceSchema();
  }
}
