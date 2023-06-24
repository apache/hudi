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
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.internal.schema.HoodieSchemaException;
import org.apache.hudi.utilities.config.ProtoClassBasedSchemaProviderConfig;
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
  @Deprecated
  public static class Config {
    // Use {@link ProtoClassBasedSchemaProviderConfig} instead
    @Deprecated
    public static final ConfigProperty<String> PROTO_SCHEMA_CLASS_NAME =
        ProtoClassBasedSchemaProviderConfig.PROTO_SCHEMA_CLASS_NAME;
    @Deprecated
    public static final ConfigProperty<Boolean> PROTO_SCHEMA_WRAPPED_PRIMITIVES_AS_RECORDS =
        ProtoClassBasedSchemaProviderConfig.PROTO_SCHEMA_WRAPPED_PRIMITIVES_AS_RECORDS;
    @Deprecated
    public static final ConfigProperty<Boolean> PROTO_SCHEMA_TIMESTAMPS_AS_RECORDS =
        ProtoClassBasedSchemaProviderConfig.PROTO_SCHEMA_TIMESTAMPS_AS_RECORDS;
    @Deprecated
    public static final ConfigProperty<Integer> PROTO_SCHEMA_MAX_RECURSION_DEPTH =
        ProtoClassBasedSchemaProviderConfig.PROTO_SCHEMA_MAX_RECURSION_DEPTH;
  }

  private final String schemaString;

  /**
   * To be lazily initiated on executors.
   */
  private transient Schema schema;

  public ProtoClassBasedSchemaProvider(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
    DataSourceUtils.checkRequiredProperties(props, Collections.singletonList(
        ProtoClassBasedSchemaProviderConfig.PROTO_SCHEMA_CLASS_NAME.key()));
    String className = config.getString(ProtoClassBasedSchemaProviderConfig.PROTO_SCHEMA_CLASS_NAME.key());
    boolean wrappedPrimitivesAsRecords = props.getBoolean(ProtoClassBasedSchemaProviderConfig.PROTO_SCHEMA_WRAPPED_PRIMITIVES_AS_RECORDS.key(),
        ProtoClassBasedSchemaProviderConfig.PROTO_SCHEMA_WRAPPED_PRIMITIVES_AS_RECORDS.defaultValue());
    int maxRecursionDepth = props.getInteger(
        ProtoClassBasedSchemaProviderConfig.PROTO_SCHEMA_MAX_RECURSION_DEPTH.key(), ProtoClassBasedSchemaProviderConfig.PROTO_SCHEMA_MAX_RECURSION_DEPTH.defaultValue());
    boolean timestampsAsRecords = props.getBoolean(ProtoClassBasedSchemaProviderConfig.PROTO_SCHEMA_TIMESTAMPS_AS_RECORDS.key(), false);
    ProtoConversionUtil.SchemaConfig schemaConfig = new ProtoConversionUtil.SchemaConfig(wrappedPrimitivesAsRecords, maxRecursionDepth, timestampsAsRecords);
    try {
      schemaString = ProtoConversionUtil.getAvroSchemaForMessageClass(ReflectionUtils.getClass(className), schemaConfig).toString();
    } catch (Exception e) {
      throw new HoodieSchemaException(String.format("Error reading proto source schema for class: %s", className), e);
    }
  }

  @Override
  public Schema getSourceSchema() {
    if (schema == null) {
      try {
        Schema.Parser parser = new Schema.Parser();
        schema = parser.parse(schemaString);
      } catch (Exception e) {
        throw new HoodieSchemaException("Failed to parse schema: " + schemaString, e);
      }

    }
    return schema;
  }

  @Override
  public Schema getTargetSchema() {
    return getSourceSchema();
  }
}
