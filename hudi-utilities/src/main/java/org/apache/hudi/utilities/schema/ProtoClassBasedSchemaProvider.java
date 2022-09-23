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
    private static final String PREFIX = "hoodie.deltastreamer.schemaprovider.proto";
    public static final ConfigProperty<String> PROTO_SCHEMA_CLASS_NAME = ConfigProperty.key(PREFIX + ".className")
        .noDefaultValue()
        .sinceVersion("0.13.0")
        .withDocumentation("The Protobuf Message class used as the source for the schema.");

    public static final ConfigProperty<Boolean> PROTO_SCHEMA_FLATTEN_WRAPPED_PRIMITIVES = ConfigProperty.key(PREFIX + ".flattenWrappers")
        .defaultValue(false)
        .sinceVersion("0.13.0")
        .withDocumentation("When set to false wrapped primitives like Int64Value are translated to a record with a single 'value' field instead of simply a nullable value");
    public static final ConfigProperty<Integer> PROTO_SCHEMA_MAX_RECURSION_DEPTH = ConfigProperty.key(PREFIX + ".maxRecursionDepth")
        .defaultValue(5)
        .sinceVersion("0.13.0")
        .withDocumentation("The max depth to unravel the Proto schema when translating into an Avro schema. Setting this depth allows the user to convert a schema that is recursive in proto into "
            + "something that can be represented in their lake format like Parquet. After a given class has been seen N times within a single branch, the schema provider will create a record with a "
            + "byte array to hold the remaining proto data and a string to hold the message descriptor's name for context.");
  }

  private final String schemaString;

  /**
   * To be lazily initiated on executors.
   */
  private transient Schema schema;

  public ProtoClassBasedSchemaProvider(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
    DataSourceUtils.checkRequiredProperties(props, Collections.singletonList(
        Config.PROTO_SCHEMA_CLASS_NAME.key()));
    String className = config.getString(Config.PROTO_SCHEMA_CLASS_NAME.key());
    boolean flattenWrappedPrimitives = props.getBoolean(ProtoClassBasedSchemaProvider.Config.PROTO_SCHEMA_FLATTEN_WRAPPED_PRIMITIVES.key(),
        Config.PROTO_SCHEMA_FLATTEN_WRAPPED_PRIMITIVES.defaultValue());
    int maxRecursionDepth = props.getInteger(Config.PROTO_SCHEMA_MAX_RECURSION_DEPTH.key(), Config.PROTO_SCHEMA_MAX_RECURSION_DEPTH.defaultValue());
    try {
      schemaString = ProtoConversionUtil.getAvroSchemaForMessageClass(ReflectionUtils.getClass(className), flattenWrappedPrimitives, maxRecursionDepth).toString();
    } catch (Exception e) {
      throw new HoodieSchemaException(String.format("Error reading proto source schema for class: %s", className), e);
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
