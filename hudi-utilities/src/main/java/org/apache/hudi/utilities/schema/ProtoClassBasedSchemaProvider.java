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
    private static final String PROTO_SCHEMA_PROVIDER_PREFIX = "hoodie.deltastreamer.schemaprovider.proto";
    public static final ConfigProperty<String> PROTO_SCHEMA_CLASS_NAME = ConfigProperty.key(PROTO_SCHEMA_PROVIDER_PREFIX + ".class.name")
        .noDefaultValue()
        .sinceVersion("0.13.0")
        .withDocumentation("The Protobuf Message class used as the source for the schema.");

    public static final ConfigProperty<Boolean> PROTO_SCHEMA_WRAPPED_PRIMITIVES_AS_RECORDS = ConfigProperty.key(PROTO_SCHEMA_PROVIDER_PREFIX + ".flatten.wrappers")
        .defaultValue(false)
        .sinceVersion("0.13.0")
        .withDocumentation("When set to true wrapped primitives like Int64Value are translated to a record with a single 'value' field. By default, the value is false and the wrapped primitives are "
            + "treated as a nullable value");

    public static final ConfigProperty<Boolean> PROTO_SCHEMA_TIMESTAMPS_AS_RECORDS = ConfigProperty.key(PROTO_SCHEMA_PROVIDER_PREFIX + ".timestamps.as.records")
        .defaultValue(false)
        .sinceVersion("0.13.0")
        .withDocumentation("When set to true Timestamp fields are translated to a record with a seconds and nanos field. By default, the value is false and the timestamp is converted to a long with "
            + "the timestamp-micros logical type");

    public static final ConfigProperty<Integer> PROTO_SCHEMA_MAX_RECURSION_DEPTH = ConfigProperty.key(PROTO_SCHEMA_PROVIDER_PREFIX + ".max.recursion.depth")
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
    boolean wrappedPrimitivesAsRecords = props.getBoolean(Config.PROTO_SCHEMA_WRAPPED_PRIMITIVES_AS_RECORDS.key(),
        Config.PROTO_SCHEMA_WRAPPED_PRIMITIVES_AS_RECORDS.defaultValue());
    int maxRecursionDepth = props.getInteger(Config.PROTO_SCHEMA_MAX_RECURSION_DEPTH.key(), Config.PROTO_SCHEMA_MAX_RECURSION_DEPTH.defaultValue());
    boolean timestampsAsRecords = props.getBoolean(Config.PROTO_SCHEMA_TIMESTAMPS_AS_RECORDS.key(), false);
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
