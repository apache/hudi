/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.config;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

import javax.annotation.concurrent.Immutable;

import static org.apache.hudi.common.util.ConfigUtils.OLD_SCHEMAPROVIDER_CONFIG_PREFIX;
import static org.apache.hudi.common.util.ConfigUtils.SCHEMAPROVIDER_CONFIG_PREFIX;

/**
 * JDBC-based Schema Provider Configs.
 */
@Immutable
@ConfigClassProperty(name = "JDBC-based Schema Provider Configs",
    groupName = ConfigGroups.Names.HUDI_STREAMER,
    subGroupName = ConfigGroups.SubGroupNames.SCHEMA_PROVIDER,
    description = "Configurations for Proto schema provider.")
public class ProtoClassBasedSchemaProviderConfig extends HoodieConfig {
  private static final String PROTO_SCHEMA_PROVIDER_PREFIX = SCHEMAPROVIDER_CONFIG_PREFIX + "proto.";
  private static final String OLD_PROTO_SCHEMA_PROVIDER_PREFIX = OLD_SCHEMAPROVIDER_CONFIG_PREFIX + "proto.";

  public static final ConfigProperty<String> PROTO_SCHEMA_CLASS_NAME = ConfigProperty
      .key(PROTO_SCHEMA_PROVIDER_PREFIX + "class.name")
      .noDefaultValue()
      .withAlternatives(OLD_PROTO_SCHEMA_PROVIDER_PREFIX + "class.name")
      .markAdvanced()
      .sinceVersion("0.13.0")
      .withDocumentation("The Protobuf Message class used as the source for the schema.");

  public static final ConfigProperty<Boolean> PROTO_SCHEMA_WRAPPED_PRIMITIVES_AS_RECORDS = ConfigProperty
      .key(PROTO_SCHEMA_PROVIDER_PREFIX + "flatten.wrappers")
      .defaultValue(false)
      .withAlternatives(OLD_PROTO_SCHEMA_PROVIDER_PREFIX + "flatten.wrappers")
      .markAdvanced()
      .sinceVersion("0.13.0")
      .withDocumentation("When set to true wrapped primitives like Int64Value are translated to a record with a single 'value' field. By default, the value is false and the wrapped primitives are "
          + "treated as a nullable value");

  public static final ConfigProperty<Boolean> PROTO_SCHEMA_TIMESTAMPS_AS_RECORDS = ConfigProperty
      .key(PROTO_SCHEMA_PROVIDER_PREFIX + "timestamps.as.records")
      .defaultValue(false)
      .withAlternatives(OLD_PROTO_SCHEMA_PROVIDER_PREFIX + "timestamps.as.records")
      .markAdvanced()
      .sinceVersion("0.13.0")
      .withDocumentation("When set to true Timestamp fields are translated to a record with a seconds and nanos field. By default, the value is false and the timestamp is converted to a long with "
          + "the timestamp-micros logical type");

  public static final ConfigProperty<Integer> PROTO_SCHEMA_MAX_RECURSION_DEPTH = ConfigProperty
      .key(PROTO_SCHEMA_PROVIDER_PREFIX + "max.recursion.depth")
      .defaultValue(5)
      .withAlternatives(OLD_PROTO_SCHEMA_PROVIDER_PREFIX + "max.recursion.depth")
      .markAdvanced()
      .sinceVersion("0.13.0")
      .withDocumentation("The max depth to unravel the Proto schema when translating into an Avro schema. Setting this depth allows the user to convert a schema that is recursive in proto into "
          + "something that can be represented in their lake format like Parquet. After a given class has been seen N times within a single branch, the schema provider will create a record with a "
          + "byte array to hold the remaining proto data and a string to hold the message descriptor's name for context.");
}
