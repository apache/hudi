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

package org.apache.hudi.utilities.applied.schema;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

import javax.annotation.concurrent.Immutable;

import static org.apache.hudi.common.util.ConfigUtils.SCHEMAPROVIDER_CONFIG_PREFIX;


/**
 * JDBC-based Schema Provider Configs.
 */
@Immutable
@ConfigClassProperty(name = "Ursa Proto File Descriptor Based Schema Provider Configs",
    groupName = ConfigGroups.Names.HUDI_STREAMER,
    subGroupName = ConfigGroups.SubGroupNames.SCHEMA_PROVIDER,
    description = "Configurations for Ursa Proto schema provider.")
public class UrsaBatchProtoBasedSchemaProviderConfig extends HoodieConfig {
  private static final String PROTO_SCHEMA_PROVIDER_PREFIX = SCHEMAPROVIDER_CONFIG_PREFIX + "ursa.proto.";


  public static final ConfigProperty<String> PROTO_SCHEMA_BUCKET_REGION = ConfigProperty
      .key(PROTO_SCHEMA_PROVIDER_PREFIX + "file.descriptor.set.s3.bucket.region")
      .noDefaultValue()
      .markAdvanced()
      .sinceVersion("0.15.0")
      .withDocumentation("The S3 region where the bucket which contains the protobuf file descriptor set is present.");

  public static final ConfigProperty<String> PROTO_SCHEMA_BUCKET = ConfigProperty
      .key(PROTO_SCHEMA_PROVIDER_PREFIX + "file.descriptor.set.s3.bucket.name")
      .noDefaultValue()
      .markAdvanced()
      .sinceVersion("0.15.0")
      .withDocumentation("The bucket name where the protobuf file descriptor set is present.");

  public static final ConfigProperty<String> PROTO_SCHEMA_PREFIX = ConfigProperty
      .key(PROTO_SCHEMA_PROVIDER_PREFIX + "file.descriptor.set.s3.prefix")
      .noDefaultValue()
      .markAdvanced()
      .sinceVersion("0.15.0")
      .withDocumentation("The prefix name used to identify the schemas."
          + "For example for prefix : framegen/schemas/prediction_feature_file_descriptor_set, "
          + "the system will list schemas named"
          + "framegen/schemas/prediction_feature_file_descriptor_set.v<version_number>... "
          + "and pick the schema with the highest version (numeric).");

  public static final ConfigProperty<String> PROTO_SCHEMA_MESSAGE_TYPE = ConfigProperty
      .key(PROTO_SCHEMA_PROVIDER_PREFIX + "file.descriptor.set.message.type.name")
      .noDefaultValue()
      .markAdvanced()
      .sinceVersion("0.15.0")
      .withDocumentation("Message Type Name found in the file descriptor set which is used as the main message type");

  public static final ConfigProperty<String> PROTO_SERIALIZED_INPUT_COL = ConfigProperty
          .key(PROTO_SCHEMA_PROVIDER_PREFIX + "proto.serialized.col")
          .noDefaultValue()
          .markAdvanced()
          .sinceVersion("0.15.0")
          .withDocumentation("Column in the input parquet files which has serialized protobufs.");

  public static final ConfigProperty<String> PROTO_DESERIALIZED_PARENT_COL_NAME = ConfigProperty
      .key(PROTO_SCHEMA_PROVIDER_PREFIX + "deserialized.col.parent")
      .noDefaultValue()
      .markAdvanced()
      .sinceVersion("0.15.0")
      .withDocumentation("Parent Column name to be used in the final source frame-gen schema.");
}
