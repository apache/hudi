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

import static org.apache.hudi.utilities.config.HoodieStreamerConfig.DELTA_STREAMER_CONFIG_PREFIX;

/**
 * Hudi Streamer Schema Provider related config.
 */
@Immutable
@ConfigClassProperty(name = "Hudi Streamer Schema Provider Configs",
    groupName = ConfigGroups.Names.HUDI_STREAMER,
    subGroupName = ConfigGroups.SubGroupNames.SCHEMA_PROVIDER,
    areCommonConfigs = true,
    description = "")
public class HoodieSchemaProviderConfig extends HoodieConfig {
  public static final String SCHEMAPROVIDER_CONFIG_PREFIX = DELTA_STREAMER_CONFIG_PREFIX + "schemaprovider.";

  public static final ConfigProperty<String> SRC_SCHEMA_REGISTRY_URL = ConfigProperty
      .key(SCHEMAPROVIDER_CONFIG_PREFIX + "registry.url")
      .noDefaultValue()
      .withDocumentation("The schema of the source you are reading from e.g. https://foo:bar@schemaregistry.org");

  public static final ConfigProperty<String> TARGET_SCHEMA_REGISTRY_URL = ConfigProperty
      .key(SCHEMAPROVIDER_CONFIG_PREFIX + "registry.targetUrl")
      .noDefaultValue()
      .withDocumentation("The schema of the target you are writing to e.g. https://foo:bar@schemaregistry.org");

  public static final ConfigProperty<String> SCHEMA_CONVERTER = ConfigProperty
      .key(SCHEMAPROVIDER_CONFIG_PREFIX + "registry.schemaconverter")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("The class name of the custom schema converter to use.");

  public static final ConfigProperty<Boolean> SPARK_AVRO_POST_PROCESSOR_ENABLE = ConfigProperty
      .key(SCHEMAPROVIDER_CONFIG_PREFIX + "spark_avro_post_processor.enable")
      .defaultValue(false)
      .markAdvanced()
      .withDocumentation("Whether to enable Spark Avro post processor.");

  public static final ConfigProperty<String> SCHEMA_REGISTRY_BASE_URL = ConfigProperty
      .key(SCHEMAPROVIDER_CONFIG_PREFIX + "registry.baseUrl")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("The base URL of the schema registry.");

  public static final ConfigProperty<String> SCHEMA_REGISTRY_URL_SUFFIX = ConfigProperty
      .key(SCHEMAPROVIDER_CONFIG_PREFIX + "registry.urlSuffix")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("The suffix of the URL for the schema registry.");

  public static final ConfigProperty<String> SCHEMA_REGISTRY_SOURCE_URL_SUFFIX = ConfigProperty
      .key(SCHEMAPROVIDER_CONFIG_PREFIX + "registry.sourceUrlSuffix")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("The source URL suffix.");

  public static final ConfigProperty<String> SCHEMA_REGISTRY_TARGET_URL_SUFFIX = ConfigProperty
      .key(SCHEMAPROVIDER_CONFIG_PREFIX + "registry.targetUrlSuffix")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("The target URL suffix.");
}
