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
 * File-based Schema Provider Configs.
 */
@Immutable
@ConfigClassProperty(name = "File-based Schema Provider Configs",
    groupName = ConfigGroups.Names.HUDI_STREAMER,
    subGroupName = ConfigGroups.SubGroupNames.SCHEMA_PROVIDER,
    description = "Configurations for file-based schema provider.")
public class FilebasedSchemaProviderConfig extends HoodieConfig {
  public static final ConfigProperty<String> SOURCE_SCHEMA_FILE = ConfigProperty
      .key(SCHEMAPROVIDER_CONFIG_PREFIX + "source.schema.file")
      .noDefaultValue()
      .withAlternatives(OLD_SCHEMAPROVIDER_CONFIG_PREFIX + "source.schema.file")
      .withDocumentation("The schema of the source you are reading from");

  public static final ConfigProperty<String> TARGET_SCHEMA_FILE = ConfigProperty
      .key(SCHEMAPROVIDER_CONFIG_PREFIX + "target.schema.file")
      .noDefaultValue()
      .withAlternatives(OLD_SCHEMAPROVIDER_CONFIG_PREFIX + "target.schema.file")
      .withDocumentation("The schema of the target you are writing to");
}
