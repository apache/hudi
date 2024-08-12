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
 * Hive Schema Provider Configs.
 */
@Immutable
@ConfigClassProperty(name = "Hive Schema Provider Configs",
    groupName = ConfigGroups.Names.HUDI_STREAMER,
    subGroupName = ConfigGroups.SubGroupNames.SCHEMA_PROVIDER,
    description = "Configurations for Hive schema provider.")
public class HiveSchemaProviderConfig extends HoodieConfig {
  public static final ConfigProperty<String> SOURCE_SCHEMA_DATABASE = ConfigProperty
      .key(SCHEMAPROVIDER_CONFIG_PREFIX + "source.schema.hive.database")
      .defaultValue("default")
      .withAlternatives(OLD_SCHEMAPROVIDER_CONFIG_PREFIX + "source.schema.hive.database")
      .markAdvanced()
      .withDocumentation("Hive database from where source schema can be fetched");

  public static final ConfigProperty<String> SOURCE_SCHEMA_TABLE = ConfigProperty
      .key(SCHEMAPROVIDER_CONFIG_PREFIX + "source.schema.hive.table")
      .noDefaultValue()
      .withAlternatives(OLD_SCHEMAPROVIDER_CONFIG_PREFIX + "source.schema.hive.table")
      .markAdvanced()
      .withDocumentation("Hive table from where source schema can be fetched");

  public static final ConfigProperty<String> TARGET_SCHEMA_DATABASE = ConfigProperty
      .key(SCHEMAPROVIDER_CONFIG_PREFIX + "target.schema.hive.database")
      .defaultValue("default")
      .withAlternatives(OLD_SCHEMAPROVIDER_CONFIG_PREFIX + "target.schema.hive.database")
      .markAdvanced()
      .withDocumentation("Hive database from where target schema can be fetched");

  public static final ConfigProperty<String> TARGET_SCHEMA_TABLE = ConfigProperty
      .key(SCHEMAPROVIDER_CONFIG_PREFIX + "target.schema.hive.table")
      .noDefaultValue()
      .withAlternatives(OLD_SCHEMAPROVIDER_CONFIG_PREFIX + "target.schema.hive.table")
      .markAdvanced()
      .withDocumentation("Hive table from where target schema can be fetched");
}
