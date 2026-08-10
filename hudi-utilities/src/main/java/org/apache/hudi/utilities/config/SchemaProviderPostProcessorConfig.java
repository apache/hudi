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
 * Configurations for Schema Post Processor.
 */
@Immutable
@ConfigClassProperty(name = "Schema Post Processor Config Configs",
    groupName = ConfigGroups.Names.HUDI_STREAMER,
    subGroupName = ConfigGroups.SubGroupNames.SCHEMA_PROVIDER,
    description = "Configurations for Schema Post Processor")
public class SchemaProviderPostProcessorConfig extends HoodieConfig {

  private static final String PREFIX = SCHEMAPROVIDER_CONFIG_PREFIX + "schema_post_processor.";
  private static final String OLD_PREFIX = OLD_SCHEMAPROVIDER_CONFIG_PREFIX + "schema_post_processor.";

  public static final ConfigProperty<String> SCHEMA_POST_PROCESSOR = ConfigProperty
      .key(SCHEMAPROVIDER_CONFIG_PREFIX + "schema_post_processor")
      .noDefaultValue()
      .withAlternatives(OLD_SCHEMAPROVIDER_CONFIG_PREFIX + "schema_post_processor")
      .markAdvanced()
      .withDocumentation("The class name of the schema post processor.");

  public static final ConfigProperty<String> DELETE_COLUMN_POST_PROCESSOR_COLUMN = ConfigProperty
      .key(PREFIX + "delete.columns")
      .noDefaultValue()
      .withAlternatives(OLD_PREFIX + "delete.columns")
      .markAdvanced()
      .withDocumentation("Columns to delete in the schema post processor.");

  public static final ConfigProperty<String> SCHEMA_POST_PROCESSOR_ADD_COLUMN_NAME_PROP = ConfigProperty
      .key(PREFIX + "add.column.name")
      .noDefaultValue()
      .withAlternatives(OLD_PREFIX + "add.column.name")
      .markAdvanced()
      .withDocumentation("New column's name");

  public static final ConfigProperty<String> SCHEMA_POST_PROCESSOR_ADD_COLUMN_TYPE_PROP = ConfigProperty
      .key(PREFIX + "add.column.type")
      .noDefaultValue()
      .withAlternatives(OLD_PREFIX + "add.column.type")
      .markAdvanced()
      .withDocumentation("New column's type");

  public static final ConfigProperty<Boolean> SCHEMA_POST_PROCESSOR_ADD_COLUMN_NULLABLE_PROP = ConfigProperty
      .key(PREFIX + "add.column.nullable")
      .defaultValue(true)
      .withAlternatives(OLD_PREFIX + "add.column.nullable")
      .markAdvanced()
      .withDocumentation("New column's nullable");

  public static final ConfigProperty<String> SCHEMA_POST_PROCESSOR_ADD_COLUMN_DEFAULT_PROP = ConfigProperty
      .key(PREFIX + "add.column.default")
      .noDefaultValue()
      .withAlternatives(OLD_PREFIX + "add.column.default")
      .markAdvanced()
      .withDocumentation("New column's default value");

  public static final ConfigProperty<String> SCHEMA_POST_PROCESSOR_ADD_COLUMN_DOC_PROP = ConfigProperty
      .key(PREFIX + "add.column.doc")
      .noDefaultValue()
      .withAlternatives(OLD_PREFIX + "add.column.doc")
      .markAdvanced()
      .withDocumentation("Docs about new column");

}
