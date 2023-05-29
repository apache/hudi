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
 */

package org.apache.hudi.config;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

/**
 * Hoodie Configs for Glue.
 */
@ConfigClassProperty(name = "Glue catalog sync based client Configurations",
    groupName = ConfigGroups.Names.META_SYNC,
    subGroupName = ConfigGroups.SubGroupNames.NONE,
    description = "Configs that control Glue catalog sync based client.")
public class GlueCatalogSyncClientConfig extends HoodieConfig {
  public static final String GLUE_CLIENT_PROPERTY_PREFIX = "hoodie.datasource.meta.sync.glue.";

  public static final ConfigProperty<Boolean> GLUE_SKIP_TABLE_ARCHIVE = ConfigProperty
      .key(GLUE_CLIENT_PROPERTY_PREFIX + "skip_table_archive")
      .defaultValue(true)
      .markAdvanced()
      .sinceVersion("0.14.0")
      .withDocumentation("Glue catalog sync based client will skip archiving the table version if this config is set to true");
}
