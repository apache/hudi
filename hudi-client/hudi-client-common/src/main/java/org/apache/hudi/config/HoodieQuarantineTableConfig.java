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

import javax.annotation.concurrent.Immutable;

@Immutable
@ConfigClassProperty(name = "Quarantine table Configs",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    description = "Configurations that are required for Quarantine table configs")
public class HoodieQuarantineTableConfig {
  public static final ConfigProperty<Boolean> QUARANTINE_TABLE_ENABLED = ConfigProperty
      .key("hoodie.deltastreamer.quarantinetable.enable")
      .defaultValue(false)
      .withDocumentation("config to enable quarantine table");

  public static final ConfigProperty<String> QUARANTINE_TABLE_BASE_PATH = ConfigProperty
      .key("hoodie.deltastreamer.quarantinetable.base.path")
      .noDefaultValue()
      .withDocumentation("base path for quarantine table");

  public static final ConfigProperty<String> QUARANTINE_TARGET_TABLE = ConfigProperty
      .key("hoodie.deltastreamer.quarantinetable.target.table.name")
      .noDefaultValue()
      .withDocumentation("target table name for quarantine table");

  public static final ConfigProperty<Integer> QUARANTINE_TABLE_UPSERT_PARALLELISM_VALUE = ConfigProperty
      .key("hoodie.deltastreamer.quarantinetable.upsert.shuffle.parallelism")
      .defaultValue(200)
      .withDocumentation("config to set upsert shuffle parallelism");

  public static final ConfigProperty<Integer> QUARANTINE_TABLE_INSERT_PARALLELISM_VALUE = ConfigProperty
      .key("hoodie.deltastreamer.quarantinetable.insert.shuffle.parallelism")
      .defaultValue(200)
      .withDocumentation("config to set insert shuffle parallelism");

  public static final ConfigProperty<String> QUARANTINE_TABLE_WRITER_CLASS = ConfigProperty
      .key("hoodie.deltastreamer.quarantinetable.writer.class")
      .noDefaultValue()
      .withDocumentation("Class which handles the quarantine table writes");
}
