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

import java.util.Arrays;

@Immutable
@ConfigClassProperty(name = "Quarantine table Configs",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    description = "Configurations that are required for Quarantine table configs")
public class HoodieQuarantineTableConfig {
  public static final ConfigProperty<Boolean> QUARANTINE_TABLE_ENABLED = ConfigProperty
      .key("hoodie.deltastreamer.quarantinetable.enable")
      .defaultValue(false)
      .withDocumentation("Config to enable quarantine table. If the config is enabled, " +
          "all the records with processing error in DeltaStreamer are transferred to quarantine table.");

  public static final ConfigProperty<String> QUARANTINE_TABLE_BASE_PATH = ConfigProperty
      .key("hoodie.deltastreamer.quarantinetable.base.path")
      .noDefaultValue()
      .withDocumentation("Base path for quarantine table under which all quarantine records " +
          "would be stored.");

  public static final ConfigProperty<String> QUARANTINE_TARGET_TABLE = ConfigProperty
      .key("hoodie.deltastreamer.quarantinetable.target.table.name")
      .noDefaultValue()
      .withDocumentation("Table name to be used for the quarantine table");

  public static final ConfigProperty<Integer> QUARANTINE_TABLE_UPSERT_PARALLELISM_VALUE = ConfigProperty
      .key("hoodie.deltastreamer.quarantinetable.upsert.shuffle.parallelism")
      .defaultValue(200)
      .withDocumentation("Config to set upsert shuffle parallelism. The config is similar to " +
          "hoodie.upsert.shuffle.parallelism config but applies to the quarantine table.");

  public static final ConfigProperty<Integer> QUARANTINE_TABLE_INSERT_PARALLELISM_VALUE = ConfigProperty
      .key("hoodie.deltastreamer.quarantinetable.insert.shuffle.parallelism")
      .defaultValue(200)
      .withDocumentation("Config to set insert shuffle parallelism. The config is similar to " +
          "hoodie.insert.shuffle.parallelism config but applies to the quarantine table.");

  public static final ConfigProperty<String> QUARANTINE_TABLE_WRITE_CLASS = ConfigProperty
      .key("hoodie.deltastreamer.quarantinetable.write.class")
      .noDefaultValue()
      .withDocumentation("Class which handles the quarantine table writes. This config is used to configure "
          + "a custom implementation for Quarantine Table Writer. Specify the full class name of the custom "
          + "quarantine table writer as a value for this config");

  public static final ConfigProperty<Boolean> QUARANTINE_ENABLE_VALIDATE_TARGET_SCHEMA = ConfigProperty
      .key("hoodie.deltastreamer.quarantinetable.validate.targetschema.enable")
      .defaultValue(false)
      .withDocumentation("Records with schema mismatch with Target Schema are sent to Quarantine Table.");

  public static final ConfigProperty<String> QUARANTINE_TABLE_WRITE_FAILURE_STRATEGY = ConfigProperty
      .key("hoodie.deltastreamer.quarantinetable.write.failure.strategy")
      .defaultValue(QuarantineWriteFailureStrategy.ROLLBACK_COMMIT.name())
      .withDocumentation("The config specifies the failure strategy if quarantine table write fails. "
          + "Use one of - " + Arrays.toString(QuarantineWriteFailureStrategy.values()));

  public enum QuarantineWriteFailureStrategy {
    ROLLBACK_COMMIT("Rollback the corresponding base table write commit for which the error events were triggered"),
    LOG_ERROR("Error is logged but the base table write succeeds");

    private final String description;

    QuarantineWriteFailureStrategy(String description) {
      this.description = description;
    }

    public String getDescription() {
      return description;
    }

    @Override
    public String toString() {
      return super.toString() + " (" + description + ")\n";
    }
  }
}
