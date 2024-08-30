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

import javax.annotation.concurrent.Immutable;

import java.util.Arrays;

@Immutable
@ConfigClassProperty(name = "Error table Configs",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    description = "Configurations that are required for Error table configs")
public class HoodieErrorTableConfig extends HoodieConfig {
  public static final ConfigProperty<Boolean> ERROR_TABLE_ENABLED = ConfigProperty
      .key("hoodie.errortable.enable")
      .defaultValue(false)
      .withDocumentation("Config to enable error table. If the config is enabled, "
          + "all the records with processing error in DeltaStreamer are transferred to error table.");

  public static final ConfigProperty<String> ERROR_TABLE_BASE_PATH = ConfigProperty
      .key("hoodie.errortable.base.path")
      .noDefaultValue()
      .withDocumentation("Base path for error table under which all error records "
          + "would be stored.");

  public static final ConfigProperty<String> ERROR_TARGET_TABLE = ConfigProperty
      .key("hoodie.errortable.target.table.name")
      .noDefaultValue()
      .withDocumentation("Table name to be used for the error table");

  public static final ConfigProperty<Integer> ERROR_TABLE_UPSERT_PARALLELISM_VALUE = ConfigProperty
      .key("hoodie.errortable.upsert.shuffle.parallelism")
      .defaultValue(200)
      .withDocumentation("Config to set upsert shuffle parallelism. The config is similar to "
          + "hoodie.upsert.shuffle.parallelism config but applies to the error table.");

  public static final ConfigProperty<Integer> ERROR_TABLE_INSERT_PARALLELISM_VALUE = ConfigProperty
      .key("hoodie.errortable.insert.shuffle.parallelism")
      .defaultValue(200)
      .withDocumentation("Config to set insert shuffle parallelism. The config is similar to "
          + "hoodie.insert.shuffle.parallelism config but applies to the error table.");

  public static final ConfigProperty<String> ERROR_TABLE_WRITE_CLASS = ConfigProperty
      .key("hoodie.errortable.write.class")
      .noDefaultValue()
      .withDocumentation("Class which handles the error table writes. This config is used to configure "
          + "a custom implementation for Error Table Writer. Specify the full class name of the custom "
          + "error table writer as a value for this config");

  public static final ConfigProperty<Boolean> ERROR_ENABLE_VALIDATE_TARGET_SCHEMA = ConfigProperty
      .key("hoodie.errortable.validate.targetschema.enable")
      .defaultValue(false)
      .withDocumentation("Records with schema mismatch with Target Schema are sent to Error Table.");

  public static final ConfigProperty<Boolean> ERROR_ENABLE_VALIDATE_RECORD_CREATION = ConfigProperty
      .key("hoodie.errortable.validate.recordcreation.enable")
      .defaultValue(true)
      .sinceVersion("0.15.0")
      .withDocumentation("Records that fail to be created due to keygeneration failure or other issues will be sent to the Error Table");

  public static final ConfigProperty<String> ERROR_TABLE_WRITE_FAILURE_STRATEGY = ConfigProperty
      .key("hoodie.errortable.write.failure.strategy")
      .defaultValue(ErrorWriteFailureStrategy.ROLLBACK_COMMIT.name())
      .withDocumentation("The config specifies the failure strategy if error table write fails. "
          + "Use one of - " + Arrays.toString(ErrorWriteFailureStrategy.values()));

  public enum ErrorWriteFailureStrategy {
    ROLLBACK_COMMIT("Rollback the corresponding base table write commit for which the error events were triggered"),
    LOG_ERROR("Error is logged but the base table write succeeds");

    private final String description;

    ErrorWriteFailureStrategy(String description) {
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
