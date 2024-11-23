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

package org.apache.hudi.config.internal;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.config.HoodieIndexConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.Immutable;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

@Immutable
@ConfigClassProperty(name = "Write Configurations",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    description = "Configurations that control write behavior on Hudi tables. These can be directly passed down from even "
        + "higher level frameworks (e.g Spark datasources, Flink sink) and utilities (e.g Hudi Streamer).")

public class OnehouseInternalConfig extends HoodieConfig {

  private static final Logger LOG = LoggerFactory.getLogger(OnehouseInternalConfig.class);
  private static final long serialVersionUID = 0L;
  private static final String ONEHOUSE_INTERNAL_CONFIG_PREFIX = "onehouse.internal.";

  public static final ConfigProperty<Integer> MARKER_NUM_FILE_ENTRIES_TO_PRINT = ConfigProperty
      .key(ONEHOUSE_INTERNAL_CONFIG_PREFIX + "hoodie.marker.num.file.entries.to.print")
      .defaultValue(500)
      .markAdvanced()
      .sinceVersion("0.14.1")
      .withDocumentation("Controls the number of marker entries to print when we fetch all markers during reconciliation or commit metadata validation");

  public static final ConfigProperty<String> RECORD_INDEX_VALIDATE_AGAINST_FILES_PARTITION_ON_WRITES = ConfigProperty
      .key(ONEHOUSE_INTERNAL_CONFIG_PREFIX + "hoodie.record.index.validate.against.files.partition.on.writes")
      .defaultValue("false")
      .withAlternatives(ONEHOUSE_INTERNAL_CONFIG_PREFIX + "hoodie.record.index.validate.against.files.partition")
      .markAdvanced()
      .sinceVersion("0.14.1")
      .withDocumentation("Validates that all fileIds from RLI records during write are part of the HoodieCommitMetadata");

  public static final ConfigProperty<String> RECORD_INDEX_VALIDATE_AGAINST_FILES_PARTITION_ON_READS = ConfigProperty
      .key(ONEHOUSE_INTERNAL_CONFIG_PREFIX + "hoodie.record.index.validate.against.files.partition.on.reads")
      .defaultValue("false")
      .markAdvanced()
      .sinceVersion("0.14.1")
      .withDocumentation("Validates that all fileIds returned from RLI lookup is part of files partition in MDT");

  public static OnehouseInternalConfig.Builder newBuilder() {
    return new OnehouseInternalConfig.Builder();
  }

  public static class Builder {

    private final OnehouseInternalConfig onehouseInternalConfig = new OnehouseInternalConfig();

    public OnehouseInternalConfig.Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.onehouseInternalConfig.getProps().load(reader);
        return this;
      }
    }

    public OnehouseInternalConfig.Builder fromProperties(Properties props) {
      this.onehouseInternalConfig.getProps().putAll(props);
      return this;
    }

    public OnehouseInternalConfig.Builder withNumFileEntriesToPrintForMarkers(int numFileEntriesToPrintForMarkers) {
      this.onehouseInternalConfig.setValue(MARKER_NUM_FILE_ENTRIES_TO_PRINT, String.valueOf(numFileEntriesToPrintForMarkers));
      return this;
    }

    public OnehouseInternalConfig.Builder withRecordIndexValidateAgainstFilesPartitionsOnWrites(boolean validateAgainstFilesPartitions) {
      this.onehouseInternalConfig.setValue(RECORD_INDEX_VALIDATE_AGAINST_FILES_PARTITION_ON_WRITES, String.valueOf(validateAgainstFilesPartitions));
      return this;
    }

    public OnehouseInternalConfig.Builder withRecordIndexValidateAgainstFilesPartitionsOnReads(boolean validateAgainstFilesPartitionsOnReads) {
      this.onehouseInternalConfig.setValue(RECORD_INDEX_VALIDATE_AGAINST_FILES_PARTITION_ON_READS, String.valueOf(validateAgainstFilesPartitionsOnReads));
      return this;
    }

    public OnehouseInternalConfig build() {
      onehouseInternalConfig.setDefaults(HoodieIndexConfig.class.getName());
      return onehouseInternalConfig;
    }
  }
}
