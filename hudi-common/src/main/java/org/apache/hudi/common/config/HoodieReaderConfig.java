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

package org.apache.hudi.common.config;

import javax.annotation.concurrent.Immutable;

/**
 * Configurations for reading a file group
 */
@Immutable
@ConfigClassProperty(name = "Reader Configs",
    groupName = ConfigGroups.Names.READER,
    description = "Configurations that control file group reading.")
public class HoodieReaderConfig extends HoodieConfig {
  public static final ConfigProperty<String> COMPACTION_LAZY_BLOCK_READ_ENABLE = ConfigProperty
      .key("hoodie.compaction.lazy.block.read")
      .defaultValue("true")
      .markAdvanced()
      .withDocumentation("When merging the delta log files, this config helps to choose whether the log blocks "
          + "should be read lazily or not. Choose true to use lazy block reading (low memory usage, but incurs seeks to each block"
          + " header) or false for immediate block read (higher memory usage)");

  public static final ConfigProperty<String> COMPACTION_REVERSE_LOG_READ_ENABLE = ConfigProperty
      .key("hoodie.compaction.reverse.log.read")
      .defaultValue("false")
      .markAdvanced()
      .withDocumentation("HoodieLogFormatReader reads a logfile in the forward direction starting from pos=0 to pos=file_length. "
          + "If this config is set to true, the reader reads the logfile in reverse direction, from pos=file_length to pos=0");

  public static final ConfigProperty<String> ENABLE_OPTIMIZED_LOG_BLOCKS_SCAN = ConfigProperty
      .key("hoodie" + HoodieMetadataConfig.OPTIMIZED_LOG_BLOCKS_SCAN)
      .defaultValue("false")
      .markAdvanced()
      .sinceVersion("0.13.0")
      .withDocumentation("New optimized scan for log blocks that handles all multi-writer use-cases while appending to log files. "
          + "It also differentiates original blocks written by ingestion writers and compacted blocks written log compaction.");

  public static final ConfigProperty<Boolean> FILE_GROUP_READER_ENABLED = ConfigProperty
      .key("hoodie.file.group.reader.enabled")
      .defaultValue(false)
      .markAdvanced()
      .sinceVersion("1.0.0")
      .withDocumentation("Use engine agnostic file group reader if enabled");

  public static final ConfigProperty<Boolean> MERGE_USE_RECORD_POSITIONS = ConfigProperty
      .key("hoodie.merge.use.record.positions")
      .defaultValue(false)
      .markAdvanced()
      .sinceVersion("1.0.0")
      .withDocumentation("Whether to use positions in the block header for data blocks containing updates and delete blocks for merging.");
}
