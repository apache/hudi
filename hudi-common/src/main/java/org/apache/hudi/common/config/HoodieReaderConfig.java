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
  public static final ConfigProperty<Boolean> USE_NATIVE_HFILE_READER = ConfigProperty
      .key("_hoodie.hfile.use.native.reader")
      .defaultValue(true)
      .markAdvanced()
      .sinceVersion("1.0.0")
      .withDocumentation("When enabled, the native HFile reader is used to read HFiles.  This is an internal config.");

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
      .defaultValue(true)
      .markAdvanced()
      .sinceVersion("1.0.0")
      .withDocumentation("Use engine agnostic file group reader if enabled");

  public static final ConfigProperty<Boolean> MERGE_USE_RECORD_POSITIONS = ConfigProperty
      .key("hoodie.merge.use.record.positions")
      .defaultValue(true)
      .markAdvanced()
      .sinceVersion("1.0.0")
      .withDocumentation("Whether to use positions in the block header for data blocks containing updates and delete blocks for merging.");

  public static final String REALTIME_SKIP_MERGE = "skip_merge";
  public static final String REALTIME_PAYLOAD_COMBINE = "payload_combine";
  public static final ConfigProperty<String> MERGE_TYPE = ConfigProperty
      .key("hoodie.datasource.merge.type")
      .defaultValue(REALTIME_PAYLOAD_COMBINE)
      .markAdvanced()
      .withValidValues(REALTIME_PAYLOAD_COMBINE, REALTIME_SKIP_MERGE)
      .withDocumentation("For Snapshot query on merge on read table. Use this key to define how the payloads are merged, in\n"
          + "1) skip_merge: read the base file records plus the log file records without merging;\n"
          + "2) payload_combine: read the base file records first, for each record in base file, checks whether the key is in the\n"
          + "   log file records (combines the two records with same key for base and log file records), then read the left log file records");

  public static final String RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY =
      "hoodie.write.record.merge.custom.implementation.classes";
  public static final String RECORD_MERGE_IMPL_CLASSES_DEPRECATED_WRITE_CONFIG_KEY =
      "hoodie.datasource.write.record.merger.impls";

  public static final ConfigProperty<Boolean> HFILE_BLOCK_CACHE_ENABLED = ConfigProperty
      .key("hoodie.hfile.block.cache.enabled")
      .defaultValue(true)
      .markAdvanced()
      .sinceVersion("1.1.0")
      .withDocumentation("Enable HFile block-level caching for metadata files. This caches frequently "
          + "accessed HFile blocks in memory to reduce I/O operations during metadata queries. "
          + "Improves performance for workloads with repeated metadata access patterns.");

  public static final ConfigProperty<Integer> HFILE_BLOCK_CACHE_SIZE = ConfigProperty
      .key("hoodie.hfile.block.cache.size")
      .defaultValue(100)
      .markAdvanced()
      .sinceVersion("1.1.0")
      .withDocumentation("Maximum number of HFile blocks to cache in memory per metadata file reader. "
          + "Higher values improve cache hit rates but consume more memory. "
          + "Only effective when hfile.block.cache.enabled is true.");

  public static final ConfigProperty<Integer> HFILE_BLOCK_CACHE_TTL_MINUTES = ConfigProperty
      .key("hoodie.hfile.block.cache.ttl.minutes")
      .defaultValue(60)
      .markAdvanced()
      .sinceVersion("1.1.0")
      .withDocumentation("Time-to-live (TTL) in minutes for cached HFile blocks. Blocks are evicted "
          + "from the cache after this duration to prevent memory leaks. "
          + "Only effective when hfile.block.cache.enabled is true.");

}
