/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.configuration;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;

/**
 * Flink sql read config.
 */
@ConfigClassProperty(name = "Flink Read Options",
    groupName = ConfigGroups.Names.FLINK_SQL,
    description = "Flink jobs using the SQL can be configured through the options in WITH clause."
        + "Configurations that control flink read behavior on Hudi tables are listed below.")
public class FlinkReadOptions {

  public static final ConfigOption<Integer> READ_TASKS = ConfigOptions
      .key("read.tasks")
      .intType()
      .defaultValue(4)
      .withDescription("Parallelism of tasks that do actual read, default is 4");

  public static final ConfigOption<String> SOURCE_AVRO_SCHEMA_PATH = ConfigOptions
       .key("source.avro-schema.path")
       .stringType()
       .noDefaultValue()
       .withDescription("Source avro schema file path, the parsed schema is used for deserialization");

  public static final ConfigOption<String> SOURCE_AVRO_SCHEMA = ConfigOptions
      .key("source.avro-schema")
      .stringType()
      .noDefaultValue()
      .withDescription("Source avro schema string, the parsed schema is used for deserialization");

  public static final String QUERY_TYPE_SNAPSHOT = "snapshot";
  public static final String QUERY_TYPE_READ_OPTIMIZED = "read_optimized";
  public static final String QUERY_TYPE_INCREMENTAL = "incremental";
  public static final ConfigOption<String> QUERY_TYPE = ConfigOptions
      .key("hoodie.datasource.query.type")
      .stringType()
      .defaultValue(QUERY_TYPE_SNAPSHOT)
      .withDescription("Decides how data files need to be read, in\n"
          + "1) Snapshot mode (obtain latest view, based on row & columnar data);\n"
          + "2) incremental mode (new data since an instantTime);\n"
          + "3) Read Optimized mode (obtain latest view, based on columnar data)\n."
          + "Default: snapshot");

  public static final String REALTIME_SKIP_MERGE = "skip_merge";
  public static final String REALTIME_PAYLOAD_COMBINE = "payload_combine";
  public static final ConfigOption<String> MERGE_TYPE = ConfigOptions
      .key("hoodie.datasource.merge.type")
      .stringType()
      .defaultValue(REALTIME_PAYLOAD_COMBINE)
      .withDescription("For Snapshot query on merge on read table. Use this key to define how the payloads are merged, in\n"
          + "1) skip_merge: read the base file records plus the log file records;\n"
          + "2) payload_combine: read the base file records first, for each record in base file, checks whether the key is in the\n"
          + "   log file records(combines the two records with same key for base and log file records), then read the left log file records");

  public static final ConfigOption<Boolean> UTC_TIMEZONE = ConfigOptions
      .key("read.utc-timezone")
      .booleanType()
      .defaultValue(true)
      .withDescription("Use UTC timezone or local timezone to the conversion between epoch"
          + " time and LocalDateTime. Hive 0.x/1.x/2.x use local timezone. But Hive 3.x"
          + " use UTC timezone, by default true");

  public static final ConfigOption<Boolean> READ_AS_STREAMING = ConfigOptions
      .key("read.streaming.enabled")
      .booleanType()
      .defaultValue(false)// default read as batch
      .withDescription("Whether to read as streaming source, default false");

  public static final ConfigOption<Integer> READ_STREAMING_CHECK_INTERVAL = ConfigOptions
      .key("read.streaming.check-interval")
      .intType()
      .defaultValue(60)// default 1 minute
      .withDescription("Check interval for streaming read of SECOND, default 1 minute");

  public static final String START_COMMIT_EARLIEST = "earliest";
  public static final ConfigOption<String> READ_STREAMING_START_COMMIT = ConfigOptions
      .key("read.streaming.start-commit")
      .stringType()
      .noDefaultValue()
      .withDescription("Start commit instant for streaming read, the commit time format should be 'yyyyMMddHHmmss', "
          + "by default reading from the latest instant");
}
