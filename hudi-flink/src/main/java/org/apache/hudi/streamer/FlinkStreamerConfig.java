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

package org.apache.hudi.streamer;

import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.keygen.SimpleAvroKeyGenerator;

import com.beust.jcommander.Parameter;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * Configurations for Hoodie Flink streamer.
 */
public class FlinkStreamerConfig extends Configuration {
  @Parameter(names = {"--kafka-topic"}, description = "Kafka topic name.", required = true)
  public String kafkaTopic;

  @Parameter(names = {"--kafka-group-id"}, description = "Kafka consumer group id.", required = true)
  public String kafkaGroupId;

  @Parameter(names = {"--kafka-bootstrap-servers"}, description = "Kafka bootstrap.servers.", required = true)
  public String kafkaBootstrapServers;

  @Parameter(names = {"--flink-checkpoint-path"}, description = "Flink checkpoint path.")
  public String flinkCheckPointPath;

  @Parameter(names = {"--instant-retry-times"}, description = "Times to retry when latest instant has not completed.")
  public String instantRetryTimes = "10";

  @Parameter(names = {"--instant-retry-interval"}, description = "Seconds between two tries when latest instant has not completed.")
  public String instantRetryInterval = "1";

  @Parameter(names = {"--target-base-path"},
      description = "Base path for the target hoodie table. "
          + "(Will be created if did not exist first time around. If exists, expected to be a hoodie table).",
      required = true)
  public String targetBasePath;

  @Parameter(names = {"--read-schema-path"},
      description = "Avro schema file path, the parsed schema is used for deserializing.",
      required = true)
  public String readSchemaFilePath;

  @Parameter(names = {"--target-table"}, description = "Name of the target table in Hive.", required = true)
  public String targetTableName;

  @Parameter(names = {"--table-type"}, description = "Type of table. COPY_ON_WRITE (or) MERGE_ON_READ.", required = true)
  public String tableType;

  @Parameter(names = {"--props"}, description = "Path to properties file on localfs or dfs, with configurations for "
      + "hoodie client, schema provider, key generator and data source. For hoodie client props, sane defaults are "
      + "used, but recommend use to provide basic things like metrics endpoints, hive configs etc. For sources, refer"
      + "to individual classes, for supported properties.")
  public String propsFilePath = "";

  @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
      + "(using the CLI parameter \"--props\") can also be passed command line using this parameter.")
  public List<String> configs = new ArrayList<>();

  @Parameter(names = {"--record-key-field"}, description = "Record key field. Value to be used as the `recordKey` component of `HoodieKey`.\n"
      + "Actual value will be obtained by invoking .toString() on the field value. Nested fields can be specified using "
      + "the dot notation eg: `a.b.c`. By default `uuid`.")
  public String recordKeyField = "uuid";

  @Parameter(names = {"--partition-path-field"}, description = "Partition path field. Value to be used at \n"
      + "the `partitionPath` component of `HoodieKey`. Actual value obtained by invoking .toString(). By default `partitionpath`.")
  public String partitionPathField = "partitionpath";

  @Parameter(names = {"--keygen-class"}, description = "Key generator class, that implements will extract the key out of incoming record.\n"
      + "By default `SimpleAvroKeyGenerator`.")
  public String keygenClass = SimpleAvroKeyGenerator.class.getName();

  @Parameter(names = {"--source-ordering-field"}, description = "Field within source record to decide how"
      + " to break ties between records with same key in input data. Default: 'ts' holding unix timestamp of record.")
  public String sourceOrderingField = "ts";

  @Parameter(names = {"--payload-class"}, description = "Subclass of HoodieRecordPayload, that works off "
      + "a GenericRecord. Implement your own, if you want to do something other than overwriting existing value.")
  public String payloadClassName = OverwriteWithLatestAvroPayload.class.getName();

  @Parameter(names = {"--op"}, description = "Takes one of these values : UPSERT (default), INSERT (use when input "
      + "is purely new data/inserts to gain speed).", converter = OperationConverter.class)
  public WriteOperationType operation = WriteOperationType.UPSERT;

  @Parameter(names = {"--filter-dupes"},
      description = "Should duplicate records from source be dropped/filtered out before insert/bulk-insert.")
  public Boolean filterDupes = false;

  @Parameter(names = {"--commit-on-errors"}, description = "Commit even when some records failed to be written.")
  public Boolean commitOnErrors = false;

  /**
   * Flink checkpoint interval.
   */
  @Parameter(names = {"--checkpoint-interval"}, description = "Flink checkpoint interval.")
  public Long checkpointInterval = 1000 * 5L;

  @Parameter(names = {"--help", "-h"}, help = true)
  public Boolean help = false;

  @Parameter(names = {"--write-task-num"}, description = "Parallelism of tasks that do actual write, default is 4.")
  public Integer writeTaskNum = 4;
}
