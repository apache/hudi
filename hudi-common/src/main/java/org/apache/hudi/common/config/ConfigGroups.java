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

package org.apache.hudi.common.config;

/**
 * In Hudi, we have multiple superclasses, aka Config Classes of {@link HoodieConfig} that maintain
 * several configs. This class group one or more of these superclasses into higher
 * level groups, such as Spark Config, Flink Configs, Metrics ....
 * This class maintains the human readable name and description of each config group.
 */
public class ConfigGroups {
  public enum Names {
    SPARK_DATASOURCE("Spark Datasource Configs"),
    FLINK_SQL("Flink Sql Configs"),
    WRITE_CLIENT("Write Client Configs"),
    METRICS("Metrics Configs"),
    RECORD_PAYLOAD("Record Payload Config"),
    KAFKA_CONNECT("Kafka Connect Configs"),
    AWS("Amazon Web Services Configs");

    public final String name;

    Names(String name) {
      this.name = name;
    }
  }

  public static String getDescription(Names names) {
    String description;
    switch (names) {
      case SPARK_DATASOURCE:
        description =  "These configs control the Hudi Spark Datasource, "
            + "providing ability to define keys/partitioning, pick out the write operation, "
            + "specify how to merge records or choosing query type to read.";
        break;
      case FLINK_SQL:
        description = "These configs control the Hudi Flink SQL source/sink connectors, "
            + "providing ability to define record keys, pick out the write operation, "
            + "specify how to merge records, enable/disable asynchronous compaction "
            + "or choosing query type to read.";
        break;
      case WRITE_CLIENT:
        description = "Internally, the Hudi datasource uses a RDD based HoodieWriteClient API "
            + "to actually perform writes to storage. These configs provide deep control over "
            + "lower level aspects like file sizing, compression, parallelism, compaction, "
            + "write schema, cleaning etc. Although Hudi provides sane defaults, from time-time "
            + "these configs may need to be tweaked to optimize for specific workloads.";
        break;
      case RECORD_PAYLOAD:
        description =  "This is the lowest level of customization offered by Hudi. "
            + "Record payloads define how to produce new values to upsert based on incoming "
            + "new record and stored old record. Hudi provides default implementations such as "
            + "OverwriteWithLatestAvroPayload which simply update table with the latest/last-written record. "
            + "This can be overridden to a custom class extending HoodieRecordPayload class, "
            + "on both datasource and WriteClient levels.";
        break;
      case METRICS:
        description = "These set of configs are used to enable monitoring and reporting of key"
            + "Hudi stats and metrics.";
        break;
      case KAFKA_CONNECT:
        description = "These set of configs are used for Kafka Connect Sink Connector for writing Hudi Tables";
        break;
      default:
        description = "Please fill in the description for Config Group Name: " + names.name;
        break;
    }
    return description;
  }
}
