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
  /**
   * Config group names. Please add the description of each group in
   * {@link ConfigGroups#getDescription}.
   */
  public enum Names {
    TABLE_CONFIG("Hudi Table Config"),
    ENVIRONMENT_CONFIG("Environment Config"),
    SPARK_DATASOURCE("Spark Datasource Configs"),
    FLINK_SQL("Flink Sql Configs"),
    WRITE_CLIENT("Write Client Configs"),
    READER("Reader Configs"),
    META_SYNC("Metastore and Catalog Sync Configs"),
    METRICS("Metrics Configs"),
    RECORD_PAYLOAD("Record Payload Config"),
    KAFKA_CONNECT("Kafka Connect Configs"),
    AWS("Amazon Web Services Configs"),
    HUDI_STREAMER("Hudi Streamer Configs");

    public final String name;

    Names(String name) {
      this.name = name;
    }
  }

  public enum SubGroupNames {
    INDEX(
        "Index Configs",
        "Configurations that control indexing behavior, "
            + "which tags incoming records as either inserts or updates to older records."),
    KEY_GENERATOR(
        "Key Generator Configs",
        "Hudi maintains keys (record key + partition path) for uniquely identifying a "
            + "particular record. These configs allow developers to setup the Key generator class "
            + "that extracts these out of incoming records."),
    LOCK(
        "Lock Configs",
        "Configurations that control locking mechanisms required for concurrency control "
            + " between writers to a Hudi table. Concurrency between Hudi's own table services "
            + " are auto managed internally."),
    COMMIT_CALLBACK(
        "Commit Callback Configs",
        "Configurations controlling callback behavior into HTTP endpoints, to push "
            + "notifications on commits on hudi tables."),
    SCHEMA_PROVIDER(
        "Hudi Streamer Schema Provider Configs",
        "Configurations that control the schema provider for Hudi Streamer."),
    DELTA_STREAMER_SOURCE(
        "Hudi Streamer Source Configs",
        "Configurations controlling the behavior of reading source data."),
    NONE(
        "None",
        "No subgroup. This description should be hidden.");

    public final String name;
    private final String description;

    SubGroupNames(String name, String description) {
      this.name = name;
      this.description = description;
    }

    public String getDescription() {
      return description;
    }
  }

  public static String getDescription(Names names) {
    String description;
    switch (names) {
      case TABLE_CONFIG:
        description = "Basic Hudi Table configuration parameters.";
        break;
      case ENVIRONMENT_CONFIG:
        description = "Hudi supports passing configurations via a configuration file "
            + "`hudi-default.conf` in which each line consists of a key and a value "
            + "separated by whitespace or = sign. For example:\n"
            + "```\n"
            + "hoodie.datasource.hive_sync.mode               jdbc\n"
            + "hoodie.datasource.hive_sync.jdbcurl            jdbc:hive2://localhost:10000\n"
            + "hoodie.datasource.hive_sync.support_timestamp  false\n"
            + "```\n"
            + "It helps to have a central configuration file for your common cross "
            + "job configurations/tunings, so all the jobs on your cluster can utilize it. "
            + "It also works with Spark SQL DML/DDL, and helps avoid having to pass configs "
            + "inside the SQL statements.\n\n"
            + "By default, Hudi would load the configuration file under `/etc/hudi/conf` "
            + "directory. You can specify a different configuration directory location by "
            + "setting the `HUDI_CONF_DIR` environment variable.";
        break;
      case SPARK_DATASOURCE:
        description = "These configs control the Hudi Spark Datasource, "
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
      case META_SYNC:
        description = "Configurations used by the Hudi to sync metadata to external metastores and catalogs.";
        break;
      case RECORD_PAYLOAD:
        description = "This is the lowest level of customization offered by Hudi. "
            + "Record payloads define how to produce new values to upsert based on incoming "
            + "new record and stored old record. Hudi provides default implementations such as "
            + "OverwriteWithLatestAvroPayload which simply update table with the latest/last-written record. "
            + "This can be overridden to a custom class extending HoodieRecordPayload class, "
            + "on both datasource and WriteClient levels.";
        break;
      case METRICS:
        description = "These set of configs are used to enable monitoring and reporting of key "
            + "Hudi stats and metrics.";
        break;
      case KAFKA_CONNECT:
        description = "These set of configs are used for Kafka Connect Sink Connector for writing Hudi Tables";
        break;
      case AWS:
        description = "Configurations specific to Amazon Web Services.";
        break;
      case HUDI_STREAMER:
        description = "These set of configs are used for Hudi Streamer utility which provides "
            + "the way to ingest from different sources such as DFS or Kafka.";
        break;
      default:
        description = "Please fill in the description for Config Group Name: " + names.name;
        break;
    }
    return description;
  }
}
