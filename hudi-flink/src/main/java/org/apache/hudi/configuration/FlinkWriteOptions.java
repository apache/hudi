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
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.keygen.constant.KeyGeneratorType;

/**
 * Flink sql write related config.
 */
@ConfigClassProperty(name = "Flink Write Options",
    groupName = ConfigGroups.Names.FLINK_SQL,
    description = "Flink jobs using the SQL can be configured through the options in WITH clause."
        + "Configurations that control flink write behavior on Hudi tables are listed below.")
public class FlinkWriteOptions {

  public static final ConfigOption<String> TABLE_NAME = ConfigOptions
      .key(HoodieWriteConfig.TBL_NAME.key())
      .stringType()
      .noDefaultValue()
      .withDescription("Table name to register to Hive metastore");

  public static final String TABLE_TYPE_COPY_ON_WRITE = HoodieTableType.COPY_ON_WRITE.name();
  public static final String TABLE_TYPE_MERGE_ON_READ = HoodieTableType.MERGE_ON_READ.name();
  public static final ConfigOption<String> TABLE_TYPE = ConfigOptions
      .key("table.type")
      .stringType()
      .defaultValue(TABLE_TYPE_COPY_ON_WRITE)
      .withDescription("Type of table to write. COPY_ON_WRITE (or) MERGE_ON_READ");

  public static final ConfigOption<Boolean> INSERT_DEDUP = ConfigOptions
      .key("write.insert.deduplicate")
      .booleanType()
      .defaultValue(true)
      .withDescription("Whether to deduplicate for INSERT operation, if disabled, writes the base files directly, default true");

  public static final ConfigOption<String> OPERATION = ConfigOptions
      .key("write.operation")
      .stringType()
      .defaultValue("upsert")
      .withDescription("The write operation, that this write should do");

  public static final ConfigOption<String> PRECOMBINE_FIELD = ConfigOptions
      .key("write.precombine.field")
      .stringType()
      .defaultValue("ts")
      .withDescription("Field used in preCombining before actual write. When two records have the same\n"
          + "key value, we will pick the one with the largest value for the precombine field,\n"
          + "determined by Object.compareTo(..)");

  public static final ConfigOption<String> PAYLOAD_CLASS_NAME = ConfigOptions
      .key("write.payload.class")
      .stringType()
      .defaultValue(OverwriteWithLatestAvroPayload.class.getName())
      .withDescription("Payload class used. Override this, if you like to roll your own merge logic, when upserting/inserting.\n"
          + "This will render any value set for the option in-effective");

  /**
   * Flag to indicate whether to drop duplicates upon insert.
   * By default insert will accept duplicates, to gain extra performance.
   */
  public static final ConfigOption<Boolean> INSERT_DROP_DUPS = ConfigOptions
      .key("write.insert.drop.duplicates")
      .booleanType()
      .defaultValue(false)
      .withDescription("Flag to indicate whether to drop duplicates upon insert.\n"
          + "By default insert will accept duplicates, to gain extra performance");

  public static final ConfigOption<Integer> RETRY_TIMES = ConfigOptions
      .key("write.retry.times")
      .intType()
      .defaultValue(3)
      .withDescription("Flag to indicate how many times streaming job should retry for a failed checkpoint batch.\n"
          + "By default 3");

  public static final ConfigOption<Long> RETRY_INTERVAL_MS = ConfigOptions
      .key("write.retry.interval.ms")
      .longType()
      .defaultValue(2000L)
      .withDescription("Flag to indicate how long (by millisecond) before a retry should issued for failed checkpoint batch.\n"
          + "By default 2000 and it will be doubled by every retry");

  public static final ConfigOption<Boolean> IGNORE_FAILED = ConfigOptions
      .key("write.ignore.failed")
      .booleanType()
      .defaultValue(true)
      .withDescription("Flag to indicate whether to ignore any non exception error (e.g. writestatus error). within a checkpoint batch.\n"
          + "By default true (in favor of streaming progressing over data integrity)");

  public static final ConfigOption<String> RECORD_KEY_FIELD = ConfigOptions
      .key(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key())
      .stringType()
      .defaultValue("uuid")
      .withDescription("Record key field. Value to be used as the `recordKey` component of `HoodieKey`.\n"
          + "Actual value will be obtained by invoking .toString() on the field value. Nested fields can be specified using "
          + "the dot notation eg: `a.b.c`");

  public static final ConfigOption<String> PARTITION_PATH_FIELD = ConfigOptions
      .key(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key())
      .stringType()
      .defaultValue("")
      .withDescription("Partition path field. Value to be used at the `partitionPath` component of `HoodieKey`.\n"
          + "Actual value obtained by invoking .toString(), default ''");

  public static final ConfigOption<Boolean> URL_ENCODE_PARTITIONING = ConfigOptions
      .key(KeyGeneratorOptions.URL_ENCODE_PARTITIONING.key())
      .booleanType()
      .defaultValue(false)
      .withDescription("Whether to encode the partition path url, default false");

  public static final ConfigOption<Boolean> HIVE_STYLE_PARTITIONING = ConfigOptions
      .key(KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE.key())
      .booleanType()
      .defaultValue(false)
      .withDescription("Whether to use Hive style partitioning.\n"
          + "If set true, the names of partition folders follow <partition_column_name>=<partition_value> format.\n"
          + "By default false (the names of partition folders are only partition values)");

  public static final ConfigOption<String> KEYGEN_CLASS_NAME = ConfigOptions
      .key(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key())
      .stringType()
      .defaultValue("")
      .withDescription("Key generator class, that implements will extract the key out of incoming record");

  public static final ConfigOption<String> KEYGEN_TYPE = ConfigOptions
      .key(HoodieWriteConfig.KEYGENERATOR_TYPE.key())
      .stringType()
      .defaultValue(KeyGeneratorType.SIMPLE.name())
      .withDescription("Key generator type, that implements will extract the key out of incoming record");

  public static final ConfigOption<Integer> INDEX_BOOTSTRAP_TASKS = ConfigOptions
      .key("write.index_bootstrap.tasks")
      .intType()
      .noDefaultValue()
      .withDescription("Parallelism of tasks that do index bootstrap, default is 4");

  public static final ConfigOption<Integer> BUCKET_ASSIGN_TASKS = ConfigOptions
      .key("write.bucket_assign.tasks")
      .intType()
      .noDefaultValue()
      .withDescription("Parallelism of tasks that do bucket assign, default is 4");

  public static final ConfigOption<Integer> WRITE_TASKS = ConfigOptions
      .key("write.tasks")
      .intType()
      .defaultValue(4)
      .withDescription("Parallelism of tasks that do actual write, default is 4");

  public static final ConfigOption<Double> WRITE_TASK_MAX_SIZE = ConfigOptions
      .key("write.task.max.size")
      .doubleType()
      .defaultValue(1024D) // 1GB
      .withDescription("Maximum memory in MB for a write task, when the threshold hits,\n"
          + "it flushes the max size data bucket to avoid OOM, default 1GB");

  public static final ConfigOption<Long> WRITE_RATE_LIMIT = ConfigOptions
      .key("write.rate.limit")
      .longType()
      .defaultValue(0L) // default no limit
      .withDescription("Write record rate limit per second to prevent traffic jitter and improve stability, default 0 (no limit)");

  public static final ConfigOption<Double> WRITE_BATCH_SIZE = ConfigOptions
      .key("write.batch.size")
      .doubleType()
      .defaultValue(64D) // 64MB
      .withDescription("Batch buffer size in MB to flush data into the underneath filesystem, default 64MB");

  public static final ConfigOption<Integer> WRITE_LOG_BLOCK_SIZE = ConfigOptions
      .key("write.log_block.size")
      .intType()
      .defaultValue(128)
      .withDescription("Max log block size in MB for log file, default 128MB");

  public static final ConfigOption<Integer> WRITE_LOG_MAX_SIZE = ConfigOptions
      .key("write.log.max.size")
      .intType()
      .defaultValue(1024)
      .withDescription("Maximum size allowed in MB for a log file before it is rolled over to the next version, default 1GB");

  public static final ConfigOption<Integer> WRITE_MERGE_MAX_MEMORY = ConfigOptions
      .key("write.merge.max_memory")
      .intType()
      .defaultValue(100) // default 100 MB
      .withDescription("Max memory in MB for merge, default 100MB");

  // this is only for internal use
  public static final ConfigOption<Long> WRITE_COMMIT_ACK_TIMEOUT = ConfigOptions
      .key("write.commit.ack.timeout")
      .longType()
      .defaultValue(-1L) // default at least once
      .withDescription("Timeout limit for a writer task after it finishes a checkpoint and\n"
          + "waits for the instant commit success, only for internal use");

  public static final ConfigOption<Boolean> WRITE_BULK_INSERT_SHUFFLE_BY_PARTITION = ConfigOptions
      .key("write.bulk_insert.shuffle_by_partition")
      .booleanType()
      .defaultValue(true)
      .withDescription("Whether to shuffle the inputs by partition path for bulk insert tasks, default true");

  public static final ConfigOption<Boolean> WRITE_BULK_INSERT_SORT_BY_PARTITION = ConfigOptions
      .key("write.bulk_insert.sort_by_partition")
      .booleanType()
      .defaultValue(true)
      .withDescription("Whether to sort the inputs by partition path for bulk insert tasks, default true");

  public static final ConfigOption<Integer> WRITE_SORT_MEMORY = ConfigOptions
      .key("write.sort.memory")
      .intType()
      .defaultValue(128)
      .withDescription("Sort memory in MB, default 128MB");

}
