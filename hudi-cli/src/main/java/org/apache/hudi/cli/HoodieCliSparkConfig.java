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

package org.apache.hudi.cli;

/**
 * Class storing configs for init spark.
 */
public class HoodieCliSparkConfig {
  /**
   * Configs to start spark application.
   */
  public static final String CLI_SPARK_MASTER = "SPARK_MASTER";
  public static final String CLI_SERIALIZER = "spark.serializer";
  public static final String CLI_DRIVER_MAX_RESULT_SIZE = "spark.driver.maxResultSize";
  public static final String CLI_EVENT_LOG_OVERWRITE = "spark.eventLog.overwrite";
  public static final String CLI_EVENT_LOG_ENABLED = "spark.eventLog.enabled";
  public static final String CLI_EXECUTOR_MEMORY = "spark.executor.memory";

  /**
   * Hadoop output config.
   */
  public static final String CLI_MAPRED_OUTPUT_COMPRESS = "spark.hadoop.mapred.output.compress";
  public static final String CLI_MAPRED_OUTPUT_COMPRESSION_CODEC = "spark.hadoop.mapred.output.compression.codec";
  public static final String CLI_MAPRED_OUTPUT_COMPRESSION_TYPE = "spark.hadoop.mapred.output.compression.type";

  /**
   * Parquet file config.
   */
  public static final String CLI_PARQUET_ENABLE_SUMMARY_METADATA = "parquet.enable.summary-metadata";
}
