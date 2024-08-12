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

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.utilities.config.CloudSourceConfig;

/**
 * Configs that are common during ingestion across different cloud stores
 */
public class CloudStoreIngestionConfig {

  /**
   * How many metadata messages to pull at a time.
   * Also see {@link #DEFAULT_BATCH_SIZE}.
   */
  @Deprecated
  public static final String BATCH_SIZE_CONF = CloudSourceConfig.BATCH_SIZE_CONF.key();

  /**
   * Provide a reasonable setting to use for default batch size when fetching File Metadata as part of Cloud Ingestion.
   * If batch size is too big, two possible issues can happen:
   * i) Acknowledgement takes too long (given that Hudi needs to commit first).
   * ii) In the case of Google Cloud Pubsub:
   * a) it will keep delivering the same message since it wasn't acknowledged in time.
   * b) The size of the request that acknowledges outstanding messages may exceed the limit,
   * which is 512KB as per Google's docs. See: https://cloud.google.com/pubsub/quotas#resource_limits
   */
  @Deprecated
  public static final int DEFAULT_BATCH_SIZE = CloudSourceConfig.BATCH_SIZE_CONF.defaultValue();

  /**
   * Whether to acknowledge Metadata messages during Cloud Ingestion or not. This is useful during dev and testing.
   * In Prod this should always be true.
   * In case of Cloud Pubsub, not acknowledging means Pubsub will keep redelivering the same messages.
   */
  @Deprecated
  public static final String ACK_MESSAGES = CloudSourceConfig.ACK_MESSAGES.key();

  /**
   * Default value for {@link #ACK_MESSAGES}
   */
  @Deprecated
  public static final boolean ACK_MESSAGES_DEFAULT_VALUE = CloudSourceConfig.ACK_MESSAGES.defaultValue();

  /**
   * Check whether file exists before attempting to pull it
   */
  @Deprecated
  public static final String ENABLE_EXISTS_CHECK = CloudSourceConfig.ENABLE_EXISTS_CHECK.key();

  /**
   * Default value for {@link #ENABLE_EXISTS_CHECK}
   */
  @Deprecated
  public static final Boolean DEFAULT_ENABLE_EXISTS_CHECK = CloudSourceConfig.ENABLE_EXISTS_CHECK.defaultValue();

  // Only select objects in the bucket whose relative path matches this prefix
  @Deprecated
  public static final String SELECT_RELATIVE_PATH_PREFIX = CloudSourceConfig.SELECT_RELATIVE_PATH_PREFIX.key();

  // Ignore objects in the bucket whose relative path matches this prefix
  @Deprecated
  public static final String IGNORE_RELATIVE_PATH_PREFIX = CloudSourceConfig.IGNORE_RELATIVE_PATH_PREFIX.key();

  // Ignore objects in the bucket whose relative path contains this substring
  @Deprecated
  public static final String IGNORE_RELATIVE_PATH_SUBSTR = CloudSourceConfig.IGNORE_RELATIVE_PATH_SUBSTR.key();

  /**
   * A JSON string passed to the Spark DataFrameReader while loading the dataset.
   * Example: hoodie.streamer.gcp.spark.datasource.options={"header":"true","encoding":"UTF-8"}
   */
  @Deprecated
  public static final String SPARK_DATASOURCE_OPTIONS = CloudSourceConfig.SPARK_DATASOURCE_OPTIONS.key();

  /**
   * Only match files with this extension. By default, this is the same as
   * {@link org.apache.hudi.utilities.config.HoodieIncrSourceConfig#SOURCE_FILE_FORMAT}.
   */
  @Deprecated
  public static final String CLOUD_DATAFILE_EXTENSION = CloudSourceConfig.CLOUD_DATAFILE_EXTENSION.key();

  /**
   * Format of the data file. By default, this will be the same as
   * {@link org.apache.hudi.utilities.config.HoodieIncrSourceConfig#SOURCE_FILE_FORMAT}.
   */
  @Deprecated
  public static final String DATAFILE_FORMAT = CloudSourceConfig.DATAFILE_FORMAT.key();

  /**
   * A comma delimited list of path-based partition fields in the source file structure
   */
  public static final String PATH_BASED_PARTITION_FIELDS = "hoodie.deltastreamer.source.cloud.data.partition.fields.from.path";
}
