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

/**
 * Configs that are common during ingestion across different cloud stores
 */
public class CloudStoreIngestionConfig {

  /**
   * How many metadata messages to pull at a time.
   * Also see {@link #DEFAULT_BATCH_SIZE}.
   */
  public static final String BATCH_SIZE_CONF = "hoodie.deltastreamer.source.cloud.meta.batch.size";

  /**
   * Provide a reasonable setting to use for default batch size when fetching File Metadata as part of Cloud Ingestion.
   * If batch size is too big, two possible issues can happen:
   * i) Acknowledgement takes too long (given that Hudi needs to commit first).
   * ii) In the case of Google Cloud Pubsub:
   *   a) it will keep delivering the same message since it wasn't acked in time.
   *   b) The size of the request that acks outstanding messages may exceed the limit,
   *      which is 512KB as per Google's docs. See: https://cloud.google.com/pubsub/quotas#resource_limits
   */
  public static final int DEFAULT_BATCH_SIZE = 10;

  /**
   * Whether to acknowledge Metadata messages during Cloud Ingestion or not. This is useful during dev and testing.
   * In Prod this should always be true.
   * In case of Cloud Pubsub, not acknowledging means Pubsub will keep redelivering the same messages.
   */
  public static final String ACK_MESSAGES = "hoodie.deltastreamer.source.cloud.meta.ack";

  /**
   * Default value for {@link #ACK_MESSAGES}
   */
  public static final boolean ACK_MESSAGES_DEFAULT_VALUE = true;

  /**
   * Check whether file exists before attempting to pull it
   */
  public static final String ENABLE_EXISTS_CHECK = "hoodie.deltastreamer.source.cloud.data.check.file.exists";

  /**
   * Default value for {@link #ENABLE_EXISTS_CHECK}
   */
  public static final Boolean DEFAULT_ENABLE_EXISTS_CHECK = false;

  // Only select objects in the bucket whose relative path matches this prefix
  public static final String SELECT_RELATIVE_PATH_PREFIX =
          "hoodie.deltastreamer.source.cloud.data.select.relpath.prefix";

  // Ignore objects in the bucket whose relative path matches this prefix
  public static final String IGNORE_RELATIVE_PATH_PREFIX =
          "hoodie.deltastreamer.source.cloud.data.ignore.relpath.prefix";

  // Ignore objects in the bucket whose relative path contains this substring
  public static final String IGNORE_RELATIVE_PATH_SUBSTR =
          "hoodie.deltastreamer.source.cloud.data.ignore.relpath.substring";

  /**
   * A JSON string passed to the Spark DataFrameReader while loading the dataset.
   * Example: hoodie.deltastreamer.gcp.spark.datasource.options={"header":"true","encoding":"UTF-8"}
   */
  public static final String SPARK_DATASOURCE_OPTIONS = "hoodie.deltastreamer.source.cloud.data.datasource.options";

  /**
   * Only match files with this extension. By default, this is the same as
   * {@link HoodieIncrSource.Config#SOURCE_FILE_FORMAT}.
   */
  public static final String CLOUD_DATAFILE_EXTENSION =
          "hoodie.deltastreamer.source.cloud.data.select.file.extension";

  /**
   * Format of the data file. By default, this will be the same as
   * {@link HoodieIncrSource.Config#SOURCE_FILE_FORMAT}.
   */
  public static final String DATAFILE_FORMAT = "hoodie.deltastreamer.source.cloud.data.datafile.format";

}
