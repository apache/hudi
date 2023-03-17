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
 * <p>
 * Please use {@link CloudSourceConfig} instead.
 */
@Deprecated
public class CloudStoreIngestionConfig {
  @Deprecated
  public static final String BATCH_SIZE_CONF = CloudSourceConfig.BATCH_SIZE.key();
  @Deprecated
  public static final int DEFAULT_BATCH_SIZE = CloudSourceConfig.BATCH_SIZE.defaultValue();
  @Deprecated
  public static final String ACK_MESSAGES = CloudSourceConfig.ACK_MESSAGES.key();
  @Deprecated
  public static final boolean ACK_MESSAGES_DEFAULT_VALUE =
      CloudSourceConfig.ACK_MESSAGES.defaultValue();
  @Deprecated
  public static final String ENABLE_EXISTS_CHECK = CloudSourceConfig.ENABLE_EXISTS_CHECK.key();
  @Deprecated
  public static final Boolean DEFAULT_ENABLE_EXISTS_CHECK =
      CloudSourceConfig.ENABLE_EXISTS_CHECK.defaultValue();
  @Deprecated
  public static final String SELECT_RELATIVE_PATH_PREFIX =
      CloudSourceConfig.SELECT_RELATIVE_PATH_PREFIX.key();
  @Deprecated
  public static final String IGNORE_RELATIVE_PATH_PREFIX =
      CloudSourceConfig.IGNORE_RELATIVE_PATH_PREFIX.key();
  @Deprecated
  public static final String IGNORE_RELATIVE_PATH_SUBSTR =
      CloudSourceConfig.IGNORE_RELATIVE_PATH_SUBSTR.key();
  @Deprecated
  public static final String SPARK_DATASOURCE_OPTIONS =
      CloudSourceConfig.SPARK_DATASOURCE_OPTIONS.key();
  @Deprecated
  public static final String CLOUD_DATAFILE_EXTENSION =
      CloudSourceConfig.CLOUD_DATAFILE_EXTENSION.key();
  @Deprecated
  public static final String DATAFILE_FORMAT = CloudSourceConfig.DATAFILE_FORMAT.key();

}
