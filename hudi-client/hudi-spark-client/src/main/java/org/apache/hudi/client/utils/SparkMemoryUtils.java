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

package org.apache.hudi.client.utils;

import org.apache.hudi.common.config.HoodieConfig;

import org.apache.spark.storage.StorageLevel;

import java.util.Properties;

import static org.apache.hudi.config.HoodieWriteConfig.WRITE_STATUS_STORAGE_LEVEL_VALUE;

/**
 * Spark config utils.
 */
public class SparkMemoryUtils {
  public static StorageLevel getWriteStatusStorageLevel(Properties properties) {
    return StorageLevel.fromString(new HoodieConfig(properties).getString(WRITE_STATUS_STORAGE_LEVEL_VALUE));
  }
}
