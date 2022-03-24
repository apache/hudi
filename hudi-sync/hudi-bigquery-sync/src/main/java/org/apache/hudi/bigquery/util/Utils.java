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

package org.apache.hudi.bigquery.util;

import org.apache.hudi.bigquery.BigQuerySyncConfig;
import org.apache.hudi.common.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class Utils {
  public static String BIGQUERY_PROJECT_OPT_KEY = "hoodie.datasource.bigquery_sync.project";
  public static String BIGQUERY_DATASET_OPT_KEY = "hoodie.datasource.bigquery_sync.dataset";
  public static String BIGQUERY_TABLE_OPT_KEY = "hoodie.datasource.bigquery_sync.table";
  public static String BIGQUERY_SOURCE_URI_OPT_KEY = "hoodie.datasource.bigquery_sync.source_uri";
  public static String BIGQUERY_SOURCE_URI_PREFIX_OPT_KEY = "hoodie.datasource.bigquery_sync.source_uri_prefix";
  public static String BATH_PATH = "basePath";
  public static String BIGQUERY_PARTITION_FIELDS_OPT_KEY = "hoodie.datasource.bigquery_sync.partition_fields";

  public static Properties configToProperties(BigQuerySyncConfig cfg) {
    Properties properties = new Properties();
    properties.put(BIGQUERY_PROJECT_OPT_KEY, cfg.projectId);
    properties.put(BIGQUERY_DATASET_OPT_KEY, cfg.datasetName);
    properties.put(BIGQUERY_TABLE_OPT_KEY, cfg.tableName);
    properties.put(BIGQUERY_SOURCE_URI_OPT_KEY, cfg.sourceUri);
    properties.put(BIGQUERY_SOURCE_URI_PREFIX_OPT_KEY, cfg.sourceUriPrefix);
    properties.put(BATH_PATH, cfg.basePath);
    properties.put(BIGQUERY_PARTITION_FIELDS_OPT_KEY, cfg.partitionFields);
    return properties;
  }

  public static BigQuerySyncConfig propertiesToConfig(Properties properties) {
    BigQuerySyncConfig config = new BigQuerySyncConfig();
    config.projectId = properties.getProperty(BIGQUERY_PROJECT_OPT_KEY);
    config.datasetName = properties.getProperty(BIGQUERY_DATASET_OPT_KEY);
    config.tableName = properties.getProperty(BIGQUERY_TABLE_OPT_KEY);
    config.sourceUri = properties.getProperty(BIGQUERY_SOURCE_URI_OPT_KEY);
    config.sourceUriPrefix = properties.getProperty(BIGQUERY_SOURCE_URI_PREFIX_OPT_KEY);
    config.basePath = properties.getProperty(BATH_PATH);
    if (StringUtils.isNullOrEmpty(properties.getProperty(BIGQUERY_PARTITION_FIELDS_OPT_KEY))) {
      config.partitionFields = new ArrayList<>();
    } else {
      config.partitionFields = Arrays.asList(properties.getProperty(BIGQUERY_PARTITION_FIELDS_OPT_KEY).split(","));
    }
    return config;
  }
}
