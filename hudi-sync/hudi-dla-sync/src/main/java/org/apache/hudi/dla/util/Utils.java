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

package org.apache.hudi.dla.util;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.dla.DLASyncConfig;

import java.util.ArrayList;
import java.util.Arrays;

public class Utils {
  public static String DLA_DATABASE_OPT_KEY = "hoodie.datasource.dla_sync.database";
  public static String DLA_TABLE_OPT_KEY = "hoodie.datasource.dla_sync.table";
  public static String DLA_USER_OPT_KEY = "hoodie.datasource.dla_sync.username";
  public static String DLA_PASS_OPT_KEY = "hoodie.datasource.dla_sync.password";
  public static String DLA_URL_OPT_KEY = "hoodie.datasource.dla_sync.jdbcurl";
  public static String BATH_PATH = "basePath";
  public static String DLA_PARTITION_FIELDS_OPT_KEY = "hoodie.datasource.dla_sync.partition_fields";
  public static String DLA_PARTITION_EXTRACTOR_CLASS_OPT_KEY = "hoodie.datasource.dla_sync.partition_extractor_class";
  public static String DLA_ASSUME_DATE_PARTITIONING = "hoodie.datasource.dla_sync.assume_date_partitioning";
  public static String DLA_SKIP_RO_SUFFIX = "hoodie.datasource.dla_sync.skip_ro_suffix";
  public static String DLA_SKIP_RT_SYNC = "hoodie.datasource.dla_sync.skip_rt_sync";
  public static String DLA_SYNC_HIVE_STYLE_PARTITIONING = "hoodie.datasource.dla_sync.hive.style.partitioning";

  public static TypedProperties configToProperties(DLASyncConfig cfg) {
    TypedProperties properties = new TypedProperties();
    properties.put(DLA_DATABASE_OPT_KEY, cfg.databaseName);
    properties.put(DLA_TABLE_OPT_KEY, cfg.tableName);
    properties.put(DLA_USER_OPT_KEY, cfg.dlaUser);
    properties.put(DLA_PASS_OPT_KEY, cfg.dlaPass);
    properties.put(DLA_URL_OPT_KEY, cfg.jdbcUrl);
    properties.put(BATH_PATH, cfg.basePath);
    properties.put(DLA_PARTITION_EXTRACTOR_CLASS_OPT_KEY, cfg.partitionValueExtractorClass);
    properties.put(DLA_ASSUME_DATE_PARTITIONING, String.valueOf(cfg.assumeDatePartitioning));
    properties.put(DLA_SKIP_RO_SUFFIX, String.valueOf(cfg.skipROSuffix));
    properties.put(DLA_SYNC_HIVE_STYLE_PARTITIONING, String.valueOf(cfg.useDLASyncHiveStylePartitioning));
    return properties;
  }

  public static DLASyncConfig propertiesToConfig(TypedProperties properties) {
    DLASyncConfig config = new DLASyncConfig();
    config.databaseName = properties.getProperty(DLA_DATABASE_OPT_KEY);
    config.tableName = properties.getProperty(DLA_TABLE_OPT_KEY);
    config.dlaUser = properties.getProperty(DLA_USER_OPT_KEY);
    config.dlaPass = properties.getProperty(DLA_PASS_OPT_KEY);
    config.jdbcUrl = properties.getProperty(DLA_URL_OPT_KEY);
    config.basePath = properties.getProperty(BATH_PATH);
    if (StringUtils.isNullOrEmpty(properties.getProperty(DLA_PARTITION_FIELDS_OPT_KEY))) {
      config.partitionFields = new ArrayList<>();
    } else {
      config.partitionFields = Arrays.asList(properties.getProperty(DLA_PARTITION_FIELDS_OPT_KEY).split(","));
    }
    config.partitionValueExtractorClass = properties.getProperty(DLA_PARTITION_EXTRACTOR_CLASS_OPT_KEY);
    config.assumeDatePartitioning = Boolean.parseBoolean(properties.getProperty(DLA_ASSUME_DATE_PARTITIONING, "false"));
    config.skipROSuffix = Boolean.parseBoolean(properties.getProperty(DLA_SKIP_RO_SUFFIX, "false"));
    config.skipRTSync = Boolean.parseBoolean(properties.getProperty(DLA_SKIP_RT_SYNC, "false"));
    config.useDLASyncHiveStylePartitioning = Boolean.parseBoolean(properties.getProperty(DLA_SYNC_HIVE_STYLE_PARTITIONING, "false"));
    return config;
  }
}
