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

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.util.FlinkClientUtil;

import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.util.Map;

/**
 * Utilities for fetching hadoop configurations.
 */
public class HadoopConfigurations {
  private static final String HADOOP_PREFIX = "hadoop.";
  private static final String PARQUET_PREFIX = "parquet.";

  /**
   * Creates a merged hadoop configuration with given flink configuration and hadoop configuration.
   */
  public static org.apache.hadoop.conf.Configuration getParquetConf(
      org.apache.flink.configuration.Configuration options,
      org.apache.hadoop.conf.Configuration hadoopConf) {
    org.apache.hadoop.conf.Configuration copy = new org.apache.hadoop.conf.Configuration(hadoopConf);
    Map<String, String> parquetOptions = FlinkOptions.getPropertiesWithPrefix(options.toMap(), PARQUET_PREFIX);
    parquetOptions.forEach((k, v) -> copy.set(PARQUET_PREFIX + k, v));
    return copy;
  }

  /**
   * Creates a new hadoop configuration that is initialized with the given flink configuration.
   */
  public static org.apache.hadoop.conf.Configuration getHadoopConf(Configuration conf) {
    // Choose the base configuration source: explicit conf dir wins;
    // fall back to environment-based discovery when the option is absent
    // or the directory is invalid.
    String hadoopConfDir = conf.getString(FlinkOptions.HADOOP_CONF_DIR.key(), null);
    org.apache.hadoop.conf.Configuration hadoopConf;
    if (hadoopConfDir != null) {
      validateHadoopConfDir(hadoopConfDir);
      org.apache.hadoop.conf.Configuration dirHadoopConf= FlinkClientUtil.getHadoopConf(hadoopConfDir);
      hadoopConf = dirHadoopConf != null ? dirHadoopConf : FlinkClientUtil.getHadoopConf();
    }else {
      hadoopConf = FlinkClientUtil.getHadoopConf();
    }
    Map<String, String> options = FlinkOptions.getPropertiesWithPrefix(conf.toMap(), HADOOP_PREFIX);
    options.forEach(hadoopConf::set);
    return hadoopConf;
  }

  /**
   * Validates an explicitly configured {@code hadoop_conf.dir}. Any invalid value must
   * fail fast rather than silently falling back, because an empty configuration would
   * resolve {@code fs.defaultFS} to the local FS and mask mis-configuration.
   */
  private static void validateHadoopConfDir(String hadoopConfDir) {
    String optionKey = FlinkOptions.HADOOP_CONF_DIR.key();
    if (StringUtils.isNullOrEmpty(hadoopConfDir.trim())) {
      throw new HoodieValidationException(String.format(
              "'%s' must not be empty. Either set a valid directory or leave it unset to use environment discovery",
              optionKey));
    }
    File dir = new File(hadoopConfDir);
    if (!dir.exists()) {
      throw new HoodieValidationException(String.format(
              "Invalid '%s': the specified Hadoop conf directory does not exist: %s",
              optionKey, hadoopConfDir));
    }
    if (!dir.isDirectory()) {
      throw new HoodieValidationException(String.format(
              "Invalid '%s': the specified path is not a directory: %s",
              optionKey, dir.getAbsolutePath()));
    }
    File[] siteFiles = dir.listFiles((d, name) -> name.endsWith("-site.xml"));
    if (siteFiles == null || siteFiles.length == 0) {
      throw new HoodieValidationException(String.format(
              "Invalid '%s': no *-site.xml (e.g. core-site.xml) found under directory: %s",
              optionKey, dir.getAbsolutePath()));
    }
  }

  /**
   * Creates a Hive configuration with configured dir path or empty if no Hive conf dir is set.
   */
  public static org.apache.hadoop.conf.Configuration getHiveConf(Configuration conf) {
    String explicitDir = conf.getString(FlinkOptions.HIVE_SYNC_CONF_DIR, System.getenv("HIVE_CONF_DIR"));
    org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
    if (explicitDir != null) {
      hadoopConf.addResource(new Path(explicitDir, "hive-site.xml"));
    }
    return hadoopConf;
  }
}
