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

import org.apache.hudi.util.FlinkClientUtil;

import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.Map;

/**
 * Utilities for fetching hadoop configurations.
 */
public class HadoopConfigurations {
  public static final String HADOOP_PREFIX = "hadoop.";
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
   * Creates a new hadoop configuration that is initialized with the given flink configuration
   * along with some configurations necessary to construct file readers/writers.
   */
  public static org.apache.hadoop.conf.Configuration getHadoopConf(Configuration conf) {
    org.apache.hadoop.conf.Configuration hadoopConf = FlinkClientUtil.getHadoopConf();
    Map<String, String> hadoopOptions = FlinkOptions.getPropertiesWithPrefix(conf.toMap(), HADOOP_PREFIX);
    hadoopOptions.forEach(hadoopConf::set);
    // kind of hacky: flink specific IO options.
    Map<String, String> ioOptions = OptionsResolver.getIOOptions(conf);
    ioOptions.forEach(hadoopConf::set);
    return hadoopConf;
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
