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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Utilities for fetching hadoop configurations.
 */
public class HadoopConfigurations {
  private static final String HADOOP_PREFIX = "hadoop.";
  private static final  String PARQUET_PREFIX = "parquet.";

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
    org.apache.hadoop.conf.Configuration hadoopConf = FlinkClientUtil.getHadoopConf();
    Map<String, String> options = FlinkOptions.getPropertiesWithPrefix(conf.toMap(), HADOOP_PREFIX);
    options.forEach(hadoopConf::set);
    return hadoopConf;
  }

  /**
   * Returns a new hadoop configuration that is initialized with the given hadoopConfDir.
   *
   * @param hadoopConfDir Hadoop conf directory path.
   * @return A Hadoop configuration instance.
   */
  public static org.apache.hadoop.conf.Configuration getHadoopConfiguration(String hadoopConfDir) {
    if (new File(hadoopConfDir).exists()) {
      List<File> possiableConfFiles = new ArrayList<File>();
      File coreSite = new File(hadoopConfDir, "core-site.xml");
      if (coreSite.exists()) {
        possiableConfFiles.add(coreSite);
      }
      File hdfsSite = new File(hadoopConfDir, "hdfs-site.xml");
      if (hdfsSite.exists()) {
        possiableConfFiles.add(hdfsSite);
      }
      File yarnSite = new File(hadoopConfDir, "yarn-site.xml");
      if (yarnSite.exists()) {
        possiableConfFiles.add(yarnSite);
      }
      // Add mapred-site.xml. We need to read configurations like compression codec.
      File mapredSite = new File(hadoopConfDir, "mapred-site.xml");
      if (mapredSite.exists()) {
        possiableConfFiles.add(mapredSite);
      }
      if (possiableConfFiles.isEmpty()) {
        return null;
      } else {
        org.apache.hadoop.conf.Configuration hadoopConfiguration = new org.apache.hadoop.conf.Configuration();
        for (File confFile : possiableConfFiles) {
          hadoopConfiguration.addResource(new Path(confFile.getAbsolutePath()));
        }
        return hadoopConfiguration;
      }
    }
    return null;
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
