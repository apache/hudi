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

package org.apache.hudi.util;

import org.apache.hudi.common.table.HoodieTableMetaClient;

import org.apache.flink.api.java.hadoop.mapred.utils.HadoopUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.File;

/**
 * Utilities for Hoodie Flink client.
 */
public class FlinkClientUtil {

  /**
   * Creates the meta client.
   */
  public static HoodieTableMetaClient createMetaClient(String basePath) {
    return HoodieTableMetaClient.builder().setBasePath(basePath).setConf(FlinkClientUtil.getHadoopConf()).build();
  }

  /**
   * Returns the hadoop configuration with possible hadoop conf paths.
   * E.G. the configurations under path $HADOOP_CONF_DIR and $HADOOP_HOME.
   */
  public static org.apache.hadoop.conf.Configuration getHadoopConf() {
    // create hadoop configuration with hadoop conf directory configured.
    org.apache.hadoop.conf.Configuration hadoopConf = null;
    for (String possibleHadoopConfPath : HadoopUtils.possibleHadoopConfPaths(new Configuration())) {
      hadoopConf = getHadoopConfiguration(possibleHadoopConfPath);
      if (hadoopConf != null) {
        break;
      }
    }
    if (hadoopConf == null) {
      hadoopConf = new org.apache.hadoop.conf.Configuration();
    }
    return hadoopConf;
  }

  /**
   * Returns a new Hadoop Configuration object using the path to the hadoop conf configured.
   *
   * @param hadoopConfDir Hadoop conf directory path.
   * @return A Hadoop configuration instance.
   */
  private static org.apache.hadoop.conf.Configuration getHadoopConfiguration(String hadoopConfDir) {
    if (new File(hadoopConfDir).exists()) {
      org.apache.hadoop.conf.Configuration hadoopConfiguration = new org.apache.hadoop.conf.Configuration();
      File coreSite = new File(hadoopConfDir, "core-site.xml");
      if (coreSite.exists()) {
        hadoopConfiguration.addResource(new Path(coreSite.getAbsolutePath()));
      }
      File hdfsSite = new File(hadoopConfDir, "hdfs-site.xml");
      if (hdfsSite.exists()) {
        hadoopConfiguration.addResource(new Path(hdfsSite.getAbsolutePath()));
      }
      File yarnSite = new File(hadoopConfDir, "yarn-site.xml");
      if (yarnSite.exists()) {
        hadoopConfiguration.addResource(new Path(yarnSite.getAbsolutePath()));
      }
      // Add mapred-site.xml. We need to read configurations like compression codec.
      File mapredSite = new File(hadoopConfDir, "mapred-site.xml");
      if (mapredSite.exists()) {
        hadoopConfiguration.addResource(new Path(mapredSite.getAbsolutePath()));
      }
      return hadoopConfiguration;
    }
    return null;
  }
}
