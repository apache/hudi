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

import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;

import org.apache.flink.api.java.hadoop.mapred.utils.HadoopUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;

/**
 * Utilities for Hoodie Flink client.
 */
public class FlinkClientUtil {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkClientUtil.class);
  public static String FLINK_HADOOP_CONFIG_PREFIX = "flink.hadoop";
  public static String FLINK_CONF_DIR = "FLINK_CONF_DIR";
  private static final String[] HADOOP_SITE_FILES = {"core-site.xml", "hdfs-site.xml", "yarn-site.xml", "mapred-site.xml"};

  /**
   * Creates the meta client.
   */
  public static HoodieTableMetaClient createMetaClient(String basePath) {
    return HoodieTableMetaClient.builder().setBasePath(basePath)
        .setConf(HadoopFSUtils.getStorageConfWithCopy(FlinkClientUtil.getHadoopConf())).build();
  }

  /**
   * Returns the hadoop configuration with possible hadoop conf paths.
   * E.G. the configurations under path $HADOOP_CONF_DIR and $HADOOP_HOME.
   */
  public static org.apache.hadoop.conf.Configuration getHadoopConf() {
    // create hadoop configuration with hadoop conf directory configured.
    org.apache.hadoop.conf.Configuration hadoopConf = null;
    for (String possibleHadoopConfPath : HadoopUtils.possibleHadoopConfPaths(new Configuration())) {
      hadoopConf = getHadoopConfiguration(possibleHadoopConfPath, false);
      if (hadoopConf != null) {
        break;
      }
    }
    if (hadoopConf == null) {
      hadoopConf = new org.apache.hadoop.conf.Configuration();
    }

    return applyFlinkHadoopOptions(hadoopConf);
  }

  /**
   * Loads an explicit Hadoop configuration directory, then applies the Hadoop options from
   * FLINK_CONF_DIR, as in environment-based discovery. Invalid explicit directories fail fast.
   */
  public static org.apache.hadoop.conf.Configuration getHadoopConf(String hadoopConfDir) {
    return applyFlinkHadoopOptions(getHadoopConfiguration(hadoopConfDir, true));
  }

  private static org.apache.hadoop.conf.Configuration applyFlinkHadoopOptions(org.apache.hadoop.conf.Configuration hadoopConf) {
    String confDir = System.getenv(FLINK_CONF_DIR);
    if (!StringUtils.isNullOrEmpty(confDir)) {
      LOG.info("Flink conf dir: {}", confDir);
      try {
        Configuration configuration = GlobalConfiguration.loadConfiguration(confDir);
        for (Map.Entry<String, String> kv : ConfigurationUtils.getPrefixedKeyValuePairs(FLINK_HADOOP_CONFIG_PREFIX, configuration).entrySet()) {
          hadoopConf.set(kv.getKey(), kv.getValue());
        }
      } catch (Exception e) {
        LOG.warn("Fail to load flink configuration from path {}", confDir, e);
      }
    }

    return hadoopConf;
  }

  private static org.apache.hadoop.conf.Configuration getHadoopConfiguration(String hadoopConfDir, boolean required) {
    if (required && StringUtils.isNullOrEmpty(hadoopConfDir == null ? null : hadoopConfDir.trim())) {
      throw new HoodieValidationException("'hadoop_conf.dir' must not be empty. Leave it unset to use environment discovery");
    }
    File dir = new File(hadoopConfDir);
    if (required && !dir.isDirectory()) {
      throw new HoodieValidationException("Invalid 'hadoop_conf.dir': directory does not exist or is not a directory: " + hadoopConfDir);
    }
    if (!dir.exists()) {
      return null;
    }
    org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
    boolean loaded = false;
    for (String name : HADOOP_SITE_FILES) {
      File site = new File(dir, name);
      if (required && site.exists() && (!site.isFile() || !site.canRead())) {
        throw new HoodieValidationException("Invalid 'hadoop_conf.dir': site file is not a readable regular file: " + site);
      }
      if (site.exists()) {
        hadoopConf.addResource(new Path(site.getAbsolutePath()));
        loaded = true;
      }
    }
    if (required && !loaded) {
      throw new HoodieValidationException("Invalid 'hadoop_conf.dir': no supported Hadoop site files "
          + "(core-site.xml, hdfs-site.xml, yarn-site.xml, mapred-site.xml) found under directory: " + hadoopConfDir);
    }
    return hadoopConf;
  }

  /**
   * Get merged {@link TypedProperties} from {@link HoodieTableConfig} and {@link HoodieWriteConfig}, which is used by FileGroup reader.
   *
   * @param tableConfig The hoodie table configuration
   * @param writeConfig the hoodie write configuration
   *
   * @return Merged {@link TypedProperties} from {@link HoodieTableConfig} and {@link HoodieWriteConfig}
   */
  public static TypedProperties getReadProps(HoodieTableConfig tableConfig, HoodieWriteConfig writeConfig) {
    TypedProperties props = new TypedProperties();
    props.putAll(tableConfig.getProps());
    writeConfig.getProps().forEach(props::putIfAbsent);
    // For compatibility, log scanner uses MAX_MEMORY_FOR_COMPACTION for merging and FileGroup reader uses MAX_MEMORY_FOR_MERGE for merging.
    props.put(HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE.key(), writeConfig.getString(HoodieMemoryConfig.MAX_MEMORY_FOR_COMPACTION));
    return props;
  }
}
