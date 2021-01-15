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

package org.apache.hudi.utilities;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.client.HoodieWriteResult;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.config.HoodieWriteConfig;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HiveSyncTool;
import org.apache.hudi.sync.common.AbstractSyncTool;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Util Class help to delete hoodie data in specific partitions.
 */
public class HoodiePartitionCleaner {

  private static final Logger LOG = LogManager.getLogger(HoodiePartitionCleaner.class);

  /**
   * Config for Cleaner.
   */
  private final Config cfg;

  /**
   * Spark context.
   */
  private transient JavaSparkContext jssc;

  /**
   * Bag of properties with source, hoodie client etc.
   */
  private TypedProperties props;

  public HoodiePartitionCleaner(Config cfg, JavaSparkContext jssc) {
    this.cfg = cfg;
    this.jssc = jssc;
    /*
     * Filesystem used.
     */
    FileSystem fs = FSUtils.getFs(cfg.basePath, jssc.hadoopConfiguration());
    this.props = cfg.propsFilePath == null ? UtilHelpers.buildProperties(cfg.configs)
        : UtilHelpers.readConfig(fs, new Path(cfg.propsFilePath), cfg.configs).getConfig();
    LOG.info("Creating Cleaner with configs : " + props.toString());
  }

  public void run() {
    HoodieWriteConfig hoodieCfg = getHoodieClientConfig();
    SparkRDDWriteClient client = new SparkRDDWriteClient<>(new HoodieSparkEngineContext(jssc), hoodieCfg, true);
    String cleanInstant = HoodieActiveTimeline.createNewInstantTime();
    client.startCommitWithTime(cleanInstant, HoodieTimeline.REPLACE_COMMIT_ACTION);
    HoodieWriteResult writeResult = client.deletePartitions(cfg.partitions, cleanInstant);
    commit(client, cleanInstant, writeResult);

    // sync meta if needed
    if (cfg.enableMetaSync) {
      syncMeta(cfg);
    }
  }

  public void syncMeta(Config cfg) {
    Set<String> syncClientToolClasses = new HashSet<>(Arrays.asList(cfg.syncClientToolClass.split(",")));
    for (String impl : syncClientToolClasses) {
      impl = impl.trim();
      AbstractSyncTool syncTool;
      switch (impl) {
        case "org.apache.hudi.hive.HiveSyncTool":
          HiveSyncConfig hiveSyncConfig = DataSourceUtils.buildHiveSyncConfig(props, cfg.basePath, cfg.baseFileFormat);
          LOG.info("Syncing target hoodie table with hive table(" + hiveSyncConfig.tableName + "). Hive metastore URL :"
              + hiveSyncConfig.jdbcUrl + ", basePath :" + cfg.baseFileFormat);
          syncTool = new HiveSyncTool(hiveSyncConfig, new HiveConf(jssc.hadoopConfiguration(), HiveConf.class), FSUtils.getFs(cfg.basePath, jssc.hadoopConfiguration()));
          break;
        default:
          FileSystem fs = FSUtils.getFs(cfg.basePath, jssc.hadoopConfiguration());
          Properties properties = new Properties();
          properties.putAll(props);
          properties.put("basePath", cfg.basePath);
          syncTool = (AbstractSyncTool) ReflectionUtils.loadClass(impl, new Class[]{Properties.class, FileSystem.class}, properties, fs);
      }
      syncTool.syncHoodieTable();
    }
  }

  private void commit(SparkRDDWriteClient client, String instantTime, HoodieWriteResult writeResult) {
    boolean commitSuccess = client.commit(instantTime, writeResult.getWriteStatuses(), Option.empty(),
        HoodieTimeline.REPLACE_COMMIT_ACTION,
        writeResult.getPartitionToReplaceFileIds());
    if (commitSuccess) {
      LOG.info("Commit " + instantTime + " successful!");
    } else {
      LOG.info("Commit " + instantTime + " failed!");
      throw new HoodieException("Commit " + instantTime + " failed!");
    }
  }

  private HoodieWriteConfig getHoodieClientConfig() {
    return HoodieWriteConfig.newBuilder().withPath(cfg.basePath).withAutoCommit(false)
        .withProps(props).build();
  }

  public static class Config implements Serializable {

    @Parameter(names = {"--target-base-path"}, description = "base path for the hoodie table to be clean.",
        required = true)
    public String basePath;

    @Parameter(names = {"--props"}, description = "path to properties file on localfs or dfs, with configurations for "
        + "hoodie client for cleaning")
    public String propsFilePath = null;

    @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
        + "(using the CLI parameter \"--props\") can also be passed command line using this parameter. This can be repeated",
        splitter = IdentitySplitter.class)
    public List<String> configs = new ArrayList<>();

    @Parameter(names = {"--spark-master"}, description = "spark master to use.")
    public String sparkMaster = "local[2]";

    @Parameter(names = {"--partitions"}, description = "partitions to clean.")
    public List<String> partitions;

    @Parameter(names = {"--enable-sync"}, description = "Enable syncing meta")
    public Boolean enableMetaSync = false;

    @Parameter(names = {"--sync-tool-classes"}, description = "Meta sync client tool, using comma to separate multi tools")
    public String syncClientToolClass = HiveSyncTool.class.getName();

    @Parameter(names = {"--base-file-format"}, description = "File format for the base files. PARQUET (or) HFILE")
    public String baseFileFormat = "PARQUET";

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;
  }

  public static void main(String[] args) {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }

    String dirName = new Path(cfg.basePath).getName();
    JavaSparkContext jssc = UtilHelpers.buildSparkContext("hoodie-partition-cleaner-" + dirName, cfg.sparkMaster);
    new HoodiePartitionCleaner(cfg, jssc).run();
    jssc.stop();
  }
}
