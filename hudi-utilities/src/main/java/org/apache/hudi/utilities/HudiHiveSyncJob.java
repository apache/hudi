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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hive.HiveSyncTool;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_FILE_FORMAT;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;

/**
 * Utility job for running Hive sync on-demand for Hudi tables.
 * <p>
 * This tool allows you to synchronize Hudi table metadata with Hive metastore
 * independently from ingestion workflows, useful for backfills, manual data
 * corrections, or quick metadata reconciliation.
 * <p>
 * Example usage:
 * <pre>
 * spark-submit \
 *   --class org.apache.hudi.utilities.HudiHiveSyncJob \
 *   hudi-utilities.jar \
 *   --base-path /path/to/hudi/table \
 *   --base-file-format PARQUET \
 *   --props-file-path /path/to/hive-sync.properties \
 *   --hoodie-conf hoodie.datasource.hive_sync.database=my_db \
 *   --hoodie-conf hoodie.datasource.hive_sync.table=my_table
 * </pre>
 */
public class HudiHiveSyncJob {

  private static final Logger LOG = LoggerFactory.getLogger(HudiHiveSyncJob.class);

  private final Config cfg;
  private final Configuration hadoopConf;
  private final TypedProperties props;

  public HudiHiveSyncJob(JavaSparkContext jsc, Config cfg) {
    this.cfg = cfg;
    this.hadoopConf = jsc.hadoopConfiguration();
    this.props = UtilHelpers.buildProperties(hadoopConf, cfg.propsFilePath, cfg.configs);
  }

  public static void main(String[] args) throws IOException {
    final Config cfg = new Config();
    new JCommander(cfg, null, args);
    LOG.info("Cfg received: {}", cfg);
    JavaSparkContext jsc = UtilHelpers.buildSparkContext("HudiHiveSyncJob", "local[2]", true);
    new HudiHiveSyncJob(jsc, cfg).run();
  }

  public void run() throws IOException {
    LOG.info("Starting hive sync for {}", cfg.basePath);
    HoodieTimer timer = HoodieTimer.start();
    HiveSyncTool syncTool = null;
    try {
      props.put(META_SYNC_BASE_PATH.key(), cfg.basePath);
      props.put(META_SYNC_BASE_FILE_FORMAT.key(), cfg.baseFileFormat);

      LOG.info("HiveSyncConfig props used to sync data {}", props);
      syncTool = new HiveSyncTool(props, new HiveConf(hadoopConf, HiveConf.class));
      syncTool.syncHoodieTable();
    } catch (Exception e) {
      LOG.error("Exception in running hive-sync", e);
      throw new HoodieException("Hive sync failed", e);
    } finally {
      if (syncTool != null) {
        syncTool.close();
      }
      LOG.info("Hive-sync duration in ms {}", timer.endTimer());
    }
  }

  public static class Config implements Serializable {
    @Parameter(names = {"--base-path", "-sp"}, description = "Base path for the table", required = true)
    public String basePath = null;

    @Parameter(names = {"--base-file-format", "bff"}, description = "Base file format of the dataset")
    public String baseFileFormat = "PARQUET";

    @Parameter(names = {"--props-file-path"}, description = "Path to properties file on localfs or dfs.")
    public String propsFilePath = null;

    @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
        + "(using the CLI parameter \"--props\") can also be passed command line using this parameter. This can be repeated",
        splitter = IdentitySplitter.class)
    public List<String> configs = new ArrayList<>();

    @Override
    public String toString() {
      return "Config{"
          + "basePath='" + basePath + '\''
          + ", baseFileFormat='" + baseFileFormat + '\''
          + ", propsFilePath='" + propsFilePath + '\''
          + ", configs=" + configs
          + '}';
    }
  }
}
