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

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.Option;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class HoodieCompactor {

  private static final Logger LOG = LogManager.getLogger(HoodieCompactor.class);
  private final Config cfg;
  private transient FileSystem fs;
  private TypedProperties props;
  private final JavaSparkContext jsc;

  public HoodieCompactor(JavaSparkContext jsc, Config cfg) {
    this.cfg = cfg;
    this.jsc = jsc;
    this.props = cfg.propsFilePath == null
        ? UtilHelpers.buildProperties(cfg.configs)
        : readConfigFromFileSystem(jsc, cfg);
  }

  private TypedProperties readConfigFromFileSystem(JavaSparkContext jsc, Config cfg) {
    final FileSystem fs = FSUtils.getFs(cfg.basePath, jsc.hadoopConfiguration());

    return UtilHelpers
        .readConfig(fs, new Path(cfg.propsFilePath), cfg.configs)
        .getConfig();
  }

  public static class Config implements Serializable {
    @Parameter(names = {"--base-path", "-sp"}, description = "Base path for the table", required = true)
    public String basePath = null;
    @Parameter(names = {"--table-name", "-tn"}, description = "Table name", required = true)
    public String tableName = null;
    @Parameter(names = {"--instant-time", "-it"}, description = "Compaction Instant time", required = true)
    public String compactionInstantTime = null;
    @Parameter(names = {"--parallelism", "-pl"}, description = "Parallelism for hoodie insert", required = true)
    public int parallelism = 1;
    @Parameter(names = {"--schema-file", "-sf"}, description = "path for Avro schema file", required = true)
    public String schemaFile = null;
    @Parameter(names = {"--spark-master", "-ms"}, description = "Spark master", required = false)
    public String sparkMaster = null;
    @Parameter(names = {"--spark-memory", "-sm"}, description = "spark memory to use", required = true)
    public String sparkMemory = null;
    @Parameter(names = {"--retry", "-rt"}, description = "number of retries", required = false)
    public int retry = 0;
    @Parameter(names = {"--schedule", "-sc"}, description = "Schedule compaction", required = false)
    public Boolean runSchedule = false;
    @Parameter(names = {"--strategy", "-st"}, description = "Strategy Class", required = false)
    public String strategyClassName = null;
    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;

    @Parameter(names = {"--props"}, description = "path to properties file on localfs or dfs, with configurations for "
        + "hoodie client for compacting")
    public String propsFilePath = null;

    @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
        + "(using the CLI parameter \"--props\") can also be passed command line using this parameter. This can be repeated",
            splitter = IdentitySplitter.class)
    public List<String> configs = new ArrayList<>();
  }

  public static void main(String[] args) {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }
    final JavaSparkContext jsc = UtilHelpers.buildSparkContext("compactor-" + cfg.tableName, cfg.sparkMaster, cfg.sparkMemory);
    HoodieCompactor compactor = new HoodieCompactor(jsc, cfg);
    compactor.compact(cfg.retry);
  }

  public int compact(int retry) {
    this.fs = FSUtils.getFs(cfg.basePath, jsc.hadoopConfiguration());
    int ret = UtilHelpers.retry(retry, () -> {
      if (cfg.runSchedule) {
        if (null == cfg.strategyClassName) {
          throw new IllegalArgumentException("Missing Strategy class name for running compaction");
        }
        return doSchedule(jsc);
      } else {
        return doCompact(jsc);
      }
    }, "Compact failed");
    return ret;
  }

  private int doCompact(JavaSparkContext jsc) throws Exception {
    // Get schema.
    String schemaStr = UtilHelpers.parseSchema(fs, cfg.schemaFile);
    SparkRDDWriteClient client =
        UtilHelpers.createHoodieClient(jsc, cfg.basePath, schemaStr, cfg.parallelism, Option.empty(), props);
    JavaRDD<WriteStatus> writeResponse = (JavaRDD<WriteStatus>) client.compact(cfg.compactionInstantTime);
    return UtilHelpers.handleErrors(jsc, cfg.compactionInstantTime, writeResponse);
  }

  private int doSchedule(JavaSparkContext jsc) throws Exception {
    // Get schema.
    SparkRDDWriteClient client =
        UtilHelpers.createHoodieClient(jsc, cfg.basePath, "", cfg.parallelism, Option.of(cfg.strategyClassName), props);
    client.scheduleCompactionAtInstant(cfg.compactionInstantTime, Option.empty());
    return 0;
  }
}
