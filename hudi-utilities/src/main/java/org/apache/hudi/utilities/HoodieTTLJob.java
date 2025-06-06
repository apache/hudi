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

import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.client.HoodieWriteResult;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.exception.HoodieException;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class to run TTL management.
 */
public class HoodieTTLJob {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieTTLJob.class);
  private final Config cfg;
  private final TypedProperties props;
  private final JavaSparkContext jsc;
  private final HoodieTableMetaClient metaClient;

  public HoodieTTLJob(JavaSparkContext jsc, Config cfg) {
    this(jsc, cfg, UtilHelpers.buildProperties(jsc.hadoopConfiguration(), cfg.propsFilePath, cfg.configs),
        UtilHelpers.createMetaClient(jsc, cfg.basePath, true));
  }

  public HoodieTTLJob(JavaSparkContext jsc, Config cfg, TypedProperties props, HoodieTableMetaClient metaClient) {
    this.cfg = cfg;
    this.jsc = jsc;
    this.props = props;
    this.metaClient = metaClient;
    LOG.info("Creating TTL job with configs : " + props.toString());
    // Disable async cleaning, will trigger synchronous cleaning manually.
    this.props.put(HoodieCleanConfig.ASYNC_CLEAN.key(), false);
    if (this.metaClient.getTableConfig().isMetadataTableAvailable()) {
      // add default lock config options if MDT is enabled.
      UtilHelpers.addLockOptions(cfg.basePath, this.metaClient.getBasePath().toUri().getScheme(),  this.props);
    }
  }

  public void run() {
    // need to do commit in SparkDeletePartitionCommitActionExecutor#execute
    try (SparkRDDWriteClient<HoodieRecordPayload> client =
             UtilHelpers.createHoodieClient(jsc, cfg.basePath, "", cfg.parallelism, Option.empty(), props)) {
      String instantTime = client.startDeletePartitionCommit(metaClient);
      HoodieWriteResult result = client.managePartitionTTL(instantTime);
      client.commit(instantTime, result.getWriteStatuses(), Option.empty(), HoodieTimeline.REPLACE_COMMIT_ACTION,
          result.getPartitionToReplaceFileIds(), Option.empty());
    }
  }

  public static class Config implements Serializable {
    @Parameter(names = {"--base-path", "-sp"}, description = "Base path for the table", required = true)
    public String basePath = null;
    @Parameter(names = {"--parallelism", "-pl"}, description = "Parallelism for hoodie insert/upsert/delete", required = false)
    public int parallelism = 1500;
    @Parameter(names = {"--spark-master", "-ms"}, description = "Spark master")
    public String sparkMaster = null;
    @Parameter(names = {"--spark-memory", "-sm"}, description = "spark memory to use", required = false)
    public String sparkMemory = null;

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;


    @Parameter(names = {"--props"}, description = "path to properties file on localfs or dfs, with configurations for "
        + "hoodie client for clustering")
    public String propsFilePath = null;

    @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
        + "(using the CLI parameter \"--props\") can also be passed command line using this parameter. This can be repeated",
        splitter = IdentitySplitter.class)
    public List<String> configs = new ArrayList<>();
  }

  public static void main(String[] args) {
    final HoodieTTLJob.Config cfg = new HoodieTTLJob.Config();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      throw new HoodieException("Failed to run ttl for " + cfg.basePath);
    }

    String dirName = new Path(cfg.basePath).getName();
    JavaSparkContext jssc = UtilHelpers.buildSparkContext("hoodie-ttl-job-" + dirName, cfg.sparkMaster);

    int exitCode = 0;
    try {
      new HoodieTTLJob(jssc, cfg).run();
    } catch (Throwable throwable) {
      exitCode = 1;
      throw new HoodieException("Failed to run ttl for " + cfg.basePath, throwable);
    } finally {
      SparkAdapterSupport$.MODULE$.sparkAdapter().stopSparkContext(jssc, exitCode);
    }

    LOG.info("Hoodie TTL job ran successfully");
  }

}
