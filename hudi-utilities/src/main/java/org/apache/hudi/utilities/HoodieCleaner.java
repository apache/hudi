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
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.config.HoodieWriteConfig;
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

public class HoodieCleaner {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieCleaner.class);

  /**
   * Config for Cleaner.
   */
  private final Config cfg;

  /**
   * Spark context.
   */
  private transient JavaSparkContext jssc;

  /**
   * Bag of properties with source, hoodie client, key generator etc.
   */
  private final TypedProperties props;

  public HoodieCleaner(Config cfg, JavaSparkContext jssc) {
    this(cfg, jssc, UtilHelpers.buildProperties(jssc.hadoopConfiguration(), cfg.propsFilePath, cfg.configs));
  }

  public HoodieCleaner(Config cfg, JavaSparkContext jssc, TypedProperties props) {
    this.cfg = cfg;
    this.jssc = jssc;
    this.props = props;
    LOG.info("Creating Cleaner with configs : " + props.toString());
  }

  public void run() {
    HoodieWriteConfig hoodieCfg = getHoodieClientConfig();
    try (SparkRDDWriteClient client = new SparkRDDWriteClient<>(new HoodieSparkEngineContext(jssc), hoodieCfg)) {
      client.clean();
    }
  }

  private HoodieWriteConfig getHoodieClientConfig() {
    return HoodieWriteConfig.newBuilder().combineInput(true, true).withPath(cfg.basePath)
        .withProps(props).build();
  }

  public static class Config implements Serializable {

    @Parameter(names = {"--target-base-path"}, description = "base path for the hoodie table to be cleaner.",
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

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;
  }

  public static void main(String[] args) {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      throw new HoodieException("Failed to run cleaning for " + cfg.basePath);
    }

    String dirName = new Path(cfg.basePath).getName();
    JavaSparkContext jssc = UtilHelpers.buildSparkContext("hoodie-cleaner-" + dirName, cfg.sparkMaster);

    int exitCode = 0;
    try {
      new HoodieCleaner(cfg, jssc).run();
    } catch (Throwable throwable) {
      exitCode = 1;
      throw new HoodieException("Failed to run cleaning for " + cfg.basePath, throwable);
    } finally {
      SparkAdapterSupport$.MODULE$.sparkAdapter().stopSparkContext(jssc, exitCode);
    }

    LOG.info("Cleaner ran successfully");
  }
}
