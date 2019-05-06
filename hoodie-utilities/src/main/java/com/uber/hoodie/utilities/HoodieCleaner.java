/*
 * Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.utilities;

import com.uber.hoodie.HoodieWriteClient;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.TypedProperties;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.configs.HoodieCleanerJobConfig;
import com.uber.hoodie.utilities.deltastreamer.HoodieDeltaStreamer;
import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

public class HoodieCleaner {

  private static volatile Logger log = LogManager.getLogger(HoodieDeltaStreamer.class);

  /**
   * Config for Cleaner
   */
  private final HoodieCleanerJobConfig cfg;

  /**
   * Filesystem used
   */
  private transient FileSystem fs;

  /**
   * Spark context
   */
  private transient JavaSparkContext jssc;

  /**
   * Bag of properties with source, hoodie client, key generator etc.
   */
  TypedProperties props;

  public HoodieCleaner(HoodieCleanerJobConfig cfg, JavaSparkContext jssc) throws IOException {
    this.cfg = cfg;
    this.jssc = jssc;
    this.fs = FSUtils.getFs(cfg.basePath, jssc.hadoopConfiguration());

    this.props = UtilHelpers.readConfig(fs, new Path(cfg.propsFilePath), cfg.configs).getConfig();
    log.info("Creating Cleaner with configs : " + props.toString());
  }

  public void run() throws Exception {
    HoodieWriteConfig hoodieCfg = getHoodieClientConfig();
    HoodieWriteClient client = new HoodieWriteClient<>(jssc, hoodieCfg, false);
    client.clean();
  }

  private HoodieWriteConfig getHoodieClientConfig() throws Exception {
    return HoodieWriteConfig.newBuilder().combineInput(true, true).withPath(cfg.basePath)
        .withAutoCommit(false)
        .withProps(props).build();
  }

  public static void main(String[] args) throws Exception {
    final HoodieCleanerJobConfig cfg = new HoodieCleanerJobConfig();
    cfg.parseJobConfig(args, true);

    String dirName = new Path(cfg.basePath).getName();
    JavaSparkContext jssc = UtilHelpers.buildSparkContext("hoodie-cleaner-" + dirName, cfg.sparkMaster);
    new HoodieCleaner(cfg, jssc).run();
  }
}
