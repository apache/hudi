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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.util.Option;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Utility Class to generate Spark Scheduling allocation file. This kicks in only when user sets
 * spark.scheduler.mode=FAIR at spark-submit time
 */
public class SchedulerConfGenerator {

  protected static volatile Logger log = LogManager.getLogger(SchedulerConfGenerator.class);

  public static final String DELTASYNC_POOL_NAME = "hoodiedeltasync";
  public static final String COMPACT_POOL_NAME = "hoodiecompact";
  public static final String SPARK_SCHEDULER_MODE_KEY = "spark.scheduler.mode";
  public static final String SPARK_SCHEDULER_ALLOCATION_FILE_KEY = "spark.scheduler.allocation.file";

  private static String SPARK_SCHEDULING_PATTERN =
      "<?xml version=\"1.0\"?>\n" + "<allocations>\n" + "  <pool name=\"%s\">\n"
          + "    <schedulingMode>%s</schedulingMode>\n" + "    <weight>%s</weight>\n" + "    <minShare>%s</minShare>\n"
          + "  </pool>\n" + "  <pool name=\"%s\">\n" + "    <schedulingMode>%s</schedulingMode>\n"
          + "    <weight>%s</weight>\n" + "    <minShare>%s</minShare>\n" + "  </pool>\n" + "</allocations>";

  private static String generateConfig(Integer deltaSyncWeight, Integer compactionWeight, Integer deltaSyncMinShare,
      Integer compactionMinShare) {
    return String.format(SPARK_SCHEDULING_PATTERN, DELTASYNC_POOL_NAME, "FAIR", deltaSyncWeight.toString(),
        deltaSyncMinShare.toString(), COMPACT_POOL_NAME, "FAIR", compactionWeight.toString(),
        compactionMinShare.toString());
  }

  /**
   * Helper to set Spark Scheduling Configs dynamically.
   *
   * @param cfg Config
   */
  public static Map<String, String> getSparkSchedulingConfigs(HoodieDeltaStreamer.Config cfg) throws Exception {
    scala.Option<String> scheduleModeKeyOption = new SparkConf().getOption(SPARK_SCHEDULER_MODE_KEY);
    final Option<String> sparkSchedulerMode =
        scheduleModeKeyOption.isDefined() ? Option.of(scheduleModeKeyOption.get()) : Option.empty();

    Map<String, String> additionalSparkConfigs = new HashMap<>();
    if (sparkSchedulerMode.isPresent() && "FAIR".equals(sparkSchedulerMode.get()) && cfg.continuousMode
        && cfg.storageType.equals(HoodieTableType.MERGE_ON_READ.name())) {
      String sparkSchedulingConfFile = generateAndStoreConfig(cfg.deltaSyncSchedulingWeight,
          cfg.compactSchedulingWeight, cfg.deltaSyncSchedulingMinShare, cfg.compactSchedulingMinShare);
      additionalSparkConfigs.put(SPARK_SCHEDULER_ALLOCATION_FILE_KEY, sparkSchedulingConfFile);
    } else {
      log.warn("Job Scheduling Configs will not be in effect as spark.scheduler.mode "
          + "is not set to FAIR at instatiation time. Continuing without scheduling configs");
    }
    return additionalSparkConfigs;
  }

  private static String generateAndStoreConfig(Integer deltaSyncWeight, Integer compactionWeight,
      Integer deltaSyncMinShare, Integer compactionMinShare) throws IOException {
    File tempConfigFile = File.createTempFile(UUID.randomUUID().toString(), ".xml");
    BufferedWriter bw = new BufferedWriter(new FileWriter(tempConfigFile));
    bw.write(generateConfig(deltaSyncWeight, compactionWeight, deltaSyncMinShare, compactionMinShare));
    bw.close();
    log.info("Configs written to file" + tempConfigFile.getAbsolutePath());
    return tempConfigFile.getAbsolutePath();
  }
}
