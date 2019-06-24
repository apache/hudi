/*
 *  Copyright (c) 2019 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.uber.hoodie.utilities.deltastreamer;

import com.uber.hoodie.common.model.HoodieTableType;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import scala.Option;

/**
 * Utility Class to generate Spark Scheduling allocation file. This kicks in only when user
 * sets spark.scheduler.mode=FAIR at spark-submit time
 */
public class SchedulerConfGenerator {

  protected static volatile Logger log = LogManager.getLogger(SchedulerConfGenerator.class);

  public static final String DELTASYNC_POOL_NAME = "hoodiedeltasync";
  public static final String COMPACT_POOL_NAME = "hoodiecompact";
  public static final String SPARK_SCHEDULER_MODE_KEY = "spark.scheduler.mode";
  public static final String SPARK_SCHEDULER_ALLOCATION_FILE_KEY = "spark.scheduler.allocation.file";


  private static final String DELTASYNC_POOL_KEY = "deltasync_pool";
  private static final String COMPACT_POOL_KEY = "compact_pool";
  private static final String DELTASYNC_POLICY_KEY = "deltasync_policy";
  private static final String COMPACT_POLICY_KEY = "compact_policy";
  private static final String DELTASYNC_WEIGHT_KEY = "deltasync_weight";
  private static final String DELTASYNC_MINSHARE_KEY = "deltasync_minshare";
  private static final String COMPACT_WEIGHT_KEY = "compact_weight";
  private static final String COMPACT_MINSHARE_KEY = "compact_minshare";

  private static String SPARK_SCHEDULING_PATTERN =
      "<?xml version=\"1.0\"?>\n"
          + "<allocations>\n"
          + "  <pool name=\"%(deltasync_pool)\">\n"
          + "    <schedulingMode>%(deltasync_policy)</schedulingMode>\n"
          + "    <weight>%(deltasync_weight)</weight>\n"
          + "    <minShare>%(deltasync_minshare)</minShare>\n"
          + "  </pool>\n"
          + "  <pool name=\"%(compact_pool)\">\n"
          + "    <schedulingMode>%(compact_policy)</schedulingMode>\n"
          + "    <weight>%(compact_weight)</weight>\n"
          + "    <minShare>%(compact_minshare)</minShare>\n"
          + "  </pool>\n"
          + "</allocations>";

  private static String generateConfig(Integer deltaSyncWeight, Integer compactionWeight, Integer deltaSyncMinShare,
      Integer compactionMinShare) {
    Map<String, String> schedulingProps = new HashMap<>();
    schedulingProps.put(DELTASYNC_POOL_KEY, DELTASYNC_POOL_NAME);
    schedulingProps.put(COMPACT_POOL_KEY, COMPACT_POOL_NAME);
    schedulingProps.put(DELTASYNC_POLICY_KEY, "FAIR");
    schedulingProps.put(COMPACT_POLICY_KEY, "FAIR");
    schedulingProps.put(DELTASYNC_WEIGHT_KEY, deltaSyncWeight.toString());
    schedulingProps.put(DELTASYNC_MINSHARE_KEY, deltaSyncMinShare.toString());
    schedulingProps.put(COMPACT_WEIGHT_KEY, compactionWeight.toString());
    schedulingProps.put(COMPACT_MINSHARE_KEY, compactionMinShare.toString());

    StrSubstitutor sub = new StrSubstitutor(schedulingProps, "%(", ")");
    String xmlString = sub.replace(SPARK_SCHEDULING_PATTERN);
    log.info("Scheduling Configurations generated. Config=\n" + xmlString);
    return xmlString;
  }


  /**
   * Helper to set Spark Scheduling Configs dynamically
   *
   * @param cfg Config
   */
  public static Map<String, String> getSparkSchedulingConfigs(HoodieDeltaStreamer.Config cfg) throws Exception {
    final Option<String> sparkSchedulerMode = new SparkConf().getOption(SPARK_SCHEDULER_MODE_KEY);

    Map<String, String> additionalSparkConfigs = new HashMap<>();
    if (sparkSchedulerMode.isDefined() && "FAIR".equals(sparkSchedulerMode.get())
        && cfg.continuousMode && cfg.storageType.equals(HoodieTableType.MERGE_ON_READ.name())) {
      String sparkSchedulingConfFile = generateAndStoreConfig(cfg.deltaSyncSchedulingWeight,
          cfg.compactSchedulingWeight, cfg.deltaSyncSchedulingMinShare, cfg.compactSchedulingMinShare);
      additionalSparkConfigs.put(SPARK_SCHEDULER_ALLOCATION_FILE_KEY, sparkSchedulingConfFile);
    } else {
      log.warn("Job Scheduling Configs will not be in effect as spark.scheduler.mode "
          + "is not set to FAIR at instatiation time. Continuing without scheduling configs");
    }
    return additionalSparkConfigs;
  }

  private static String generateAndStoreConfig(Integer deltaSyncWeight,
                                               Integer compactionWeight,
                                               Integer deltaSyncMinShare,
                                               Integer compactionMinShare) throws IOException {
    File tempConfigFile = File.createTempFile(UUID.randomUUID().toString(), ".xml");
    BufferedWriter bw = new BufferedWriter(new FileWriter(tempConfigFile));
    bw.write(generateConfig(deltaSyncWeight, compactionWeight, deltaSyncMinShare, compactionMinShare));
    bw.close();
    log.info("Configs written to file" + tempConfigFile.getAbsolutePath());
    return tempConfigFile.getAbsolutePath();
  }
}
