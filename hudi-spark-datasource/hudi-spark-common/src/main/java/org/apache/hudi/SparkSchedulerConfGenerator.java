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

package org.apache.hudi;

import org.apache.hudi.common.config.TypedProperties;
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

import static org.apache.hudi.SparkSchedulerConfigs.COMPACT_POOL_NAME;
import static org.apache.hudi.SparkSchedulerConfigs.SPARK_SCHEDULER_ALLOCATION_FILE_KEY;
import static org.apache.hudi.SparkSchedulerConfigs.SPARK_SCHEDULER_FAIR_MODE;
import static org.apache.hudi.SparkSchedulerConfigs.SPARK_SCHEDULER_MODE_KEY;
import static org.apache.hudi.SparkSchedulerConfigs.SPARK_SCHEDULING_PATTERN;

/**
 * Utility Class to generate Spark Scheduling allocation file. This kicks in only when user sets
 * spark.scheduler.mode=FAIR at spark-submit time
 */
public class SparkSchedulerConfGenerator {

  private static final Logger LOG = LogManager.getLogger(SparkSchedulerConfGenerator.class);

  /**
   * Helper to generate spark scheduling configs in XML format with input params.
   *
   * @param sparkDataSourceWritesWeight   Scheduling weight for spark data source write
   * @param compactionWeight              Scheduling weight for compaction
   * @param sparkDatasourceWritesMinShare Minshare for spark data source writes
   * @param compactionMinShare            Minshare for compaction
   * @return Spark scheduling configs
   */
  private static String generateConfig(Integer sparkDataSourceWritesWeight, Integer compactionWeight, Integer sparkDatasourceWritesMinShare,
                                       Integer compactionMinShare) {
    return String.format(SPARK_SCHEDULING_PATTERN, DataSourceWriteOptions.SPARK_DATASOURCE_WRITER_POOL_NAME(), SPARK_SCHEDULER_FAIR_MODE,
        sparkDataSourceWritesWeight.toString(), sparkDatasourceWritesMinShare.toString(), COMPACT_POOL_NAME, SPARK_SCHEDULER_FAIR_MODE,
        compactionWeight.toString(), compactionMinShare.toString());
  }

  /**
   * Helper to set Spark Scheduling Configs dynamically.
   */
  public static Map<String, String> getSparkSchedulingConfigs(TypedProperties typedProperties, HoodieTableType tableType, SparkConf sparkConf) throws Exception {
    scala.Option<String> scheduleModeKeyOption = sparkConf.getOption(SPARK_SCHEDULER_MODE_KEY);
    final Option<String> sparkSchedulerMode =
        scheduleModeKeyOption.isDefined() ? Option.of(scheduleModeKeyOption.get()) : Option.empty();

    Map<String, String> additionalSparkConfigs = new HashMap<>(1);
    if (sparkSchedulerMode.isPresent() && SPARK_SCHEDULER_FAIR_MODE.equals(sparkSchedulerMode.get()) && tableType == HoodieTableType.MERGE_ON_READ) {
      String sparkSchedulingConfFile = generateAndStoreConfig(typedProperties.getInteger(DataSourceWriteOptions.SPARK_DATASOURCE_WRITES_SCHEDULING_WEIGHT().key(),
              (Integer) DataSourceWriteOptions.SPARK_DATASOURCE_WRITES_SCHEDULING_WEIGHT().defaultValue()),
          typedProperties.getInteger(DataSourceWriteOptions.COMPACTION_SCHEDULING_WEIGHT().key(),
              (Integer) DataSourceWriteOptions.COMPACTION_SCHEDULING_WEIGHT().defaultValue()),
          typedProperties.getInteger(DataSourceWriteOptions.SPARK_DATASOURCE_WRITES_SCHEDULING_MIN_SHARE().key(),
              (Integer) DataSourceWriteOptions.SPARK_DATASOURCE_WRITES_SCHEDULING_MIN_SHARE().defaultValue()),
          typedProperties.getInteger(DataSourceWriteOptions.COMPACTION_SCHEDULING_MIN_SHARE().key(),
              (Integer) DataSourceWriteOptions.COMPACTION_SCHEDULING_MIN_SHARE().defaultValue()));
      additionalSparkConfigs.put(SPARK_SCHEDULER_ALLOCATION_FILE_KEY, sparkSchedulingConfFile);
    } else {
      LOG.warn("Job Scheduling Configs will not be in effect as spark.scheduler.mode "
          + "is not set to FAIR at instantiation time. Continuing without scheduling configs");
    }
    return additionalSparkConfigs;
  }

  /**
   * Generate spark scheduling configs and store it to a randomly generated tmp file.
   *
   * @param sparkDataSourceWritesWeight   Scheduling weight for delta sync
   * @param compactionWeight              Scheduling weight for compaction
   * @param sparkDataSourceWritesMinShare Minshare for delta sync
   * @param compactionMinShare            Minshare for compaction
   * @return Return the absolute path of the tmp file which stores the spark schedule configs
   * @throws IOException Throws an IOException when write configs to file failed
   */
  private static String generateAndStoreConfig(Integer sparkDataSourceWritesWeight, Integer compactionWeight,
                                               Integer sparkDataSourceWritesMinShare, Integer compactionMinShare) throws IOException {
    File tempConfigFile = File.createTempFile(UUID.randomUUID().toString(), ".xml");
    BufferedWriter bw = new BufferedWriter(new FileWriter(tempConfigFile));
    bw.write(generateConfig(sparkDataSourceWritesWeight, compactionWeight, sparkDataSourceWritesMinShare, compactionMinShare));
    bw.close();
    LOG.info("Configs written to file" + tempConfigFile.getAbsolutePath());
    return tempConfigFile.getAbsolutePath();
  }
}
