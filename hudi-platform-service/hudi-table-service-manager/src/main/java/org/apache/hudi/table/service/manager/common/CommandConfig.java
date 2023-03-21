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

package org.apache.hudi.table.service.manager.common;

import com.beust.jcommander.Parameter;

public class CommandConfig {
  @Parameter(names = {"--server-port", "-p"}, description = "Server Port")
  public Integer serverPort = 9092;

  @Parameter(names = {"-schedule-interval-ms"}, description = "Schedule Interval Ms")
  public Long scheduleIntervalMs = 30000L;

  @Parameter(names = {"-schedule-core-executor-size"}, description = "Schedule Core Execute Size")
  public Integer scheduleCoreExecuteSize = 300;

  @Parameter(names = {"-schedule-max-executor-size"}, description = "Schedule Max Execute Size")
  public Integer scheduleMaxExecuteSize = 1000;

  @Parameter(names = {"-metadata-store-class"}, description = "Metadata Store Class")
  public String metadataStoreClass = "org.apache.hudi.table.service.manager.store.impl.RelationDBBasedStore";

  @Parameter(names = {"-instance-cache-enable"}, description = "Instance Cache Enable")
  public boolean instanceCacheEnable = true;

  @Parameter(names = {"-instance-max-retry-num"}, description = "Instance Max Retry Num")
  public Integer instanceMaxRetryNum = 3;

  @Parameter(names = {"-instance-submit-timeout-sec"}, description = "Instance Submit Timeout Sec")
  public Integer instanceSubmitTimeoutSec = 600;

  @Parameter(names = {"-spark-submit-jar-path"}, description = "Spark Submit Jar Path")
  public String sparkSubmitJarPath;

  @Parameter(names = {"-spark-parallelism"}, description = "Spark Parallelism")
  public Integer sparkParallelism = 100;

  @Parameter(names = {"-spark-master"}, description = "Spark Master")
  public String sparkMaster = "yarn";

  @Parameter(names = {"-spark-executor-memory"}, description = "Spark Executor Memory")
  public String sparkExecutorMemory = "4g";

  @Parameter(names = {"-spark-driver-memory"}, description = "Spark Driver Memory")
  public String sparkDriverMemory = "2g";

  @Parameter(names = {"-spark-executor-memory-overhead"}, description = "Spark Executor Memory Overhead")
  public String sparkExecutorMemoryOverhead = "200m";

  @Parameter(names = {"-spark-executor-cores"}, description = "Spark Executor Cores")
  public Integer sparkExecutorCores = 1;

  @Parameter(names = {"-spark-min-executors"}, description = "Spark Min Executors")
  public Integer sparkMinExecutors = 1;

  @Parameter(names = {"-spark-max-executors"}, description = "Spark Max Executors")
  public Integer sparkMaxExecutors = 100;

  @Parameter(names = {"--help", "-h"})
  public Boolean help = false;

  public static HoodieTableServiceManagerConfig toTableServiceManagerConfig(CommandConfig config) {
    return HoodieTableServiceManagerConfig.newBuilder()
        .withScheduleIntervalMs(config.scheduleIntervalMs)
        .withScheduleCoreExecuteSize(config.scheduleCoreExecuteSize)
        .withScheduleMaxExecuteSize(config.scheduleMaxExecuteSize)
        .withMetadataStoreClass(config.metadataStoreClass)
        .withInstanceCacheEnable(config.instanceCacheEnable)
        .withInstanceMaxRetryNum(config.instanceMaxRetryNum)
        .withInstanceSubmitTimeoutSec(config.instanceSubmitTimeoutSec)
        .withSparkSubmitJarPath(config.sparkSubmitJarPath)
        .withSparkParallelism(config.sparkParallelism)
        .withSparkMaster(config.sparkMaster)
        .withSparkExecutorMemory(config.sparkExecutorMemory)
        .withSparkDriverMemory(config.sparkDriverMemory)
        .withSparkExecutorMemoryOverhead(config.sparkExecutorMemoryOverhead)
        .withSparkExecutorCores(config.sparkExecutorCores)
        .withSparkMinExecutors(config.sparkMinExecutors)
        .withSparkMaxExecutors(config.sparkMaxExecutors)
        .build();
  }
}
