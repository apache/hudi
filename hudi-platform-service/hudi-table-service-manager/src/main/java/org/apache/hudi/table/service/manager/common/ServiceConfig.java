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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Properties;

public class ServiceConfig extends Properties {

  private static Logger LOG = LogManager.getLogger(ServiceConfig.class);
  private static final String HOODIE_ENV_PROPS_PREFIX = "hoodie_table_management_";

  private static ServiceConfig CONFIG = new ServiceConfig();

  /**
   * Constructor.
   */
  private ServiceConfig() {
    LOG.info("Start init ServiceConfig");
    Map<String, String> envs = System.getenv();
    for (Map.Entry<String, String> env : envs.entrySet()) {
      if (env.getKey().toLowerCase().startsWith(HOODIE_ENV_PROPS_PREFIX)) {
        String key = env.getKey().toLowerCase().replaceAll("_", ".");
        String value = env.getValue().trim();
        setProperty(key, value);
        LOG.info("Set property " + key + " to " + value);
      }
    }
    LOG.info("Finish init ServiceConfig");
  }

  public String getString(ServiceConfVars confVars) {
    return this.getProperty(confVars.key(), confVars.defVal());
  }

  public void setString(ServiceConfVars confVars, String value) {
    this.setProperty(confVars.key(), value);
  }

  public Boolean getBool(ServiceConfVars confVars) {
    return Boolean.valueOf(this.getProperty(confVars.key(), confVars.defVal()));
  }

  public int getInt(ServiceConfVars confVars) {
    return Integer.parseInt(this.getProperty(confVars.key(), confVars.defVal()));
  }

  public static ServiceConfig getInstance() {
    return CONFIG;
  }

  public enum ServiceConfVars {
    CompactionScheduleWaitInterval("hoodie.table.management.schedule.wait.interval", "30000"),
    MaxFailTolerance("hoodie.table.management.max.fail.tolerance", "5"),
    MaxRetryNum("hoodie.table.management.instance.max.retry", "3"),
    MetadataStoreClass("hoodie.table.management.metadata.store.class",
        "org.apache.hudi.table.management.store.impl.RelationDBBasedStore"),
    CompactionCacheEnable("hoodie.table.management.compaction.cache.enable", "true"),
    SubmitJobTimeoutSec("hoodie.table.management.instance.submit.timeout.seconds", "600"),
    SparkSubmitJarPath("hoodie.table.management.submit.jar.path", "/tmp/hoodie_submit_jar/spark/"),
    SparkParallelism("hoodie.table.management.spark.parallelism", "100"),
    SparkMaster("hoodie.table.management.spark.master", "yarn"),
    SparkSpeculation("hoodie.table.management.spark.speculation", "false"),
    ExecutorMemory("hoodie.table.management.executor.memory", "4g"),
    DriverMemory("hoodie.table.management.driver.memory", "2g"),
    ExecutorMemoryOverhead("hoodie.table.management.executor.memory.overhead", "200m"),
    ExecutorCores("hoodie.table.management.executor.cores", "1"),
    MinExecutors("hoodie.table.management.min.executors", "1"),
    MaxExecutors("hoodie.table.management.max.executors", "10"),
    CoreExecuteSize("hoodie.table.management.core.executor.pool.size", "300"),
    MaxExecuteSize("hoodie.table.management.max.executor.pool.size", "1000");

    private final String key;
    private final String defaultVal;

    ServiceConfVars(String key, String defaultVal) {
      this.key = key;
      this.defaultVal = defaultVal;
    }

    public String key() {
      return this.key;
    }

    public String defVal() {
      return this.defaultVal;
    }
  }

}
