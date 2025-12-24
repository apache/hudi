/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.configuration;

import org.apache.flink.configuration.Configuration;

/**
 * Tool helping to infer the flink options {@link FlinkOptions}.
 */
public class OptionsInference {

  /**
   * Sets up the default source task parallelism if it is not specified.
   *
   * @param conf     The configuration
   * @param envTasks The parallelism of the execution env
   */
  public static void setupSourceTasks(Configuration conf, int envTasks) {
    if (!conf.contains(FlinkOptions.READ_TASKS)) {
      conf.setInteger(FlinkOptions.READ_TASKS, envTasks);
    }
  }

  /**
   * Sets up the default sink tasks parallelism if it is not specified.
   *
   * @param conf     The configuration
   * @param envTasks The parallelism of the execution env
   */
  public static void setupSinkTasks(Configuration conf, int envTasks) {
    // write task number, default same as execution env tasks
    if (!conf.contains(FlinkOptions.WRITE_TASKS)) {
      conf.setInteger(FlinkOptions.WRITE_TASKS, envTasks);
    }
    int writeTasks = conf.getInteger(FlinkOptions.WRITE_TASKS);
    // bucket assign tasks, default same as write tasks
    if (!conf.contains(FlinkOptions.BUCKET_ASSIGN_TASKS)) {
      conf.setInteger(FlinkOptions.BUCKET_ASSIGN_TASKS, writeTasks);
    }
    // compaction tasks, default same as write tasks
    if (!conf.contains(FlinkOptions.COMPACTION_TASKS)) {
      conf.setInteger(FlinkOptions.COMPACTION_TASKS, writeTasks);
    }
    // clustering tasks, default same as write tasks
    if (!conf.contains(FlinkOptions.CLUSTERING_TASKS)) {
      conf.setInteger(FlinkOptions.CLUSTERING_TASKS, writeTasks);
    }
    // lsm clustering tasks, default same as write tasks
    if (!conf.contains(FlinkOptions.LSM_CLUSTERING_TASKS)) {
      conf.setInteger(FlinkOptions.LSM_CLUSTERING_TASKS, writeTasks);
    }
    if (!conf.contains(FlinkOptions.CLUSTERING_READ_FOOTER_TASKS)) {
      conf.setInteger(FlinkOptions.CLUSTERING_READ_FOOTER_TASKS, writeTasks);
    }
    if (!conf.contains(FlinkOptions.LSM_CLUSTERING_LIST_TASKS)) {
      conf.setInteger(FlinkOptions.LSM_CLUSTERING_LIST_TASKS, writeTasks);
    }
  }
}
