/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.multitable;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.spark.api.java.JavaSparkContext;

import static org.apache.hudi.config.HoodieCompactionConfig.COMPACTION_STRATEGY;

/**
 * Wrapper class for {@link TableServiceTask}.
 * As TableServiceTask is protected, this is used for exposing a static method to create a task
 * and provide a run method to run the task.
 */
public class TableServiceTaskWrapper {

  private TableServiceTask task;

  public TableServiceTaskWrapper(TableServiceTask task) {
    this.task = task;
  }

  public static TableServiceTaskWrapper compactionTaskBuilder(String runningMode, JavaSparkContext jsc, String basePath, TypedProperties props) {
    return new TableServiceTaskWrapper(
        CompactionTask.newBuilder()
            .withJsc(jsc)
            .withBasePath(basePath)
            .withParallelism(1)
            .withCompactionRunningMode(runningMode)
            .withCompactionStrategyName(props.getString(COMPACTION_STRATEGY.key(), COMPACTION_STRATEGY.defaultValue()))
            .withProps(props)
            .withRetry(1)
            .build()
    );
  }

  public static TableServiceTaskWrapper clusteringTaskBuilder(String runningMode, JavaSparkContext jsc, String basePath, TypedProperties props) {
    return new TableServiceTaskWrapper(
        ClusteringTask.newBuilder()
            .withJsc(jsc)
            .withBasePath(basePath)
            .withParallelism(1)
            .withClusteringRunningMode(runningMode)
            .withProps(props)
            .withRetry(1)
            .build()
    );
  }

  public static TableServiceTaskWrapper cleanTaskBuilder(JavaSparkContext jsc, String basePath, TypedProperties props) {
    return new TableServiceTaskWrapper(
        CleanTask.newBuilder()
            .withJsc(jsc)
            .withBasePath(basePath)
            .withProps(props)
            .withRetry(1)
            .build()
    );
  }

  public static TableServiceTaskWrapper archiveTaskBuilder(JavaSparkContext jsc, String basePath, TypedProperties props) {
    return new TableServiceTaskWrapper(
        ArchiveTask.newBuilder()
            .withJsc(jsc)
            .withBasePath(basePath)
            .withProps(props)
            .withRetry(1)
            .build()
    );
  }

  public void run() {
    task.run();
  }
}
