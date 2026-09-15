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

import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.util.ClientIds;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.SchedulerExecutionMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link OptionsInference}.
 */
public class TestOptionsInference {
  @TempDir
  File tempFile;

  @Test
  void testSetupSourceAndSinkTasks() {
    Configuration conf = new Configuration();

    OptionsInference.setupSourceTasks(conf, 3);
    OptionsInference.setupSinkTasks(conf, 4);

    assertEquals(3, conf.get(FlinkOptions.READ_TASKS));
    assertEquals(4, conf.get(FlinkOptions.WRITE_TASKS));
    assertEquals(4, conf.get(FlinkOptions.BUCKET_ASSIGN_TASKS));
    assertEquals(4, conf.get(FlinkOptions.COMPACTION_TASKS));
    assertEquals(4, conf.get(FlinkOptions.CLUSTERING_TASKS));
    assertEquals(4, conf.get(FlinkOptions.INDEX_WRITE_TASKS));

    conf.set(FlinkOptions.READ_TASKS, 7);
    conf.set(FlinkOptions.WRITE_TASKS, 8);
    conf.set(FlinkOptions.BUCKET_ASSIGN_TASKS, 9);
    conf.set(FlinkOptions.COMPACTION_TASKS, 10);
    conf.set(FlinkOptions.CLUSTERING_TASKS, 11);
    conf.set(FlinkOptions.INDEX_WRITE_TASKS, 12);

    OptionsInference.setupSourceTasks(conf, 20);
    OptionsInference.setupSinkTasks(conf, 20);

    assertEquals(7, conf.get(FlinkOptions.READ_TASKS));
    assertEquals(8, conf.get(FlinkOptions.WRITE_TASKS));
    assertEquals(9, conf.get(FlinkOptions.BUCKET_ASSIGN_TASKS));
    assertEquals(10, conf.get(FlinkOptions.COMPACTION_TASKS));
    assertEquals(11, conf.get(FlinkOptions.CLUSTERING_TASKS));
    assertEquals(12, conf.get(FlinkOptions.INDEX_WRITE_TASKS));
  }

  @Test
  void testSetupRuntimeConfigurations() {
    Configuration conf = new Configuration();
    conf.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.AdaptiveBatch);
    Configuration runtimeConf = new Configuration();
    runtimeConf.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);

    OptionsInference.setupRuntimeConfigs(conf, runtimeConf);

    if (FlinkVersion.current().toString().compareTo("2.0") >= 0) {
      assertTrue(conf.get(FlinkOptions.WRITE_INCREMENTAL_JOB_GRAPH_GENERATION));
    } else {
      assertFalse(conf.get(FlinkOptions.WRITE_INCREMENTAL_JOB_GRAPH_GENERATION));
    }

    conf.set(FlinkOptions.WRITE_INCREMENTAL_JOB_GRAPH_GENERATION, false);
    runtimeConf.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.STREAMING);
    OptionsInference.setupRuntimeConfigs(conf, runtimeConf);
    assertFalse(conf.get(FlinkOptions.WRITE_INCREMENTAL_JOB_GRAPH_GENERATION));
  }

  @Test
  void testSchedulerTypeResolutionThroughRuntimeSetup() {
    Configuration runtimeConf = new Configuration();
    runtimeConf.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
    boolean isFlink2 = FlinkVersion.current().toString().compareTo("2.0") >= 0;

    Configuration reactive = new Configuration();
    reactive.set(JobManagerOptions.SCHEDULER_MODE, SchedulerExecutionMode.REACTIVE);
    OptionsInference.setupRuntimeConfigs(reactive, runtimeConf);
    assertEquals(isFlink2,
        reactive.get(FlinkOptions.WRITE_INCREMENTAL_JOB_GRAPH_GENERATION));

    Configuration adaptive = new Configuration();
    adaptive.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);
    OptionsInference.setupRuntimeConfigs(adaptive, runtimeConf);
    assertEquals(isFlink2,
        adaptive.get(FlinkOptions.WRITE_INCREMENTAL_JOB_GRAPH_GENERATION));

    Configuration defaultScheduler = new Configuration();
    defaultScheduler.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Default);
    OptionsInference.setupRuntimeConfigs(defaultScheduler, runtimeConf);
    assertFalse(defaultScheduler.get(FlinkOptions.WRITE_INCREMENTAL_JOB_GRAPH_GENERATION));
  }

  @Test
  void testSetupClientId() throws Exception {
    Configuration conf = getConf();
    conf.set(FlinkOptions.WRITE_CLIENT_ID, "2");
    OptionsInference.setupClientId(conf);
    assertThat("Explicit client id has higher priority",
        conf.get(FlinkOptions.WRITE_CLIENT_ID), is("2"));

    for (int i = 0; i < 3; i++) {
      conf = getConf();
      try (ClientIds clientIds = ClientIds.builder().conf(conf).build()) {
        OptionsInference.setupClientId(conf);
        String expectedId = i == 0 ? ClientIds.INIT_CLIENT_ID : i + "";
        assertThat("The client id should auto inc to " + expectedId,
            conf.get(FlinkOptions.WRITE_CLIENT_ID), is(expectedId));
      }
    }

    // sleep 1 second to simulate a zombie heartbeat
    Thread.sleep(1000);
    conf = getConf();
    try (ClientIds clientIds = ClientIds.builder()
        .conf(conf)
        .heartbeatIntervalInMs(10) // max 10 milliseconds tolerable heartbeat timeout
        .numTolerableHeartbeatMisses(1). build()) {
      String nextId = clientIds.nextId(conf);
      assertThat("The inactive client id should be reused",
          nextId, is(""));
    }
  }

  private Configuration getConf() {
    Configuration conf = new Configuration();
    conf.setString(HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key(), WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL.name());
    conf.set(FlinkOptions.PATH, tempFile.getAbsolutePath());
    return conf;
  }

  @Test
  void testClientIdAndIndexSetupAreNoOpsWhenNotApplicable() {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.PATH, tempFile.getAbsolutePath());
    conf.set(FlinkOptions.INDEX_TYPE, "BLOOM");

    OptionsInference.setupClientId(conf);
    OptionsInference.setupIndexConfigs(conf);

    assertFalse(conf.contains(FlinkOptions.WRITE_CLIENT_ID));
    assertFalse(conf.contains(FlinkOptions.BUCKET_INDEX_PARTITION_EXPRESSIONS));
  }
}
