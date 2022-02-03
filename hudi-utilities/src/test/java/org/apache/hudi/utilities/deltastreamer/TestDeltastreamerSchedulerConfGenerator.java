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

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.apache.hudi.SparkSchedulerConfigs.SPARK_SCHEDULER_ALLOCATION_FILE_KEY;
import static org.apache.hudi.SparkSchedulerConfigs.SPARK_SCHEDULER_MODE_KEY;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestDeltastreamerSchedulerConfGenerator {

  @Test
  public void testGenerateSparkSchedulingConf() throws Exception {
    HoodieDeltaStreamer.Config cfg = new HoodieDeltaStreamer.Config();
    Map<String, String> configs = DeltastreamerSchedulerConfGenerator.getSparkSchedulingConfigs(cfg);
    assertNull(configs.get(SPARK_SCHEDULER_ALLOCATION_FILE_KEY), "spark.scheduler.mode not set");

    System.setProperty(SPARK_SCHEDULER_MODE_KEY, "FAIR");
    cfg.continuousMode = false;
    configs = DeltastreamerSchedulerConfGenerator.getSparkSchedulingConfigs(cfg);
    assertNull(configs.get(SPARK_SCHEDULER_ALLOCATION_FILE_KEY), "continuousMode is false");

    cfg.continuousMode = true;
    cfg.tableType = HoodieTableType.COPY_ON_WRITE.name();
    configs = DeltastreamerSchedulerConfGenerator.getSparkSchedulingConfigs(cfg);
    assertNull(configs.get(SPARK_SCHEDULER_ALLOCATION_FILE_KEY),
        "table type is not MERGE_ON_READ");

    cfg.tableType = HoodieTableType.MERGE_ON_READ.name();
    configs = DeltastreamerSchedulerConfGenerator.getSparkSchedulingConfigs(cfg);
    assertNotNull(configs.get(SPARK_SCHEDULER_ALLOCATION_FILE_KEY), "all satisfies");
  }
}
