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

package org.apache.hudi.utilities;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer;
import org.apache.hudi.utilities.deltastreamer.SchedulerConfGenerator;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestSchedulerConfGenerator {

  @Test
  public void testGenerateSparkSchedulingConf() throws Exception {
    HoodieDeltaStreamer.Config cfg = new HoodieDeltaStreamer.Config();
    Map<String, String> configs = SchedulerConfGenerator.getSparkSchedulingConfigs(cfg);
    assertNull("spark.scheduler.mode not set", configs.get(SchedulerConfGenerator.SPARK_SCHEDULER_ALLOCATION_FILE_KEY));

    System.setProperty(SchedulerConfGenerator.SPARK_SCHEDULER_MODE_KEY, "FAIR");
    cfg.continuousMode = false;
    configs = SchedulerConfGenerator.getSparkSchedulingConfigs(cfg);
    assertNull("continuousMode is false", configs.get(SchedulerConfGenerator.SPARK_SCHEDULER_ALLOCATION_FILE_KEY));

    cfg.continuousMode = true;
    cfg.storageType = HoodieTableType.COPY_ON_WRITE.name();
    configs = SchedulerConfGenerator.getSparkSchedulingConfigs(cfg);
    assertNull("storageType is not MERGE_ON_READ",
        configs.get(SchedulerConfGenerator.SPARK_SCHEDULER_ALLOCATION_FILE_KEY));

    cfg.storageType = HoodieTableType.MERGE_ON_READ.name();
    configs = SchedulerConfGenerator.getSparkSchedulingConfigs(cfg);
    assertNotNull("all satisfies", configs.get(SchedulerConfGenerator.SPARK_SCHEDULER_ALLOCATION_FILE_KEY));
  }
}
