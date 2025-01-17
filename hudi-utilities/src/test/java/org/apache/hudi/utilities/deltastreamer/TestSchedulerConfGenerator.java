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

import org.apache.hudi.SparkConfigs;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.utilities.streamer.HoodieStreamer;
import org.apache.hudi.utilities.streamer.SchedulerConfGenerator;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestSchedulerConfGenerator {

  @Test
  public void testGenerateSparkSchedulingConf() throws Exception {
    HoodieStreamer.Config cfg = new HoodieStreamer.Config();
    Map<String, String> configs = SchedulerConfGenerator.getSparkSchedulingConfigs(cfg);
    assertNull(configs.get(SparkConfigs.SPARK_SCHEDULER_ALLOCATION_FILE_KEY()), "spark.scheduler.mode not set");

    System.setProperty(SchedulerConfGenerator.SPARK_SCHEDULER_MODE_KEY, "FAIR");
    cfg.continuousMode = false;
    configs = SchedulerConfGenerator.getSparkSchedulingConfigs(cfg);
    assertNull(configs.get(SparkConfigs.SPARK_SCHEDULER_ALLOCATION_FILE_KEY()), "continuousMode is false");

    cfg.continuousMode = true;
    cfg.tableType = HoodieTableType.COPY_ON_WRITE.name();
    configs = SchedulerConfGenerator.getSparkSchedulingConfigs(cfg);
    assertNull(configs.get(SparkConfigs.SPARK_SCHEDULER_ALLOCATION_FILE_KEY()),
        "table type is not MERGE_ON_READ");

    cfg.tableType = HoodieTableType.MERGE_ON_READ.name();
    configs = SchedulerConfGenerator.getSparkSchedulingConfigs(cfg);
    assertNotNull(configs.get(SparkConfigs.SPARK_SCHEDULER_ALLOCATION_FILE_KEY()), "all satisfies");
  }

  @Test
  public void testGenerateConfig() {
    String targetConfig =
        "<?xml version=\"1.0\"?>\n"
            + "<allocations>\n"
            + "    <pool name=\"hoodiedeltasync\">\n"
            + "        <schedulingMode>FAIR</schedulingMode>\n"
            + "        <weight>1</weight>\n"
            + "        <minShare>2</minShare>\n"
            + "    </pool>\n"
            + "    <pool name=\"hoodiecompact\">\n"
            + "        <schedulingMode>FAIR</schedulingMode>\n"
            + "        <weight>3</weight>\n"
            + "        <minShare>4</minShare>\n"
            + "    </pool>\n"
            + "    <pool name=\"hoodiecluster\">\n"
            + "        <schedulingMode>FAIR</schedulingMode>\n"
            + "        <weight>5</weight>\n"
            + "        <minShare>6</minShare>\n"
            + "    </pool>\n"
            + "</allocations>";
    String generatedConfig = SchedulerConfGenerator.generateConfig(1, 3, 2, 4, 5, 6);
    assertEquals(targetConfig, generatedConfig);
  }

  @Test
  public void testGeneratedConfigFileScheme() throws Exception {
    System.setProperty(SchedulerConfGenerator.SPARK_SCHEDULER_MODE_KEY, "FAIR");
    HoodieStreamer.Config cfg = new HoodieStreamer.Config();
    cfg.continuousMode = true;
    cfg.tableType = HoodieTableType.MERGE_ON_READ.name();
    Map<String, String> configs = SchedulerConfGenerator.getSparkSchedulingConfigs(cfg);

    URI schedulerFile = URI.create(configs.get(SparkConfigs.SPARK_SCHEDULER_ALLOCATION_FILE_KEY()));
    assertNotNull(schedulerFile.getScheme());
  }
}
