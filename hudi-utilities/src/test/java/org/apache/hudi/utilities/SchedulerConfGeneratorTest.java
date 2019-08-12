package org.apache.hudi.utilities;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Map;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer;
import org.apache.hudi.utilities.deltastreamer.SchedulerConfGenerator;
import org.junit.Test;

public class SchedulerConfGeneratorTest {

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
    assertNull("storageType is not MERGE_ON_READ", configs.get(
        SchedulerConfGenerator.SPARK_SCHEDULER_ALLOCATION_FILE_KEY));

    cfg.storageType = HoodieTableType.MERGE_ON_READ.name();
    configs = SchedulerConfGenerator.getSparkSchedulingConfigs(cfg);
    assertNotNull("all satisfies", configs.get(SchedulerConfGenerator.SPARK_SCHEDULER_ALLOCATION_FILE_KEY));
  }
}
