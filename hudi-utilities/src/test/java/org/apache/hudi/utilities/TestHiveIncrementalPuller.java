package org.apache.hudi.utilities;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestHiveIncrementalPuller {

  private HiveIncrementalPuller.Config config;

  @Before
  public void setup() {
    config = new HiveIncrementalPuller.Config();
  }

  @Test
  public void testInitHiveIncrementalPuller() {

    try {
      new HiveIncrementalPuller(config);
    } catch (Exception e) {
      Assert.fail("Unexpected exception while initing HiveIncrementalPuller, msg: " + e.getMessage());
    }

  }

}
