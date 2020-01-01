package org.apache.hudi.utilities;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestHiveIncrementalPuller {

  private HiveIncrementalPuller.Config config;

  @Before
  public void init() {
    config = new HiveIncrementalPuller.Config();
  }

  @Test
  public void testInitHiveIncrementalPuller() throws Exception {

    HiveIncrementalPuller puller = new HiveIncrementalPuller(config);
    Assert.assertNotNull(puller);

  }

}
