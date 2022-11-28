package org.apache.hudi.hive;

import org.apache.hudi.sync.common.HoodieSyncTool;

import org.apache.hadoop.conf.Configuration;

import java.util.Properties;

public class MockHoodieHiveSyncToolException2 extends HoodieSyncTool {
  public static String EXCEPTION_STRING = "EXCEPTION SYNCING MockHoodieHiveSyncToolException2";
  public MockHoodieHiveSyncToolException2(Properties props, Configuration hadoopConf) {
    super(props, hadoopConf);
  }

  @Override
  public void syncHoodieTable() {
    throw new RuntimeException(EXCEPTION_STRING);
  }
}
