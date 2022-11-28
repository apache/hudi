package org.apache.hudi.hive;

import org.apache.hudi.sync.common.HoodieSyncTool;

import org.apache.hadoop.conf.Configuration;

import java.util.Properties;

public class MockHoodieHiveSyncToolException extends HoodieSyncTool {
  public static String EXCEPTION_STRING = "EXCEPTION SYNCING MockHoodieHiveSyncToolException";
  public MockHoodieHiveSyncToolException(Properties props, Configuration hadoopConf) {
    super(props, hadoopConf);
  }

  @Override
  public void syncHoodieTable() {
    throw new RuntimeException(EXCEPTION_STRING);
  }
}
