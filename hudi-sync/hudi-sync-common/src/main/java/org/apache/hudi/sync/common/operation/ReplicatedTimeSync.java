package org.apache.hudi.sync.common.operation;

import org.apache.hudi.common.util.Option;

public interface ReplicatedTimeSync {
  Option<String> getLastReplicatedTime(String tableName);

  void updateLastReplicatedTimeStamp(String tableName, String timeStamp);

  void deleteLastReplicatedTimeStamp(String tableName);
}
