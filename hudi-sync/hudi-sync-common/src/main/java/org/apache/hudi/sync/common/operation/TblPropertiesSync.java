package org.apache.hudi.sync.common.operation;

import org.apache.hudi.common.util.Option;

import java.util.Map;

public interface TblPropertiesSync {
  Option<String> getLastCommitTimeSynced(String tableName);

  void updateLastCommitTimeSynced(String tableName);

  void updateTableProperties(String tableName, Map<String, String> tableProperties);
}
