package org.apache.hudi.sync.common.operation;

import java.util.List;

public interface PartitionsSync {
  void addPartitionsToTable(String tableName, List<String> partitionsToAdd);

  void updatePartitionsToTable(String tableName, List<String> changedPartitions);

  void dropPartitions(String tableName, List<String> partitionsToDrop);
}
