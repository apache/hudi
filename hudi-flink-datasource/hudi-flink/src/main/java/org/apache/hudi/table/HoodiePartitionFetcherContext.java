package org.apache.hudi.table;

import org.apache.flink.table.filesystem.PartitionFetcher;

import java.util.List;
import java.util.Map;

/**
 * HoodiePartitionFetcherContext.
 */
public class HoodiePartitionFetcherContext implements PartitionFetcher.Context<Map<String, String>> {

  @Override
  public void open() throws Exception {

  }

  @Override
  public java.util.Optional<Map<String, String>> getPartition(List<String> partValues) throws Exception {
    return java.util.Optional.empty();
  }

  @Override
  public List<ComparablePartitionValue> getComparablePartitionValueList() throws Exception {
    return null;
  }

  @Override
  public void close() throws Exception {

  }
}
