package org.apache.hudi.table;

import org.apache.flink.table.filesystem.PartitionFetcher;

import java.util.List;
import java.util.Map;

/**
 * HoodiePartitionFetcher.
 */
public class HoodiePartitionFetcher implements PartitionFetcher<Map<String, String>> {

  @Override
  public List<Map<String, String>> fetch(Context<Map<String, String>> context) throws Exception {
    return null;
  }
}
