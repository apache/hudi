package org.apache.hudi.sync.datahub;

import org.apache.hudi.sync.common.model.PartitionValueExtractor;

import java.util.Collections;
import java.util.List;

public class DummyPartitionValueExtractor implements PartitionValueExtractor {

  @Override
  public List<String> extractPartitionValuesInPath(String partitionPath) {
    return Collections.emptyList();
  }

}
