package org.apache.hudi.index;

import org.apache.hudi.common.model.HoodieIndexDefinition;

import org.junit.jupiter.api.Test;

public class TestHoodieIndexDefinition {

  @Test
  public void testIndexNameWithoutPrefix() {
    HoodieIndexDefinition indexDefinition = new HoodieIndexDefinition("func_index_")
  }
}
