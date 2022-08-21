package org.apache.hudi.index.hbase;

import org.apache.hudi.config.HoodieHBaseIndexConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestRebalancedSparkHoodieHBaseIndex extends TestSparkHoodieHBaseIndex {

  @Test
  public void testGetHBaseKeyWithPrefix() {

    // default bucket count is 8, should add 0-7 prefix to hbase key
    String key = "abc";
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath("/foo").build();
    RebalancedSparkHoodieHBaseIndex index = new RebalancedSparkHoodieHBaseIndex(config);
    assertTrue(index.getHBaseKey(key).matches("[0-7]aaa"));

    // 100 hbase region count should add 00-99 prefix to hbase key
    config = HoodieWriteConfig.newBuilder().withPath("/foo").withIndexConfig(HoodieIndexConfig.newBuilder()
        .withIndexType(HoodieIndex.IndexType.HBASE).withHBaseIndexConfig(
            HoodieHBaseIndexConfig.newBuilder().hbaseRegionBucketNum("100").build()).build()).build();
    index = new RebalancedSparkHoodieHBaseIndex(config);
    assertTrue(index.getHBaseKey(key).matches("\\d{2}aaa"));

   // 321 hbase region count should add 000-320 prefix to hbase key
   config = HoodieWriteConfig.newBuilder().withPath("/foo").withIndexConfig(HoodieIndexConfig.newBuilder()
        .withIndexType(HoodieIndex.IndexType.HBASE).withHBaseIndexConfig(
            HoodieHBaseIndexConfig.newBuilder().hbaseRegionBucketNum("321").build()).build()).build();
    index = new RebalancedSparkHoodieHBaseIndex(config);
    assertTrue(index.getHBaseKey(key).matches("[0-3]\\d{2}aaa"));
  }
}
