/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

    // if set 100 hbase region count should add 00-99 prefix to hbase key
    config = HoodieWriteConfig.newBuilder().withPath("/foo").withIndexConfig(HoodieIndexConfig.newBuilder()
        .withIndexType(HoodieIndex.IndexType.HBASE).withHBaseIndexConfig(
            HoodieHBaseIndexConfig.newBuilder().hbaseRegionBucketNum("100").build()).build()).build();
    index = new RebalancedSparkHoodieHBaseIndex(config);
    assertTrue(index.getHBaseKey(key).matches("\\d{2}aaa"));

    // if set 321 hbase region count should add 000-320 prefix to hbase key
    config = HoodieWriteConfig.newBuilder().withPath("/foo").withIndexConfig(HoodieIndexConfig.newBuilder()
        .withIndexType(HoodieIndex.IndexType.HBASE).withHBaseIndexConfig(
            HoodieHBaseIndexConfig.newBuilder().hbaseRegionBucketNum("321").build()).build()).build();
    index = new RebalancedSparkHoodieHBaseIndex(config);
    assertTrue(index.getHBaseKey(key).matches("[0-3]\\d{2}aaa"));
  }
}
