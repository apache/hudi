/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.index.bucket;

import org.apache.hudi.common.model.HoodieConsistentHashingMetadata;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class TestConsistentBucketIndexUtils extends HoodieCommonTestHarness {

  @BeforeEach
  public void init() throws IOException {
    initMetaClient();
  }

  @Test
  public void testCreateMetadata() throws IOException {
    Assertions.assertTrue(!ConsistentBucketIndexUtils.loadMetadata(metaClient, "partition1").isPresent());
    HoodieConsistentHashingMetadata metadata = ConsistentBucketIndexUtils.loadOrCreateMetadata(metaClient, "partition1", 4);
    HoodieConsistentHashingMetadata reloadedMetadata0 = ConsistentBucketIndexUtils.loadOrCreateMetadata(metaClient, "partition1", 4);
    Assertions.assertArrayEquals(metadata.toBytes(), reloadedMetadata0.toBytes());
    HoodieConsistentHashingMetadata reloadedMetadata1 = ConsistentBucketIndexUtils.loadMetadata(metaClient, "partition1").get();
    Assertions.assertArrayEquals(metadata.toBytes(), reloadedMetadata1.toBytes());
  }
}
