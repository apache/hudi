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

package org.apache.hudi.metaserver.client;

import org.apache.hudi.common.config.HoodieMetaserverConfig;
import org.apache.hudi.metaserver.HoodieMetaserver;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests on hoodie meta server client.
 */
public class TestHoodieMetaserverClient {

  @Test
  public void testLocalClient() {
    HoodieMetaserverConfig config = HoodieMetaserverConfig.newBuilder().setUris("").build();
    HoodieMetaserverClient client = new HoodieMetaserverClientImp(config);
    assertTrue(client.isLocal());
    assertTrue(client.isConnected());
  }

  @Test
  public void testRemoteClient() {
    HoodieMetaserver.startServer();
    assertNotNull(HoodieMetaserver.getMetaserverStorage());
    HoodieMetaserverConfig config = HoodieMetaserverConfig.newBuilder().build();
    HoodieMetaserverClient client = new HoodieMetaserverClientImp(config);
    assertFalse(client.isLocal());
    assertTrue(client.isConnected());
  }
}
