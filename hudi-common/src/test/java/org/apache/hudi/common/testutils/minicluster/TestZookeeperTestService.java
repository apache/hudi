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

package org.apache.hudi.common.testutils.minicluster;

import org.apache.hudi.common.testutils.NetworkTestUtils;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.net.InetAddress;
import java.net.ServerSocket;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mockStatic;

class TestZookeeperTestService {

  @Test
  void testStartRetriesWhenPortIsAlreadyInUse() throws Exception {
    try (ServerSocket occupiedSocket =
             new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"));
         MockedStatic<NetworkTestUtils> networkTestUtils =
             mockStatic(NetworkTestUtils.class)) {
      int occupiedPort = occupiedSocket.getLocalPort();
      networkTestUtils.when(NetworkTestUtils::nextFreePort)
          .thenReturn(occupiedPort)
          .thenCallRealMethod();

      ZookeeperTestService service = new ZookeeperTestService(new Configuration());
      try {
        service.start();

        assertNotEquals(occupiedPort, service.clientPort());
        networkTestUtils.verify(NetworkTestUtils::nextFreePort, atLeast(2));
      } finally {
        service.stop();
      }
    }
  }
}
