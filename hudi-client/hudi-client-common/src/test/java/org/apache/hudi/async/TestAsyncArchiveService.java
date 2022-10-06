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

package org.apache.hudi.async;

import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieWriteConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TestAsyncArchiveService {

  @Mock
  BaseHoodieWriteClient writeClient;
  @Mock
  HoodieWriteConfig config;

  @Test
  void startAsyncArchiveReturnsNullWhenAutoArchiveDisabled() {
    when(config.getBoolean(HoodieArchivalConfig.AUTO_ARCHIVE)).thenReturn(false);
    when(writeClient.getConfig()).thenReturn(config);
    assertNull(AsyncArchiveService.startAsyncArchiveIfEnabled(writeClient));
  }

  @Test
  void startAsyncArchiveReturnsNullWhenAsyncArchiveDisabled() {
    when(config.getBoolean(HoodieArchivalConfig.AUTO_ARCHIVE)).thenReturn(true);
    when(config.getBoolean(HoodieArchivalConfig.ASYNC_ARCHIVE)).thenReturn(false);
    when(writeClient.getConfig()).thenReturn(config);
    assertNull(AsyncArchiveService.startAsyncArchiveIfEnabled(writeClient));
  }

  @Test
  void startAsyncArchiveIfEnabled() {
    when(config.getBoolean(HoodieArchivalConfig.AUTO_ARCHIVE)).thenReturn(true);
    when(config.getBoolean(HoodieArchivalConfig.ASYNC_ARCHIVE)).thenReturn(true);
    when(writeClient.getConfig()).thenReturn(config);
    assertNotNull(AsyncArchiveService.startAsyncArchiveIfEnabled(writeClient));
  }

  @Test
  void startServiceShouldInvokeCallArchiveMethod() throws ExecutionException, InterruptedException {
    AsyncArchiveService service = new AsyncArchiveService(writeClient);
    assertEquals(true, service.startService().getLeft().get());
    verify(writeClient).archive();
  }
}
