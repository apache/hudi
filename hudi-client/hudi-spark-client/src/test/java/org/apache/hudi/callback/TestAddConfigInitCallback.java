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

package org.apache.hudi.callback;

import org.apache.hudi.client.BaseHoodieClient;
import org.apache.hudi.config.HoodieWriteConfig;

/**
 * A test {@link HoodieClientInitCallback} implementation to add `user.defined.key1` config.
 */
public class TestAddConfigInitCallback implements HoodieClientInitCallback {
  public static final String CUSTOM_CONFIG_KEY1 = "user.defined.key1";
  public static final String CUSTOM_CONFIG_VALUE1 = "value1";

  @Override
  public void call(BaseHoodieClient hoodieClient) {
    HoodieWriteConfig config = hoodieClient.getConfig();
    config.setValue(CUSTOM_CONFIG_KEY1, CUSTOM_CONFIG_VALUE1);
  }
}
