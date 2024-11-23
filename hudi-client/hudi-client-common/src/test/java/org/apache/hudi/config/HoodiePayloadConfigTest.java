/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.config;

import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HoodiePayloadConfigTest {

  @Test
  public void testPayloadClassNameDefaultValue() {
    HoodiePayloadConfig config = HoodiePayloadConfig.newBuilder().build();
    assertEquals(DefaultHoodieRecordPayload.class.getName(),
        config.getString(HoodiePayloadConfig.PAYLOAD_CLASS_NAME));
  }

  @Test
  public void testPayloadClassNameExplicitSet() {
    String customPayloadClass = "com.example.CustomPayload";
    HoodiePayloadConfig config = HoodiePayloadConfig.newBuilder()
        .withPayloadClass(customPayloadClass)
        .build();
    assertEquals(customPayloadClass,
        config.getString(HoodiePayloadConfig.PAYLOAD_CLASS_NAME));
  }
}