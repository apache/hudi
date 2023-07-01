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

import org.apache.avro.Schema;

import static org.apache.hudi.config.HoodieWriteConfig.WRITE_SCHEMA_OVERRIDE;

/**
 * A test {@link HoodieClientInitCallback} implementation to add the property
 * `user.defined.key2=value2` to the write schema.
 */
public class TestChangeConfigInitCallback implements HoodieClientInitCallback {
  public static final String CUSTOM_CONFIG_KEY2 = "user.defined.key2";
  public static final String CUSTOM_CONFIG_VALUE2 = "value2";

  @Override
  public void call(BaseHoodieClient hoodieClient) {
    HoodieWriteConfig config = hoodieClient.getConfig();
    Schema schema = new Schema.Parser().parse(config.getWriteSchema());
    if (!schema.getObjectProps().containsKey(CUSTOM_CONFIG_KEY2)) {
      schema.addProp(CUSTOM_CONFIG_KEY2, CUSTOM_CONFIG_VALUE2);
    }
    config.getProps().setProperty(WRITE_SCHEMA_OVERRIDE.key(), schema.toString());
  }
}
