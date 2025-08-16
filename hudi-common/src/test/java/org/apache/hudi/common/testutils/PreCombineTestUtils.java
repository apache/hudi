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

package org.apache.hudi.common.testutils;

import org.apache.hudi.common.model.HoodiePayloadProps;
import org.apache.hudi.common.table.HoodieTableConfig;

import org.junit.jupiter.params.provider.Arguments;

import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Stream;

public class PreCombineTestUtils {
  private static String[] preCombineConfigs = new String[] {
      HoodieTableConfig.PRECOMBINE_FIELDS.key(),
      "hoodie.datasource.write.precombine.field",
      HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY
  };

  public static Stream<Arguments> configurePreCombine() {
    return Stream.of(
        Arrays.stream(preCombineConfigs).map(Arguments::of).toArray(Arguments[]::new)
    );
  }

  /**
   * Sets specified key to the value provided. The other preCombine related configs are
   * removed from properties.
   */
  public static void setPreCombineConfig(Properties props, String key, String value) {
    for (String config : preCombineConfigs) {
      if (key.equals(config)) {
        props.setProperty(key, value);
      } else {
        props.remove(key);
      }
    }
  }
}
