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

package org.apache.hudi.util;

import java.util.HashMap;
import java.util.Map;

public class ConfigUtil {
  /**
   * Collects the config options that start with specified prefix {@code prefix} into a 'key'='value' list.
   */
  public static Map<String, String> getPropertiesWithPrefix(Map<String, String> options, String prefix) {
    final Map<String, String> hoodieProperties = new HashMap<>();
    if (hasPropertyOptions(options, prefix)) {
      options.keySet().stream()
          .filter(key -> key.startsWith(prefix))
          .forEach(key -> {
            final String value = options.get(key);
            final String subKey = key.substring(prefix.length());
            hoodieProperties.put(subKey, value);
          });
    }
    return hoodieProperties;
  }

  private static boolean hasPropertyOptions(Map<String, String> options, String prefix) {
    return options.keySet().stream().anyMatch(k -> k.startsWith(prefix));
  }
}
