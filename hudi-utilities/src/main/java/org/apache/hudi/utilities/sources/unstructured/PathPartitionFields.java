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

package org.apache.hudi.utilities.sources.unstructured;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.utilities.config.CloudSourceConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The path partition field names of {@link CloudSourceConfig#PATH_BASED_PARTITION_FIELDS} as the unstructured stack reads them:
 * split at commas, trimmed, blank segments dropped. Each caller applies its own reserved-name filter.
 */
public final class PathPartitionFields {
  private PathPartitionFields() {
  }

  /** The configured path partition field names, in configured order; empty when the property is not set. */
  public static List<String> configured(TypedProperties props) {
    if (!ConfigUtils.containsConfigProperty(props, CloudSourceConfig.PATH_BASED_PARTITION_FIELDS)) {
      return Collections.emptyList();
    }
    List<String> fields = new ArrayList<>();
    for (String configured : ConfigUtils.getStringWithAltKeys(props, CloudSourceConfig.PATH_BASED_PARTITION_FIELDS).split(",")) {
      String field = configured.trim();
      if (!field.isEmpty()) {
        fields.add(field);
      }
    }
    return fields;
  }
}
