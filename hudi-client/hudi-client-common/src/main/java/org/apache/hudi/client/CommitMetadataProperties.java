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

package org.apache.hudi.client;

import org.apache.hudi.HoodieVersion;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * Properties to be added to commit metadata for tracking engine, version, and key configuration.
 */
public class CommitMetadataProperties {

  // Allowlist of key config properties to include in commit metadata
  private static final String[] KEY_CONFIG_PROPERTIES = {
      "hoodie.table.name",
      "hoodie.table.type",
      "hoodie.table.version",
      "hoodie.datasource.write.operation",
      "hoodie.datasource.write.recordkey.field",
      "hoodie.datasource.write.partitionpath.field",
      "hoodie.insert.shuffle.parallelism",
      "hoodie.upsert.shuffle.parallelism",
      "hoodie.bulkinsert.shuffle.parallelism",
      "hoodie.delete.shuffle.parallelism",
      "hoodie.write.concurrency.mode",
      "hoodie.metadata.enable"
  };

  /**
   * Enriches the given extra metadata with HUDI version, engine info, and key config properties.
   *
   * @param extraMetadata existing extra metadata to enrich
   * @param config        the write config
   * @param context       the engine context
   * @return enriched metadata map
   */
  public static Option<Map<String, String>> enrich(Option<Map<String, String>> extraMetadata,
                                                    HoodieWriteConfig config,
                                                    HoodieEngineContext context) {
    Map<String, String> newMetadata = new HashMap<>();
    if (extraMetadata.isPresent()) {
      newMetadata.putAll(extraMetadata.get());
    }

    // Add HUDI version, engine info, and key config properties
    newMetadata.put("hudi.version", HoodieVersion.get());
    newMetadata.put("engine", context.getClass().getSimpleName());
    newMetadata.putAll(context.getEngineProperties());

    // Add only key config properties to avoid storing sensitive values and large config dumps
    for (String key : KEY_CONFIG_PROPERTIES) {
      String value = config.getString(key);
      if (value != null && !value.isEmpty()) {
        newMetadata.put("config." + key, value);
      }
    }

    return Option.of(newMetadata);
  }
}
