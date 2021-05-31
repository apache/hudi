/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.partitioner.profile;

import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.config.HoodieWriteConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * Factory for {@link WriteProfile}.
 */
public class WriteProfiles {
  private static final Map<String, WriteProfile> PROFILES = new HashMap<>();

  private WriteProfiles() {}

  public static synchronized  WriteProfile singleton(
      boolean overwrite,
      boolean delta,
      HoodieWriteConfig config,
      HoodieFlinkEngineContext context) {
    return PROFILES.computeIfAbsent(config.getBasePath(),
        k -> getWriteProfile(overwrite, delta, config, context));
  }

  private static WriteProfile getWriteProfile(
      boolean overwrite,
      boolean delta,
      HoodieWriteConfig config,
      HoodieFlinkEngineContext context) {
    if (overwrite) {
      return new OverwriteWriteProfile(config, context);
    } else if (delta) {
      return new DeltaWriteProfile(config, context);
    } else {
      return new WriteProfile(config, context);
    }
  }

  public static void clean(String path) {
    PROFILES.remove(path);
  }
}
