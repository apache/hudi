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

package org.apache.hudi.table.upgrade;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.StoragePath;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Version 7 is going to be placeholder version for bridge release 0.16.0.
 * Version 8 is the placeholder version to track 1.x.
 */
public class EightToSevenDowngradeHandler implements DowngradeHandler {
  @Override
  public Map<ConfigProperty, String> downgrade(HoodieWriteConfig config, HoodieEngineContext context, String instantTime, SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(context.getStorageConf().newInstance()).setBasePath(config.getBasePath()).build();
    List<HoodieInstant> instants = metaClient.getActiveTimeline().getInstants();
    if (!instants.isEmpty()) {
      context.map(instants, instant -> {
        if (!instant.getFileName().contains("_")) {
          return false;
        }
        try {
          StoragePath fromPath = new StoragePath(metaClient.getMetaPath(), instant.getFileName());
          StoragePath toPath = new StoragePath(metaClient.getMetaPath(), instant.getFileName().replaceAll("_\\d+", ""));
          boolean success = metaClient.getStorage().rename(fromPath, toPath);
          if (!success) {
            throw new HoodieIOException("Error when rename the instant file: " + fromPath + " to: " + toPath);
          }
          return true;
        } catch (IOException e) {
          throw new HoodieException("Can not to complete the downgrade from version eight to version seven.", e);
        }
      }, instants.size());
    }
    return Collections.emptyMap();
  }
}
