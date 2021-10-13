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
import org.apache.hudi.config.HoodieWriteConfig;

import java.util.Map;

/**
 * Interface to assist in upgrading Hoodie table.
 */
public interface UpgradeHandler {

  /**
   * to be invoked to upgrade hoodie table from one version to a higher version.
   *
   * @param config                 instance of {@link HoodieWriteConfig} to be used.
   * @param context                instance of {@link HoodieEngineContext} to be used.
   * @param instantTime            current instant time that should not be touched.
   * @param upgradeDowngradeHelper instance of {@link BaseUpgradeDowngradeHelper} to be used.
   * @return Map of config properties and its values to be added to table properties.
   */
  Map<ConfigProperty, String> upgrade(
      HoodieWriteConfig config, HoodieEngineContext context, String instantTime,
      BaseUpgradeDowngradeHelper upgradeDowngradeHelper);
}
