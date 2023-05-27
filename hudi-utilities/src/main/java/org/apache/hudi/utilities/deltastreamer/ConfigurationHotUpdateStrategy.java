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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.common.config.TypedProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hudi.utilities.deltastreamer.ConfigurationHotUpdateStrategyUtils.mergeProperties;

/**
 * Configuration of delta sync hot update strategy.
 */
public abstract class ConfigurationHotUpdateStrategy {
  private static final Logger LOG = LoggerFactory.getLogger(ConfigurationHotUpdateStrategy.class);

  protected HoodieDeltaStreamer.Config cfg;
  protected TypedProperties properties;

  public ConfigurationHotUpdateStrategy(HoodieDeltaStreamer.Config cfg,
                                        TypedProperties properties) {
    this.cfg = cfg;
    this.properties = properties;
  }

  /**
   * Configuration hot update. The method is called in main on driver, so we can update the properties in place.
   * @param properties props may be updated.
   * @return true if props updated.
   */
  public final boolean updatePropertiesInPlace(final TypedProperties properties) {
    try {
      TypedProperties newProps = new TypedProperties(properties);
      updateProperties(newProps);
      return mergeProperties(properties, newProps);
    } catch (Exception e) {
      LOG.warn("update properties in place failed", e);
      return false;
    }
  }

  /**
   * user defined function to update the properties
   * @param properties to be updated
   */
  public abstract void updateProperties(TypedProperties properties);
}
