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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.streamer.ConfigurationHotUpdateStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

import static org.apache.hudi.config.HoodieWriteConfig.UPSERT_PARALLELISM_VALUE;

/**
 * ConfigurationHotUpdateStrategy for test purpose.
 */
public class MockConfigurationHotUpdateStrategy extends ConfigurationHotUpdateStrategy implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(MockConfigurationHotUpdateStrategy.class);

  public MockConfigurationHotUpdateStrategy(HoodieDeltaStreamer.Config cfg, TypedProperties properties) {
    super(cfg, properties);
  }

  @Override
  public Option<TypedProperties> updateProperties(TypedProperties currentProps) {
    if (currentProps.containsKey(UPSERT_PARALLELISM_VALUE.key())) {
      long upsertShuffleParallelism = currentProps.getLong(UPSERT_PARALLELISM_VALUE.key());
      TypedProperties newProps = TypedProperties.copy(currentProps);
      newProps.setProperty(UPSERT_PARALLELISM_VALUE.key(), String.valueOf(upsertShuffleParallelism + 5));
      LOG.info("update {} from [{}] to [{}]", UPSERT_PARALLELISM_VALUE.key(), upsertShuffleParallelism, upsertShuffleParallelism + 5);
      return Option.of(newProps);
    }
    return Option.empty();
  }
}