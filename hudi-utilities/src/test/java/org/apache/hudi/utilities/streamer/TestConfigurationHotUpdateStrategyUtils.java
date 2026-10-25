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

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link ConfigurationHotUpdateStrategyUtils}.
 */
class TestConfigurationHotUpdateStrategyUtils {

  private final HoodieStreamer.Config cfg = new HoodieStreamer.Config();
  private final TypedProperties props = new TypedProperties();

  @Test
  void unsetStrategyClassYieldsEmptyOption() {
    assertFalse(ConfigurationHotUpdateStrategyUtils
        .createConfigurationHotUpdateStrategy(null, cfg, props).isPresent());
    assertFalse(ConfigurationHotUpdateStrategyUtils
        .createConfigurationHotUpdateStrategy("", cfg, props).isPresent());
  }

  @Test
  void configuredStrategyClassIsInstantiatedReflectivelyWithCfgAndProps() {
    Option<ConfigurationHotUpdateStrategy> strategy = ConfigurationHotUpdateStrategyUtils
        .createConfigurationHotUpdateStrategy(EchoHotUpdateStrategy.class.getName(), cfg, props);

    assertTrue(strategy.isPresent());
    assertTrue(strategy.get() instanceof EchoHotUpdateStrategy);
    // The reflective ctor call must have handed both args down to the base class.
    assertSame(cfg, ((EchoHotUpdateStrategy) strategy.get()).getCfg());
    assertSame(props, strategy.get().updateProperties(new TypedProperties()).get());
  }

  @Test
  void unresolvableStrategyClassThrows() {
    String bogusClass = "org.apache.hudi.utilities.streamer.DoesNotExistHotUpdateStrategy";

    HoodieException e = assertThrows(HoodieException.class, () -> ConfigurationHotUpdateStrategyUtils
        .createConfigurationHotUpdateStrategy(bogusClass, cfg, props));
    assertTrue(e.getMessage().contains("Could not create configuration hot update strategy class"), e.getMessage());
    assertTrue(e.getMessage().contains(bogusClass), e.getMessage());
  }

  /**
   * Strategy whose ctor signature matches exactly what {@code ReflectionUtils.loadClass} infers from
   * {@link HoodieStreamer.Config} and {@link TypedProperties}, and which echoes back what it was given.
   */
  public static class EchoHotUpdateStrategy extends ConfigurationHotUpdateStrategy {

    public EchoHotUpdateStrategy(HoodieStreamer.Config cfg, TypedProperties properties) {
      super(cfg, properties);
    }

    HoodieStreamer.Config getCfg() {
      return cfg;
    }

    @Override
    public Option<TypedProperties> updateProperties(TypedProperties currentProps) {
      return Option.of(properties);
    }
  }
}
