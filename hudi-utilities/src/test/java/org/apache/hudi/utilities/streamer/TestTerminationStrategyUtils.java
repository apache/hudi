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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link TerminationStrategyUtils}.
 */
class TestTerminationStrategyUtils {

  private final TypedProperties props = new TypedProperties();

  @Test
  void unsetStrategyClassYieldsEmptyOption() {
    assertFalse(TerminationStrategyUtils.createPostWriteTerminationStrategy(props, null).isPresent());
    assertFalse(TerminationStrategyUtils.createPostWriteTerminationStrategy(props, "").isPresent());
  }

  @Test
  void configuredStrategyClassIsInstantiatedReflectively() {
    Option<PostWriteTerminationStrategy> strategy = TerminationStrategyUtils.createPostWriteTerminationStrategy(
        props, NoNewDataTerminationStrategy.class.getName());

    assertTrue(strategy.isPresent());
    assertTrue(strategy.get() instanceof NoNewDataTerminationStrategy);
  }

  @Test
  void unresolvableStrategyClassThrows() {
    String bogusClass = "org.apache.hudi.utilities.streamer.DoesNotExistTerminationStrategy";

    HoodieException e = assertThrows(HoodieException.class,
        () -> TerminationStrategyUtils.createPostWriteTerminationStrategy(props, bogusClass));
    assertTrue(e.getMessage().contains("Could not create"), e.getMessage());
    assertTrue(e.getMessage().contains(bogusClass), e.getMessage());
  }
}
