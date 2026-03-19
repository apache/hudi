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

package org.apache.hudi.common.testutils;

import org.apache.hudi.common.config.HoodieTimeGeneratorConfig;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.TimeGenerator;
import org.apache.hudi.common.table.timeline.TimeGenerators;

/**
 * An in-process time generator that always use in-process lock for time generation.
 *
 * <p>Only for testing purposes.
 */
public class InProcessTimeGenerator {

  private static final TimeGenerator TIME_GENERATOR = TimeGenerators.getTimeGenerator(
      HoodieTimeGeneratorConfig.defaultConfig(""));

  public static String createNewInstantTime() {
    return createNewInstantTime(0L);
  }

  public static String createNewInstantTime(long milliseconds) {
    // We don't lock here since many callers are in hudi-common, which doesn't contain InProcessLockProvider
    return HoodieInstantTimeGenerator.createNewInstantTime(TIME_GENERATOR, milliseconds);
  }
}
