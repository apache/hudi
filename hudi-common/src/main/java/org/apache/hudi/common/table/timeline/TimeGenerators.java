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

package org.apache.hudi.common.table.timeline;

import org.apache.hudi.common.config.HoodieTimeGeneratorConfig;
import org.apache.hudi.common.util.ValidationUtils;

import java.util.Objects;

import static org.apache.hudi.common.config.HoodieCommonConfig.BASE_PATH;

/**
 * Holds all different {@link TimeGenerator} implementations, use {@link org.apache.hudi.common.config.HoodieCommonConfig#BASE_PATH}
 * to cache the existing instances.
 */
public class TimeGenerators {

  public static TimeGenerator getTimeGenerator(HoodieTimeGeneratorConfig timeGeneratorConfig) {
    ValidationUtils.checkState(timeGeneratorConfig.contains(BASE_PATH), () -> "Option [" + BASE_PATH.key() + "] is required");
    return getNewTimeGenerator(timeGeneratorConfig);
  }

  private static TimeGenerator getNewTimeGenerator(HoodieTimeGeneratorConfig timeGeneratorConfig) {
    // reuse is set to false.
    TimeGeneratorType type = timeGeneratorConfig.getTimeGeneratorType();
    if (Objects.requireNonNull(type) == TimeGeneratorType.WAIT_TO_ADJUST_SKEW) {
      return new SkewAdjustingTimeGenerator(timeGeneratorConfig);
    }
    throw new IllegalArgumentException("Unsupported TimeGenerator Type " + type);
  }
}
