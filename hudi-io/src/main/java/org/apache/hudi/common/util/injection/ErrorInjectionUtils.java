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

package org.apache.hudi.common.util.injection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.apache.hudi.common.util.FileIOUtils.readAsUTFString;
import static org.apache.hudi.common.util.injection.ErrorInjectionCategory.SKIP_ERROR_INJECTION_AT_BEGINNING;

/**
 * Utils for injecting errors for testing reliability and robustness.
 */
public class ErrorInjectionUtils {
  public static final Logger LOG = LoggerFactory.getLogger(ErrorInjectionUtils.class);
  public static final Map<ErrorInjectionCategory, Random> RANDOM_MAP = new HashMap<>();
  public static final double PROBABILITY_SKIP_ERROR_INJECTION = 0.1;
  public static final long SKIP_ERROR_INJECTION_MS = 300000L; // 5 minutes
  public static final List<Boolean> SHOULD_SKIP_ERROR_INJECTION = new ArrayList<>();
  public static final List<Long> SKIP_UNTIL_TS = new ArrayList<>();

  public static void maybeInjectErrorWithFileByKillingJVM(ErrorInjectionCategory errorInjCategory, String msg) {
    try {
      final String val = readAsUTFString(Files.newInputStream(Paths.get(errorInjCategory.getSignalFilename())));
      boolean kill = Boolean.parseBoolean(val.trim());
      if (kill) {
        LOG.error("[{}] Injecting error, reason: {}", errorInjCategory.getLabel(), msg);
        System.exit(1);
      }
    } catch (Exception e) {
      LOG.error("[{}] Failed to inject error by killing the JVM ...", errorInjCategory.getLabel());
      LOG.error(e.getMessage(), e);
    }
  }

  /**
   * Maybe inject error by killing JVM, based on state ({@link SHOULD_SKIP_ERROR_INJECTION}) and probability.
   *
   * @param errorInjCategory error injection category
   * @param msg              message to log
   */
  public static void maybeInjectErrorByKillingJVM(ErrorInjectionCategory errorInjCategory, String msg) {
    if (SHOULD_SKIP_ERROR_INJECTION.isEmpty()) {
      if (getRandom(SKIP_ERROR_INJECTION_AT_BEGINNING).nextDouble()
          <= SKIP_ERROR_INJECTION_AT_BEGINNING.getProbability()) {
        SHOULD_SKIP_ERROR_INJECTION.add(true);
        SKIP_UNTIL_TS.add(System.currentTimeMillis() + SKIP_ERROR_INJECTION_MS);
      } else {
        SHOULD_SKIP_ERROR_INJECTION.add(false);
      }
    }

    if (SHOULD_SKIP_ERROR_INJECTION.get(0) && System.currentTimeMillis() < SKIP_UNTIL_TS.get(0)) {
      LOG.warn("[{}] Skip error injection for 5 minutes", errorInjCategory.getLabel());
      return;
    }

    try {
      boolean kill = getRandom(errorInjCategory).nextDouble() <= errorInjCategory.getProbability();
      if (kill) {
        LOG.error("[{}] Injecting error, reason: {}", errorInjCategory.getLabel(), msg);
        System.exit(1);
      } else {
        LOG.warn("[{}] Skip error injection and continue the job", errorInjCategory.getLabel());
      }
    } catch (Exception e) {
      LOG.error("[{}] Failed to inject error by killing the JVM ...", errorInjCategory.getLabel());
      LOG.error(e.getMessage(), e);
    }
  }

  private static Random getRandom(ErrorInjectionCategory errorInjCategory) {
    return RANDOM_MAP.computeIfAbsent(errorInjCategory, key -> new Random());
  }
}
