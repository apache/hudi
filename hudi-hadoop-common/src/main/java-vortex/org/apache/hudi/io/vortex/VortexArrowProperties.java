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

package org.apache.hudi.io.vortex;

import lombok.extern.slf4j.Slf4j;

/**
 * Sets the global Arrow memory properties for Vortex reads and writes, as Iceberg does for its
 * vectorized Parquet and Vortex readers:
 * <ul>
 *   <li>{@code arrow.enable_unsafe_memory_access=true}: Arrow does not do a bounds check for each
 *   access to a buffer.</li>
 *   <li>{@code arrow.enable_null_check_for_get=false}: Arrow does not do a null check for each
 *   {@code get}. The Vortex readers examine the validity of a value before they read it.</li>
 * </ul>
 *
 * <p>Arrow reads these properties in static initializers. Thus, call {@link #ensureConfigured()}
 * before Arrow buffers and vectors are used, for example from a static block. A property that
 * the user sets as a system property or an environment variable is not changed.
 */
@Slf4j
public final class VortexArrowProperties {

  private static final String ENABLE_UNSAFE_MEMORY_ACCESS = "arrow.enable_unsafe_memory_access";
  private static final String ENABLE_UNSAFE_MEMORY_ACCESS_ENV = "ARROW_ENABLE_UNSAFE_MEMORY_ACCESS";
  private static final String ENABLE_NULL_CHECK_FOR_GET = "arrow.enable_null_check_for_get";
  private static final String ENABLE_NULL_CHECK_FOR_GET_ENV = "ARROW_ENABLE_NULL_CHECK_FOR_GET";

  private static volatile boolean configured = false;

  static {
    ensureConfigured();
  }

  private VortexArrowProperties() {
  }

  /**
   * Sets the Arrow properties one time for each JVM.
   */
  public static void ensureConfigured() {
    if (configured) {
      return;
    }

    synchronized (VortexArrowProperties.class) {
      if (configured) {
        return;
      }

      try {
        setDefault(ENABLE_UNSAFE_MEMORY_ACCESS, ENABLE_UNSAFE_MEMORY_ACCESS_ENV, "true");
        setDefault(ENABLE_NULL_CHECK_FOR_GET, ENABLE_NULL_CHECK_FOR_GET_ENV, "false");
      } catch (Exception e) {
        log.warn("Cannot set the Arrow properties. Vortex reads and writes can be slower.", e);
      }

      configured = true;
    }
  }

  private static void setDefault(String property, String environmentVariable, String value) {
    String current = System.getProperty(property);
    if (current == null) {
      current = System.getenv(environmentVariable);
    }

    if (current == null) {
      log.info("Setting {} to {}", property, value);
      System.setProperty(property, value);
    } else {
      log.info("{} is set explicitly to {}", property, current);
    }
  }
}
