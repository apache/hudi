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

package org.apache.hudi.common.metrics;

import java.util.Collections;
import java.util.Map;

/**
 * Discards everything, for emission inside a task naming a registry nothing bound. Does not throw: a
 * metrics gap must not fail a write.
 */
public class NoOpRegistry implements Registry {

  public static final NoOpRegistry INSTANCE = new NoOpRegistry();

  private NoOpRegistry() {
  }

  @Override
  public String getName() {
    return "no-op";
  }

  @Override
  public void clear() {
  }

  @Override
  public void increment(String name) {
  }

  @Override
  public void add(String name, long value) {
  }

  @Override
  public void set(String name, long value) {
  }

  @Override
  public Map<String, Long> getAllCounts(boolean prefixWithRegistryName) {
    return Collections.emptyMap();
  }
}
