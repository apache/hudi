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

package org.apache.hudi.client.clustering.run.strategy;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Cached lookup for the {@link ClusteringGroupWriter} provider on the runtime classpath.
 * Resolved once per JVM via {@link ServiceLoader}; absence of a provider yields {@link Option#empty()}
 * and lets {@link SparkSortAndSizeExecutionStrategy} run its default path with no penalty.
 */
public final class ClusteringGroupWriterRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(ClusteringGroupWriterRegistry.class);

  private static final Option<ClusteringGroupWriter> INSTANCE = loadFirst();

  /** Test-only override slot. AtomicReference is the documented thread-safe type. */
  private static final AtomicReference<Option<ClusteringGroupWriter>> OVERRIDE = new AtomicReference<>();

  private ClusteringGroupWriterRegistry() {
  }

  public static Option<ClusteringGroupWriter> get() {
    Option<ClusteringGroupWriter> current = OVERRIDE.get();
    return current != null ? current : INSTANCE;
  }

  @VisibleForTesting
  static void setOverrideForTesting(Option<ClusteringGroupWriter> writer) {
    OVERRIDE.set(writer);
  }

  private static Option<ClusteringGroupWriter> loadFirst() {
    try {
      Iterator<ClusteringGroupWriter> it =
          ServiceLoader.load(ClusteringGroupWriter.class, ClusteringGroupWriterRegistry.class.getClassLoader())
              .iterator();
      if (it.hasNext()) {
        ClusteringGroupWriter writer = it.next();
        LOG.info("Loaded clustering group writer: {} ({})", writer.name(), writer.getClass().getName());
        return Option.of(writer);
      }
    } catch (ServiceConfigurationError | RuntimeException t) {
      LOG.warn("Failed to load ClusteringGroupWriter via ServiceLoader; using default clustering path.", t);
    }
    return Option.empty();
  }
}
