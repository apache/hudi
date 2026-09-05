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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.util.Option;

import java.util.concurrent.CompletableFuture;

/**
 * SPI for an external writer that can perform the write phase of clustering for a single
 * input group end-to-end (scan, sort, write) and return the resulting {@link WriteStatus}es.
 * Implementations are discovered via {@link java.util.ServiceLoader} from the runtime classpath.
 *
 * <p>{@link SparkSortAndSizeExecutionStrategy} consults the registered writer (if any) before
 * falling back to the default Spark scan + sort + bulk-insert path. A writer that cannot
 * serve a particular group (for example, MOR groups with delta log files) returns
 * {@link Option#empty()} so the caller falls back per-group rather than per-plan.
 *
 * <p>The single argument is a {@link ClusteringGroupWriteContext} parameter object so the
 * SPI can grow without breaking implementers when more inputs become necessary.
 */
public interface ClusteringGroupWriter {

  /**
   * Write a single clustering input group asynchronously and return a future of the
   * resulting {@link WriteStatus}es. Returns {@link Option#empty()} when this writer cannot
   * serve the group; the caller will then run the default path.
   */
  Option<CompletableFuture<HoodieData<WriteStatus>>> runClusteringForGroupAsync(
      ClusteringGroupWriteContext context);

  /**
   * Identifier used in logs to indicate which writer implementation is loaded.
   */
  String name();

  /**
   * Whether delegation should run. Lets a provider self-gate via its own configuration without
   * leaking that configuration into the Hudi strategy. Returning {@code false} skips delegation
   * and falls through to the default Spark path.
   */
  default boolean isEnabled() {
    return true;
  }
}
