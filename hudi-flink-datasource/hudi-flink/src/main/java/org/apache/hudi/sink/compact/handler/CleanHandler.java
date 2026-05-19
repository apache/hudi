/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.compact.handler;

import java.io.Closeable;

/**
 * Abstraction for async clean handling in the Flink sink pipeline.
 *
 * <p>The interface lets callers trigger clean lifecycle actions without knowing whether cleaning is
 * performed by a single handler instance or by a composite handler that forwards the same lifecycle
 * calls to both data-table and metadata-table cleaners.
 *
 * <p>Implementations are responsible for starting async cleaning, waiting for in-flight cleaning
 * to finish, and performing final cleanup during close.
 */
public interface CleanHandler extends Closeable {
  void clean();

  void waitForCleaningFinish();

  void startAsyncCleaning();

  @Override
  void close();
}
