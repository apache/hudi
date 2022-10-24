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

package org.apache.hudi.common.util.queue;

import java.io.Closeable;

/**
 * HoodieExecutor which orchestrates concurrent producers and consumers communicating through a bounded in message queue.
 */
public interface HoodieExecutor<I, O, E> extends Closeable {

  /**
   * Main API to
   * 1. Set up and run all the production
   * 2. Set up and run all the consumption.
   * 3. Shutdown and return the result.
   */
  E execute();

  boolean isRemaining();

  /**
   * Shutdown all the consumers and producers.
   */
  void shutdownNow();

  boolean awaitTermination();
}
