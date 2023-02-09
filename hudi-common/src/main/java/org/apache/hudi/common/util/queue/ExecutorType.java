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

/**
 * Types of {@link org.apache.hudi.common.util.queue.HoodieExecutor}.
 */
public enum ExecutorType {

  /**
   * Executor which orchestrates concurrent producers and consumers communicating through a bounded in-memory message queue using LinkedBlockingQueue.
   */
  BOUNDED_IN_MEMORY,

  /**
   * Executor which orchestrates concurrent producers and consumers communicating through disruptor as a lock free message queue
   * to gain better writing performance. Although DisruptorExecutor is still an experimental feature.
   */
  DISRUPTOR,

  /**
   * Executor with no inner message queue and no inner lock. Consuming and writing records from iterator directly.
   * The advantage is that there is no need for additional memory and cpu resources due to lock or multithreading.
   * The disadvantage is that the executor is a single-write-single-read model, cannot support functions such as speed limit
   * and can not de-coupe the network read (shuffle read) and network write (writing objects/files to storage) anymore.
   */
  SIMPLE
}
