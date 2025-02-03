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

package org.apache.hudi.client.transaction.lock.models;

/**
 * The heartbeat manager interface is meant to manage the lifecycle of heartbeat tasks.
 *
 */
public interface HeartbeatManager extends AutoCloseable {

  /**
   * Starts the heartbeat for the given thread and does not stop until stopHeartbeat is called or the thread has died.
   * @param threadToMonitor The thread to pass to/monitor when running the heartbeat task.
   * @return Whether we successfully started the heartbeat.
   */
  boolean startHeartbeatForThread(Thread threadToMonitor);

  /**
   * Stops the heartbeat, if one is active.
   * This is a blocking call.
   * One should assume that the heartbeat task can still be triggered
   * until we return true from this method.
   * @param mayInterruptIfRunning Whether we may interrupt the heartbeat if it is still running.
   * @return Whether the heartbeat task was successfully stopped.
   * Note: this should return true if the heartbeat task was already stopped.
   */
  boolean stopHeartbeat(boolean mayInterruptIfRunning);

  /**
   * Whether the heartbeat manager has an active heartbeat task currently.
   * @return A boolean.
   */
  boolean hasActiveHeartbeat();
}
