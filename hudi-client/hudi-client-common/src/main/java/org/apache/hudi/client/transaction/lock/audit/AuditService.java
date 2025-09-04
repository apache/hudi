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

package org.apache.hudi.client.transaction.lock.audit;

/**
 * Generic audit service interface for tracking operation lifecycles.
 * Provides basic operations for starting, updating, and ending audit sessions.
 */
public interface AuditService extends AutoCloseable {
  
  /**
   * Starts a new audit operation session.
   * 
   * @param sessionId Unique identifier for this audit session
   * @param timestamp When the operation started
   * @throws Exception if the session cannot be started
   */
  void startOperation(String sessionId, long timestamp) throws Exception;
  
  /**
   * Updates the current audit operation (e.g., for renewals).
   * 
   * @param timestamp When the update occurred
   * @throws Exception if the operation cannot be updated
   */
  void updateOperation(long timestamp) throws Exception;
  
  /**
   * Ends the current audit operation session.
   * 
   * @param timestamp When the operation ended
   * @param finalState The final state of the operation
   * @throws Exception if the operation cannot be finalized
   */
  void endOperation(long timestamp, String finalState) throws Exception;
}