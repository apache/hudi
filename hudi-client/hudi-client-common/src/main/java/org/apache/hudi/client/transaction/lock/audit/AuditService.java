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

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIClass;
import org.apache.hudi.PublicAPIMethod;

/**
 * Generic audit service interface for tracking operation lifecycles.
 * Provides a single method for recording all types of audit operations.
 */
@PublicAPIClass(maturity = ApiMaturityLevel.EVOLVING)
public interface AuditService extends AutoCloseable {

  /**
   * Records an audit operation with the given state and timestamp.
   *
   * @param state     The type of operation (START, RENEW, END)
   * @param timestamp When the operation occurred
   * @throws Exception if the operation cannot be recorded
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  void recordOperation(AuditOperationState state, long timestamp) throws Exception;
}
