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

package org.apache.hudi.client.transaction;

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIMethod;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieWriteConflictException;

import java.util.Set;
import java.util.stream.Stream;

/**
 * Strategy interface for conflict resolution with multiple writers.
 * Users can provide pluggable implementations for different kinds of strategies to resolve conflicts when multiple
 * writers are mutating the hoodie table.
 */
public interface ConflictResolutionStrategy {

  /**
   * Stream of instants to check conflicts against.
   * @return
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  Stream<HoodieInstant> getCandidateInstants(HoodieTableMetaClient metaClient, HoodieInstant currentInstant,
                                             Option<Set<String>> pendingInstantsBeforeWritten);

  /**
   * Implementations of this method will determine whether a conflict exists between 2 commits.
   * @param thisOperation
   * @param otherOperation
   * @return
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  boolean hasConflict(ConcurrentOperation thisOperation, ConcurrentOperation otherOperation);

  /**
   * Implementations of this method will determine how to resolve a conflict between 2 commits.
   * @param thisOperation
   * @param otherOperation
   * @return
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  void resolveConflict(ConcurrentOperation thisOperation, ConcurrentOperation otherOperation) throws HoodieWriteConflictException;

  /**
   * Whether to record the pending instants before write.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  boolean isPendingInstantsBeforeWriteRequired();

  /**
   * Whether the write operation needs to do conflict detection.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  boolean isConflictResolveRequired(WriteOperationType operationType);
}
