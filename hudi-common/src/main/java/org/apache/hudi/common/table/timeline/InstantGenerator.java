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

package org.apache.hudi.common.table.timeline;

import org.apache.hudi.storage.StoragePathInfo;

import java.io.Serializable;

import static org.apache.hudi.common.table.timeline.HoodieInstant.State;

/**
 * Factory for generating Instants.
 **/
public interface InstantGenerator extends Serializable {

  HoodieInstant createNewInstant(State state, String action, String timestamp);

  HoodieInstant createNewInstant(State state, String action, String timestamp, String completionTime);

  HoodieInstant createNewInstant(State state, String action, String timestamp, String completionTime, boolean isLegacy);

  HoodieInstant createNewInstant(StoragePathInfo pathInfo);

  HoodieInstant getRequestedInstant(final HoodieInstant instant);

  HoodieInstant getCleanRequestedInstant(final String timestamp);

  HoodieInstant getCleanInflightInstant(final String timestamp);

  HoodieInstant getCompactionRequestedInstant(final String timestamp);

  HoodieInstant getCompactionInflightInstant(final String timestamp);

  HoodieInstant getLogCompactionRequestedInstant(final String timestamp);

  HoodieInstant getLogCompactionInflightInstant(final String timestamp);

  HoodieInstant getReplaceCommitRequestedInstant(final String timestamp);

  HoodieInstant getReplaceCommitInflightInstant(final String timestamp);

  HoodieInstant getClusteringCommitRequestedInstant(final String timestamp);

  HoodieInstant getClusteringCommitInflightInstant(final String timestamp);

  HoodieInstant getRollbackRequestedInstant(HoodieInstant instant);

  HoodieInstant getRestoreRequestedInstant(HoodieInstant instant);

  HoodieInstant getIndexRequestedInstant(final String timestamp);

  HoodieInstant getIndexInflightInstant(final String timestamp);
}
