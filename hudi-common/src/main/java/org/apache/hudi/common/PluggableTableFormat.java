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

package org.apache.hudi.common;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.TimelineFactory;
import org.apache.hudi.common.table.view.FileSystemViewManager;

import java.io.Serializable;
import java.util.List;

/**
 * External Table Format needs to implement this interface
 */
public interface PluggableTableFormat extends Serializable {

  /**
   * Called just after marking the write action as complete in hoodie timeline. Implementation expected to save additional state needed in
   * extraMetadata.
   *
   * @param commitMetadata HoodieCommitMetadata for commit or clustering action.
   * @param completedInstant completed instant in hoodie timeline
   * @param engineContext engine context used for execution - local,spark or flink etc.
   * @param metaClient metaClient from HoodieTable.
   * @param viewManager viewManager from HoodieTable.
   */

  default void commit(
      HoodieCommitMetadata commitMetadata,
      HoodieInstant completedInstant,
      HoodieEngineContext engineContext,
      HoodieTableMetaClient metaClient,
      FileSystemViewManager viewManager) {
  }

  /**
   * Called after marking the clean action as complete in hoodie timeline.
   *
   * @param cleanMetadata HoodieCleanMetadata for clean action.
   * @param completedInstant completed instant in hoodie timeline
   * @param engineContext engine context used for execution - local,spark or flink etc.
   * @param metaClient metaClient from HoodieTable.
   * @param viewManager viewManager from HoodieTable.
   */
  default void clean(
      HoodieCleanMetadata cleanMetadata,
      HoodieInstant completedInstant,
      HoodieEngineContext engineContext,
      HoodieTableMetaClient metaClient,
      FileSystemViewManager viewManager) {
  }

  /**
   * Called after archiving the instants in hoodie timeline.
   *
   * @param archivedInstants List of instants archived in hoodie timeline
   * @param engineContext engine context used for execution - local,spark or flink etc.
   * @param metaClient metaClient from HoodieTable.
   * @param viewManager  viewManager from HoodieTable.
   */
  default void archive(
      List<HoodieInstant> archivedInstants,
      HoodieEngineContext engineContext,
      HoodieTableMetaClient metaClient,
      FileSystemViewManager viewManager) {
  }

  /**
   * Called before rolling back the instant  in hoodie timeline.
   *
   * @param completedInstant completed rollback instant in hoodie timeline
   * @param engineContext engine context used for execution - local,spark or flink etc.
   * @param metaClient metaClient from HoodieTable.
   * @param viewManager viewManager from HoodieTable.
   */
  default void rollback(
      HoodieInstant completedInstant,
      HoodieEngineContext engineContext,
      HoodieTableMetaClient metaClient,
      FileSystemViewManager viewManager) {
  }

  /**
   * Called after marking a complete write action as "savepoint" in the hoodie timeline.
   *
   * @param instant The completed instant to be marked as savepoint.
   * @param engineContext engine context used for execution - local,spark or flink etc.
   * @param metaClient metaClient from HoodieTable.
   * @param viewManager viewManager from HoodieTable.
   */
  default void savepoint(
      HoodieInstant instant,
      HoodieEngineContext engineContext,
      HoodieTableMetaClient metaClient,
      FileSystemViewManager viewManager) {
  }

  /**
   * Called before restoring to the instant
   *
   * @param savepoint The savepoint instant to be restored back.
   * @param engineContext engine context used for execution - local,spark or flink etc.
   * @param metaClient  metaClient from HoodieTable.
   * @param viewManager viewManager from HoodieTable.
   */
  default void restore(
      HoodieInstant savepoint,
      HoodieEngineContext engineContext,
      HoodieTableMetaClient metaClient,
      FileSystemViewManager viewManager) {
  }

  TimelineFactory getTimelineFactory();
}
