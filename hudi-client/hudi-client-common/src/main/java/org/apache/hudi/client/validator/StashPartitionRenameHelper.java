/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.client.validator;

import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import java.io.IOException;
import java.io.Serializable;

/**
 * Interface for moving partition files between a table's partition path and a stash path.
 * <p>
 * This is used by {@link StashPartitionsPreCommitValidator} to move data files
 * to a stash location before a deletePartitions commit completes, and by the
 * stash tool to restore files back from stash during recovery or rollback.
 * <p>
 * Two separate methods are provided because the stash and restore directions may
 * require different storage APIs or strategies. For example, an implementation
 * may use a fast filesystem-level rename for stashing but a verified copy for
 * restoring data back to the table.
 * <p>
 * Both methods must be idempotent: if some files already exist in the target
 * from a prior partial attempt, only the remaining files should be moved.
 * <p>
 * The default implementation ({@link DefaultStashPartitionRenameHelper}) copies
 * files individually and then deletes from source for both directions. Custom
 * implementations can leverage efficient filesystem-level rename/move APIs where
 * available.
 */
public interface StashPartitionRenameHelper extends Serializable {

  /**
   * Move all files from a table partition path to the stash path (stash direction).
   * <p>
   * Called by the pre-commit validator during a deletePartitions operation to back up
   * partition data before the replace commit lands.
   * <p>
   * Must be idempotent — if files already exist in stash from a prior partial
   * attempt, only move files not yet present in stash.
   *
   * @param storage        the storage instance to use for file operations
   * @param partitionPath  the source partition directory (e.g., basepath/datestr=2023-01-01)
   * @param stashPath      the target stash directory (e.g., stashpath/datestr=2023-01-01)
   * @throws IOException if any file operation fails
   */
  void stashPartitionFiles(HoodieStorage storage, StoragePath partitionPath, StoragePath stashPath) throws IOException;

  /**
   * Restore all files from the stash path back to the table partition path (restore direction).
   * <p>
   * Called during pre-check recovery (to move partially stashed files back before retry)
   * and during rollback_stash (to undo a stash operation).
   * <p>
   * Must be idempotent — if files already exist in the partition from a prior partial
   * attempt, only move files not yet present in the partition.
   *
   * @param storage        the storage instance to use for file operations
   * @param stashPath      the source stash directory (e.g., stashpath/datestr=2023-01-01)
   * @param partitionPath  the target partition directory (e.g., basepath/datestr=2023-01-01)
   * @throws IOException if any file operation fails
   */
  void restorePartitionFiles(HoodieStorage storage, StoragePath stashPath, StoragePath partitionPath) throws IOException;
}
