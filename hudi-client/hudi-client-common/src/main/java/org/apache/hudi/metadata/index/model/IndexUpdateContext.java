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

package org.apache.hudi.metadata.index.model;

import org.apache.hudi.common.data.HoodieBroadcastScope;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Lazy;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.Accessors;

/**
 * Shared input context for updating a metadata index partition from commit metadata.
 */
@AllArgsConstructor(staticName = "of")
@Getter
@Accessors(fluent = true)
public class IndexUpdateContext {
  private final String instantTime;
  private final HoodieBackedTableMetadata tableMetadata;
  private final Lazy<HoodieTableFileSystemView> lazyFileSystemView;
  private final HoodieCommitMetadata commitMetadata;
  /**
   * Scope of the broadcasts that the index updates of the commit share, released by the metadata writer once the updates
   * are written. Without it, each update makes its own broadcasts and leaves them to the engine to clean up.
   */
  private final Option<HoodieBroadcastScope> broadcastScope;

  public static IndexUpdateContext of(String instantTime, HoodieBackedTableMetadata tableMetadata,
                                      Lazy<HoodieTableFileSystemView> lazyFileSystemView, HoodieCommitMetadata commitMetadata) {
    return of(instantTime, tableMetadata, lazyFileSystemView, commitMetadata, Option.empty());
  }
}
