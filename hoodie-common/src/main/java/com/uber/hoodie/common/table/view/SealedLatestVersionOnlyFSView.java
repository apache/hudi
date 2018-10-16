/*
 *  Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.common.table.view;

import com.uber.hoodie.common.table.TableFileSystemView.ReadOptimizedViewWithLatestSlice;
import com.uber.hoodie.common.table.TableFileSystemView.RealtimeViewWithLatestSlice;

/**
 * A latest file-slice only FS view with method to seal the view to track the state when the cache is fully populated.
 * Useful to add pre-conditions to check if the cache is indeed effective
 */
public interface SealedLatestVersionOnlyFSView extends RealtimeViewWithLatestSlice,
    ReadOptimizedViewWithLatestSlice {

  /**
   * Seal the file-system view. New files will no longer be added to the file-system view after sealed.
   * Allows for 2 phases ->
   *  (a) Growing phase where the view is getting constructed. once this phase is done, seal the view
   *  (b) Immutable View where the view represents a consistent snapshot
   */
  public void seal();
}
