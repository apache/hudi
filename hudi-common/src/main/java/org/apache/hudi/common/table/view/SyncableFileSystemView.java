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

package org.apache.hudi.common.table.view;

import org.apache.hudi.common.table.view.TableFileSystemView.BaseFileOnlyView;
import org.apache.hudi.common.table.view.TableFileSystemView.SliceView;

/**
 * A consolidated file-system view interface exposing both complete slice and basefile only views along with
 * update operations.
 */
public interface SyncableFileSystemView
    extends TableFileSystemView, BaseFileOnlyView, SliceView, AutoCloseable {



  /**
   * Allow View to release resources and close.
   */
  void close();

  /**
   * Reset View so that they can be refreshed.
   */
  void reset();

  /**
   * Read the latest timeline and refresh the file-system view to match the current state of the file-system. The
   * refresh can either be done incrementally (from reading file-slices in metadata files) or from scratch by resetting
   * view storage.
   */
  void sync();
}
