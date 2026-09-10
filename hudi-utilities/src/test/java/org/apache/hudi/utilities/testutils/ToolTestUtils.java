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

package org.apache.hudi.utilities.testutils;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;

/**
 * Assertion helpers shared by the tests of the standalone spark tools.
 */
public final class ToolTestUtils {

  private ToolTestUtils() {
  }

  /**
   * The message of {@code throwable} and of every cause under it, one per line. The tools wrap what they catch,
   * so the detail a test cares about is usually a few causes down.
   */
  public static String stackMessages(Throwable throwable) {
    StringBuilder sb = new StringBuilder();
    for (Throwable t = throwable; t != null; t = t.getCause()) {
      sb.append(t.getMessage()).append('\n');
    }
    return sb.toString();
  }

  /**
   * How many base files the latest file slices of {@code partition} hold, listed off the file system rather than
   * the metadata table so that a partition dropped by a replacecommit reads as empty.
   */
  public static long latestBaseFileCount(HoodieEngineContext context, HoodieTableMetaClient metaClient,
                                         String partition) {
    HoodieTableMetaClient reloaded = HoodieTableMetaClient.reload(metaClient);
    try (HoodieTableFileSystemView fsView = FileSystemViewManager.createInMemoryFileSystemView(
        context, reloaded, HoodieMetadataConfig.newBuilder().enable(false).build())) {
      return fsView.getLatestBaseFiles(partition).count();
    }
  }
}
