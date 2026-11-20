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

package org.apache.hudi.utilities.unstructured.storage;

import org.apache.hudi.common.table.HoodieTableMetaClient;

/**
 * Where managed out-of-line files (the embedding stream's stored inputs) live: under {@code .hoodie/.blob} of the table, named
 * relative to the table's base path. The one place in the unstructured code that knows the folder: writers name files under
 * it, and a check whether a table-relative path is a managed out-of-line file uses {@link #isManagedBlobPath(String)}. Core
 * Hudi does not know the folder; its commit-time marker reconciliation skips every marked file under the table's meta folder,
 * which includes this one.
 */
public final class ManagedBlobPaths {

  /** The folder, relative to the table's base path. */
  public static final String FOLDER = HoodieTableMetaClient.METAFOLDER_NAME + "/.blob";

  private ManagedBlobPaths() {
  }

  /** Whether {@code relativePath}, relative to the table's base path, is a managed out-of-line file. */
  public static boolean isManagedBlobPath(String relativePath) {
    return relativePath != null && relativePath.startsWith(FOLDER + "/");
  }
}
