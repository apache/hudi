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

import org.apache.hudi.storage.StoragePath;

/** Ownership rules for completed out-of-line blob references. */
public final class StorageReferenceRules {

  private StorageReferenceRules() {
  }

  /** Enforces layout-one ownership when writing a completed blob reference. */
  public static void validateWriteOwnership(String externalPath, boolean managed) {
    if (externalPath == null || externalPath.isEmpty()) {
      throw new IllegalArgumentException("A written blob reference must have a non-empty external_path");
    }
    boolean hasScheme = new StoragePath(externalPath).toUri().getScheme() != null;
    if (hasScheme && managed) {
      throw new IllegalArgumentException("Blob external_path '" + externalPath + "' has a URI scheme and requires managed=false");
    }
    if (!hasScheme && !managed) {
      throw new IllegalArgumentException("Blob external_path '" + externalPath + "' is table-relative and requires managed=true");
    }
  }

}
