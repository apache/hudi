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

package org.apache.hudi.utilities.unstructured.embedding.source;

import org.apache.hudi.utilities.unstructured.storage.StorageReferenceRules;

import java.io.IOException;

/**
 * A source row's {@code blob} value that cannot name a readable range — null, INLINE without data, an unknown storage type,
 * OUT_OF_LINE without a reference, an empty, unparseable or relative path, an invalid range, a scheme outside the served
 * set ({@link StorageReferenceRules#isServedScheme}), or a malformed URI/authority rejected during storage
 * construction. Storage initialization and read I/O failures are not
 * reference faults. A reference fault is the row's own fault: reading it again gives the same answer, so data prep classifies
 * it as a content failure of its file.
 */
public class BlobReferenceException extends IOException {

  private static final long serialVersionUID = 1L;

  public BlobReferenceException(String message) {
    super(message);
  }

  public BlobReferenceException(String message, Throwable cause) {
    super(message, cause);
  }
}
