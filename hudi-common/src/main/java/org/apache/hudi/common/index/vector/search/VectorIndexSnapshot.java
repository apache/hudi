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

package org.apache.hudi.common.index.vector.search;

import java.io.Serializable;

/**
 * Immutable identity of the active vector-index generation used to serve a query (RFC-109).
 * Every field is versioned through the manifest so readers can reject unsupported or mismatched
 * encodings rather than silently mis-scoring. Pinned for the whole request alongside the table
 * instant in {@link VectorSearchSnapshot}.
 */
public final class VectorIndexSnapshot implements Serializable {

  private static final long serialVersionUID = 1L;

  private final int generationId;
  private final int factorVersion;
  private final int blockFormatVersion;
  private final String rotationVersion;
  private final String quantizerVersion;

  public VectorIndexSnapshot(int generationId,
                             int factorVersion,
                             int blockFormatVersion,
                             String rotationVersion,
                             String quantizerVersion) {
    this.generationId = generationId;
    this.factorVersion = factorVersion;
    this.blockFormatVersion = blockFormatVersion;
    this.rotationVersion = rotationVersion;
    this.quantizerVersion = quantizerVersion;
  }

  public int getGenerationId() {
    return generationId;
  }

  public int getFactorVersion() {
    return factorVersion;
  }

  public int getBlockFormatVersion() {
    return blockFormatVersion;
  }

  public String getRotationVersion() {
    return rotationVersion;
  }

  public String getQuantizerVersion() {
    return quantizerVersion;
  }
}
