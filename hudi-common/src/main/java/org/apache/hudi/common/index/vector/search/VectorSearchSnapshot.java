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
import java.util.Objects;

/**
 * The single pinned snapshot used for an entire vector search: one table instant shared by the MDT
 * index read, the RLI finalist lookup, file-slice resolution, and the base-table exact fetch, plus
 * the resolved {@link VectorIndexSnapshot} generation identity (RFC-104 v3 §7). Using one instant
 * across all reads is what makes freshness arbitration correct.
 */
public final class VectorSearchSnapshot implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String tableInstant;
  private final VectorIndexSnapshot vectorIndex;

  public VectorSearchSnapshot(String tableInstant, VectorIndexSnapshot vectorIndex) {
    this.tableInstant = Objects.requireNonNull(tableInstant, "tableInstant");
    this.vectorIndex = Objects.requireNonNull(vectorIndex, "vectorIndex");
  }

  public String getTableInstant() {
    return tableInstant;
  }

  public VectorIndexSnapshot getVectorIndex() {
    return vectorIndex;
  }
}
