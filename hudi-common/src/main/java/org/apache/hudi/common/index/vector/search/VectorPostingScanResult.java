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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Immutable output of one MDT posting scan before canonical overlay resolution. */
public final class VectorPostingScanResult implements Serializable {

  private static final long serialVersionUID = 1L;

  private final List<VectorCandidate> packedCandidates;
  private final List<VectorCandidate> deltaCandidates;
  private final List<VectorPostingKey> tombstonedPostingKeys;

  public VectorPostingScanResult(Collection<VectorCandidate> packedCandidates,
                                 Collection<VectorCandidate> deltaCandidates,
                                 Collection<VectorPostingKey> tombstonedPostingKeys) {
    this.packedCandidates = immutableCopy(packedCandidates, "packedCandidates");
    this.deltaCandidates = immutableCopy(deltaCandidates, "deltaCandidates");
    this.tombstonedPostingKeys = immutableCopy(tombstonedPostingKeys, "tombstonedPostingKeys");
  }

  public List<VectorCandidate> getPackedCandidates() {
    return packedCandidates;
  }

  public List<VectorCandidate> getDeltaCandidates() {
    return deltaCandidates;
  }

  public List<VectorPostingKey> getTombstonedPostingKeys() {
    return tombstonedPostingKeys;
  }

  private static <T> List<T> immutableCopy(Collection<T> values, String name) {
    Objects.requireNonNull(values, name);
    return Collections.unmodifiableList(new ArrayList<>(values));
  }
}
