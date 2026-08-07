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
 * The recorded outcome of execution-locality selection (RFC-109 §11A). Carries both the
 * requested and selected mode plus the inputs to the decision so it can be emitted verbatim into
 * query metrics and workload-profile results.
 */
public final class VectorExecutionDecision implements Serializable {

  private static final long serialVersionUID = 1L;

  private final VectorExecutionMode requestedMode;
  private final VectorExecutionMode selectedMode;
  private final int maxRerankCandidates;
  private final int localExecutionThreshold;
  private final String selectorVersion;

  public VectorExecutionDecision(VectorExecutionMode requestedMode,
                                 VectorExecutionMode selectedMode,
                                 int maxRerankCandidates,
                                 int localExecutionThreshold,
                                 String selectorVersion) {
    this.requestedMode = requestedMode;
    this.selectedMode = selectedMode;
    this.maxRerankCandidates = maxRerankCandidates;
    this.localExecutionThreshold = localExecutionThreshold;
    this.selectorVersion = selectorVersion;
  }

  public VectorExecutionMode getRequestedMode() {
    return requestedMode;
  }

  public VectorExecutionMode getSelectedMode() {
    return selectedMode;
  }

  public int getMaxRerankCandidates() {
    return maxRerankCandidates;
  }

  public int getLocalExecutionThreshold() {
    return localExecutionThreshold;
  }

  public String getSelectorVersion() {
    return selectorVersion;
  }

  @Override
  public String toString() {
    return "VectorExecutionDecision{requested=" + requestedMode
        + ", selected=" + selectedMode
        + ", maxRerankCandidates=" + maxRerankCandidates
        + ", localThreshold=" + localExecutionThreshold
        + ", selectorVersion=" + selectorVersion + '}';
  }
}
