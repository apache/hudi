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

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;

import java.io.Serializable;

/**
 * Produces ANN candidates for a plan (RFC-104 v3 §4). The MDT implementation owns posting decoding,
 * overlay resolution, pass-1 filtering, pass-2 scoring, and bounded candidate retention — and it
 * MUST NOT invoke Spark SQL, file-format readers, or exact-read code. It decodes record keys and
 * locators only for retained candidates and returns at most {@code maxRerankCandidates} ordered by
 * approximate distance, in a single scan.
 */
public interface VectorCandidateSource extends Serializable {

  HoodieData<VectorCandidate> scan(VectorSearchPlan plan, HoodieEngineContext engineContext);
}
