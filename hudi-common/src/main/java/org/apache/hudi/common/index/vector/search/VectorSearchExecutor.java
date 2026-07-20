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
 * The single engine-neutral entry point for vector search (RFC-109 v3 §11). Pins one snapshot,
 * probes IVF clusters, scans MDT postings, reduces the candidate pool, RLI-arbitrates, plans
 * file-slice fetches, chooses LOCAL/DISTRIBUTED execution, performs projected positional/key reads,
 * scores exactly, and reduces to top-K — all under one request deadline. Never invokes
 * {@code spark.sql(...)} and never reconstructs SQL/DataFrames to execute exact fetches.
 */
public interface VectorSearchExecutor extends Serializable {

  HoodieData<VectorSearchResult> execute(VectorSearchRequest request, HoodieEngineContext engineContext);
}
