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
 * Resolves the single pinned {@link VectorSearchSnapshot} for a request (RFC-109 v3 §7, §11):
 * the table instant (from {@code request.queryInstant} or the latest completed instant) plus the
 * active {@link VectorIndexSnapshot} generation identity. Injected so the common executor stays
 * engine-neutral — engine adapters provide the metadata-backed implementation.
 */
@FunctionalInterface
public interface VectorSnapshotResolver extends Serializable {

  VectorSearchSnapshot resolve(VectorSearchRequest request);
}
