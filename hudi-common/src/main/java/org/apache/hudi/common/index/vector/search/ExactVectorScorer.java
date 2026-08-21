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

import org.apache.hudi.common.index.vector.VectorDistanceMetric;

import java.io.Serializable;

/**
 * Computes the exact metric distance between a query and a full-precision candidate vector
 * (RFC-109 §10). Accumulates in float64 and keeps squared L2 internally; the surfaced value
 * follows the requested {@link VectorDistanceMetric}.
 */
public interface ExactVectorScorer extends Serializable {

  double distance(float[] query, float[] candidate, VectorDistanceMetric metric);
}
