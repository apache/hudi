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

/**
 * Terminal status of a vector search request (RFC-104 v3 §1).
 *
 * <ul>
 *   <li>{@code COMPLETED}: K live exact results returned.</li>
 *   <li>{@code PARTIAL}: fewer than K returned because the retained candidate pool was exhausted
 *       (not a deadline), with {@link DeadlinePolicy#RETURN_PARTIAL} in effect.</li>
 *   <li>{@code DEADLINE_EXCEEDED}: the request budget expired; results may be partial.</li>
 *   <li>{@code FAILED}: an error prevented completion.</li>
 * </ul>
 */
public enum VectorSearchStatus {
  COMPLETED,
  PARTIAL,
  DEADLINE_EXCEEDED,
  FAILED
}
