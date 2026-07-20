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
 * Freshness verdict for a finalist candidate, produced by RLI arbitration (RFC-109 v3 §7).
 * Engine-neutral successor to the internal arbiter decision enum.
 *
 * <ul>
 *   <li>{@code SERVE}: posting still faithfully represents the live record; positional trust holds.</li>
 *   <li>{@code STALE}: record was rewritten/moved; posting locator is a stale hint, exact mode must
 *       key-fetch at the live location.</li>
 *   <li>{@code DELETED}: record no longer exists; drop.</li>
 * </ul>
 */
public enum VectorCandidateState {
  SERVE,
  STALE,
  DELETED
}
