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

package org.apache.hudi.utilities.util;

/**
 * The four states an instant can be in on a timeline, and nothing else.
 *
 * <p>Whether the state could be <em>determined</em> is a separate axis — see
 * {@link InstantStateIndex.Resolution}. Earlier drafts of this design merged the two and invented
 * values ("never completed", which is {@link #NULL}; "unknown", which is not a state at all), which
 * made the reconciliation switch impossible to reason about.
 */
public enum InstantState {
  /** Not found on the timeline. */
  NULL,
  REQUESTED,
  INFLIGHT,
  COMPLETED
}
