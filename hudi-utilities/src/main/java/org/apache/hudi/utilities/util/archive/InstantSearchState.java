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

package org.apache.hudi.utilities.util.archive;

/**
 * How a search resolved one target instant.
 *
 * <p>The distinction between {@link #NOT_FOUND} and {@link #INDETERMINATE} is the safety property of
 * this package. Collapsing the two is a data-loss bug: downstream, absence of an instant licenses
 * treating a file as unreferenced, and therefore a delete.
 */
public enum InstantSearchState {

  /** Located in an archive file. The record was handed to the consumer. */
  FOUND,

  /**
   * Every byte that could have held the instant was read cleanly, and it was not there.
   *
   * <p>This is a claim a caller may act on.
   */
  NOT_FOUND,

  /**
   * The instant was not located, but the search could not cover everything it needed to: a file
   * whose range covered the instant was damaged, or the run was interrupted.
   *
   * <p>This is <b>not</b> absence, and a caller must never treat it as one.
   */
  INDETERMINATE
}
