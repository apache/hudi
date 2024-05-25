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

package org.apache.hudi.common.model;

/**
 * Hudi table could be queried in one of the 3 following ways:
 *
 * <ol>
 *   <li>Snapshot: snapshot of the table at the given (latest if not provided) instant is queried</li>
 *   <li>Read Optimized (MOR only): snapshot of the table at the given (latest if not provided)
 *   instant is queried, but w/o reading any of the delta-log files (only reading base-files)</li>
 *   <li>Incremental: only records added w/in the given time-window (defined by beginning and ending instant)
 *   are queried</li>
 * </ol>
 */
public enum HoodieTableQueryType {
  SNAPSHOT,
  INCREMENTAL,
  READ_OPTIMIZED
}
