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

package org.apache.hudi.common.engine;

/**
 * Properties specific to each engine, that can be set/obtained from.
 */
public enum EngineProperty {
  // hostname to bind embedded timeline server to
  EMBEDDED_SERVER_HOST,
  // Pool/queue to use to run compaction.
  COMPACTION_POOL_NAME,
  TOTAL_CORES_PER_EXECUTOR,
  // Amount of total memory available to each engine executor
  TOTAL_MEMORY_AVAILABLE,
  // Fraction of that memory, that is already in use by the engine
  MEMORY_FRACTION_IN_USE
}
