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
 * In some cases, the Hudi engine needs to know what operation mode the current merging belongs to.
 * {@link HoodieRecordMerger} that wants to distinguish the operation mode should implement this interface.
 */
public interface OperationModeAwareness {
  /**
   * Specifies the legacy operation mode as preCombining.
   *
   * <p>The preCombining takes place in two cases:
   * i). In memory records merging during data ingestion;
   * ii). Log records merging for MOR reader.
   */
  HoodieRecordMerger asPreCombiningMode();
}
