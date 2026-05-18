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

package org.apache.hudi.common.table.timeline.versioning;

import java.io.Serializable;

/**
 * Responsible for upgrading and downgrading metadata versions for a specific metadata.
 * 
 * @param <T> Metadata Type
 */
public interface VersionMigrator<T> extends Serializable {

  /**
   * Version of Metadata that this class will handle.
   * 
   * @return Version handled by this class.
   */
  Integer getManagedVersion();

  /**
   * Upgrades metadata of type T from previous version to this version.
   * 
   * @param input Metadata as of previous version.
   * @return Metadata compatible with the version managed by this class
   */
  T upgradeFrom(T input);

  /**
   * Downgrades metadata of type T from next version to this version.
   * 
   * @param input Metadata as of next highest version
   * @return Metadata compatible with the version managed by this class
   */
  T downgradeFrom(T input);
}
