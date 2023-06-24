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

package org.apache.hudi.internal.schema.io;

import org.apache.hudi.common.util.Option;

import java.util.List;

abstract class AbstractInternalSchemaStorageManager {

  /**
   * Persist history schema str.
   */
  public abstract void persistHistorySchemaStr(String instantTime, String historySchemaStr);

  /**
   * Get latest history schema string.
   */
  public abstract String getHistorySchemaStr();

  /**
   * Get latest history schema string.
   * Using give validCommits to validate all legal history Schema files, and return the latest one.
   * If the passed valid commits is null or empty, valid instants will be fetched from the file-system and used.
   */
  public abstract String getHistorySchemaStrByGivenValidCommits(List<String> validCommits);

  /**
   * Get internalSchema by using given versionId
   *
   * @param versionId schema version_id need to search
   * @return internalSchema
   */
  public abstract Option getSchemaByKey(String versionId);
}
