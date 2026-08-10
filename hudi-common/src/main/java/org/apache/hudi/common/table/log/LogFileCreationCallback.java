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

package org.apache.hudi.common.table.log;

import org.apache.hudi.common.model.HoodieLogFile;

/**
 * A callback allowing plug-in logic for log file creation.
 */
public interface LogFileCreationCallback {
  /**
   * Executes action right before log file is created.
   *
   * @param logFile The log file.
   * @return true if the action executes successfully.
   */
  default boolean preFileCreation(HoodieLogFile logFile) {
    return true;
  }
}
