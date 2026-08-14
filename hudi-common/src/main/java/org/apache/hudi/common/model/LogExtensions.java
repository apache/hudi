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
 * Logical extensions used in Hudi log file names.
 */
public final class LogExtensions {

  public static final String DATA_LOG_EXTENSION = "log";
  public static final String DELETE_LOG_EXTENSION = "deletes";
  public static final String CDC_LOG_EXTENSION = "cdc";
  public static final String ARCHIVE_LOG_EXTENSION = "archive";

  private LogExtensions() {
  }
}
