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

package org.apache.hudi.common.config;

/**
 * Constants for lock audit configuration shared across modules.
 */
public class HoodieLockAuditConfig {
  
  /**
   * Name of the audit configuration file.
   */
  public static final String AUDIT_CONFIG_FILE_NAME = "audit_enabled.json";
  
  /**
   * JSON field name for the audit service enabled flag.
   */
  public static final String STORAGE_LOCK_AUDIT_SERVICE_ENABLED_FIELD = "STORAGE_LOCK_AUDIT_SERVICE_ENABLED";
}