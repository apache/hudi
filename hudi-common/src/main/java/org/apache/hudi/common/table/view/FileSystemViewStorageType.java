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

package org.apache.hudi.common.table.view;

/**
 * Storage Type used to store/retrieve File system view of a table.
 */
public enum FileSystemViewStorageType {
  // In-memory storage of file-system view
  MEMORY,
  // Constrained Memory storage for file-system view with overflow data spilled to disk
  SPILLABLE_DISK,
  // EMBEDDED Key Value Storage for file-system view
  EMBEDDED_KV_STORE,
  // Delegate file-system view to remote server
  REMOTE_ONLY,
  // A composite storage where file-system view calls are first delegated to Remote server ( REMOTE_ONLY )
  // In case of failures, switches subsequent calls to secondary local storage type
  REMOTE_FIRST
}
