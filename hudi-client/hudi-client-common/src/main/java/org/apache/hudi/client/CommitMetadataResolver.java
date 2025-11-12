/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.client;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;

/**
 * Provides reconciliation between the commit metadata and files on storage
 */
public interface CommitMetadataResolver {
  /**
   * Reconcile the commit metadata against the files on storage to add missing files
   *
   * @param config         the write config
   * @param context        {@link HoodieEngineContext} instance
   * @param table          {@link HoodieTable} instance
   * @param instantTime    instant time of the transaction
   * @param commitMetadata input commit metadata
   * @return reconciled commit metadata with all files
   * @throws HoodieIOException upon I/O errors
   */
  HoodieCommitMetadata reconcileMetadataForMissingFiles(HoodieWriteConfig config,
                                                        HoodieEngineContext context,
                                                        HoodieTable table,
                                                        String instantTime,
                                                        HoodieCommitMetadata commitMetadata) throws HoodieIOException;
}
