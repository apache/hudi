/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.io;

import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

/**
 * Create handle factory for Flink writer, use the specified write handle directly.
 */
public class ExplicitWriteHandleFactory<T extends HoodieRecordPayload, I, K, O>
    extends WriteHandleFactory<T, I, K, O> {
  private final HoodieWriteHandle<T, I, K, O> writeHandle;

  public ExplicitWriteHandleFactory(HoodieWriteHandle<T, I, K, O> writeHandle) {
    this.writeHandle = writeHandle;
  }

  @Override
  public HoodieWriteHandle<T, I, K, O> create(
      HoodieWriteConfig hoodieConfig, String commitTime,
      HoodieTable<T, I, K, O> hoodieTable, String partitionPath,
      String fileIdPrefix, TaskContextSupplier taskContextSupplier) {
    return writeHandle;
  }

  public HoodieWriteHandle<T, I, K, O> getWriteHandle() {
    return writeHandle;
  }
}
