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

package org.apache.hudi.io;

import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A SingleFileHandleCreateFactory is used to write all data in the spark partition into a single data file.
 * <p>
 * Please use this with caution. This can end up creating very large files if not used correctly.
 */
public class SingleFileHandleCreateFactory<T extends HoodieRecordPayload, I, K, O> extends CreateHandleFactory<T, I, K, O> implements Serializable {

  private final AtomicBoolean isHandleCreated = new AtomicBoolean(false);
  private final String fileId;
  private final boolean preserveHoodieMetadata;

  public SingleFileHandleCreateFactory(String fileId, boolean preserveHoodieMetadata) {
    super();
    this.fileId = fileId;
    this.preserveHoodieMetadata = preserveHoodieMetadata;
  }

  @Override
  public HoodieWriteHandle<T, I, K, O> create(final HoodieWriteConfig hoodieConfig, final String commitTime,
                                              final HoodieTable<T, I, K, O> hoodieTable, final String partitionPath,
                                              final String fileIdPrefix, TaskContextSupplier taskContextSupplier) {

    if (isHandleCreated.compareAndSet(false, true)) {
      return new HoodieUnboundedCreateHandle(hoodieConfig, commitTime, hoodieTable, partitionPath,
          fileId, // ignore idPfx, always use same fileId
          taskContextSupplier, preserveHoodieMetadata);
    }

    throw new HoodieIOException("Fixed handle create is only expected to be invoked once");
  }
}
