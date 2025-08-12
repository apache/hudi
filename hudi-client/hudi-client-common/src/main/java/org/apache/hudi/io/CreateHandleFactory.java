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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.HoodieTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class CreateHandleFactory<T, I, K, O> extends WriteHandleFactory<T, I, K, O> implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(CreateHandleFactory.class);

  private boolean preserveMetadata = false;

  public CreateHandleFactory() {
    this(false);
  }

  public CreateHandleFactory(boolean preserveMetadata) {
    this.preserveMetadata = preserveMetadata;
  }

  @Override
  public HoodieWriteHandle<T, I, K, O> create(final HoodieWriteConfig hoodieConfig, final String commitTime,
                                              final HoodieTable<T, I, K, O> hoodieTable, final String partitionPath,
                                              final String fileIdPrefix, TaskContextSupplier taskContextSupplier) {
    boolean isFallbackEnabled = hoodieConfig.isCreateHandleFallbackEnabled();
    String createHandleClass = hoodieConfig.getCreateHandleClassName();
    LOG.info("Create CreateHandle implementation {} for fileIdPrefix {} and partition path {} at commit {}",
        createHandleClass, fileIdPrefix, partitionPath, commitTime);

    Class<?>[] constructorArgTypes = new Class<?>[] {HoodieWriteConfig.class, String.class, HoodieTable.class, String.class,
      String.class, Option.class, TaskContextSupplier.class, boolean.class};
    String nextFileId = getNextFileId(fileIdPrefix);
    try {
      return ReflectionUtils.loadClass(createHandleClass, constructorArgTypes,
          hoodieConfig, commitTime, hoodieTable, partitionPath, nextFileId, Option.empty(), taskContextSupplier, preserveMetadata);
    } catch (Throwable e1) {
      if (isFallbackEnabled) {
        String fallbackCreateHandleClass = HoodieWriteConfig.CREATE_HANDLE_CLASS_NAME.defaultValue();
        try {
          LOG.warn("HoodieCreateHandle implementation {} failed, now creating fallback implementation {} for fileIdPrefix {} and partitionPath {} at commit {}",
              createHandleClass, fallbackCreateHandleClass, fileIdPrefix, partitionPath, commitTime);
          return ReflectionUtils.loadClass(fallbackCreateHandleClass, constructorArgTypes,
              hoodieConfig, commitTime, hoodieTable, partitionPath, nextFileId, Option.empty(), taskContextSupplier, preserveMetadata);
        } catch (Throwable e2) {
          throw new HoodieException("Could not instantiate the fallback HoodieCreateHandle implementation: " + fallbackCreateHandleClass, e2);
        }
      }
      throw new HoodieException("Could not instantiate the HoodieCreateHandle implementation: " + createHandleClass, e1);
    }
  }
}
