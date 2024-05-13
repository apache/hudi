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

package org.apache.hudi.io.storage;

import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.fs.ConsistencyGuard;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

/**
 * Base class to get HoodieFileReaderFactory and HoodieFileWriterFactory
 */
public abstract class HoodieIOFactory {

  public static HoodieIOFactory getIOFactory(StorageConfiguration<?> storageConf) {
    String ioFactoryClass = storageConf.getString(HoodieStorageConfig.HOODIE_IO_FACTORY_CLASS.key())
        .orElse(HoodieStorageConfig.HOODIE_IO_FACTORY_CLASS.defaultValue());
    try {
      return (HoodieIOFactory) ReflectionUtils
          .loadClass(ioFactoryClass, new Class<?>[] {StorageConfiguration.class}, storageConf);
    } catch (Exception e) {
      throw new HoodieException("Unable to create " + ioFactoryClass, e);
    }
  }

  public abstract HoodieFileReaderFactory getReaderFactory(HoodieRecord.HoodieRecordType recordType);

  public abstract HoodieFileWriterFactory getWriterFactory(HoodieRecord.HoodieRecordType recordType);

  public abstract HoodieStorage getStorage(StoragePath storagePath);

  public abstract HoodieStorage getStorage(StoragePath path,
                                           boolean enableRetry,
                                           long maxRetryIntervalMs,
                                           int maxRetryNumbers,
                                           long initialRetryIntervalMs,
                                           String retryExceptions,
                                           ConsistencyGuard consistencyGuard);
}
