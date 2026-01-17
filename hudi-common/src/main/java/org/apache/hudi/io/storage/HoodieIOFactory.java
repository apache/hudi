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

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIClass;
import org.apache.hudi.PublicAPIMethod;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.fs.ConsistencyGuard;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.FileFormatUtils;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

/**
 * Base class to get {@link HoodieFileReaderFactory}, {@link HoodieFileWriterFactory}, and {@link FileFormatUtils}
 */
@PublicAPIClass(maturity = ApiMaturityLevel.EVOLVING)
public abstract class HoodieIOFactory {
  protected final HoodieStorage storage;

  public HoodieIOFactory(HoodieStorage storage) {
    this.storage = storage;
  }

  public static HoodieIOFactory getIOFactory(HoodieStorage storage) {
    String ioFactoryClass = storage.getConf().getString(HoodieStorageConfig.HOODIE_IO_FACTORY_CLASS.key())
        .orElse(HoodieStorageConfig.HOODIE_IO_FACTORY_CLASS.defaultValue());
    try {
      return (HoodieIOFactory) ReflectionUtils
          .loadClass(ioFactoryClass, new Class<?>[] {HoodieStorage.class}, storage);
    } catch (Exception e) {
      throw new HoodieException("Unable to create " + ioFactoryClass, e);
    }
  }

  /**
   * @param recordType {@link HoodieRecord} type.
   * @return a factory to create file readers.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract HoodieFileReaderFactory getReaderFactory(HoodieRecord.HoodieRecordType recordType);

  /**
   * @param recordType {@link HoodieRecord} type.
   * @return a factory to create file writers.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract HoodieFileWriterFactory getWriterFactory(HoodieRecord.HoodieRecordType recordType);

  /**
   * @param fileFormat file format supported in Hudi.
   * @return a util class to support read and write in the file format.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract FileFormatUtils getFileFormatUtils(HoodieFileFormat fileFormat);

  /**
   * @param storagePath file path.
   * @return {@link HoodieStorage} instance.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract HoodieStorage getStorage(StoragePath storagePath);

  /**
   * @param path                   file path.
   * @param enableRetry            whether to retry operations.
   * @param maxRetryIntervalMs     maximum retry interval in milliseconds.
   * @param maxRetryNumbers        maximum number of retries.
   * @param initialRetryIntervalMs initial delay before retry in milliseconds.
   * @param retryExceptions        retry exception list.
   * @param consistencyGuard       {@link ConsistencyGuard} instance.
   * @return {@link HoodieStorage} instance with retry capability if applicable.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract HoodieStorage getStorage(StoragePath path,
                                           boolean enableRetry,
                                           long maxRetryIntervalMs,
                                           int maxRetryNumbers,
                                           long initialRetryIntervalMs,
                                           String retryExceptions,
                                           ConsistencyGuard consistencyGuard);

  /**
   * @param path file path.
   * @return a util class to support read and write in the file format.
   */
  public final FileFormatUtils getFileFormatUtils(StoragePath path) {
    if (path.getFileExtension().equals(HoodieFileFormat.PARQUET.getFileExtension())) {
      return getFileFormatUtils(HoodieFileFormat.PARQUET);
    } else if (path.getFileExtension().equals(HoodieFileFormat.ORC.getFileExtension())) {
      return getFileFormatUtils(HoodieFileFormat.ORC);
    } else if (path.getFileExtension().equals(HoodieFileFormat.HFILE.getFileExtension())) {
      return getFileFormatUtils(HoodieFileFormat.HFILE);
    } else if (path.getFileExtension().equals(HoodieFileFormat.LANCE.getFileExtension())) {
      return getFileFormatUtils(HoodieFileFormat.LANCE);
    }
    throw new UnsupportedOperationException("The format for file " + path + " is not supported yet.");
  }
}
