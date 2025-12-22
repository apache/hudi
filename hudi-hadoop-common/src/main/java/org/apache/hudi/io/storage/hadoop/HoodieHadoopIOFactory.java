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

package org.apache.hudi.io.storage.hadoop;

import org.apache.hudi.common.fs.ConsistencyGuard;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.FileFormatUtils;
import org.apache.hudi.common.util.HFileUtils;
import org.apache.hudi.common.util.OrcUtils;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;

/**
 * Creates readers and writers for AVRO record payloads.
 * Currently uses reflection to support SPARK record payloads but
 * this ability should be removed with [HUDI-7746]
 */
public class HoodieHadoopIOFactory extends HoodieIOFactory {

  public HoodieHadoopIOFactory(HoodieStorage storage) {
    super(storage);
  }

  @Override
  public HoodieFileReaderFactory getReaderFactory(HoodieRecord.HoodieRecordType recordType) {
    switch (recordType) {
      case AVRO:
        return new HoodieAvroFileReaderFactory(storage);
      case SPARK:
        //TODO: remove this case [HUDI-7746]
        try {
          return (HoodieFileReaderFactory) ReflectionUtils
              .loadClass("org.apache.hudi.io.storage.HoodieSparkFileReaderFactory",
                  new Class<?>[] {HoodieStorage.class}, storage);
        } catch (Exception e) {
          throw new HoodieException("Unable to create HoodieSparkFileReaderFactory", e);
        }
      case FLINK:
        //TODO: remove this case [HUDI-7746]
        try {
          return (HoodieFileReaderFactory) ReflectionUtils
              .loadClass("org.apache.hudi.table.format.HoodieRowDataFileReaderFactory",
                  new Class<?>[] {HoodieStorage.class}, storage);
        } catch (Exception e) {
          throw new HoodieException("Unable to create HoodieRowDataFileReaderFactory", e);
        }
      default:
        throw new UnsupportedOperationException(recordType + " record type not supported");
    }
  }

  @Override
  public HoodieFileWriterFactory getWriterFactory(HoodieRecord.HoodieRecordType recordType) {
    switch (recordType) {
      case AVRO:
        return new HoodieAvroFileWriterFactory(storage);
      case SPARK:
        //TODO: remove this case [HUDI-7746]
        try {
          return (HoodieFileWriterFactory) ReflectionUtils
              .loadClass("org.apache.hudi.io.storage.HoodieSparkFileWriterFactory",
                  new Class<?>[] {HoodieStorage.class}, storage);
        } catch (Exception e) {
          throw new HoodieException("Unable to create HoodieSparkFileWriterFactory", e);
        }
      case FLINK:
        //TODO: remove this case [HUDI-7746]
        try {
          return (HoodieFileWriterFactory) ReflectionUtils
              .loadClass("org.apache.hudi.io.storage.row.HoodieRowDataFileWriterFactory",
                  new Class<?>[] {HoodieStorage.class}, storage);
        } catch (Exception e) {
          throw new HoodieException("Unable to create HoodieRowDataFileWriterFactory", e);
        }
      default:
        throw new UnsupportedOperationException(recordType + " record type not supported");
    }
  }

  @Override
  public FileFormatUtils getFileFormatUtils(HoodieFileFormat fileFormat) {
    switch (fileFormat) {
      case PARQUET:
        return new ParquetUtils();
      case ORC:
        return new OrcUtils();
      case HFILE:
        return new HFileUtils();
      default:
        throw new UnsupportedOperationException(fileFormat.name() + " format not supported yet.");
    }
  }

  @Override
  public HoodieStorage getStorage(StoragePath storagePath) {
    return storage.newInstance(storagePath, storage.getConf());
  }

  @Override
  public HoodieStorage getStorage(StoragePath path,
                                  boolean enableRetry,
                                  long maxRetryIntervalMs,
                                  int maxRetryNumbers,
                                  long initialRetryIntervalMs,
                                  String retryExceptions,
                                  ConsistencyGuard consistencyGuard) {
    return new HoodieHadoopStorage(path, storage.getConf(), enableRetry, maxRetryIntervalMs,
        maxRetryNumbers, maxRetryIntervalMs, retryExceptions, consistencyGuard);
  }
}
