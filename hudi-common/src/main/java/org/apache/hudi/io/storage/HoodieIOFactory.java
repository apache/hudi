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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.StorageConfiguration;

public abstract class HoodieIOFactory {

  public static HoodieIOFactory getIOFactory(StorageConfiguration<?> storageConf) {
    String ioFactoryClass = storageConf.getString(HoodieStorageConfig.HOODIE_IO_FACTORY_CLASS.key())
        .orElse(HoodieStorageConfig.HOODIE_IO_FACTORY_CLASS.defaultValue());
    return getIOFactory(ioFactoryClass);
  }

  private static HoodieIOFactory getIOFactory(String ioFactoryClass) {
    try {
      Class<?> clazz =
          ReflectionUtils.getClass(ioFactoryClass);
      return (HoodieIOFactory) clazz.newInstance();
    } catch (IllegalArgumentException | IllegalAccessException | InstantiationException e) {
      throw new HoodieException("Unable to create " + ioFactoryClass, e);
    }
  }

  public HoodieFileReaderFactory getReaderFactory(HoodieRecord.HoodieRecordType recordType) {
    throw new UnsupportedOperationException(recordType + " record type not supported");
  }

  public HoodieFileWriterFactory getWriterFactory(HoodieRecord.HoodieRecordType recordType) {
    throw new UnsupportedOperationException(recordType + " record type not supported");
  }
}
