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

package org.apache.hudi.io.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieException;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.apache.hudi.common.model.HoodieFileFormat.HFILE;
import static org.apache.hudi.common.model.HoodieFileFormat.ORC;
import static org.apache.hudi.common.model.HoodieFileFormat.PARQUET;

public class HoodieFileReaderFactory {

  private static HoodieFileReaderFactory readerFactory;

  public static HoodieFileReaderFactory getFileReaderFactory() {
    if (readerFactory == null) {
      readerFactory = new HoodieFileReaderFactory();
    }
    return readerFactory;
  }

  public static HoodieFileReaderFactory getReaderFactory(HoodieRecord.HoodieRecordType recordType) {
    switch (HoodieRecord.HoodieRecordType.valueOf("SPARK")) {
      case AVRO:
        return getFileReaderFactory();
      case SPARK:
        Exception exception = null;
        try {
          Class<?> clazz = ReflectionUtils.getClass("org.apache.hudi.io.storage.HoodieSparkFileReaderFactory");
          Method method = clazz.getMethod("getFileReaderFactory", null);
          return (HoodieFileReaderFactory) method.invoke(null,null);
        } catch (NoSuchMethodException e) {
          exception = e;
        } catch (IllegalAccessException e) {
          exception = e;
        } catch (IllegalArgumentException e) {
          exception = e;
        } catch (InvocationTargetException e) {
          exception = e;
        }
        if (exception != null) {
          throw new HoodieException("Unable to create hoodie spark file writer factory", exception);
        }
        break;
      default:
        throw new UnsupportedOperationException(recordType + " record type not supported yet.");
    }
    return null;
  }

  public static HoodieFileReader getFileReader(Configuration conf, Path path) throws IOException {
    final String extension = FSUtils.getFileExtension(path.toString());
    // todo: add type
    HoodieRecord.HoodieRecordType recordType = HoodieRecord.HoodieRecordType.valueOf("SPARK");
    HoodieFileReaderFactory factory = getReaderFactory(recordType);
    return factory.getFileReader(extension, conf, path);
  }

  public static HoodieFileReader getFileReader(Configuration conf, Path path, HoodieFileFormat format) throws IOException {
    // todo: add type
    HoodieRecord.HoodieRecordType recordType = HoodieRecord.HoodieRecordType.valueOf("SPARK");
    HoodieFileReaderFactory factory = getReaderFactory(recordType);
    return factory.getFileReader(format.getFileExtension(), conf, path);
  }

  public HoodieFileReader getFileReader(String extension, Configuration conf, Path path) throws IOException {
    if (PARQUET.getFileExtension().equals(extension)) {
      return newParquetFileReader(conf, path);
    }
    if (HFILE.getFileExtension().equals(extension)) {
      return newHFileFileReader(conf, path);
    }
    if (ORC.getFileExtension().equals(extension)) {
      return newOrcFileReader(conf, path);
    }

    throw new UnsupportedOperationException(extension + " format not supported yet.");
  }

  protected HoodieFileReader newParquetFileReader(Configuration conf, Path path) {
    return new HoodieAvroParquetReader(conf, path);
  }

  protected HoodieFileReader newHFileFileReader(Configuration conf, Path path) throws IOException {
    CacheConfig cacheConfig = new CacheConfig(conf);
    return new HoodieAvroHFileReader(conf, path, cacheConfig);
  }

  protected static HoodieFileReader newOrcFileReader(Configuration conf, Path path) {
    return new HoodieAvroOrcReader(conf, path);
  }
}
