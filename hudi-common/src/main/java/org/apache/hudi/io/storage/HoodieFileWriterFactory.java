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

import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieException;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.apache.hudi.common.model.HoodieFileFormat.HFILE;
import static org.apache.hudi.common.model.HoodieFileFormat.ORC;
import static org.apache.hudi.common.model.HoodieFileFormat.PARQUET;

public class HoodieFileWriterFactory {

  private static HoodieFileWriterFactory getWriterFactory(HoodieRecord.HoodieRecordType recordType) {
    switch (recordType) {
      case AVRO:
        return HoodieAvroFileWriterFactory.getFileReaderFactory();
      case SPARK:
        try {
          Class<?> clazz = ReflectionUtils.getClass("org.apache.hudi.io.storage.HoodieSparkFileWriterFactory");
          Method method = clazz.getMethod("getFileWriterFactory", null);
          return (HoodieFileWriterFactory) method.invoke(null, null);
        } catch (NoSuchMethodException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
          throw new HoodieException("Unable to create hoodie spark file writer factory", e);
        }
      default:
        throw new UnsupportedOperationException(recordType + " record type not supported yet.");
    }
  }

  public static <T, I, K, O> HoodieFileWriter getFileWriter(
      String instantTime, Path path, Configuration conf, HoodieConfig config, Schema schema,
      TaskContextSupplier taskContextSupplier, HoodieRecordType recordType) throws IOException {
    final String extension = FSUtils.getFileExtension(path.getName());
    HoodieFileWriterFactory factory = getWriterFactory(recordType);
    return factory.getFileWriterByFormat(extension, instantTime, path, conf, config, schema, taskContextSupplier);
  }

  public static <T, I, K, O> HoodieFileWriter getFileWriter(HoodieFileFormat format,
      FSDataOutputStream outputStream, Configuration conf, HoodieConfig config, Schema schema, HoodieRecordType recordType) throws IOException {
    HoodieFileWriterFactory factory = getWriterFactory(recordType);
    return factory.getFileWriterByFormat(format, outputStream, conf, config, schema);
  }

  protected <T, I, K, O> HoodieFileWriter getFileWriterByFormat(
      String extension, String instantTime, Path path, Configuration conf, HoodieConfig config, Schema schema,
      TaskContextSupplier taskContextSupplier) throws IOException {
    if (PARQUET.getFileExtension().equals(extension)) {
      return newParquetFileWriter(instantTime, path, conf, config, schema, taskContextSupplier);
    }
    if (HFILE.getFileExtension().equals(extension)) {
      return newHFileFileWriter(instantTime, path, conf, config, schema, taskContextSupplier);
    }
    if (ORC.getFileExtension().equals(extension)) {
      return newOrcFileWriter(instantTime, path, conf, config, schema, taskContextSupplier);
    }
    throw new UnsupportedOperationException(extension + " format not supported yet.");
  }

  protected <T, I, K, O> HoodieFileWriter getFileWriterByFormat(HoodieFileFormat format,
      FSDataOutputStream outputStream, Configuration conf, HoodieConfig config, Schema schema) throws IOException {
    switch (format) {
      case PARQUET:
        return newParquetFileWriter(outputStream, conf, config, schema);
      default:
        throw new UnsupportedOperationException(format + " format not supported yet.");
    }
  }

  protected HoodieFileWriter newParquetFileWriter(
      String instantTime, Path path, Configuration conf, HoodieConfig config, Schema schema,
      TaskContextSupplier taskContextSupplier) throws IOException {
    throw new UnsupportedOperationException();
  }

  protected HoodieFileWriter newParquetFileWriter(
      FSDataOutputStream outputStream, Configuration conf, HoodieConfig config, Schema schema) throws IOException {
    throw new UnsupportedOperationException();
  }

  protected HoodieFileWriter newHFileFileWriter(
      String instantTime, Path path, Configuration conf, HoodieConfig config, Schema schema,
      TaskContextSupplier taskContextSupplier) throws IOException {
    throw new UnsupportedOperationException();
  }

  protected HoodieFileWriter newOrcFileWriter(
      String instantTime, Path path, Configuration conf, HoodieConfig config, Schema schema,
      TaskContextSupplier taskContextSupplier) throws IOException {
    throw new UnsupportedOperationException();
  }

  protected BloomFilter createBloomFilter(HoodieConfig config) {
    return BloomFilterFactory.createBloomFilter(60000, 0.000000001, 100000,
        BloomFilterTypeCode.DYNAMIC_V0.name());
  }
}
