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

package org.apache.hudi.core.io.storage;

import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import java.io.IOException;
import java.io.OutputStream;

import static org.apache.hudi.common.model.HoodieFileFormat.HOODIE_LOG;

public class HoodieFileWriterFactory {
  protected final HoodieStorage storage;

  public HoodieFileWriterFactory(HoodieStorage storage) {
    this.storage = storage;
  }

  public static <T, I, K, O> HoodieFileWriter getFileWriter(
      String instantTime, StoragePath path, HoodieStorage storage, HoodieConfig config, HoodieSchema schema,
      TaskContextSupplier taskContextSupplier, HoodieRecordType recordType) throws IOException {
    final String extension = FSUtils.getFileExtension(path.getName());
    HoodieFileWriterFactory factory = HoodieIOFactory.getIOFactory(storage).getWriterFactory(recordType);
    return factory.getFileWriterByFormat(extension, instantTime, path, config, schema, taskContextSupplier);
  }

  public static <T, I, K, O> HoodieFileWriter getFileWriter(HoodieFileFormat format, OutputStream outputStream,
                                                            HoodieStorage storage, HoodieConfig config, HoodieSchema schema, HoodieRecordType recordType)
      throws IOException {
    HoodieFileWriterFactory factory = HoodieIOFactory.getIOFactory(storage).getWriterFactory(recordType);
    return factory.getFileWriterByFormat(format, outputStream, config, schema);
  }

  protected <T, I, K, O> HoodieFileWriter getFileWriterByFormat(
      String extension, String instantTime, StoragePath path, HoodieConfig config, HoodieSchema schema,
      TaskContextSupplier taskContextSupplier) throws IOException {
    return newWriterByFormat(getFormatByFileExtension(extension), instantTime, path, config, schema,
        taskContextSupplier);
  }

  protected <T, I, K, O> HoodieFileWriter getFileWriterByFormat(HoodieFileFormat format, OutputStream outputStream,
                                                                HoodieConfig config, HoodieSchema schema) throws IOException {
    switch (format) {
      case PARQUET:
        return newParquetFileWriter(outputStream, config, schema);
      default:
        throw new UnsupportedOperationException(format + " format not supported yet.");
    }
  }

  /**
   * Single dispatch point mapping a {@link HoodieFileFormat} to the corresponding writer creation hook.
   *
   * <p>Support for a new file format must be added HERE, and only here, by adding a case that calls
   * the corresponding {@code newXxxFileWriter} hook; the path-based {@code getFileWriter} entry
   * points funnel through this switch. The only dispatch outside this method is the
   * {@link OutputStream}-based entry point, which only supports Parquet.
   *
   * @param format              the base file format to create a writer for.
   * @param instantTime         instant time of the write.
   * @param path                the file path.
   * @param config              Hudi configs.
   * @param schema              schema to write the file with.
   * @param taskContextSupplier task context supplier.
   * @return a new file writer for the given format.
   * @throws IOException upon writer creation error.
   */
  private HoodieFileWriter newWriterByFormat(
      HoodieFileFormat format, String instantTime, StoragePath path, HoodieConfig config, HoodieSchema schema,
      TaskContextSupplier taskContextSupplier) throws IOException {
    switch (format) {
      case PARQUET:
        return newParquetFileWriter(instantTime, path, config, schema, taskContextSupplier);
      case HFILE:
        return newHFileFileWriter(instantTime, path, config, schema, taskContextSupplier);
      case ORC:
        return newOrcFileWriter(instantTime, path, config, schema, taskContextSupplier);
      case LANCE:
        return newLanceFileWriter(instantTime, path, config, schema, taskContextSupplier);
      case VORTEX:
        return newVortexFileWriter(instantTime, path, config, schema, taskContextSupplier);
      default:
        throw new UnsupportedOperationException(format + " format not supported yet.");
    }
  }

  /**
   * Maps a base file extension to its {@link HoodieFileFormat}. This mapping is format-agnostic,
   * so new formats do not require any change here.
   *
   * @param extension the file extension including the leading dot, e.g. ".parquet".
   * @return the matching base file format.
   */
  private static HoodieFileFormat getFormatByFileExtension(String extension) {
    for (HoodieFileFormat format : HoodieFileFormat.values()) {
      if (format != HOODIE_LOG && format.getFileExtension().equals(extension)) {
        return format;
      }
    }
    throw new UnsupportedOperationException(extension + " format not supported yet.");
  }

  protected HoodieFileWriter newParquetFileWriter(
      String instantTime, StoragePath path, HoodieConfig config, HoodieSchema schema,
      TaskContextSupplier taskContextSupplier) throws IOException {
    throw new UnsupportedOperationException();
  }

  protected HoodieFileWriter newParquetFileWriter(
      OutputStream outputStream, HoodieConfig config, HoodieSchema schema) throws IOException {
    throw new UnsupportedOperationException();
  }

  protected HoodieFileWriter newHFileFileWriter(
      String instantTime, StoragePath path, HoodieConfig config, HoodieSchema schema,
      TaskContextSupplier taskContextSupplier) throws IOException {
    throw new UnsupportedOperationException();
  }

  protected HoodieFileWriter newOrcFileWriter(
      String instantTime, StoragePath path, HoodieConfig config, HoodieSchema schema,
      TaskContextSupplier taskContextSupplier) throws IOException {
    throw new UnsupportedOperationException();
  }

  protected HoodieFileWriter newLanceFileWriter(
      String instantTime, StoragePath path, HoodieConfig config, HoodieSchema schema,
      TaskContextSupplier taskContextSupplier) throws IOException {
    throw new UnsupportedOperationException(HoodieFileFormat.LANCE_UNSUPPORTED_ERROR_MSG);
  }

  protected HoodieFileWriter newVortexFileWriter(
      String instantTime, StoragePath path, HoodieConfig config, HoodieSchema schema,
      TaskContextSupplier taskContextSupplier) throws IOException {
    throw new UnsupportedOperationException(HoodieFileFormat.VORTEX_SPARK_ONLY_ERROR_MSG);
  }

  public static BloomFilter createBloomFilter(HoodieConfig config) {
    return BloomFilterFactory.createBloomFilter(
        config.getIntOrDefault(HoodieStorageConfig.BLOOM_FILTER_NUM_ENTRIES_VALUE),
        config.getDoubleOrDefault(HoodieStorageConfig.BLOOM_FILTER_FPP_VALUE),
        config.getIntOrDefault(HoodieStorageConfig.BLOOM_FILTER_DYNAMIC_MAX_ENTRIES),
        config.getStringOrDefault(HoodieStorageConfig.BLOOM_FILTER_TYPE));
  }

  /**
   * Check if need to enable bloom filter.
   */
  public static boolean enableBloomFilter(boolean populateMetaFields, HoodieConfig config) {
    return populateMetaFields && (config.getBooleanOrDefault(HoodieStorageConfig.PARQUET_WITH_BLOOM_FILTER_ENABLED)
        // HoodieIndexConfig is located in the package hudi-client-common, and the package hudi-client-common depends on the package hudi-common,
        // so the class HoodieIndexConfig cannot be accessed in hudi-common, otherwise there will be a circular dependency problem
        || (config.contains("hoodie.index.type") && config.getString("hoodie.index.type").contains("BLOOM")));
  }
}
