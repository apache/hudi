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

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import java.io.IOException;

import static org.apache.hudi.common.model.HoodieFileFormat.HFILE;
import static org.apache.hudi.common.model.HoodieFileFormat.HOODIE_LOG;

/**
 * Factory methods to create Hudi file reader.
 */
public class HoodieFileReaderFactory {

  protected final HoodieStorage storage;

  public HoodieFileReaderFactory(HoodieStorage storage) {
    this.storage = storage;
  }

  public HoodieFileReader getFileReader(HoodieConfig hoodieConfig, StoragePath path) throws IOException {
    final String extension = FSUtils.getFileExtension(path.toString());
    return getFileReader(hoodieConfig, path, getFormatByFileExtension(extension), Option.empty());
  }

  public HoodieFileReader getFileReader(HoodieConfig hoodieConfig, StoragePath path, HoodieFileFormat format)
      throws IOException {
    return getFileReader(hoodieConfig, path, format, Option.empty());
  }

  public HoodieFileReader getFileReader(HoodieConfig hoodieConfig, StoragePath path, HoodieFileFormat format,
                                        Option<HoodieSchema> schemaOption) throws IOException {
    return newReaderByFormat(hoodieConfig, format, path, schemaOption);
  }

  public HoodieFileReader getFileReader(HoodieConfig hoodieConfig, StoragePathInfo pathInfo, HoodieFileFormat format,
                                        Option<HoodieSchema> schemaOption) throws IOException {
    if (format == HFILE) {
      // The HFile reader can leverage the file size available in StoragePathInfo,
      // so dispatch to the StoragePathInfo-specific hook instead of the path-based one.
      return newHFileFileReader(hoodieConfig, pathInfo, schemaOption);
    }
    return newReaderByFormat(hoodieConfig, format, pathInfo.getPath(), schemaOption);
  }

  public HoodieFileReader getContentReader(HoodieConfig hoodieConfig, StoragePath path, HoodieFileFormat format,
                                           HoodieStorage storage, byte[] content,
                                           Option<HoodieSchema> schemaOption) throws IOException {
    switch (format) {
      case HFILE:
        return newHFileFileReader(hoodieConfig, path, storage, content, schemaOption);
      default:
        throw new UnsupportedOperationException(format + " format not supported yet.");
    }
  }

  /**
   * Single dispatch point mapping a {@link HoodieFileFormat} to the corresponding reader creation hook.
   *
   * <p>Support for a new file format must be added HERE, and only here, by adding a case that calls
   * the corresponding {@code newXxxFileReader} hook; every {@code getFileReader} overload funnels
   * through this switch. The only format-specific dispatch outside this method is the
   * {@link #newHFileFileReader(HoodieConfig, StoragePathInfo, Option)} hook invoked from
   * {@link #getFileReader(HoodieConfig, StoragePathInfo, HoodieFileFormat, Option)}, which exists
   * because the HFile reader can leverage the file size in {@link StoragePathInfo}.
   *
   * @param hoodieConfig Hudi configs.
   * @param format       the base file format to create a reader for.
   * @param path         the file path.
   * @param schemaOption schema to read the file with, if present.
   * @return a new file reader for the given format.
   * @throws IOException upon reader creation error.
   */
  private HoodieFileReader newReaderByFormat(HoodieConfig hoodieConfig, HoodieFileFormat format, StoragePath path,
                                             Option<HoodieSchema> schemaOption) throws IOException {
    switch (format) {
      case PARQUET:
        return newParquetFileReader(path);
      case HFILE:
        return newHFileFileReader(hoodieConfig, path, schemaOption);
      case ORC:
        return newOrcFileReader(path);
      case LANCE:
        return newLanceFileReader(hoodieConfig, path);
      case VORTEX:
        return newVortexFileReader(hoodieConfig, path);
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

  protected HoodieFileReader newParquetFileReader(StoragePath path) {
    throw new UnsupportedOperationException();
  }

  protected HoodieFileReader newHFileFileReader(HoodieConfig hoodieConfig, StoragePath path,
                                                Option<HoodieSchema> schemaOption) throws IOException {
    throw new UnsupportedOperationException();
  }

  protected HoodieFileReader newHFileFileReader(HoodieConfig hoodieConfig, StoragePathInfo pathInfo,
                                                Option<HoodieSchema> schemaOption) throws IOException {
    throw new UnsupportedOperationException();
  }

  protected HoodieFileReader newHFileFileReader(HoodieConfig hoodieConfig, StoragePath path,
                                                HoodieStorage storage, byte[] content, Option<HoodieSchema> schemaOption)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  protected HoodieFileReader newOrcFileReader(StoragePath path) {
    throw new UnsupportedOperationException();
  }

  protected HoodieFileReader newLanceFileReader(HoodieConfig hoodieConfig, StoragePath path) {
    throw new UnsupportedOperationException(HoodieFileFormat.LANCE_UNSUPPORTED_ERROR_MSG);
  }

  protected HoodieFileReader newVortexFileReader(HoodieConfig hoodieConfig, StoragePath path) {
    throw new UnsupportedOperationException(HoodieFileFormat.VORTEX_SPARK_ONLY_ERROR_MSG);
  }

  public HoodieFileReader newBootstrapFileReader(HoodieFileReader skeletonFileReader,
                                                 HoodieFileReader dataFileReader,
                                                 Option<String[]> partitionFields,
                                                 Object[] partitionValues) {
    throw new UnsupportedOperationException();
  }

}
