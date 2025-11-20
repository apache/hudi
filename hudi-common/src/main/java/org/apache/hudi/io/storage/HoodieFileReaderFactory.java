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

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.apache.avro.Schema;

import java.io.IOException;

import static org.apache.hudi.common.model.HoodieFileFormat.HFILE;
import static org.apache.hudi.common.model.HoodieFileFormat.ORC;
import static org.apache.hudi.common.model.HoodieFileFormat.PARQUET;

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
    if (PARQUET.getFileExtension().equals(extension)) {
      return getFileReader(hoodieConfig, path, PARQUET, Option.empty());
    }
    if (HFILE.getFileExtension().equals(extension)) {
      return getFileReader(hoodieConfig, path, HFILE, Option.empty());
    }
    if (ORC.getFileExtension().equals(extension)) {
      return getFileReader(hoodieConfig, path, ORC, Option.empty());
    }
    throw new UnsupportedOperationException(extension + " format not supported yet.");
  }

  public HoodieFileReader getFileReader(HoodieConfig hoodieConfig, StoragePath path, HoodieFileFormat format)
      throws IOException {
    return getFileReader(hoodieConfig, path, format, Option.empty());
  }

  public HoodieFileReader getFileReader(HoodieConfig hoodieConfig, StoragePath path, HoodieFileFormat format,
                                        Option<Schema> schemaOption) throws IOException {
    switch (format) {
      case PARQUET:
        return newParquetFileReader(path);
      case HFILE:
        return newHFileFileReader(hoodieConfig, path, schemaOption);
      case ORC:
        return newOrcFileReader(path);
      default:
        throw new UnsupportedOperationException(format + " format not supported yet.");
    }
  }

  public HoodieFileReader getFileReader(HoodieConfig hoodieConfig, StoragePathInfo pathInfo, HoodieFileFormat format,
                                        Option<Schema> schemaOption) throws IOException {
    switch (format) {
      case PARQUET:
        return newParquetFileReader(pathInfo.getPath());
      case HFILE:
        return newHFileFileReader(hoodieConfig, pathInfo, schemaOption);
      case ORC:
        return newOrcFileReader(pathInfo.getPath());
      default:
        throw new UnsupportedOperationException(format + " format not supported yet.");
    }
  }

  public HoodieFileReader getContentReader(HoodieConfig hoodieConfig, StoragePath path, HoodieFileFormat format,
                                           HoodieStorage storage, byte[] content,
                                           Option<Schema> schemaOption) throws IOException {
    switch (format) {
      case HFILE:
        return newHFileFileReader(hoodieConfig, path, storage, content, schemaOption);
      default:
        throw new UnsupportedOperationException(format + " format not supported yet.");
    }
  }

  protected HoodieFileReader newParquetFileReader(StoragePath path) {
    throw new UnsupportedOperationException();
  }

  protected HoodieFileReader newHFileFileReader(HoodieConfig hoodieConfig, StoragePath path,
                                                Option<Schema> schemaOption) throws IOException {
    throw new UnsupportedOperationException();
  }

  protected HoodieFileReader newHFileFileReader(HoodieConfig hoodieConfig, StoragePathInfo pathInfo,
                                                Option<Schema> schemaOption) throws IOException {
    throw new UnsupportedOperationException();
  }

  protected HoodieFileReader newHFileFileReader(HoodieConfig hoodieConfig, StoragePath path,
                                                HoodieStorage storage, byte[] content, Option<Schema> schemaOption)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  protected HoodieFileReader newOrcFileReader(StoragePath path) {
    throw new UnsupportedOperationException();
  }

  public HoodieFileReader newBootstrapFileReader(HoodieFileReader skeletonFileReader,
                                                 HoodieFileReader dataFileReader,
                                                 Option<String[]> partitionFields,
                                                 Object[] partitionValues) {
    throw new UnsupportedOperationException();
  }

}