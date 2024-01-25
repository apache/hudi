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

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieException;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

import static org.apache.hudi.common.model.HoodieFileFormat.HFILE;
import static org.apache.hudi.common.model.HoodieFileFormat.ORC;
import static org.apache.hudi.common.model.HoodieFileFormat.PARQUET;

/**
 * Factory methods to create Hudi file reader.
 */
public class HoodieFileReaderFactory {

  public static HoodieFileReaderFactory getReaderFactory(HoodieRecord.HoodieRecordType recordType) {
    switch (recordType) {
      case AVRO:
        return new HoodieAvroFileReaderFactory();
      case SPARK:
        try {
          Class<?> clazz = ReflectionUtils.getClass("org.apache.hudi.io.storage.HoodieSparkFileReaderFactory");
          return (HoodieFileReaderFactory) clazz.newInstance();
        } catch (IllegalArgumentException | IllegalAccessException | InstantiationException e) {
          throw new HoodieException("Unable to create hoodie spark file writer factory", e);
        }
      default:
        throw new UnsupportedOperationException(recordType + " record type not supported yet.");
    }
  }

  public HoodieFileReader getFileReader(Configuration conf, HoodieLocation location) throws IOException {
    final String extension = FSUtils.getFileExtension(location.toString());
    if (PARQUET.getFileExtension().equals(extension)) {
      return newParquetFileReader(conf, location);
    }
    if (HFILE.getFileExtension().equals(extension)) {
      return newHFileFileReader(conf, location);
    }
    if (ORC.getFileExtension().equals(extension)) {
      return newOrcFileReader(conf, location);
    }
    throw new UnsupportedOperationException(extension + " format not supported yet.");
  }

  public HoodieFileReader getFileReader(Configuration conf, HoodieLocation location, HoodieFileFormat format)
      throws IOException {
    return this.newParquetFileReader(conf, location);
  }

  protected HoodieFileReader newParquetFileReader(Configuration conf, HoodieLocation location) {
    throw new UnsupportedOperationException();
  }

  protected HoodieFileReader newHFileFileReader(Configuration conf, HoodieLocation location) throws IOException {
    throw new UnsupportedOperationException();
  }

  protected HoodieFileReader newOrcFileReader(Configuration conf, HoodieLocation location) {
    throw new UnsupportedOperationException();
  }

  public HoodieFileReader newBootstrapFileReader(HoodieFileReader skeletonFileReader, HoodieFileReader dataFileReader, Option<String[]> partitionFields, Object[] partitionValues) {
    throw new UnsupportedOperationException();
  }
}
