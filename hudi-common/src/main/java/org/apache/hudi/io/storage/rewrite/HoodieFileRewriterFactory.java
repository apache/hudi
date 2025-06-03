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

package org.apache.hudi.io.storage.rewrite;

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.StoragePath;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

import static org.apache.hudi.common.model.HoodieFileFormat.PARQUET;

public class HoodieFileRewriterFactory {

  private static HoodieFileRewriterFactory getWriterFactory(HoodieRecordType recordType, String extension) {
    if (PARQUET.getFileExtension().equals(extension)) {
      try {
        Class<?> clazz = ReflectionUtils.getClass("org.apache.hudi.parquet.io.HoodieParquetRewriterFactory");
        return (HoodieFileRewriterFactory) clazz.newInstance();
      } catch (IllegalAccessException | IllegalArgumentException | InstantiationException e) {
        throw new HoodieException("Unable to create hoodie avro parquet file writer factory", e);
      }
    }
    throw new UnsupportedOperationException(extension + " file format not supported yet.");
  }

  public static <T, I, K, O> HoodieFileRewriter getFileRewriter(
      StoragePath targetFilePath,
      Configuration conf,
      HoodieConfig config,
      HoodieFileMetadataMerger metadataMerger,
      HoodieRecordType recordType) throws IOException {
    String extension = FSUtils.getFileExtension(targetFilePath.getName());
    HoodieFileRewriterFactory factory = getWriterFactory(recordType, extension);
    String compressionCodecName = config.getStringOrDefault(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME);
    return factory.newFileRewriter(conf, compressionCodecName, metadataMerger);
  }

  protected <T> HoodieFileRewriter newFileRewriter(
      Configuration conf,
      String compressionCodecName,
      HoodieFileMetadataMerger metadataMerger) {

    throw new UnsupportedOperationException();
  }
}
