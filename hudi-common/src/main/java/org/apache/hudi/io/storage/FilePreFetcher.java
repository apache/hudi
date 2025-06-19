/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.io.storage;

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIClass;
import org.apache.hudi.PublicAPIMethod;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.FileIOUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Pre-fetches (loads) the contents of a file prior to processing.
 * Specifically, used by the {@link org.apache.hudi.metadata.HoodieBackedTableMetadata}
 * to load HFiles prior to key look-ups to avoid several GET calls for small sized files.
 */
@PublicAPIClass(maturity = ApiMaturityLevel.EVOLVING)
public class FilePreFetcher {

  protected final TypedProperties properties;
  protected final Configuration hadoopConf;

  public FilePreFetcher(TypedProperties properties, Configuration hadoopConf) {
    this.properties = properties;
    this.hadoopConf = hadoopConf;
  }

  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public byte[] fetchFileContents(Path filePath, long fileLen) throws IOException {
    FileSystem fs = FSUtils.getFs(filePath, hadoopConf);
    if (fileLen < 0) {
      FileStatus fileStatus = fs.getFileStatus(filePath);
      fileLen = fileStatus.getLen();
    }

    if (fileLen < 0 || fileLen > Integer.MAX_VALUE) {
      throw new IOException(String.format("Fatal error downloading file %s with length %d", filePath, fileLen));
    }

    try (FSDataInputStream inputStream = fs.open(filePath)) {
      return FileIOUtils.readAsByteArray(inputStream, (int) fileLen);
    }
  }
}
