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

package org.apache.hudi.hadoop.fs.inline;

import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.inline.InLineFSUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import static org.apache.hudi.hadoop.fs.HadoopFSUtils.convertToHadoopPath;
import static org.apache.hudi.hadoop.fs.HadoopFSUtils.convertToStoragePath;

/**
 * Utils to parse InLineFileSystem paths.
 * Inline FS format:
 * "inlinefs://<path_to_outer_file>/<outer_file_scheme>/?start_offset=start_offset>&length=<length>"
 * Eg: "inlinefs://<path_to_outer_file>/s3a/?start_offset=20&length=40"
 */
public class HadoopInLineFSUtils extends InLineFSUtils {

  public static StorageConfiguration<Configuration> buildInlineConf(StorageConfiguration<Configuration> storageConf) {
    StorageConfiguration<Configuration> inlineConf = storageConf.newInstance();
    inlineConf.set("fs." + InLineFileSystem.SCHEME + ".impl", InLineFileSystem.class.getName());
    (inlineConf.unwrapAs(Configuration.class)).setClassLoader(InLineFileSystem.class.getClassLoader());
    return inlineConf;
  }

  /**
   * InlineFS Path format:
   * "inlinefs://path/to/outer/file/outer_file_scheme/?start_offset=start_offset>&length=<length>"
   * <p>
   * Outer File Path format:
   * "outer_file_scheme://path/to/outer/file"
   * <p>
   * Example
   * Input: "inlinefs://file1/s3a/?start_offset=20&length=40".
   * Output: "s3a://file1"
   *
   * @param inlineFSPath InLineFS Path to get the outer file Path
   * @return Outer file Path from the InLineFS Path
   */
  public static Path getOuterFilePathFromInlinePath(Path inlineFSPath) {
    StoragePath storagePath = convertToStoragePath(inlineFSPath);
    StoragePath outerFilePath = getOuterFilePathFromInlinePath(storagePath);
    return convertToHadoopPath(outerFilePath);
  }
}
