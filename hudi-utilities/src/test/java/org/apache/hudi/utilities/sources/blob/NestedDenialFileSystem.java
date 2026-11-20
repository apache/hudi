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

package org.apache.hudi.utilities.sources.blob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.AccessDeniedException;

/** TEST-ONLY source that wraps a denial as missing and reports false on the existence recheck. */
public class NestedDenialFileSystem extends BucketFolderFileSystem {

  static final String DENIED_FILE = "zz-denied.pdf";
  private static final String STAGE = "test.blob.nested.denial.stage";

  static void install(Configuration conf, java.nio.file.Path root, String stage) {
    BucketFolderFileSystem.install(conf, root);
    conf.set("fs." + SCHEME + ".impl", NestedDenialFileSystem.class.getName());
    conf.set(STAGE, stage);
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    if (DENIED_FILE.equals(path.getName()) && "status".equals(getConf().get(STAGE))) {
      throw missingWithDenial(path);
    }
    return super.getFileStatus(path);
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    if (DENIED_FILE.equals(path.getName()) && "open".equals(getConf().get(STAGE))) {
      throw missingWithDenial(path);
    }
    return super.open(path, bufferSize);
  }

  @Override
  public boolean exists(Path path) throws IOException {
    if (DENIED_FILE.equals(path.getName())) {
      // Hadoop's default exists also returns false for an outer FileNotFoundException without inspecting its cause.
      return false;
    }
    return super.exists(path);
  }

  private static FileNotFoundException missingWithDenial(Path path) {
    FileNotFoundException missing = new FileNotFoundException("injected missing wrapper for " + path);
    missing.initCause(new AccessDeniedException(path.toString(), null, "injected 403"));
    return missing;
  }
}
