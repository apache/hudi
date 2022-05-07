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

package org.apache.hudi.common.fs;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * Scheme aware FSDataInputStream so that we manipulate seeks for GS filesystem.
 */
public class SchemeAwareFSDataInputStream extends FSDataInputStream {

  private final boolean isGCSFileSystem;

  public SchemeAwareFSDataInputStream(InputStream in, boolean isGCSFileSystem) {
    super(in);
    this.isGCSFileSystem = isGCSFileSystem;
  }

  @Override
  public void seek(long desired) throws IOException {
    try {
      super.seek(desired);
    } catch (EOFException e) {
      // with GCSFileSystem, accessing the last byte might throw EOFException and hence this fix.
      if (isGCSFileSystem) {
        super.seek(desired - 1);
      } else {
        throw e;
      }
    }
  }
}
