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

package org.apache.hudi.hadoop.fs;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * Implementation of {@link FSDataInputStream} with bound check based on file size.
 */
public class BoundedFsDataInputStream extends FSDataInputStream {
  private final FileSystem fs;
  private final Path file;
  private long fileLen = -1L;

  public BoundedFsDataInputStream(FileSystem fs, Path file, InputStream in) {
    super(in);
    this.fs = fs;
    this.file = file;
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  /* Return the file length */
  private long getFileLength() throws IOException {
    if (fileLen == -1L) {
      fileLen = fs.getContentSummary(file).getLength();
    }
    return fileLen;
  }

  @Override
  public synchronized void seek(long pos) throws IOException {
    if (pos < 0 || pos > getFileLength()) {
      throw new EOFException("Try to seek pos[" + pos + "] , but fileSize is " + getFileLength());
    }
    super.seek(pos);
  }

  @Override
  public synchronized long skip(long n) throws IOException {
    long curPos = getPos();
    long fileLength = getFileLength();
    if (n + curPos > fileLength) {
      n = fileLength - curPos;
    }
    return super.skip(n);
  }

}
