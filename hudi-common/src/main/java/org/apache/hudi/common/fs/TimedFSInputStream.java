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
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * A wrapper over <code>FSInputStream</code> that also times the operations.
 */
public class TimedFSInputStream extends FSInputStream {

  private FSDataInputStream in;
  private Path path;

  public TimedFSInputStream(Path p, FSDataInputStream in) {
    this.in = in;
    this.path = p;
  }

  @Override
  public int read() throws IOException {
    return HoodieWrapperFileSystem.executeFuncWithTimeAndByteMetrics(HoodieWrapperFileSystem.MetricName.read.name(),
        path, 4, () -> {
          return in.read();
        });
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return HoodieWrapperFileSystem.executeFuncWithTimeAndByteMetrics(HoodieWrapperFileSystem.MetricName.read.name(),
        path, () -> {
          return in.read(b, off, len);
        });
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    return HoodieWrapperFileSystem.executeFuncWithTimeAndByteMetrics(HoodieWrapperFileSystem.MetricName.read.name(),
        path, () -> {
          return in.read(position, buffer, offset, length);
        });
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    HoodieWrapperFileSystem.executeFuncWithTimeAndByteMetrics(HoodieWrapperFileSystem.MetricName.read.name(),
        path, length, () -> {
          in.readFully(position, buffer, offset, length);
          return null;
        });
  }

  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    HoodieWrapperFileSystem.executeFuncWithTimeAndByteMetrics(HoodieWrapperFileSystem.MetricName.read.name(),
        path, buffer.length, () -> {
          in.readFully(position, buffer);
          return null;
        });
  }

  @Override
  public void seek(long pos) throws IOException {
    in.seek(pos);
  }

  @Override
  public long skip(long pos) throws IOException {
    return in.skip(pos);
  }

  @Override
  public int available() throws IOException {
    return in.available();
  }

  @Override
  public void close() throws IOException {
    in.close();
  }

  @Override
  public long getPos() throws IOException {
    return in.getPos();
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return in.seekToNewSource(targetPos);
  }
}
