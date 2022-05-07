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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.io.ByteBufferPool;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;

/**
 * Wrapper over <code>FSDataInputStream</code> that also times the operations.
 */
public class TimedFSDataInputStream extends FSDataInputStream {

  // Path
  private final Path path;

  public TimedFSDataInputStream(Path path, FSDataInputStream in) {
    super(in);
    this.path = path;
  }

  @Override
  public int read(ByteBuffer buf) throws IOException {
    return HoodieWrapperFileSystem.executeFuncWithTimeAndByteMetrics(HoodieWrapperFileSystem.MetricName.read.name(),
        path, 0, () -> super.read(buf));
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    return HoodieWrapperFileSystem.executeFuncWithTimeAndByteMetrics(HoodieWrapperFileSystem.MetricName.read.name(),
        path, length, () -> super.read(position, buffer, offset, length));
  }

  @Override
  public ByteBuffer read(ByteBufferPool bufferPool, int maxLength, EnumSet<ReadOption> opts)
      throws IOException, UnsupportedOperationException {
    return HoodieWrapperFileSystem.executeFuncWithTimeAndByteMetrics(HoodieWrapperFileSystem.MetricName.read.name(),
        path, maxLength, () -> super.read(bufferPool, maxLength, opts));
  }

  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    HoodieWrapperFileSystem.executeFuncWithTimeAndByteMetrics(HoodieWrapperFileSystem.MetricName.read.name(),
        path, buffer.length, () -> {
          super.readFully(position, buffer);
          return null;
        });
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    HoodieWrapperFileSystem.executeFuncWithTimeAndByteMetrics(HoodieWrapperFileSystem.MetricName.read.name(),
        path, length, () -> {
          super.readFully(position, buffer, offset, length);
          return null;
        });
  }
}
