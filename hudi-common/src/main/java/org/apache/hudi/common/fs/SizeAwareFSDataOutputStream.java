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

import org.apache.hudi.exception.HoodieException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Wrapper over <code>FSDataOutputStream</code> to keep track of the size of the written bytes. This gives a cheap way
 * to check on the underlying file size.
 */
public class SizeAwareFSDataOutputStream extends FSDataOutputStream {

  // A callback to call when the output stream is closed.
  private final Runnable closeCallback;
  // Keep track of the bytes written
  private final AtomicLong bytesWritten = new AtomicLong(0L);
  // Path
  private final Path path;
  // Consistency guard
  private final ConsistencyGuard consistencyGuard;

  public SizeAwareFSDataOutputStream(Path path, FSDataOutputStream out, ConsistencyGuard consistencyGuard,
                                     Runnable closeCallback) throws IOException {
    super(out, null);
    this.path = path;
    this.closeCallback = closeCallback;
    this.consistencyGuard = consistencyGuard;
  }

  @Override
  public synchronized void write(byte[] b, int off, int len) throws IOException {
    HoodieWrapperFileSystem.executeFuncWithTimeAndByteMetrics(HoodieWrapperFileSystem.MetricName.write.name(), path,
        len, () -> {
          bytesWritten.addAndGet(len);
          super.write(b, off, len);
          return null;
        });
  }

  @Override
  public void write(byte[] b) throws IOException {
    HoodieWrapperFileSystem.executeFuncWithTimeAndByteMetrics(HoodieWrapperFileSystem.MetricName.write.name(), path,
        b.length, () -> {
          bytesWritten.addAndGet(b.length);
          super.write(b);
          return null;
        });
  }

  @Override
  public void close() throws IOException {
    super.close();
    try {
      consistencyGuard.waitTillFileAppears(path);
    } catch (TimeoutException e) {
      throw new HoodieException(e);
    }
    closeCallback.run();
  }

  public long getBytesWritten() {
    return bytesWritten.get();
  }
}
