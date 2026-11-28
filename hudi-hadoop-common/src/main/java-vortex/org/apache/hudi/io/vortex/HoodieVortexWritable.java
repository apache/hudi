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

package org.apache.hudi.io.vortex;

import dev.vortex.io.NativeWritable;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A {@link NativeWritable} that writes to an output stream, for example a stream from
 * {@code HoodieStorage#create}. The Vortex writer writes and flushes the stream, but does not
 * close it. The owner of this object closes it after the Vortex writer is finished.
 */
final class HoodieVortexWritable implements NativeWritable {

  private final OutputStream stream;

  HoodieVortexWritable(OutputStream stream) {
    this.stream = stream;
  }

  @Override
  public void write(byte[] buffer, int offset, int length) throws IOException {
    stream.write(buffer, offset, length);
  }

  @Override
  public void flush() throws IOException {
    stream.flush();
  }

  @Override
  public void close() throws IOException {
    stream.close();
  }
}
