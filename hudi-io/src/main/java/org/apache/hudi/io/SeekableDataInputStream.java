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

package org.apache.hudi.io;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * A {@link InputStream} that supports random access by allowing to seek to
 * an arbitrary position within the stream and read the content.
 */
public abstract class SeekableDataInputStream extends DataInputStream {
  /**
   * Creates a DataInputStream that uses the specified
   * underlying InputStream.
   *
   * @param in the specified input stream
   */
  public SeekableDataInputStream(InputStream in) {
    super(in);
  }

  /**
   * @return current position of the stream. The next read() will be from that location.
   */
  public abstract long getPos() throws IOException;

  /**
   * Seeks to a position within the stream.
   *
   * @param pos target position to seek to.
   * @throws IOException upon error.
   */
  public abstract void seek(long pos) throws IOException;
}
