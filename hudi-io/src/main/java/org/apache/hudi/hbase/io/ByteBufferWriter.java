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

package org.apache.hudi.hbase.io;

import org.apache.yetus.audience.InterfaceAudience;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * This interface marks a class to support writing ByteBuffers into it.
 * @see ByteArrayOutputStream
 * @see ByteBufferOutputStream
 */
@InterfaceAudience.Private
public interface ByteBufferWriter {

  /**
   * Writes <code>len</code> bytes from the specified ByteBuffer starting at offset <code>off</code>
   *
   * @param b the data.
   * @param off the start offset in the data.
   * @param len the number of bytes to write.
   * @exception IOException if an I/O error occurs.
   */
  void write(ByteBuffer b, int off, int len) throws IOException;

  /**
   * Writes an <code>int</code> to the underlying output stream as four bytes, high byte first.
   * @param i the <code>int</code> to write
   * @throws IOException if an I/O error occurs.
   */
  // This is pure performance oriented API been added here. It has nothing to do with
  // ByteBuffer and so not fully belong to here. This allows an int to be written at one go instead
  // of 4 (4 bytes one by one).
  // TODO remove it from here?
  void writeInt(int i) throws IOException;
}
