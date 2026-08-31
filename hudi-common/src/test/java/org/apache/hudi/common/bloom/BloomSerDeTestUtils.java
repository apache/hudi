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

package org.apache.hudi.common.bloom;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Write/readFields helpers shared by the {@code org.apache.hudi.common.bloom} tests.
 *
 * <p>{@link Key} and {@link InternalFilter} both expose {@code write(DataOutput)} /
 * {@code readFields(DataInput)} but share no supertype declaring them, so the helper takes the
 * write call itself rather than the object being written.
 */
final class BloomSerDeTestUtils {

  /**
   * The {@code write(DataOutput)} method of the object under test, e.g. {@code key::write}.
   */
  @FunctionalInterface
  interface DataWriter {
    void write(DataOutput out) throws IOException;
  }

  private BloomSerDeTestUtils() {
  }

  /**
   * Serializes through {@code writer} and returns the bytes it produced.
   */
  static byte[] serialize(DataWriter writer) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    writer.write(new DataOutputStream(baos));
    return baos.toByteArray();
  }

  /**
   * Wraps {@code bytes} as a {@link DataInput} for the matching {@code readFields} call.
   */
  static DataInput asDataInput(byte[] bytes) {
    return new DataInputStream(new ByteArrayInputStream(bytes));
  }
}
