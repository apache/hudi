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

package org.apache.hudi.hbase.shaded.protobuf;

import static org.apache.hudi.hbase.protobuf.ProtobufMagic.PB_MAGIC;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.hudi.hbase.Cell;
import org.apache.hudi.hbase.client.ColumnFamilyDescriptor;
import org.apache.hudi.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hudi.hbase.exceptions.DeserializationException;
import org.apache.hudi.hbase.protobuf.ProtobufMagic;
import org.apache.hudi.hbase.shaded.protobuf.generated.HBaseProtos.BytesBytesPair;
import org.apache.hudi.hbase.shaded.protobuf.generated.HBaseProtos.ColumnFamilySchema;
import org.apache.hudi.hbase.shaded.protobuf.generated.HBaseProtos.NameStringPair;
import org.apache.hudi.hbase.util.Bytes;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.CodedInputStream;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Protobufs utility.
 * Be aware that a class named org.apache.hadoop.hbase.protobuf.ProtobufUtil (i.e. no 'shaded' in
 * the package name) carries a COPY of a subset of this class for non-shaded
 * users; e.g. Coprocessor Endpoints. If you make change in here, be sure to make change in
 * the companion class too (not the end of the world, especially if you are adding new functionality
 * but something to be aware of.
 */
@InterfaceAudience.Private // TODO: some clients (Hive, etc) use this class
public final class ProtobufUtil {

  private ProtobufUtil() {
  }

  /**
   * Many results are simple: no cell, exists true or false. To save on object creations,
   *  we reuse them across calls.
   */
  private final static Cell[] EMPTY_CELL_ARRAY = new Cell[]{};

  private static volatile boolean classLoaderLoaded = false;
  
  /**
   * Prepend the passed bytes with four bytes of magic, {@link ProtobufMagic#PB_MAGIC},
   * to flag what follows as a protobuf in hbase.  Prepend these bytes to all content written to
   * znodes, etc.
   * @param bytes Bytes to decorate
   * @return The passed <code>bytes</code> with magic prepended (Creates a new
   * byte array that is <code>bytes.length</code> plus {@link ProtobufMagic#PB_MAGIC}.length.
   */
  public static byte [] prependPBMagic(final byte [] bytes) {
    return Bytes.add(PB_MAGIC, bytes);
  }

  /**
   * @param bytes Bytes to check.
   * @return True if passed <code>bytes</code> has {@link ProtobufMagic#PB_MAGIC} for a prefix.
   */
  public static boolean isPBMagicPrefix(final byte [] bytes) {
    return ProtobufMagic.isPBMagicPrefix(bytes);
  }

  /**
   * @param bytes Bytes to check.
   * @param offset offset to start at
   * @param len length to use
   * @return True if passed <code>bytes</code> has {@link ProtobufMagic#PB_MAGIC} for a prefix.
   */
  public static boolean isPBMagicPrefix(final byte [] bytes, int offset, int len) {
    return ProtobufMagic.isPBMagicPrefix(bytes, offset, len);
  }

  /**
   * @param bytes bytes to check
   * @throws DeserializationException if we are missing the pb magic prefix
   */
  public static void expectPBMagicPrefix(final byte[] bytes) throws DeserializationException {
    if (!isPBMagicPrefix(bytes)) {
      String bytesPrefix = bytes == null ? "null" : Bytes.toStringBinary(bytes, 0, PB_MAGIC.length);
      throw new DeserializationException(
          "Missing pb magic " + Bytes.toString(PB_MAGIC) + " prefix" + ", bytes: " + bytesPrefix);
    }
  }

  /**
   * @return Length of {@link ProtobufMagic#lengthOfPBMagic()}
   */
  public static int lengthOfPBMagic() {
    return ProtobufMagic.lengthOfPBMagic();
  }

  /**
   * This version of protobuf's mergeFrom avoids the hard-coded 64MB limit for decoding
   * buffers where the message size is known
   * @param builder current message builder
   * @param in InputStream containing protobuf data
   * @param size known size of protobuf data
   * @throws IOException
   */
  public static void mergeFrom(Message.Builder builder, InputStream in, int size)
      throws IOException {
    final CodedInputStream codedInput = CodedInputStream.newInstance(in);
    codedInput.setSizeLimit(size);
    builder.mergeFrom(codedInput);
    codedInput.checkLastTagWas(0);
  }

  /**
   * This version of protobuf's mergeFrom avoids the hard-coded 64MB limit for decoding
   * buffers where the message size is not known
   * @param builder current message builder
   * @param in InputStream containing protobuf data
   * @throws IOException
   */
  public static void mergeFrom(Message.Builder builder, InputStream in)
      throws IOException {
    final CodedInputStream codedInput = CodedInputStream.newInstance(in);
    codedInput.setSizeLimit(Integer.MAX_VALUE);
    builder.mergeFrom(codedInput);
    codedInput.checkLastTagWas(0);
  }

  /**
   * This version of protobuf's mergeFrom avoids the hard-coded 64MB limit for decoding
   * buffers when working with ByteStrings
   * @param builder current message builder
   * @param bs ByteString containing the
   * @throws IOException
   */
  public static void mergeFrom(Message.Builder builder, ByteString bs) throws IOException {
    final CodedInputStream codedInput = bs.newCodedInput();
    codedInput.setSizeLimit(bs.size());
    builder.mergeFrom(codedInput);
    codedInput.checkLastTagWas(0);
  }

  /**
   * This version of protobuf's mergeFrom avoids the hard-coded 64MB limit for decoding
   * buffers when working with byte arrays
   * @param builder current message builder
   * @param b byte array
   * @throws IOException
   */
  public static void mergeFrom(Message.Builder builder, byte[] b) throws IOException {
    final CodedInputStream codedInput = CodedInputStream.newInstance(b);
    codedInput.setSizeLimit(b.length);
    builder.mergeFrom(codedInput);
    codedInput.checkLastTagWas(0);
  }

  /**
   * This version of protobuf's mergeFrom avoids the hard-coded 64MB limit for decoding
   * buffers when working with byte arrays
   * @param builder current message builder
   * @param b byte array
   * @param offset
   * @param length
   * @throws IOException
   */
  public static void mergeFrom(Message.Builder builder, byte[] b, int offset, int length)
      throws IOException {
    final CodedInputStream codedInput = CodedInputStream.newInstance(b, offset, length);
    codedInput.setSizeLimit(length);
    builder.mergeFrom(codedInput);
    codedInput.checkLastTagWas(0);
  }

  public static void mergeFrom(Message.Builder builder, CodedInputStream codedInput, int length)
      throws IOException {
    codedInput.resetSizeCounter();
    int prevLimit = codedInput.setSizeLimit(length);

    int limit = codedInput.pushLimit(length);
    builder.mergeFrom(codedInput);
    codedInput.popLimit(limit);

    codedInput.checkLastTagWas(0);
    codedInput.setSizeLimit(prevLimit);
  }

  /**
   * Converts an ColumnFamilyDescriptor to ColumnFamilySchema
   * @param hcd the ColumnFamilySchema
   * @return Convert this instance to a the pb column family type
   */
  public static ColumnFamilySchema toColumnFamilySchema(ColumnFamilyDescriptor hcd) {
    ColumnFamilySchema.Builder builder = ColumnFamilySchema.newBuilder();
    builder.setName(UnsafeByteOperations.unsafeWrap(hcd.getName()));
    for (Map.Entry<Bytes, Bytes> e : hcd.getValues().entrySet()) {
      BytesBytesPair.Builder aBuilder = BytesBytesPair.newBuilder();
      aBuilder.setFirst(UnsafeByteOperations.unsafeWrap(e.getKey().get()));
      aBuilder.setSecond(UnsafeByteOperations.unsafeWrap(e.getValue().get()));
      builder.addAttributes(aBuilder.build());
    }
    for (Map.Entry<String, String> e : hcd.getConfiguration().entrySet()) {
      NameStringPair.Builder aBuilder = NameStringPair.newBuilder();
      aBuilder.setName(e.getKey());
      aBuilder.setValue(e.getValue());
      builder.addConfiguration(aBuilder.build());
    }
    return builder.build();
  }

  /**
   * Converts a ColumnFamilySchema to ColumnFamilyDescriptor
   * @param cfs the ColumnFamilySchema
   * @return An {@link ColumnFamilyDescriptor} made from the passed in <code>cfs</code>
   */
  public static ColumnFamilyDescriptor toColumnFamilyDescriptor(final ColumnFamilySchema cfs) {
    // Use the empty constructor so we preserve the initial values set on construction for things
    // like maxVersion.  Otherwise, we pick up wrong values on deserialization which makes for
    // unrelated-looking test failures that are hard to trace back to here.
    ColumnFamilyDescriptorBuilder builder
        = ColumnFamilyDescriptorBuilder.newBuilder(cfs.getName().toByteArray());
    cfs.getAttributesList().forEach(a -> builder.setValue(a.getFirst().toByteArray(), a.getSecond().toByteArray()));
    cfs.getConfigurationList().forEach(a -> builder.setConfiguration(a.getName(), a.getValue()));
    return builder.build();
  }
}
