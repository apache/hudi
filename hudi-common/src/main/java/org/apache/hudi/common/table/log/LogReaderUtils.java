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

package org.apache.hudi.common.table.log;

import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.util.Base64CodecUtil;

import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * Utils class for performing various log file reading operations.
 */
public class LogReaderUtils {
  /**
   * Encodes a list of record positions in long type.
   * <p>
   * The encoding applies the Base64 codec ({@link java.util.Base64} in Java implementation) on
   * the bytes generated from serializing {@link Roaring64NavigableMap} bitmap, which contains
   * the list of record positions in long type, using the portable
   * format.
   *
   * @param positions A list of long-typed positions.
   * @return A string of Base64-encoded bytes ({@link java.util.Base64} in Java implementation)
   * generated from serializing {@link Roaring64NavigableMap} bitmap using the portable format.
   * @throws IOException upon I/O error.
   */
  public static String encodePositions(List<Long> positions) throws IOException {
    Roaring64NavigableMap positionBitmap = new Roaring64NavigableMap();
    positions.forEach(positionBitmap::add);
    return encodePositions(positionBitmap);
  }

  /**
   * Encodes the {@link Roaring64NavigableMap} bitmap containing the record positions.
   * <p>
   * The encoding applies the Base64 codec ({@link java.util.Base64} in Java implementation) on
   * the bytes generated from serializing {@link Roaring64NavigableMap} bitmap using the portable
   * format.
   *
   * @param positionBitmap {@link Roaring64NavigableMap} bitmap containing the record positions.
   * @return A string of Base64-encoded bytes ({@link java.util.Base64} in Java implementation)
   * generated from serializing {@link Roaring64NavigableMap} bitmap using the portable format.
   * @throws IOException upon I/O error.
   */
  public static String encodePositions(Roaring64NavigableMap positionBitmap) throws IOException {
    positionBitmap.runOptimize();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    positionBitmap.serializePortable(dos);
    return Base64CodecUtil.encode(baos.toByteArray());
  }

  /**
   * Decodes the {@link HeaderMetadataType#RECORD_POSITIONS} block header into record positions.
   *
   * @param content A string of Base64-encoded bytes ({@link java.util.Base64} in Java
   *                implementation) generated from serializing {@link Roaring64NavigableMap}
   *                bitmap using the portable format.
   * @return A {@link Roaring64NavigableMap} bitmap containing the record positions in long type.
   * @throws IOException upon I/O error.
   */
  public static Roaring64NavigableMap decodeRecordPositionsHeader(String content) throws IOException {
    Roaring64NavigableMap positionBitmap = new Roaring64NavigableMap();
    ByteArrayInputStream bais = new ByteArrayInputStream(Base64CodecUtil.decode(content));
    DataInputStream dis = new DataInputStream(bais);
    positionBitmap.deserializePortable(dis);
    return positionBitmap;
  }
}
