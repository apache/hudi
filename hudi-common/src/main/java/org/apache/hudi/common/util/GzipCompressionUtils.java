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

package org.apache.hudi.common.util;

import javax.xml.bind.DatatypeConverter;
import org.apache.hudi.exception.HoodieIOException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Gzip compression utils.
 */
public class GzipCompressionUtils {

  public static final String TYPE = "GZIP";

  // Use 1MB buffer
  private static final int BUFFER_SIZE = 1024 * 1024;

  /**
   * GZip Compress a raw string.
   * @param data Uncompressed String
   * @return
   */
  public static String compress(String data) {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream(data.length())) {
      GZIPOutputStream gzip = new GZIPOutputStream(bos);
      gzip.write(data.getBytes(StandardCharsets.UTF_8));
      gzip.close();
      byte[] compressed = bos.toByteArray();
      Base64.Encoder encoder = Base64.getMimeEncoder();
      return new String(encoder.encode(compressed), StandardCharsets.UTF_8);
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

  /**
   * GZip Compress a raw byte array.
   * @param data Uncompressed String
   * @return
   */
  public static String compress(byte[] data) {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream(data.length)) {
      GZIPOutputStream gzip = new GZIPOutputStream(bos);
      gzip.write(data);
      gzip.close();
      return DatatypeConverter.printBase64Binary(bos.toByteArray());
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

  /**
   * Uncompress a gzip-compressed string.
   * @param compressed Compressed String
   * @return
   */
  public static String decompress(String compressed) {
    Base64.Decoder decoder = Base64.getMimeDecoder();
    try (final GZIPInputStream gzipInput = new GZIPInputStream(
        new ByteArrayInputStream(decoder.decode(compressed.getBytes(StandardCharsets.UTF_8))));
        final ByteArrayOutputStream byteOut = new ByteArrayOutputStream()) {
      byte[] buf = new byte[BUFFER_SIZE];
      int numRead = 0;
      while (-1 != (numRead = gzipInput.read(buf))) {
        byteOut.write(buf, 0, numRead);
      }
      return new String(byteOut.toByteArray(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage(), e);
    }
  }
}