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

package org.apache.hudi.hbase.io.hfile;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hudi.hbase.Cell;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hudi.hbase.io.encoding.DataBlockEncoding;
import org.apache.hudi.hbase.io.encoding.EncodingState;
import org.apache.hudi.hbase.io.encoding.HFileBlockDecodingContext;
import org.apache.hudi.hbase.io.encoding.HFileBlockDefaultDecodingContext;
import org.apache.hudi.hbase.io.encoding.HFileBlockDefaultEncodingContext;
import org.apache.hudi.hbase.io.encoding.HFileBlockEncodingContext;
import org.apache.hudi.hbase.io.encoding.NoneEncoder;

/**
 * Does not perform any kind of encoding/decoding.
 */
@InterfaceAudience.Private
public class NoOpDataBlockEncoder implements HFileDataBlockEncoder {

  public static final NoOpDataBlockEncoder INSTANCE =
      new NoOpDataBlockEncoder();

  private static class NoneEncodingState extends EncodingState {
    NoneEncoder encoder = null;
  }

  /** Cannot be instantiated. Use {@link #INSTANCE} instead. */
  private NoOpDataBlockEncoder() {
  }

  @Override
  public void encode(Cell cell, HFileBlockEncodingContext encodingCtx,
                     DataOutputStream out) throws IOException {
    NoneEncodingState state = (NoneEncodingState) encodingCtx
        .getEncodingState();
    NoneEncoder encoder = state.encoder;
    int size = encoder.write(cell);
    state.postCellEncode(size, size);
  }

  @Override
  public boolean useEncodedScanner() {
    return false;
  }

  @Override
  public void saveMetadata(HFile.Writer writer) {
  }

  @Override
  public DataBlockEncoding getDataBlockEncoding() {
    return DataBlockEncoding.NONE;
  }

  @Override
  public DataBlockEncoding getEffectiveEncodingInCache(boolean isCompaction) {
    return DataBlockEncoding.NONE;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

  @Override
  public HFileBlockEncodingContext newDataBlockEncodingContext(
      byte[] dummyHeader, HFileContext meta) {
    return new HFileBlockDefaultEncodingContext(null, dummyHeader, meta);
  }

  @Override
  public HFileBlockDecodingContext newDataBlockDecodingContext(HFileContext meta) {
    return new HFileBlockDefaultDecodingContext(meta);
  }

  @Override
  public void startBlockEncoding(HFileBlockEncodingContext blkEncodingCtx,
                                 DataOutputStream out) throws IOException {
    if (blkEncodingCtx.getClass() != HFileBlockDefaultEncodingContext.class) {
      throw new IOException(this.getClass().getName() + " only accepts "
          + HFileBlockDefaultEncodingContext.class.getName() + " as the "
          + "encoding context.");
    }

    HFileBlockDefaultEncodingContext encodingCtx =
        (HFileBlockDefaultEncodingContext) blkEncodingCtx;
    encodingCtx.prepareEncoding(out);

    NoneEncoder encoder = new NoneEncoder(out, encodingCtx);
    NoneEncodingState state = new NoneEncodingState();
    state.encoder = encoder;
    blkEncodingCtx.setEncodingState(state);
  }

  @Override
  public void endBlockEncoding(HFileBlockEncodingContext encodingCtx, DataOutputStream out,
                               byte[] uncompressedBytesWithHeader, BlockType blockType) throws IOException {
    encodingCtx.postEncoding(BlockType.DATA);
  }
}
