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
import org.apache.hudi.hbase.io.encoding.DataBlockEncoder;
import org.apache.hudi.hbase.io.encoding.DataBlockEncoding;
import org.apache.hudi.hbase.io.encoding.HFileBlockDecodingContext;
import org.apache.hudi.hbase.io.encoding.HFileBlockDefaultDecodingContext;
import org.apache.hudi.hbase.io.encoding.HFileBlockDefaultEncodingContext;
import org.apache.hudi.hbase.io.encoding.HFileBlockEncodingContext;
import org.apache.hudi.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Do different kinds of data block encoding according to column family
 * options.
 */
@InterfaceAudience.Private
public class HFileDataBlockEncoderImpl implements HFileDataBlockEncoder {
  private final DataBlockEncoding encoding;

  /**
   * Do data block encoding with specified options.
   * @param encoding What kind of data block encoding will be used.
   */
  public HFileDataBlockEncoderImpl(DataBlockEncoding encoding) {
    this.encoding = encoding != null ? encoding : DataBlockEncoding.NONE;
  }

  public static HFileDataBlockEncoder createFromFileInfo(
      HFileInfo fileInfo) throws IOException {
    DataBlockEncoding encoding = DataBlockEncoding.NONE;
    byte[] dataBlockEncodingType = fileInfo.get(DATA_BLOCK_ENCODING);
    if (dataBlockEncodingType != null) {
      String dataBlockEncodingStr = Bytes.toString(dataBlockEncodingType);
      try {
        encoding = DataBlockEncoding.valueOf(dataBlockEncodingStr);
      } catch (IllegalArgumentException ex) {
        throw new IOException("Invalid data block encoding type in file info: "
            + dataBlockEncodingStr, ex);
      }
    }

    if (encoding == DataBlockEncoding.NONE) {
      return NoOpDataBlockEncoder.INSTANCE;
    }
    return new HFileDataBlockEncoderImpl(encoding);
  }

  @Override
  public void saveMetadata(HFile.Writer writer) throws IOException {
    writer.appendFileInfo(DATA_BLOCK_ENCODING, encoding.getNameInBytes());
  }

  @Override
  public DataBlockEncoding getDataBlockEncoding() {
    return encoding;
  }

  public boolean useEncodedScanner(boolean isCompaction) {
    if (isCompaction && encoding == DataBlockEncoding.NONE) {
      return false;
    }
    return encoding != DataBlockEncoding.NONE;
  }

  @Override
  public DataBlockEncoding getEffectiveEncodingInCache(boolean isCompaction) {
    if (!useEncodedScanner(isCompaction)) {
      return DataBlockEncoding.NONE;
    }
    return encoding;
  }

  @Override
  public void encode(Cell cell, HFileBlockEncodingContext encodingCtx, DataOutputStream out)
      throws IOException {
    this.encoding.getEncoder().encode(cell, encodingCtx, out);
  }

  @Override
  public boolean useEncodedScanner() {
    return encoding != DataBlockEncoding.NONE;
  }


  @Override
  public String toString() {
    return getClass().getSimpleName() + "(encoding=" + encoding + ")";
  }

  @Override
  public HFileBlockEncodingContext newDataBlockEncodingContext(
      byte[] dummyHeader, HFileContext fileContext) {
    DataBlockEncoder encoder = encoding.getEncoder();
    if (encoder != null) {
      return encoder.newDataBlockEncodingContext(encoding, dummyHeader, fileContext);
    }
    return new HFileBlockDefaultEncodingContext(null, dummyHeader, fileContext);
  }

  @Override
  public HFileBlockDecodingContext newDataBlockDecodingContext(HFileContext fileContext) {
    DataBlockEncoder encoder = encoding.getEncoder();
    if (encoder != null) {
      return encoder.newDataBlockDecodingContext(fileContext);
    }
    return new HFileBlockDefaultDecodingContext(fileContext);
  }

  @Override
  public void startBlockEncoding(HFileBlockEncodingContext encodingCtx, DataOutputStream out)
      throws IOException {
    if (this.encoding != null && this.encoding != DataBlockEncoding.NONE) {
      this.encoding.getEncoder().startBlockEncoding(encodingCtx, out);
    }
  }

  @Override
  public void endBlockEncoding(HFileBlockEncodingContext encodingCtx, DataOutputStream out,
                               byte[] uncompressedBytesWithHeader, BlockType blockType) throws IOException {
    this.encoding.getEncoder().endBlockEncoding(encodingCtx, out, uncompressedBytesWithHeader);
  }
}
