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

package org.apache.hudi.parquet.io;

import java.io.IOException;

import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.SeekableInputStream;

/**
 * Class to help gather/calculate the proper values of the dictionary/first data page offset values in a column chunk.
 * This class is used by Tools (parquet-tools/-cli) that do not support encryption so this does not support it either.
 * (In some cases this tool would read the dictionary page header which might be encrypted.)
 */
class Offsets {

  /**
   * Returns the offset values for the column chunk to be written.
   *
   * @param input         the source input stream of the column chunk
   * @param chunk         the column chunk metadata read from the source file
   * @param newChunkStart the position of the column chunk to be written
   * @return the offset values
   * @throws IOException if any I/O error occurs during the reading of the input stream
   */
  public static Offsets getOffsets(SeekableInputStream input, ColumnChunkMetaData chunk, long newChunkStart)
      throws IOException {
    long firstDataPageOffset;
    long dictionaryPageOffset;
    if (chunk.hasDictionaryPage()) {
      long dictionaryPageSize;
      if (chunk.getDictionaryPageOffset() == 0 || chunk.getFirstDataPageOffset() <= chunk.getDictionaryPageOffset()) {
        /*
         * The offsets might not contain the proper values (so we need to read the dictionary page header):
         * - The dictionaryPageOffset might not be set; in this case 0 is returned
         *   (0 cannot be a valid offset because of the MAGIC bytes)
         * - The firstDataPageOffset might point to the dictionary page
         */
        dictionaryPageSize = readDictionaryPageSize(input, chunk);
      } else {
        dictionaryPageSize = chunk.getFirstDataPageOffset() - chunk.getDictionaryPageOffset();
      }
      firstDataPageOffset = newChunkStart + dictionaryPageSize;
      dictionaryPageOffset = newChunkStart;
    } else {
      firstDataPageOffset = newChunkStart;
      dictionaryPageOffset = 0;
    }
    return new Offsets(firstDataPageOffset, dictionaryPageOffset);
  }

  private static long readDictionaryPageSize(SeekableInputStream in, ColumnChunkMetaData chunk) throws IOException {
    long origPos = -1;
    try {
      origPos = in.getPos();
      in.seek(chunk.getStartingPos());
      long headerStart = in.getPos();
      PageHeader header = Util.readPageHeader(in);
      long headerSize = in.getPos() - headerStart;
      return headerSize + header.getCompressed_page_size();
    } finally {
      if (origPos != -1) {
        in.seek(origPos);
      }
    }
  }

  private Offsets(long firstDataPageOffset, long dictionaryPageOffset) {
    this.firstDataPageOffset = firstDataPageOffset;
    this.dictionaryPageOffset = dictionaryPageOffset;
  }

  public final long firstDataPageOffset;
  public final long dictionaryPageOffset;
}
