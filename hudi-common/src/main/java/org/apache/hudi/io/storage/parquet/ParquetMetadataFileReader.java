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

package org.apache.hudi.io.storage.parquet;

import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.crypto.AesCipher;
import org.apache.parquet.crypto.InternalColumnDecryptionSetup;
import org.apache.parquet.crypto.InternalFileDecryptor;
import org.apache.parquet.crypto.ModuleCipherFactory;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.format.BloomFilterHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.hadoop.metadata.IndexReference;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A wrapper for a Parquet file reader, which is created specifically for
 * reading Hoodie metadata table files of Parquet format.
 */
public final class ParquetMetadataFileReader extends ParquetFileReader {
  private static final Logger LOG = LoggerFactory.getLogger(ParquetMetadataFileReader.class);
  private final InputFile inputFile;
  private final InternalFileDecryptor fileDecryptor;

  public ParquetMetadataFileReader(InputFile file, ParquetReadOptions options) throws IOException {
    super(file, options);
    inputFile = file;
    fileDecryptor = getFooter().getFileMetaData().getFileDecryptor();
  }

  public InputFile getInputFile() {
    return inputFile;
  }

  public void setStreamPosition(long newPos) throws IOException {
    f.seek(newPos);
  }

  public void blockRead(ByteBuffer data) throws IOException {
    f.readFully(data);
  }

  public void blockRead(byte[] data, int start, int len) throws IOException {
    f.readFully(data, start, len);
  }

  /**
   * Code from {@link ParquetFileReader}#readColumnIndex
   *
   * @param cachedStream   cached stream containing the entire metadata that was pre-fetched.
   * @param startOffset    startOffset of the cachedStream relative to actual parquet file.
   * @param endOffset      startOffset of the cachedStream relative to actual parquet file.
   * @param column         the column chunk which the column index is to be returned for
   * @return               the column index for the specified column chunk or {@code null} if there is no index
   * @throws IOException   if any I/O error occurs during reading the file
   */
  public ColumnIndex readColumnIndex(
      ParquetFileReader reader,
      SeekableInputStream cachedStream,
      long startOffset,
      long endOffset,
      ColumnChunkMetaData column) throws IOException {
    IndexReference ref = column.getColumnIndexReference();
    if (ref == null) {
      return null;
    }

    // In case the cache does not contain the required bytes for the metadata, we fall back to {@link ParquetFileReader}
    if (endOffset - (startOffset + cachedStream.getPos()) < ref.getLength()) {
      LOG.warn("Reading bloom filter from storage since its size was underestimated cache startOffset: " + startOffset
          + " pos: " + cachedStream.getPos()
          + " endOffset: " + endOffset
          + " numBytes: " + ref.getLength());
      return reader.readColumnIndex(column);
    }

    cachedStream.seek(ref.getOffset() - startOffset);
    LOG.debug("Reading ColumnIndex from cache with startIndex: {} endOffset: {} {}", startOffset, endOffset, ref.getOffset() + " " + ref.getLength());

    BlockCipher.Decryptor columnIndexDecryptor = null;
    byte[] columnIndexAAD = null;
    if (null != fileDecryptor && !fileDecryptor.plaintextFile()) {
      InternalColumnDecryptionSetup columnDecryptionSetup = fileDecryptor.getColumnSetup(column.getPath());
      if (columnDecryptionSetup.isEncrypted()) {
        columnIndexDecryptor = columnDecryptionSetup.getMetaDataDecryptor();
        columnIndexAAD = AesCipher.createModuleAAD(fileDecryptor.getFileAAD(), ModuleCipherFactory.ModuleType.ColumnIndex,
            column.getRowGroupOrdinal(), columnDecryptionSetup.getOrdinal(), -1);
      }
    }

    return ParquetMetadataConverter.fromParquetColumnIndex(
        column.getPrimitiveType(),
        Util.readColumnIndex(cachedStream, columnIndexDecryptor, columnIndexAAD));
  }

  /**
   * Code from {@link ParquetFileReader}#readOffsetIndex
   *
   * @param cachedStream   cached stream containing the entire metadata that was pre-fetched.
   * @param startOffset    startOffset of the cachedStream relative to actual parquet file.
   * @param endOffset      startOffset of the cachedStream relative to actual parquet file.
   * @param column         the column chunk which the offset index is to be returned for
   * @return               the offset index for the specified column chunk or {@code null} if there is no index
   * @throws IOException if any I/O error occurs during reading the file
   */
  public OffsetIndex readOffsetIndex(ParquetFileReader reader, SeekableInputStream cachedStream, long startOffset, long endOffset, ColumnChunkMetaData column) throws IOException {
    IndexReference ref = column.getOffsetIndexReference();
    if (ref == null) {
      LOG.debug("readOffsetIndex null");
      return null;
    }

    // In case the cache does not contain the required bytes for the metadata, we fallback to {@link ParquetFileReader}
    if (endOffset - (startOffset + cachedStream.getPos()) < ref.getLength()) {
      LOG.warn("Reading bloom filter from storage since its size was underestimated cache startOffset: " + startOffset
          + " pos: " + cachedStream.getPos()
          + " endOffset: " + endOffset
          + " numBytes: " + ref.getLength());
      return reader.readOffsetIndex(column);
    }

    cachedStream.seek(ref.getOffset() - startOffset);
    LOG.debug("Reading OffsetIndex from cache with startIndex: {} endOffset: {} {}", startOffset, endOffset, ref.getOffset() + " " + ref.getLength());

    BlockCipher.Decryptor offsetIndexDecryptor = null;
    byte[] offsetIndexAAD = null;
    if (null != fileDecryptor && !fileDecryptor.plaintextFile()) {
      InternalColumnDecryptionSetup columnDecryptionSetup = fileDecryptor.getColumnSetup(column.getPath());
      if (columnDecryptionSetup.isEncrypted()) {
        offsetIndexDecryptor = columnDecryptionSetup.getMetaDataDecryptor();
        offsetIndexAAD = AesCipher.createModuleAAD(fileDecryptor.getFileAAD(), ModuleCipherFactory.ModuleType.OffsetIndex,
            column.getRowGroupOrdinal(), columnDecryptionSetup.getOrdinal(), -1);
      }
    }
    return ParquetMetadataConverter.fromParquetOffsetIndex(Util.readOffsetIndex(cachedStream, offsetIndexDecryptor, offsetIndexAAD));
  }

  /**
   * Reads Bloom filter data for the given column chunk.
   * Code from {@link ParquetFileReader}#readBloomFilter
   *
   * @param inputStream inputstream containing the entire metadata
   * @param startOffset      read the inputstream from offset
   * @param meta        a column's ColumnChunkMetaData to read the dictionary from
   * @return an BloomFilter object.
   * @throws IOException if there is an error while reading the Bloom filter.
   */
  public BloomFilter readBloomFilter(ParquetFileReader reader, SeekableInputStream inputStream, long startOffset, long endOffset, ColumnChunkMetaData meta) throws IOException {
    long bloomFilterOffset = meta.getBloomFilterOffset();
    if (bloomFilterOffset < 0) {
      return null;
    }

    // Prepare to decrypt Bloom filter (for encrypted columns)
    BlockCipher.Decryptor bloomFilterDecryptor = null;
    byte[] bloomFilterHeaderAAD = null;
    byte[] bloomFilterBitsetAAD = null;
    if (null != fileDecryptor && !fileDecryptor.plaintextFile()) {
      InternalColumnDecryptionSetup columnDecryptionSetup = fileDecryptor.getColumnSetup(meta.getPath());
      if (columnDecryptionSetup.isEncrypted()) {
        bloomFilterDecryptor = columnDecryptionSetup.getMetaDataDecryptor();
        bloomFilterHeaderAAD = AesCipher.createModuleAAD(fileDecryptor.getFileAAD(), ModuleCipherFactory.ModuleType.BloomFilterHeader,
            meta.getRowGroupOrdinal(), columnDecryptionSetup.getOrdinal(), -1);
        bloomFilterBitsetAAD = AesCipher.createModuleAAD(fileDecryptor.getFileAAD(), ModuleCipherFactory.ModuleType.BloomFilterBitset,
            meta.getRowGroupOrdinal(), columnDecryptionSetup.getOrdinal(), -1);
      }
    }

    // Read Bloom filter data header.
    inputStream.seek(bloomFilterOffset - startOffset);
    BloomFilterHeader bloomFilterHeader;
    try {
      bloomFilterHeader = Util.readBloomFilterHeader(inputStream, bloomFilterDecryptor, bloomFilterHeaderAAD);
    } catch (IOException e) {
      LOG.warn("Unable to decode the Bloom header ", e);
      return null;
    }

    int numBytes = bloomFilterHeader.getNumBytes();
    if (numBytes <= 0 || numBytes > BlockSplitBloomFilter.UPPER_BOUND_BYTES) {
      LOG.warn("the read bloom filter size is wrong, size is {}", bloomFilterHeader.getNumBytes());
      return null;
    }

    // If we underestimated the length of the bloom filter, fallback to the ParquetFileReader
    // to fetch it from the storage layer.
    if (endOffset - (startOffset + inputStream.getPos()) < numBytes) {
      LOG.warn("Reading bloom filter from storage since its size was underestimated cache startOffset: " + startOffset
          + " pos: " + inputStream.getPos()
          + " endOffset: " + endOffset
          + " numBytes: " + numBytes);
      return reader.readBloomFilter(meta);
    }

    if (!bloomFilterHeader.getHash().isSetXXHASH() || !bloomFilterHeader.getAlgorithm().isSetBLOCK()
        || !bloomFilterHeader.getCompression().isSetUNCOMPRESSED()) {
      LOG.warn("the read bloom filter is not supported yet,  algorithm = {}, hash = {}, compression = {}",
          bloomFilterHeader.getAlgorithm(), bloomFilterHeader.getHash(), bloomFilterHeader.getCompression());
      return null;
    }

    byte[] bitset;
    if (null == bloomFilterDecryptor) {
      bitset = new byte[numBytes];
      inputStream.readFully(bitset);
    } else {
      bitset = bloomFilterDecryptor.decrypt(inputStream, bloomFilterBitsetAAD);
      if (bitset.length != numBytes) {
        throw new ParquetCryptoRuntimeException("Wrong length of decrypted bloom filter bitset");
      }
    }
    return new BlockSplitBloomFilter(bitset);
  }
}