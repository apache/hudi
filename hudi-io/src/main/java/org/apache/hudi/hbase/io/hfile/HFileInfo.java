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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.SequenceInputStream;
import java.security.Key;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.hbase.Cell;
import org.apache.hudi.hbase.KeyValue;
import org.apache.hudi.hbase.io.crypto.Cipher;
import org.apache.hudi.hbase.io.crypto.Encryption;
import org.apache.hudi.hbase.protobuf.ProtobufMagic;
import org.apache.hudi.hbase.security.EncryptionUtil;
import org.apache.hudi.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hudi.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hudi.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hudi.hbase.shaded.protobuf.generated.HBaseProtos.BytesBytesPair;
import org.apache.hudi.hbase.shaded.protobuf.generated.HFileProtos;

/**
 * Metadata Map of attributes for HFile written out as HFile Trailer. Created by the Writer and
 * added to the tail of the file just before close. Metadata includes core attributes such as last
 * key seen, comparator used writing the file, etc. Clients can add their own attributes via
 * {@link #append(byte[], byte[], boolean)} and they'll be persisted and available at read time.
 * Reader creates the HFileInfo on open by reading the tail of the HFile. The parse of the HFile
 * trailer also creates a {@link HFileContext}, a read-only data structure that includes bulk of
 * the HFileInfo and extras that is safe to pass around when working on HFiles.
 * @see HFileContext
 */
@InterfaceAudience.Private
public class HFileInfo implements SortedMap<byte[], byte[]> {

  private static final Logger LOG = LoggerFactory.getLogger(HFileInfo.class);

  static final String RESERVED_PREFIX = "hfile.";
  static final byte[] RESERVED_PREFIX_BYTES = Bytes.toBytes(RESERVED_PREFIX);
  static final byte [] LASTKEY = Bytes.toBytes(RESERVED_PREFIX + "LASTKEY");
  static final byte [] AVG_KEY_LEN = Bytes.toBytes(RESERVED_PREFIX + "AVG_KEY_LEN");
  static final byte [] AVG_VALUE_LEN = Bytes.toBytes(RESERVED_PREFIX + "AVG_VALUE_LEN");
  static final byte [] CREATE_TIME_TS = Bytes.toBytes(RESERVED_PREFIX + "CREATE_TIME_TS");
  static final byte [] TAGS_COMPRESSED = Bytes.toBytes(RESERVED_PREFIX + "TAGS_COMPRESSED");
  public static final byte [] MAX_TAGS_LEN = Bytes.toBytes(RESERVED_PREFIX + "MAX_TAGS_LEN");
  private final SortedMap<byte [], byte []> map = new TreeMap<>(Bytes.BYTES_COMPARATOR);

  /**
   * We can read files whose major version is v2 IFF their minor version is at least 3.
   */
  private static final int MIN_V2_MINOR_VERSION_WITH_PB = 3;

  /** Maximum minor version supported by this HFile format */
  // We went to version 2 when we moved to pb'ing fileinfo and the trailer on
  // the file. This version can read Writables version 1.
  static final int MAX_MINOR_VERSION = 3;

  /** Last key in the file. Filled in when we read in the file info */
  private Cell lastKeyCell = null;
  /** Average key length read from file info */
  private int avgKeyLen = -1;
  /** Average value length read from file info */
  private int avgValueLen = -1;
  private boolean includesMemstoreTS = false;
  private boolean decodeMemstoreTS = false;

  /**
   * Blocks read from the load-on-open section, excluding data root index, meta
   * index, and file info.
   */
  private List<HFileBlock> loadOnOpenBlocks = new ArrayList<>();

  /**
   * The iterator will track all blocks in load-on-open section, since we use the
   * {@link org.apache.hudi.hbase.io.ByteBuffAllocator} to manage the ByteBuffers in block now,
   * so we must ensure that deallocate all ByteBuffers in the end.
   */
  private HFileBlock.BlockIterator blockIter;

  private HFileBlockIndex.CellBasedKeyBlockIndexReader dataIndexReader;
  private HFileBlockIndex.ByteArrayKeyBlockIndexReader metaIndexReader;

  private FixedFileTrailer trailer;
  private HFileContext hfileContext;

  public HFileInfo() {
    super();
  }

  public HFileInfo(ReaderContext context, Configuration conf) throws IOException {
    this.initTrailerAndContext(context, conf);
  }

  /**
   * Append the given key/value pair to the file info, optionally checking the
   * key prefix.
   *
   * @param k key to add
   * @param v value to add
   * @param checkPrefix whether to check that the provided key does not start
   *          with the reserved prefix
   * @return this file info object
   * @throws IOException if the key or value is invalid
   */
  public HFileInfo append(final byte[] k, final byte[] v,
                          final boolean checkPrefix) throws IOException {
    if (k == null || v == null) {
      throw new NullPointerException("Key nor value may be null");
    }
    if (checkPrefix && isReservedFileInfoKey(k)) {
      throw new IOException("Keys with a " + HFileInfo.RESERVED_PREFIX
          + " are reserved");
    }
    put(k, v);
    return this;
  }

  /** Return true if the given file info key is reserved for internal use. */
  public static boolean isReservedFileInfoKey(byte[] key) {
    return Bytes.startsWith(key, HFileInfo.RESERVED_PREFIX_BYTES);
  }

  @Override
  public void clear() {
    this.map.clear();
  }

  @Override
  public Comparator<? super byte[]> comparator() {
    return map.comparator();
  }

  @Override
  public boolean containsKey(Object key) {
    return map.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return map.containsValue(value);
  }

  @Override
  public Set<java.util.Map.Entry<byte[], byte[]>> entrySet() {
    return map.entrySet();
  }

  @Override
  public boolean equals(Object o) {
    return map.equals(o);
  }

  @Override
  public byte[] firstKey() {
    return map.firstKey();
  }

  @Override
  public byte[] get(Object key) {
    return map.get(key);
  }

  @Override
  public int hashCode() {
    return map.hashCode();
  }

  @Override
  public SortedMap<byte[], byte[]> headMap(byte[] toKey) {
    return this.map.headMap(toKey);
  }

  @Override
  public boolean isEmpty() {
    return map.isEmpty();
  }

  @Override
  public Set<byte[]> keySet() {
    return map.keySet();
  }

  @Override
  public byte[] lastKey() {
    return map.lastKey();
  }

  @Override
  public byte[] put(byte[] key, byte[] value) {
    return this.map.put(key, value);
  }

  @Override
  public void putAll(Map<? extends byte[], ? extends byte[]> m) {
    this.map.putAll(m);
  }

  @Override
  public byte[] remove(Object key) {
    return this.map.remove(key);
  }

  @Override
  public int size() {
    return map.size();
  }

  @Override
  public SortedMap<byte[], byte[]> subMap(byte[] fromKey, byte[] toKey) {
    return this.map.subMap(fromKey, toKey);
  }

  @Override
  public SortedMap<byte[], byte[]> tailMap(byte[] fromKey) {
    return this.map.tailMap(fromKey);
  }

  @Override
  public Collection<byte[]> values() {
    return map.values();
  }

  /**
   * Write out this instance on the passed in <code>out</code> stream.
   * We write it as a protobuf.
   * @see #read(DataInputStream)
   */
  void write(final DataOutputStream out) throws IOException {
    HFileProtos.FileInfoProto.Builder builder = HFileProtos.FileInfoProto.newBuilder();
    for (Map.Entry<byte [], byte[]> e: this.map.entrySet()) {
      HBaseProtos.BytesBytesPair.Builder bbpBuilder = HBaseProtos.BytesBytesPair.newBuilder();
      bbpBuilder.setFirst(UnsafeByteOperations.unsafeWrap(e.getKey()));
      bbpBuilder.setSecond(UnsafeByteOperations.unsafeWrap(e.getValue()));
      builder.addMapEntry(bbpBuilder.build());
    }
    out.write(ProtobufMagic.PB_MAGIC);
    builder.build().writeDelimitedTo(out);
  }

  /**
   * Populate this instance with what we find on the passed in <code>in</code> stream.
   * Can deserialize protobuf of old Writables format.
   * @see #write(DataOutputStream)
   */
  void read(final DataInputStream in) throws IOException {
    // This code is tested over in TestHFileReaderV1 where we read an old hfile w/ this new code.
    int pblen = ProtobufUtil.lengthOfPBMagic();
    byte [] pbuf = new byte[pblen];
    if (in.markSupported()) {
      in.mark(pblen);
    }
    int read = in.read(pbuf);
    if (read != pblen) {
      throw new IOException("read=" + read + ", wanted=" + pblen);
    }
    if (ProtobufUtil.isPBMagicPrefix(pbuf)) {
      parsePB(HFileProtos.FileInfoProto.parseDelimitedFrom(in));
    } else {
      if (in.markSupported()) {
        in.reset();
        parseWritable(in);
      } else {
        // We cannot use BufferedInputStream, it consumes more than we read from the underlying IS
        ByteArrayInputStream bais = new ByteArrayInputStream(pbuf);
        SequenceInputStream sis = new SequenceInputStream(bais, in); // Concatenate input streams
        // TODO: Am I leaking anything here wrapping the passed in stream?  We are not calling
        // close on the wrapped streams but they should be let go after we leave this context?
        // I see that we keep a reference to the passed in inputstream but since we no longer
        // have a reference to this after we leave, we should be ok.
        parseWritable(new DataInputStream(sis));
      }
    }
  }

  /**
   * Now parse the old Writable format.  It was a list of Map entries.  Each map entry was a
   * key and a value of a byte [].  The old map format had a byte before each entry that held
   * a code which was short for the key or value type.  We know it was a byte [] so in below
   * we just read and dump it.
   */
  void parseWritable(final DataInputStream in) throws IOException {
    // First clear the map.
    // Otherwise we will just accumulate entries every time this method is called.
    this.map.clear();
    // Read the number of entries in the map
    int entries = in.readInt();
    // Then read each key/value pair
    for (int i = 0; i < entries; i++) {
      byte [] key = Bytes.readByteArray(in);
      // We used to read a byte that encoded the class type.
      // Read and ignore it because it is always byte [] in hfile
      in.readByte();
      byte [] value = Bytes.readByteArray(in);
      this.map.put(key, value);
    }
  }

  /**
   * Fill our map with content of the pb we read off disk
   * @param fip protobuf message to read
   */
  void parsePB(final HFileProtos.FileInfoProto fip) {
    this.map.clear();
    for (BytesBytesPair pair: fip.getMapEntryList()) {
      this.map.put(pair.getFirst().toByteArray(), pair.getSecond().toByteArray());
    }
  }

  public void initTrailerAndContext(ReaderContext context, Configuration conf) throws IOException {
    try {
      boolean isHBaseChecksum = context.getInputStreamWrapper().shouldUseHBaseChecksum();
      trailer = FixedFileTrailer.readFromStream(context.getInputStreamWrapper()
          .getStream(isHBaseChecksum), context.getFileSize());
      Path path = context.getFilePath();
      checkFileVersion(path);
      this.hfileContext = createHFileContext(path, trailer, conf);
      context.getInputStreamWrapper().unbuffer();
    } catch (Throwable t) {
      // TODO(yihua): remove usage
      //IOUtils.closeQuietly(context.getInputStreamWrapper(),
      //    e -> LOG.warn("failed to close input stream wrapper", e));
      throw new CorruptHFileException("Problem reading HFile Trailer from file "
          + context.getFilePath(), t);
    }
  }

  /**
   * should be called after initTrailerAndContext
   */
  public void initMetaAndIndex(HFile.Reader reader) throws IOException {
    ReaderContext context = reader.getContext();
    try {
      HFileBlock.FSReader blockReader = reader.getUncachedBlockReader();
      // Initialize an block iterator, and parse load-on-open blocks in the following.
      blockIter = blockReader.blockRange(trailer.getLoadOnOpenDataOffset(),
          context.getFileSize() - trailer.getTrailerSize());
      // Data index. We also read statistics about the block index written after
      // the root level.
      this.dataIndexReader =
          new HFileBlockIndex.CellBasedKeyBlockIndexReader(trailer.createComparator(), trailer.getNumDataIndexLevels());
      dataIndexReader
          .readMultiLevelIndexRoot(blockIter.nextBlockWithBlockType(BlockType.ROOT_INDEX), trailer.getDataIndexCount());
      reader.setDataBlockIndexReader(dataIndexReader);
      // Meta index.
      this.metaIndexReader = new HFileBlockIndex.ByteArrayKeyBlockIndexReader(1);
      metaIndexReader.readRootIndex(blockIter.nextBlockWithBlockType(BlockType.ROOT_INDEX),
          trailer.getMetaIndexCount());
      reader.setMetaBlockIndexReader(metaIndexReader);
      loadMetaInfo(blockIter, hfileContext);
      reader.setDataBlockEncoder(HFileDataBlockEncoderImpl.createFromFileInfo(this));
      // Load-On-Open info
      HFileBlock b;
      while ((b = blockIter.nextBlock()) != null) {
        loadOnOpenBlocks.add(b);
      }
      // close the block reader
      context.getInputStreamWrapper().unbuffer();
    } catch (Throwable t) {
      // TODO(yihua): remove usage
      //IOUtils.closeQuietly(context.getInputStreamWrapper(),
      //    e -> LOG.warn("failed to close input stream wrapper", e));
      throw new CorruptHFileException(
          "Problem reading data index and meta index from file " + context.getFilePath(), t);
    }
  }

  private HFileContext createHFileContext(Path path,
                                          FixedFileTrailer trailer, Configuration conf) throws IOException {
    HFileContextBuilder builder = new HFileContextBuilder()
        .withHBaseCheckSum(true)
        .withHFileName(path.getName())
        .withCompression(trailer.getCompressionCodec())
        .withCellComparator(FixedFileTrailer.createComparator(trailer.getComparatorClassName()));
    // Check for any key material available
    byte[] keyBytes = trailer.getEncryptionKey();
    if (keyBytes != null) {
      Encryption.Context cryptoContext = Encryption.newContext(conf);
      Key key = EncryptionUtil.unwrapKey(conf, keyBytes);
      // Use the algorithm the key wants
      Cipher cipher = Encryption.getCipher(conf, key.getAlgorithm());
      if (cipher == null) {
        throw new IOException("Cipher '" + key.getAlgorithm() + "' is not available"
            + ", path=" + path);
      }
      cryptoContext.setCipher(cipher);
      cryptoContext.setKey(key);
      builder.withEncryptionContext(cryptoContext);
    }
    HFileContext context = builder.build();
    return context;
  }

  private void loadMetaInfo(HFileBlock.BlockIterator blockIter, HFileContext hfileContext)
      throws IOException {
    read(blockIter.nextBlockWithBlockType(BlockType.FILE_INFO).getByteStream());
    byte[] creationTimeBytes = get(HFileInfo.CREATE_TIME_TS);
    hfileContext.setFileCreateTime(creationTimeBytes == null ?
        0 : Bytes.toLong(creationTimeBytes));
    byte[] tmp = get(HFileInfo.MAX_TAGS_LEN);
    // max tag length is not present in the HFile means tags were not at all written to file.
    if (tmp != null) {
      hfileContext.setIncludesTags(true);
      tmp = get(HFileInfo.TAGS_COMPRESSED);
      if (tmp != null && Bytes.toBoolean(tmp)) {
        hfileContext.setCompressTags(true);
      }
    }
    // parse meta info
    if (get(HFileInfo.LASTKEY) != null) {
      lastKeyCell = new KeyValue.KeyOnlyKeyValue(get(HFileInfo.LASTKEY));
    }
    avgKeyLen = Bytes.toInt(get(HFileInfo.AVG_KEY_LEN));
    avgValueLen = Bytes.toInt(get(HFileInfo.AVG_VALUE_LEN));
    byte [] keyValueFormatVersion = get(HFileWriterImpl.KEY_VALUE_VERSION);
    includesMemstoreTS = keyValueFormatVersion != null &&
        Bytes.toInt(keyValueFormatVersion) == HFileWriterImpl.KEY_VALUE_VER_WITH_MEMSTORE;
    hfileContext.setIncludesMvcc(includesMemstoreTS);
    if (includesMemstoreTS) {
      decodeMemstoreTS = Bytes.toLong(get(HFileWriterImpl.MAX_MEMSTORE_TS_KEY)) > 0;
    }
  }

  /**
   * File version check is a little sloppy. We read v3 files but can also read v2 files if their
   * content has been pb'd; files written with 0.98.
   */
  private void checkFileVersion(Path path) {
    int majorVersion = trailer.getMajorVersion();
    if (majorVersion == getMajorVersion()) {
      return;
    }
    int minorVersion = trailer.getMinorVersion();
    if (majorVersion == 2 && minorVersion >= MIN_V2_MINOR_VERSION_WITH_PB) {
      return;
    }
    // We can read v3 or v2 versions of hfile.
    throw new IllegalArgumentException("Invalid HFile version: major=" +
        trailer.getMajorVersion() + ", minor=" + trailer.getMinorVersion() + ": expected at least " +
        "major=2 and minor=" + MAX_MINOR_VERSION + ", path=" + path);
  }

  public void close() {
    if (blockIter != null) {
      blockIter.freeBlocks();
    }
  }

  public int getMajorVersion() {
    return 3;
  }

  public void setTrailer(FixedFileTrailer trailer) {
    this.trailer = trailer;
  }

  public FixedFileTrailer getTrailer() {
    return this.trailer;
  }

  public HFileBlockIndex.CellBasedKeyBlockIndexReader getDataBlockIndexReader() {
    return this.dataIndexReader;
  }

  public HFileBlockIndex.ByteArrayKeyBlockIndexReader getMetaBlockIndexReader() {
    return this.metaIndexReader;
  }

  public HFileContext getHFileContext() {
    return this.hfileContext;
  }

  public List<HFileBlock> getLoadOnOpenBlocks() {
    return loadOnOpenBlocks;
  }

  public Cell getLastKeyCell() {
    return lastKeyCell;
  }

  public int getAvgKeyLen() {
    return avgKeyLen;
  }

  public int getAvgValueLen() {
    return avgValueLen;
  }

  public boolean shouldIncludeMemStoreTS() {
    return includesMemstoreTS;
  }

  public boolean isDecodeMemstoreTS() {
    return decodeMemstoreTS;
  }
}
