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

import static org.apache.hudi.hbase.io.ByteBuffAllocator.HEAP;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hudi.hbase.Cell;
import org.apache.hudi.hbase.HConstants;
import org.apache.hudi.hbase.fs.HFileSystem;
import org.apache.hudi.hbase.io.ByteArrayOutputStream;
import org.apache.hudi.hbase.io.ByteBuffAllocator;
import org.apache.hudi.hbase.io.ByteBuffInputStream;
import org.apache.hudi.hbase.io.ByteBufferWriterDataOutputStream;
import org.apache.hudi.hbase.io.FSDataInputStreamWrapper;
import org.apache.hudi.hbase.io.encoding.DataBlockEncoding;
import org.apache.hudi.hbase.io.encoding.EncodingState;
import org.apache.hudi.hbase.io.encoding.HFileBlockDecodingContext;
import org.apache.hudi.hbase.io.encoding.HFileBlockDefaultDecodingContext;
import org.apache.hudi.hbase.io.encoding.HFileBlockDefaultEncodingContext;
import org.apache.hudi.hbase.io.encoding.HFileBlockEncodingContext;
import org.apache.hudi.hbase.io.util.BlockIOUtils;
import org.apache.hudi.hbase.nio.ByteBuff;
import org.apache.hudi.hbase.nio.MultiByteBuff;
import org.apache.hudi.hbase.nio.SingleByteBuff;
import org.apache.hudi.hbase.regionserver.ShipperListener;
import org.apache.hudi.hbase.util.Bytes;
import org.apache.hudi.hbase.util.ChecksumType;
import org.apache.hudi.hbase.util.ClassSize;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * Cacheable Blocks of an {@link HFile} version 2 file.
 * Version 2 was introduced in hbase-0.92.0.
 *
 * <p>Version 1 was the original file block. Version 2 was introduced when we changed the hbase file
 * format to support multi-level block indexes and compound bloom filters (HBASE-3857). Support
 * for Version 1 was removed in hbase-1.3.0.
 *
 * <h3>HFileBlock: Version 2</h3>
 * In version 2, a block is structured as follows:
 * <ul>
 * <li><b>Header:</b> See Writer#putHeader() for where header is written; header total size is
 * HFILEBLOCK_HEADER_SIZE
 * <ul>
 * <li>0. blockType: Magic record identifying the {@link BlockType} (8 bytes):
 * e.g. <code>DATABLK*</code>
 * <li>1. onDiskSizeWithoutHeader: Compressed -- a.k.a 'on disk' -- block size, excluding header,
 * but including tailing checksum bytes (4 bytes)
 * <li>2. uncompressedSizeWithoutHeader: Uncompressed block size, excluding header, and excluding
 * checksum bytes (4 bytes)
 * <li>3. prevBlockOffset: The offset of the previous block of the same type (8 bytes). This is
 * used to navigate to the previous block without having to go to the block index
 * <li>4: For minorVersions &gt;=1, the ordinal describing checksum type (1 byte)
 * <li>5: For minorVersions &gt;=1, the number of data bytes/checksum chunk (4 bytes)
 * <li>6: onDiskDataSizeWithHeader: For minorVersions &gt;=1, the size of data 'on disk', including
 * header, excluding checksums (4 bytes)
 * </ul>
 * </li>
 * <li><b>Raw/Compressed/Encrypted/Encoded data:</b> The compression
 * algorithm is the same for all the blocks in an {@link HFile}. If compression is NONE, this is
 * just raw, serialized Cells.
 * <li><b>Tail:</b> For minorVersions &gt;=1, a series of 4 byte checksums, one each for
 * the number of bytes specified by bytesPerChecksum.
 * </ul>
 *
 * <h3>Caching</h3>
 * Caches cache whole blocks with trailing checksums if any. We then tag on some metadata, the
 * content of BLOCK_METADATA_SPACE which will be flag on if we are doing 'hbase'
 * checksums and then the offset into the file which is needed when we re-make a cache key
 * when we return the block to the cache as 'done'.
 * See {@link Cacheable#serialize(ByteBuffer, boolean)} and {@link Cacheable#getDeserializer()}.
 *
 * <p>TODO: Should we cache the checksums? Down in Writer#getBlockForCaching(CacheConfig) where
 * we make a block to cache-on-write, there is an attempt at turning off checksums. This is not the
 * only place we get blocks to cache. We also will cache the raw return from an hdfs read. In this
 * case, the checksums may be present. If the cache is backed by something that doesn't do ECC,
 * say an SSD, we might want to preserve checksums. For now this is open question.
 * <p>TODO: Over in BucketCache, we save a block allocation by doing a custom serialization.
 * Be sure to change it if serialization changes in here. Could we add a method here that takes an
 * IOEngine and that then serializes to it rather than expose our internals over in BucketCache?
 * IOEngine is in the bucket subpackage. Pull it up? Then this class knows about bucketcache. Ugh.
 */
@InterfaceAudience.Private
public class HFileBlock implements Cacheable {
  private static final Logger LOG = LoggerFactory.getLogger(HFileBlock.class);
  public static final long FIXED_OVERHEAD = ClassSize.estimateBase(HFileBlock.class, false);

  // Block Header fields.

  // TODO: encapsulate Header related logic in this inner class.
  static class Header {
    // Format of header is:
    // 8 bytes - block magic
    // 4 bytes int - onDiskSizeWithoutHeader
    // 4 bytes int - uncompressedSizeWithoutHeader
    // 8 bytes long - prevBlockOffset
    // The following 3 are only present if header contains checksum information
    // 1 byte - checksum type
    // 4 byte int - bytes per checksum
    // 4 byte int - onDiskDataSizeWithHeader
    static int BLOCK_MAGIC_INDEX = 0;
    static int ON_DISK_SIZE_WITHOUT_HEADER_INDEX = 8;
    static int UNCOMPRESSED_SIZE_WITHOUT_HEADER_INDEX = 12;
    static int PREV_BLOCK_OFFSET_INDEX = 16;
    static int CHECKSUM_TYPE_INDEX = 24;
    static int BYTES_PER_CHECKSUM_INDEX = 25;
    static int ON_DISK_DATA_SIZE_WITH_HEADER_INDEX = 29;
  }

  /** Type of block. Header field 0. */
  private BlockType blockType;

  /**
   * Size on disk excluding header, including checksum. Header field 1.
   * @see Writer#putHeader(byte[], int, int, int, int)
   */
  private int onDiskSizeWithoutHeader;

  /**
   * Size of pure data. Does not include header or checksums. Header field 2.
   * @see Writer#putHeader(byte[], int, int, int, int)
   */
  private int uncompressedSizeWithoutHeader;

  /**
   * The offset of the previous block on disk. Header field 3.
   * @see Writer#putHeader(byte[], int, int, int, int)
   */
  private long prevBlockOffset;

  /**
   * Size on disk of header + data. Excludes checksum. Header field 6,
   * OR calculated from {@link #onDiskSizeWithoutHeader} when using HDFS checksum.
   * @see Writer#putHeader(byte[], int, int, int, int)
   */
  private int onDiskDataSizeWithHeader;
  // End of Block Header fields.

  /**
   * The in-memory representation of the hfile block. Can be on or offheap. Can be backed by
   * a single ByteBuffer or by many. Make no assumptions.
   *
   * <p>Be careful reading from this <code>buf</code>. Duplicate and work on the duplicate or if
   * not, be sure to reset position and limit else trouble down the road.
   *
   * <p>TODO: Make this read-only once made.
   *
   * <p>We are using the ByteBuff type. ByteBuffer is not extensible yet we need to be able to have
   * a ByteBuffer-like API across multiple ByteBuffers reading from a cache such as BucketCache.
   * So, we have this ByteBuff type. Unfortunately, it is spread all about HFileBlock. Would be
   * good if could be confined to cache-use only but hard-to-do.
   */
  private ByteBuff buf;

  /** Meta data that holds meta information on the hfileblock.
   */
  private HFileContext fileContext;

  /**
   * The offset of this block in the file. Populated by the reader for
   * convenience of access. This offset is not part of the block header.
   */
  private long offset = UNSET;

  /**
   * The on-disk size of the next block, including the header and checksums if present.
   * UNSET if unknown.
   *
   * Blocks try to carry the size of the next block to read in this data member. Usually
   * we get block sizes from the hfile index but sometimes the index is not available:
   * e.g. when we read the indexes themselves (indexes are stored in blocks, we do not
   * have an index for the indexes). Saves seeks especially around file open when
   * there is a flurry of reading in hfile metadata.
   */
  private int nextBlockOnDiskSize = UNSET;

  private ByteBuffAllocator allocator;

  /**
   * On a checksum failure, do these many succeeding read requests using hdfs checksums before
   * auto-reenabling hbase checksum verification.
   */
  static final int CHECKSUM_VERIFICATION_NUM_IO_THRESHOLD = 3;

  private static int UNSET = -1;
  public static final boolean FILL_HEADER = true;
  public static final boolean DONT_FILL_HEADER = false;

  // How to get the estimate correctly? if it is a singleBB?
  public static final int MULTI_BYTE_BUFFER_HEAP_SIZE =
      (int)ClassSize.estimateBase(MultiByteBuff.class, false);

  /**
   * Space for metadata on a block that gets stored along with the block when we cache it.
   * There are a few bytes stuck on the end of the HFileBlock that we pull in from HDFS.
   * 8 bytes are for the offset of this block (long) in the file. Offset is important because is is
   * used when we remake the CacheKey when we return block to the cache when done. There is also
   * a flag on whether checksumming is being done by hbase or not. See class comment for note on
   * uncertain state of checksumming of blocks that come out of cache (should we or should we not?).
   * Finally there are 4 bytes to hold the length of the next block which can save a seek on
   * occasion if available.
   * (This EXTRA info came in with original commit of the bucketcache, HBASE-7404. It was
   * formerly known as EXTRA_SERIALIZATION_SPACE).
   */
  static final int BLOCK_METADATA_SPACE = Bytes.SIZEOF_BYTE + Bytes.SIZEOF_LONG + Bytes.SIZEOF_INT;

  /**
   * Each checksum value is an integer that can be stored in 4 bytes.
   */
  static final int CHECKSUM_SIZE = Bytes.SIZEOF_INT;

  static final byte[] DUMMY_HEADER_NO_CHECKSUM =
      new byte[HConstants.HFILEBLOCK_HEADER_SIZE_NO_CHECKSUM];

  /**
   * Used deserializing blocks from Cache.
   *
   * <code>
   * ++++++++++++++
   * + HFileBlock +
   * ++++++++++++++
   * + Checksums  + <= Optional
   * ++++++++++++++
   * + Metadata!  + <= See note on BLOCK_METADATA_SPACE above.
   * ++++++++++++++
   * </code>
   * @see #serialize(ByteBuffer, boolean)
   */
  public static final CacheableDeserializer<Cacheable> BLOCK_DESERIALIZER = new BlockDeserializer();

  public static final class BlockDeserializer implements CacheableDeserializer<Cacheable> {
    private BlockDeserializer() {
    }

    @Override
    public HFileBlock deserialize(ByteBuff buf, ByteBuffAllocator alloc)
        throws IOException {
      // The buf has the file block followed by block metadata.
      // Set limit to just before the BLOCK_METADATA_SPACE then rewind.
      buf.limit(buf.limit() - BLOCK_METADATA_SPACE).rewind();
      // Get a new buffer to pass the HFileBlock for it to 'own'.
      ByteBuff newByteBuff = buf.slice();
      // Read out the BLOCK_METADATA_SPACE content and shove into our HFileBlock.
      buf.position(buf.limit());
      buf.limit(buf.limit() + HFileBlock.BLOCK_METADATA_SPACE);
      boolean usesChecksum = buf.get() == (byte) 1;
      long offset = buf.getLong();
      int nextBlockOnDiskSize = buf.getInt();
      return createFromBuff(newByteBuff, usesChecksum, offset, nextBlockOnDiskSize, null, alloc);
    }

    @Override
    public int getDeserializerIdentifier() {
      return DESERIALIZER_IDENTIFIER;
    }
  }

  private static final int DESERIALIZER_IDENTIFIER;
  static {
    DESERIALIZER_IDENTIFIER =
        CacheableDeserializerIdManager.registerDeserializer(BLOCK_DESERIALIZER);
  }

  /**
   * Creates a new {@link HFile} block from the given fields. This constructor
   * is used only while writing blocks and caching,
   * and is sitting in a byte buffer and we want to stuff the block into cache.
   * See {@link Writer#getBlockForCaching(CacheConfig)}.
   *
   * <p>TODO: The caller presumes no checksumming
   * <p>TODO: HFile block writer can also off-heap ? </p>
   * required of this block instance since going into cache; checksum already verified on
   * underlying block data pulled in from filesystem. Is that correct? What if cache is SSD?
   *
   * @param blockType the type of this block, see {@link BlockType}
   * @param onDiskSizeWithoutHeader see {@link #onDiskSizeWithoutHeader}
   * @param uncompressedSizeWithoutHeader see {@link #uncompressedSizeWithoutHeader}
   * @param prevBlockOffset see {@link #prevBlockOffset}
   * @param buf block buffer with header ({@link HConstants#HFILEBLOCK_HEADER_SIZE} bytes)
   * @param fillHeader when true, write the first 4 header fields into passed buffer.
   * @param offset the file offset the block was read from
   * @param onDiskDataSizeWithHeader see {@link #onDiskDataSizeWithHeader}
   * @param fileContext HFile meta data
   */
  public HFileBlock(BlockType blockType, int onDiskSizeWithoutHeader,
                    int uncompressedSizeWithoutHeader, long prevBlockOffset, ByteBuff buf, boolean fillHeader,
                    long offset, int nextBlockOnDiskSize, int onDiskDataSizeWithHeader, HFileContext fileContext,
                    ByteBuffAllocator allocator) {
    this.blockType = blockType;
    this.onDiskSizeWithoutHeader = onDiskSizeWithoutHeader;
    this.uncompressedSizeWithoutHeader = uncompressedSizeWithoutHeader;
    this.prevBlockOffset = prevBlockOffset;
    this.offset = offset;
    this.onDiskDataSizeWithHeader = onDiskDataSizeWithHeader;
    this.nextBlockOnDiskSize = nextBlockOnDiskSize;
    this.fileContext = fileContext;
    this.allocator = allocator;
    this.buf = buf;
    if (fillHeader) {
      overwriteHeader();
    }
    this.buf.rewind();
  }

  /**
   * Creates a block from an existing buffer starting with a header. Rewinds
   * and takes ownership of the buffer. By definition of rewind, ignores the
   * buffer position, but if you slice the buffer beforehand, it will rewind
   * to that point.
   * @param buf Has header, content, and trailing checksums if present.
   */
  static HFileBlock createFromBuff(ByteBuff buf, boolean usesHBaseChecksum, final long offset,
                                   final int nextBlockOnDiskSize, HFileContext fileContext, ByteBuffAllocator allocator)
      throws IOException {
    buf.rewind();
    final BlockType blockType = BlockType.read(buf);
    final int onDiskSizeWithoutHeader = buf.getInt(Header.ON_DISK_SIZE_WITHOUT_HEADER_INDEX);
    final int uncompressedSizeWithoutHeader =
        buf.getInt(Header.UNCOMPRESSED_SIZE_WITHOUT_HEADER_INDEX);
    final long prevBlockOffset = buf.getLong(Header.PREV_BLOCK_OFFSET_INDEX);
    // This constructor is called when we deserialize a block from cache and when we read a block in
    // from the fs. fileCache is null when deserialized from cache so need to make up one.
    HFileContextBuilder fileContextBuilder = fileContext != null ?
        new HFileContextBuilder(fileContext) : new HFileContextBuilder();
    fileContextBuilder.withHBaseCheckSum(usesHBaseChecksum);
    int onDiskDataSizeWithHeader;
    if (usesHBaseChecksum) {
      byte checksumType = buf.get(Header.CHECKSUM_TYPE_INDEX);
      int bytesPerChecksum = buf.getInt(Header.BYTES_PER_CHECKSUM_INDEX);
      onDiskDataSizeWithHeader = buf.getInt(Header.ON_DISK_DATA_SIZE_WITH_HEADER_INDEX);
      // Use the checksum type and bytes per checksum from header, not from fileContext.
      fileContextBuilder.withChecksumType(ChecksumType.codeToType(checksumType));
      fileContextBuilder.withBytesPerCheckSum(bytesPerChecksum);
    } else {
      fileContextBuilder.withChecksumType(ChecksumType.NULL);
      fileContextBuilder.withBytesPerCheckSum(0);
      // Need to fix onDiskDataSizeWithHeader; there are not checksums after-block-data
      onDiskDataSizeWithHeader = onDiskSizeWithoutHeader + headerSize(usesHBaseChecksum);
    }
    fileContext = fileContextBuilder.build();
    assert usesHBaseChecksum == fileContext.isUseHBaseChecksum();
    return new HFileBlockBuilder()
        .withBlockType(blockType)
        .withOnDiskSizeWithoutHeader(onDiskSizeWithoutHeader)
        .withUncompressedSizeWithoutHeader(uncompressedSizeWithoutHeader)
        .withPrevBlockOffset(prevBlockOffset)
        .withOffset(offset)
        .withOnDiskDataSizeWithHeader(onDiskDataSizeWithHeader)
        .withNextBlockOnDiskSize(nextBlockOnDiskSize)
        .withHFileContext(fileContext)
        .withByteBuffAllocator(allocator)
        .withByteBuff(buf.rewind())
        .withShared(!buf.hasArray())
        .build();
  }

  /**
   * Parse total on disk size including header and checksum.
   * @param headerBuf Header ByteBuffer. Presumed exact size of header.
   * @param verifyChecksum true if checksum verification is in use.
   * @return Size of the block with header included.
   */
  private static int getOnDiskSizeWithHeader(final ByteBuff headerBuf,
                                             boolean verifyChecksum) {
    return headerBuf.getInt(Header.ON_DISK_SIZE_WITHOUT_HEADER_INDEX) + headerSize(verifyChecksum);
  }

  /**
   * @return the on-disk size of the next block (including the header size and any checksums if
   *   present) read by peeking into the next block's header; use as a hint when doing
   *   a read of the next block when scanning or running over a file.
   */
  int getNextBlockOnDiskSize() {
    return nextBlockOnDiskSize;
  }

  @Override
  public BlockType getBlockType() {
    return blockType;
  }

  @Override
  public int refCnt() {
    return buf.refCnt();
  }

  @Override
  public HFileBlock retain() {
    buf.retain();
    return this;
  }

  /**
   * Call {@link ByteBuff#release()} to decrease the reference count, if no other reference, it will
   * return back the {@link ByteBuffer} to {@link org.apache.hadoop.hbase.io.ByteBuffAllocator}
   */
  @Override
  public boolean release() {
    return buf.release();
  }

  /** @return get data block encoding id that was used to encode this block */
  short getDataBlockEncodingId() {
    if (blockType != BlockType.ENCODED_DATA) {
      throw new IllegalArgumentException("Querying encoder ID of a block " +
          "of type other than " + BlockType.ENCODED_DATA + ": " + blockType);
    }
    return buf.getShort(headerSize());
  }

  /**
   * @return the on-disk size of header + data part + checksum.
   */
  public int getOnDiskSizeWithHeader() {
    return onDiskSizeWithoutHeader + headerSize();
  }

  /**
   * @return the on-disk size of the data part + checksum (header excluded).
   */
  int getOnDiskSizeWithoutHeader() {
    return onDiskSizeWithoutHeader;
  }

  /**
   * @return the uncompressed size of data part (header and checksum excluded).
   */
  int getUncompressedSizeWithoutHeader() {
    return uncompressedSizeWithoutHeader;
  }

  /**
   * @return the offset of the previous block of the same type in the file, or
   *         -1 if unknown
   */
  long getPrevBlockOffset() {
    return prevBlockOffset;
  }

  /**
   * Rewinds {@code buf} and writes first 4 header fields. {@code buf} position
   * is modified as side-effect.
   */
  private void overwriteHeader() {
    buf.rewind();
    blockType.write(buf);
    buf.putInt(onDiskSizeWithoutHeader);
    buf.putInt(uncompressedSizeWithoutHeader);
    buf.putLong(prevBlockOffset);
    if (this.fileContext.isUseHBaseChecksum()) {
      buf.put(fileContext.getChecksumType().getCode());
      buf.putInt(fileContext.getBytesPerChecksum());
      buf.putInt(onDiskDataSizeWithHeader);
    }
  }

  /**
   * Returns a buffer that does not include the header and checksum.
   * @return the buffer with header skipped and checksum omitted.
   */
  public ByteBuff getBufferWithoutHeader() {
    return this.getBufferWithoutHeader(false);
  }

  /**
   * Returns a buffer that does not include the header or checksum.
   * @param withChecksum to indicate whether include the checksum or not.
   * @return the buffer with header skipped and checksum omitted.
   */
  public ByteBuff getBufferWithoutHeader(boolean withChecksum) {
    ByteBuff dup = getBufferReadOnly();
    int delta = withChecksum ? 0 : totalChecksumBytes();
    return dup.position(headerSize()).limit(buf.limit() - delta).slice();
  }

  /**
   * Returns a read-only duplicate of the buffer this block stores internally ready to be read.
   * Clients must not modify the buffer object though they may set position and limit on the
   * returned buffer since we pass back a duplicate. This method has to be public because it is used
   * in {@link CompoundBloomFilter} to avoid object creation on every Bloom
   * filter lookup, but has to be used with caution. Buffer holds header, block content,
   * and any follow-on checksums if present.
   *
   * @return the buffer of this block for read-only operations
   */
  public ByteBuff getBufferReadOnly() {
    // TODO: ByteBuf does not support asReadOnlyBuffer(). Fix.
    ByteBuff dup = this.buf.duplicate();
    assert dup.position() == 0;
    return dup;
  }

  public ByteBuffAllocator getByteBuffAllocator() {
    return this.allocator;
  }

  private void sanityCheckAssertion(long valueFromBuf, long valueFromField,
                                    String fieldName) throws IOException {
    if (valueFromBuf != valueFromField) {
      throw new AssertionError(fieldName + " in the buffer (" + valueFromBuf
          + ") is different from that in the field (" + valueFromField + ")");
    }
  }

  private void sanityCheckAssertion(BlockType valueFromBuf, BlockType valueFromField)
      throws IOException {
    if (valueFromBuf != valueFromField) {
      throw new IOException("Block type stored in the buffer: " +
          valueFromBuf + ", block type field: " + valueFromField);
    }
  }

  /**
   * Checks if the block is internally consistent, i.e. the first
   * {@link HConstants#HFILEBLOCK_HEADER_SIZE} bytes of the buffer contain a
   * valid header consistent with the fields. Assumes a packed block structure.
   * This function is primary for testing and debugging, and is not
   * thread-safe, because it alters the internal buffer pointer.
   * Used by tests only.
   */
  void sanityCheck() throws IOException {
    // Duplicate so no side-effects
    ByteBuff dup = this.buf.duplicate().rewind();
    sanityCheckAssertion(BlockType.read(dup), blockType);

    sanityCheckAssertion(dup.getInt(), onDiskSizeWithoutHeader, "onDiskSizeWithoutHeader");

    sanityCheckAssertion(dup.getInt(), uncompressedSizeWithoutHeader,
        "uncompressedSizeWithoutHeader");

    sanityCheckAssertion(dup.getLong(), prevBlockOffset, "prevBlockOffset");
    if (this.fileContext.isUseHBaseChecksum()) {
      sanityCheckAssertion(dup.get(), this.fileContext.getChecksumType().getCode(), "checksumType");
      sanityCheckAssertion(dup.getInt(), this.fileContext.getBytesPerChecksum(),
          "bytesPerChecksum");
      sanityCheckAssertion(dup.getInt(), onDiskDataSizeWithHeader, "onDiskDataSizeWithHeader");
    }

    int cksumBytes = totalChecksumBytes();
    int expectedBufLimit = onDiskDataSizeWithHeader + cksumBytes;
    if (dup.limit() != expectedBufLimit) {
      throw new AssertionError("Expected limit " + expectedBufLimit + ", got " + dup.limit());
    }

    // We might optionally allocate HFILEBLOCK_HEADER_SIZE more bytes to read the next
    // block's header, so there are two sensible values for buffer capacity.
    int hdrSize = headerSize();
    dup.rewind();
    if (dup.remaining() != expectedBufLimit && dup.remaining() != expectedBufLimit + hdrSize) {
      throw new AssertionError("Invalid buffer capacity: " + dup.remaining() +
          ", expected " + expectedBufLimit + " or " + (expectedBufLimit + hdrSize));
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder()
        .append("[")
        .append("blockType=").append(blockType)
        .append(", fileOffset=").append(offset)
        .append(", headerSize=").append(headerSize())
        .append(", onDiskSizeWithoutHeader=").append(onDiskSizeWithoutHeader)
        .append(", uncompressedSizeWithoutHeader=").append(uncompressedSizeWithoutHeader)
        .append(", prevBlockOffset=").append(prevBlockOffset)
        .append(", isUseHBaseChecksum=").append(fileContext.isUseHBaseChecksum());
    if (fileContext.isUseHBaseChecksum()) {
      sb.append(", checksumType=").append(ChecksumType.codeToType(this.buf.get(24)))
          .append(", bytesPerChecksum=").append(this.buf.getInt(24 + 1))
          .append(", onDiskDataSizeWithHeader=").append(onDiskDataSizeWithHeader);
    } else {
      sb.append(", onDiskDataSizeWithHeader=").append(onDiskDataSizeWithHeader)
          .append("(").append(onDiskSizeWithoutHeader)
          .append("+").append(HConstants.HFILEBLOCK_HEADER_SIZE_NO_CHECKSUM).append(")");
    }
    String dataBegin;
    if (buf.hasArray()) {
      dataBegin = Bytes.toStringBinary(buf.array(), buf.arrayOffset() + headerSize(),
          Math.min(32, buf.limit() - buf.arrayOffset() - headerSize()));
    } else {
      ByteBuff bufWithoutHeader = getBufferWithoutHeader();
      byte[] dataBeginBytes = new byte[Math.min(32,
          bufWithoutHeader.limit() - bufWithoutHeader.position())];
      bufWithoutHeader.get(dataBeginBytes);
      dataBegin = Bytes.toStringBinary(dataBeginBytes);
    }
    sb.append(", getOnDiskSizeWithHeader=").append(getOnDiskSizeWithHeader())
        .append(", totalChecksumBytes=").append(totalChecksumBytes())
        .append(", isUnpacked=").append(isUnpacked())
        .append(", buf=[").append(buf).append("]")
        .append(", dataBeginsWith=").append(dataBegin)
        .append(", fileContext=").append(fileContext)
        .append(", nextBlockOnDiskSize=").append(nextBlockOnDiskSize)
        .append("]");
    return sb.toString();
  }

  /**
   * Retrieves the decompressed/decrypted view of this block. An encoded block remains in its
   * encoded structure. Internal structures are shared between instances where applicable.
   */
  HFileBlock unpack(HFileContext fileContext, FSReader reader) throws IOException {
    if (!fileContext.isCompressedOrEncrypted()) {
      // TODO: cannot use our own fileContext here because HFileBlock(ByteBuffer, boolean),
      // which is used for block serialization to L2 cache, does not preserve encoding and
      // encryption details.
      return this;
    }

    HFileBlock unpacked = shallowClone(this);
    unpacked.allocateBuffer(); // allocates space for the decompressed block
    boolean succ = false;
    try {
      HFileBlockDecodingContext ctx = blockType == BlockType.ENCODED_DATA
          ? reader.getBlockDecodingContext() : reader.getDefaultBlockDecodingContext();
      // Create a duplicated buffer without the header part.
      ByteBuff dup = this.buf.duplicate();
      dup.position(this.headerSize());
      dup = dup.slice();
      // Decode the dup into unpacked#buf
      ctx.prepareDecoding(unpacked.getOnDiskSizeWithoutHeader(),
          unpacked.getUncompressedSizeWithoutHeader(), unpacked.getBufferWithoutHeader(true), dup);
      succ = true;
      return unpacked;
    } finally {
      if (!succ) {
        unpacked.release();
      }
    }
  }

  /**
   * Always allocates a new buffer of the correct size. Copies header bytes
   * from the existing buffer. Does not change header fields.
   * Reserve room to keep checksum bytes too.
   */
  private void allocateBuffer() {
    int cksumBytes = totalChecksumBytes();
    int headerSize = headerSize();
    int capacityNeeded = headerSize + uncompressedSizeWithoutHeader + cksumBytes;

    ByteBuff newBuf = allocator.allocate(capacityNeeded);

    // Copy header bytes into newBuf.
    buf.position(0);
    newBuf.put(0, buf, 0, headerSize);

    buf = newBuf;
    // set limit to exclude next block's header
    buf.limit(capacityNeeded);
  }

  /**
   * Return true when this block's buffer has been unpacked, false otherwise. Note this is a
   * calculated heuristic, not tracked attribute of the block.
   */
  public boolean isUnpacked() {
    final int cksumBytes = totalChecksumBytes();
    final int headerSize = headerSize();
    final int expectedCapacity = headerSize + uncompressedSizeWithoutHeader + cksumBytes;
    final int bufCapacity = buf.remaining();
    return bufCapacity == expectedCapacity || bufCapacity == expectedCapacity + headerSize;
  }

  /**
   * Cannot be {@link #UNSET}. Must be a legitimate value. Used re-making the {@link BlockCacheKey}
   * when block is returned to the cache.
   * @return the offset of this block in the file it was read from
   */
  long getOffset() {
    if (offset < 0) {
      throw new IllegalStateException("HFile block offset not initialized properly");
    }
    return offset;
  }

  /**
   * @return a byte stream reading the data + checksum of this block
   */
  DataInputStream getByteStream() {
    ByteBuff dup = this.buf.duplicate();
    dup.position(this.headerSize());
    return new DataInputStream(new ByteBuffInputStream(dup));
  }

  @Override
  public long heapSize() {
    long size = FIXED_OVERHEAD;
    size += fileContext.heapSize();
    if (buf != null) {
      // Deep overhead of the byte buffer. Needs to be aligned separately.
      size += ClassSize.align(buf.capacity() + MULTI_BYTE_BUFFER_HEAP_SIZE);
    }
    return ClassSize.align(size);
  }

  /**
   * Will be override by {@link SharedMemHFileBlock} or {@link ExclusiveMemHFileBlock}. Return true
   * by default.
   */
  public boolean isSharedMem() {
    if (this instanceof SharedMemHFileBlock) {
      return true;
    } else if (this instanceof ExclusiveMemHFileBlock) {
      return false;
    }
    return true;
  }

  /**
   * Unified version 2 {@link HFile} block writer. The intended usage pattern
   * is as follows:
   * <ol>
   * <li>Construct an {@link HFileBlock.Writer}, providing a compression algorithm.
   * <li>Call {@link Writer#startWriting} and get a data stream to write to.
   * <li>Write your data into the stream.
   * <li>Call Writer#writeHeaderAndData(FSDataOutputStream) as many times as you need to.
   * store the serialized block into an external stream.
   * <li>Repeat to write more blocks.
   * </ol>
   * <p>
   */
  static class Writer implements ShipperListener {
    private enum State {
      INIT,
      WRITING,
      BLOCK_READY
    };

    /** Writer state. Used to ensure the correct usage protocol. */
    private State state = State.INIT;

    /** Data block encoder used for data blocks */
    private final HFileDataBlockEncoder dataBlockEncoder;

    private HFileBlockEncodingContext dataBlockEncodingCtx;

    /** block encoding context for non-data blocks*/
    private HFileBlockDefaultEncodingContext defaultBlockEncodingCtx;

    /**
     * The stream we use to accumulate data into a block in an uncompressed format.
     * We reset this stream at the end of each block and reuse it. The
     * header is written as the first {@link HConstants#HFILEBLOCK_HEADER_SIZE} bytes into this
     * stream.
     */
    private ByteArrayOutputStream baosInMemory;

    /**
     * Current block type. Set in {@link #startWriting(BlockType)}. Could be
     * changed in {@link #finishBlock()} from {@link BlockType#DATA}
     * to {@link BlockType#ENCODED_DATA}.
     */
    private BlockType blockType;

    /**
     * A stream that we write uncompressed bytes to, which compresses them and
     * writes them to {@link #baosInMemory}.
     */
    private DataOutputStream userDataStream;

    /**
     * Bytes to be written to the file system, including the header. Compressed
     * if compression is turned on. It also includes the checksum data that
     * immediately follows the block data. (header + data + checksums)
     */
    private ByteArrayOutputStream onDiskBlockBytesWithHeader;

    /**
     * The size of the checksum data on disk. It is used only if data is
     * not compressed. If data is compressed, then the checksums are already
     * part of onDiskBytesWithHeader. If data is uncompressed, then this
     * variable stores the checksum data for this block.
     */
    private byte[] onDiskChecksum = HConstants.EMPTY_BYTE_ARRAY;

    /**
     * Current block's start offset in the {@link HFile}. Set in
     * {@link #writeHeaderAndData(FSDataOutputStream)}.
     */
    private long startOffset;

    /**
     * Offset of previous block by block type. Updated when the next block is
     * started.
     */
    private long[] prevOffsetByType;

    /** The offset of the previous block of the same type */
    private long prevOffset;
    /** Meta data that holds information about the hfileblock**/
    private HFileContext fileContext;

    private final ByteBuffAllocator allocator;

    @Override
    public void beforeShipped() {
      if (getEncodingState() != null) {
        getEncodingState().beforeShipped();
      }
    }

    EncodingState getEncodingState() {
      return dataBlockEncodingCtx.getEncodingState();
    }

    /**
     * @param dataBlockEncoder data block encoding algorithm to use
     */
    public Writer(HFileDataBlockEncoder dataBlockEncoder, HFileContext fileContext) {
      this(dataBlockEncoder, fileContext, ByteBuffAllocator.HEAP);
    }

    public Writer(HFileDataBlockEncoder dataBlockEncoder, HFileContext fileContext,
                  ByteBuffAllocator allocator) {
      if (fileContext.getBytesPerChecksum() < HConstants.HFILEBLOCK_HEADER_SIZE) {
        throw new RuntimeException("Unsupported value of bytesPerChecksum. " +
            " Minimum is " + HConstants.HFILEBLOCK_HEADER_SIZE + " but the configured value is " +
            fileContext.getBytesPerChecksum());
      }
      this.allocator = allocator;
      this.dataBlockEncoder = dataBlockEncoder != null?
          dataBlockEncoder: NoOpDataBlockEncoder.INSTANCE;
      this.dataBlockEncodingCtx = this.dataBlockEncoder.
          newDataBlockEncodingContext(HConstants.HFILEBLOCK_DUMMY_HEADER, fileContext);
      // TODO: This should be lazily instantiated since we usually do NOT need this default encoder
      this.defaultBlockEncodingCtx = new HFileBlockDefaultEncodingContext(null,
          HConstants.HFILEBLOCK_DUMMY_HEADER, fileContext);
      // TODO: Set BAOS initial size. Use fileContext.getBlocksize() and add for header/checksum
      baosInMemory = new ByteArrayOutputStream();
      prevOffsetByType = new long[BlockType.values().length];
      for (int i = 0; i < prevOffsetByType.length; ++i) {
        prevOffsetByType[i] = UNSET;
      }
      // TODO: Why fileContext saved away when we have dataBlockEncoder and/or
      // defaultDataBlockEncoder?
      this.fileContext = fileContext;
    }

    /**
     * Starts writing into the block. The previous block's data is discarded.
     *
     * @return the stream the user can write their data into
     */
    DataOutputStream startWriting(BlockType newBlockType)
        throws IOException {
      if (state == State.BLOCK_READY && startOffset != -1) {
        // We had a previous block that was written to a stream at a specific
        // offset. Save that offset as the last offset of a block of that type.
        prevOffsetByType[blockType.getId()] = startOffset;
      }

      startOffset = -1;
      blockType = newBlockType;

      baosInMemory.reset();
      baosInMemory.write(HConstants.HFILEBLOCK_DUMMY_HEADER);

      state = State.WRITING;

      // We will compress it later in finishBlock()
      userDataStream = new ByteBufferWriterDataOutputStream(baosInMemory);
      if (newBlockType == BlockType.DATA) {
        this.dataBlockEncoder.startBlockEncoding(dataBlockEncodingCtx, userDataStream);
      }
      return userDataStream;
    }

    /**
     * Writes the Cell to this block
     */
    void write(Cell cell) throws IOException{
      expectState(State.WRITING);
      this.dataBlockEncoder.encode(cell, dataBlockEncodingCtx, this.userDataStream);
    }

    /**
     * Transitions the block writer from the "writing" state to the "block
     * ready" state.  Does nothing if a block is already finished.
     */
    void ensureBlockReady() throws IOException {
      Preconditions.checkState(state != State.INIT,
          "Unexpected state: " + state);

      if (state == State.BLOCK_READY) {
        return;
      }

      // This will set state to BLOCK_READY.
      finishBlock();
    }

    /**
     * Finish up writing of the block.
     * Flushes the compressing stream (if using compression), fills out the header,
     * does any compression/encryption of bytes to flush out to disk, and manages
     * the cache on write content, if applicable. Sets block write state to "block ready".
     */
    private void finishBlock() throws IOException {
      if (blockType == BlockType.DATA) {
        this.dataBlockEncoder.endBlockEncoding(dataBlockEncodingCtx, userDataStream,
            baosInMemory.getBuffer(), blockType);
        blockType = dataBlockEncodingCtx.getBlockType();
      }
      userDataStream.flush();
      prevOffset = prevOffsetByType[blockType.getId()];

      // We need to set state before we can package the block up for cache-on-write. In a way, the
      // block is ready, but not yet encoded or compressed.
      state = State.BLOCK_READY;
      Bytes compressAndEncryptDat;
      if (blockType == BlockType.DATA || blockType == BlockType.ENCODED_DATA) {
        compressAndEncryptDat = dataBlockEncodingCtx.
            compressAndEncrypt(baosInMemory.getBuffer(), 0, baosInMemory.size());
      } else {
        compressAndEncryptDat = defaultBlockEncodingCtx.
            compressAndEncrypt(baosInMemory.getBuffer(), 0, baosInMemory.size());
      }
      if (compressAndEncryptDat == null) {
        compressAndEncryptDat = new Bytes(baosInMemory.getBuffer(), 0, baosInMemory.size());
      }
      if (onDiskBlockBytesWithHeader == null) {
        onDiskBlockBytesWithHeader = new ByteArrayOutputStream(compressAndEncryptDat.getLength());
      }
      onDiskBlockBytesWithHeader.reset();
      onDiskBlockBytesWithHeader.write(compressAndEncryptDat.get(),
          compressAndEncryptDat.getOffset(), compressAndEncryptDat.getLength());
      // Calculate how many bytes we need for checksum on the tail of the block.
      int numBytes = (int) ChecksumUtil.numBytes(
          onDiskBlockBytesWithHeader.size(),
          fileContext.getBytesPerChecksum());

      // Put the header for the on disk bytes; header currently is unfilled-out
      putHeader(onDiskBlockBytesWithHeader,
          onDiskBlockBytesWithHeader.size() + numBytes,
          baosInMemory.size(), onDiskBlockBytesWithHeader.size());
      if (onDiskChecksum.length != numBytes) {
        onDiskChecksum = new byte[numBytes];
      }
      ChecksumUtil.generateChecksums(
          onDiskBlockBytesWithHeader.getBuffer(), 0,onDiskBlockBytesWithHeader.size(),
          onDiskChecksum, 0, fileContext.getChecksumType(), fileContext.getBytesPerChecksum());
    }

    /**
     * Put the header into the given byte array at the given offset.
     * @param onDiskSize size of the block on disk header + data + checksum
     * @param uncompressedSize size of the block after decompression (but
     *          before optional data block decoding) including header
     * @param onDiskDataSize size of the block on disk with header
     *        and data but not including the checksums
     */
    private void putHeader(byte[] dest, int offset, int onDiskSize,
                           int uncompressedSize, int onDiskDataSize) {
      offset = blockType.put(dest, offset);
      offset = Bytes.putInt(dest, offset, onDiskSize - HConstants.HFILEBLOCK_HEADER_SIZE);
      offset = Bytes.putInt(dest, offset, uncompressedSize - HConstants.HFILEBLOCK_HEADER_SIZE);
      offset = Bytes.putLong(dest, offset, prevOffset);
      offset = Bytes.putByte(dest, offset, fileContext.getChecksumType().getCode());
      offset = Bytes.putInt(dest, offset, fileContext.getBytesPerChecksum());
      Bytes.putInt(dest, offset, onDiskDataSize);
    }

    private void putHeader(ByteBuff buff, int onDiskSize,
                           int uncompressedSize, int onDiskDataSize) {
      buff.rewind();
      blockType.write(buff);
      buff.putInt(onDiskSize - HConstants.HFILEBLOCK_HEADER_SIZE);
      buff.putInt(uncompressedSize - HConstants.HFILEBLOCK_HEADER_SIZE);
      buff.putLong(prevOffset);
      buff.put(fileContext.getChecksumType().getCode());
      buff.putInt(fileContext.getBytesPerChecksum());
      buff.putInt(onDiskDataSize);
    }

    private void putHeader(ByteArrayOutputStream dest, int onDiskSize,
                           int uncompressedSize, int onDiskDataSize) {
      putHeader(dest.getBuffer(),0, onDiskSize, uncompressedSize, onDiskDataSize);
    }

    /**
     * Similar to {@link #writeHeaderAndData(FSDataOutputStream)}, but records
     * the offset of this block so that it can be referenced in the next block
     * of the same type.
     */
    void writeHeaderAndData(FSDataOutputStream out) throws IOException {
      long offset = out.getPos();
      if (startOffset != UNSET && offset != startOffset) {
        throw new IOException("A " + blockType + " block written to a "
            + "stream twice, first at offset " + startOffset + ", then at "
            + offset);
      }
      startOffset = offset;
      finishBlockAndWriteHeaderAndData(out);
    }

    /**
     * Writes the header and the compressed data of this block (or uncompressed
     * data when not using compression) into the given stream. Can be called in
     * the "writing" state or in the "block ready" state. If called in the
     * "writing" state, transitions the writer to the "block ready" state.
     * @param out the output stream to write the
     */
    protected void finishBlockAndWriteHeaderAndData(DataOutputStream out)
        throws IOException {
      ensureBlockReady();
      long startTime = System.currentTimeMillis();
      out.write(onDiskBlockBytesWithHeader.getBuffer(), 0, onDiskBlockBytesWithHeader.size());
      out.write(onDiskChecksum);
      HFile.updateWriteLatency(System.currentTimeMillis() - startTime);
    }

    /**
     * Returns the header or the compressed data (or uncompressed data when not
     * using compression) as a byte array. Can be called in the "writing" state
     * or in the "block ready" state. If called in the "writing" state,
     * transitions the writer to the "block ready" state. This returns
     * the header + data + checksums stored on disk.
     *
     * @return header and data as they would be stored on disk in a byte array
     */
    byte[] getHeaderAndDataForTest() throws IOException {
      ensureBlockReady();
      // This is not very optimal, because we are doing an extra copy.
      // But this method is used only by unit tests.
      byte[] output =
          new byte[onDiskBlockBytesWithHeader.size()
              + onDiskChecksum.length];
      System.arraycopy(onDiskBlockBytesWithHeader.getBuffer(), 0, output, 0,
          onDiskBlockBytesWithHeader.size());
      System.arraycopy(onDiskChecksum, 0, output,
          onDiskBlockBytesWithHeader.size(), onDiskChecksum.length);
      return output;
    }

    /**
     * Releases resources used by this writer.
     */
    void release() {
      if (dataBlockEncodingCtx != null) {
        dataBlockEncodingCtx.close();
        dataBlockEncodingCtx = null;
      }
      if (defaultBlockEncodingCtx != null) {
        defaultBlockEncodingCtx.close();
        defaultBlockEncodingCtx = null;
      }
    }

    /**
     * Returns the on-disk size of the data portion of the block. This is the
     * compressed size if compression is enabled. Can only be called in the
     * "block ready" state. Header is not compressed, and its size is not
     * included in the return value.
     *
     * @return the on-disk size of the block, not including the header.
     */
    int getOnDiskSizeWithoutHeader() {
      expectState(State.BLOCK_READY);
      return onDiskBlockBytesWithHeader.size() +
          onDiskChecksum.length - HConstants.HFILEBLOCK_HEADER_SIZE;
    }

    /**
     * Returns the on-disk size of the block. Can only be called in the
     * "block ready" state.
     *
     * @return the on-disk size of the block ready to be written, including the
     *         header size, the data and the checksum data.
     */
    int getOnDiskSizeWithHeader() {
      expectState(State.BLOCK_READY);
      return onDiskBlockBytesWithHeader.size() + onDiskChecksum.length;
    }

    /**
     * The uncompressed size of the block data. Does not include header size.
     */
    int getUncompressedSizeWithoutHeader() {
      expectState(State.BLOCK_READY);
      return baosInMemory.size() - HConstants.HFILEBLOCK_HEADER_SIZE;
    }

    /**
     * The uncompressed size of the block data, including header size.
     */
    int getUncompressedSizeWithHeader() {
      expectState(State.BLOCK_READY);
      return baosInMemory.size();
    }

    /** @return true if a block is being written  */
    boolean isWriting() {
      return state == State.WRITING;
    }

    /**
     * Returns the number of bytes written into the current block so far, or
     * zero if not writing the block at the moment. Note that this will return
     * zero in the "block ready" state as well.
     *
     * @return the number of bytes written
     */
    public int encodedBlockSizeWritten() {
      return state != State.WRITING ? 0 : this.getEncodingState().getEncodedDataSizeWritten();
    }

    /**
     * Returns the number of bytes written into the current block so far, or
     * zero if not writing the block at the moment. Note that this will return
     * zero in the "block ready" state as well.
     *
     * @return the number of bytes written
     */
    int blockSizeWritten() {
      return state != State.WRITING ? 0 : this.getEncodingState().getUnencodedDataSizeWritten();
    }

    /**
     * Clones the header followed by the uncompressed data, even if using
     * compression. This is needed for storing uncompressed blocks in the block
     * cache. Can be called in the "writing" state or the "block ready" state.
     * Returns only the header and data, does not include checksum data.
     *
     * @return Returns an uncompressed block ByteBuff for caching on write
     */
    ByteBuff cloneUncompressedBufferWithHeader() {
      expectState(State.BLOCK_READY);
      ByteBuff bytebuff = allocator.allocate(baosInMemory.size());
      baosInMemory.toByteBuff(bytebuff);
      int numBytes = (int) ChecksumUtil.numBytes(
          onDiskBlockBytesWithHeader.size(),
          fileContext.getBytesPerChecksum());
      putHeader(bytebuff, onDiskBlockBytesWithHeader.size() + numBytes,
          baosInMemory.size(), onDiskBlockBytesWithHeader.size());
      bytebuff.rewind();
      return bytebuff;
    }

    /**
     * Clones the header followed by the on-disk (compressed/encoded/encrypted) data. This is needed
     * for storing packed blocks in the block cache. Returns only the header and data, Does not
     * include checksum data.
     * @return Returns a copy of block bytes for caching on write
     */
    private ByteBuff cloneOnDiskBufferWithHeader() {
      expectState(State.BLOCK_READY);
      ByteBuff bytebuff = allocator.allocate(onDiskBlockBytesWithHeader.size());
      onDiskBlockBytesWithHeader.toByteBuff(bytebuff);
      bytebuff.rewind();
      return bytebuff;
    }

    private void expectState(State expectedState) {
      if (state != expectedState) {
        throw new IllegalStateException("Expected state: " + expectedState +
            ", actual state: " + state);
      }
    }

    /**
     * Takes the given {@link BlockWritable} instance, creates a new block of
     * its appropriate type, writes the writable into this block, and flushes
     * the block into the output stream. The writer is instructed not to buffer
     * uncompressed bytes for cache-on-write.
     *
     * @param bw the block-writable object to write as a block
     * @param out the file system output stream
     */
    void writeBlock(BlockWritable bw, FSDataOutputStream out)
        throws IOException {
      bw.writeToBlock(startWriting(bw.getBlockType()));
      writeHeaderAndData(out);
    }

    /**
     * Creates a new HFileBlock. Checksums have already been validated, so
     * the byte buffer passed into the constructor of this newly created
     * block does not have checksum data even though the header minor
     * version is MINOR_VERSION_WITH_CHECKSUM. This is indicated by setting a
     * 0 value in bytesPerChecksum. This method copies the on-disk or
     * uncompressed data to build the HFileBlock which is used only
     * while writing blocks and caching.
     *
     * <p>TODO: Should there be an option where a cache can ask that hbase preserve block
     * checksums for checking after a block comes out of the cache? Otehrwise, cache is responsible
     * for blocks being wholesome (ECC memory or if file-backed, it does checksumming).
     */
    HFileBlock getBlockForCaching(CacheConfig cacheConf) {
      HFileContext newContext = new HFileContextBuilder()
          .withBlockSize(fileContext.getBlocksize())
          .withBytesPerCheckSum(0)
          .withChecksumType(ChecksumType.NULL) // no checksums in cached data
          .withCompression(fileContext.getCompression())
          .withDataBlockEncoding(fileContext.getDataBlockEncoding())
          .withHBaseCheckSum(fileContext.isUseHBaseChecksum())
          .withCompressTags(fileContext.isCompressTags())
          .withIncludesMvcc(fileContext.isIncludesMvcc())
          .withIncludesTags(fileContext.isIncludesTags())
          .withColumnFamily(fileContext.getColumnFamily())
          .withTableName(fileContext.getTableName())
          .build();
      // Build the HFileBlock.
      HFileBlockBuilder builder = new HFileBlockBuilder();
      ByteBuff buff;
      if (cacheConf.shouldCacheCompressed(blockType.getCategory())) {
        buff = cloneOnDiskBufferWithHeader();
      } else {
        buff = cloneUncompressedBufferWithHeader();
      }
      return builder.withBlockType(blockType)
          .withOnDiskSizeWithoutHeader(getOnDiskSizeWithoutHeader())
          .withUncompressedSizeWithoutHeader(getUncompressedSizeWithoutHeader())
          .withPrevBlockOffset(prevOffset)
          .withByteBuff(buff)
          .withFillHeader(FILL_HEADER)
          .withOffset(startOffset)
          .withNextBlockOnDiskSize(UNSET)
          .withOnDiskDataSizeWithHeader(onDiskBlockBytesWithHeader.size() + onDiskChecksum.length)
          .withHFileContext(newContext)
          .withByteBuffAllocator(cacheConf.getByteBuffAllocator())
          .withShared(!buff.hasArray())
          .build();
    }
  }

  /** Something that can be written into a block. */
  interface BlockWritable {
    /** The type of block this data should use. */
    BlockType getBlockType();

    /**
     * Writes the block to the provided stream. Must not write any magic
     * records.
     *
     * @param out a stream to write uncompressed data into
     */
    void writeToBlock(DataOutput out) throws IOException;
  }

  /**
   * Iterator for reading {@link HFileBlock}s in load-on-open-section, such as root data index
   * block, meta index block, file info block etc.
   */
  interface BlockIterator {
    /**
     * Get the next block, or null if there are no more blocks to iterate.
     */
    HFileBlock nextBlock() throws IOException;

    /**
     * Similar to {@link #nextBlock()} but checks block type, throws an exception if incorrect, and
     * returns the HFile block
     */
    HFileBlock nextBlockWithBlockType(BlockType blockType) throws IOException;

    /**
     * Now we use the {@link ByteBuffAllocator} to manage the nio ByteBuffers for HFileBlocks, so we
     * must deallocate all of the ByteBuffers in the end life. the BlockIterator's life cycle is
     * starting from opening an HFileReader and stopped when the HFileReader#close, so we will keep
     * track all the read blocks until we call {@link BlockIterator#freeBlocks()} when closing the
     * HFileReader. Sum bytes of those blocks in load-on-open section should be quite small, so
     * tracking them should be OK.
     */
    void freeBlocks();
  }

  /** An HFile block reader with iteration ability. */
  interface FSReader {
    /**
     * Reads the block at the given offset in the file with the given on-disk size and uncompressed
     * size.
     * @param offset of the file to read
     * @param onDiskSize the on-disk size of the entire block, including all applicable headers, or
     *          -1 if unknown
     * @param pread true to use pread, otherwise use the stream read.
     * @param updateMetrics update the metrics or not.
     * @param intoHeap allocate the block's ByteBuff by {@link ByteBuffAllocator} or JVM heap. For
     *          LRUBlockCache, we must ensure that the block to cache is an heap one, because the
     *          memory occupation is based on heap now, also for {@link CombinedBlockCache}, we use
     *          the heap LRUBlockCache as L1 cache to cache small blocks such as IndexBlock or
     *          MetaBlock for faster access. So introduce an flag here to decide whether allocate
     *          from JVM heap or not so that we can avoid an extra off-heap to heap memory copy when
     *          using LRUBlockCache. For most cases, we known what's the expected block type we'll
     *          read, while for some special case (Example: HFileReaderImpl#readNextDataBlock()), we
     *          cannot pre-decide what's the expected block type, then we can only allocate block's
     *          ByteBuff from {@link ByteBuffAllocator} firstly, and then when caching it in
     *          {@link LruBlockCache} we'll check whether the ByteBuff is from heap or not, if not
     *          then we'll clone it to an heap one and cache it.
     * @return the newly read block
     */
    HFileBlock readBlockData(long offset, long onDiskSize, boolean pread, boolean updateMetrics,
                             boolean intoHeap) throws IOException;

    /**
     * Creates a block iterator over the given portion of the {@link HFile}.
     * The iterator returns blocks starting with offset such that offset &lt;=
     * startOffset &lt; endOffset. Returned blocks are always unpacked.
     * Used when no hfile index available; e.g. reading in the hfile index
     * blocks themselves on file open.
     *
     * @param startOffset the offset of the block to start iteration with
     * @param endOffset the offset to end iteration at (exclusive)
     * @return an iterator of blocks between the two given offsets
     */
    BlockIterator blockRange(long startOffset, long endOffset);

    /** Closes the backing streams */
    void closeStreams() throws IOException;

    /** Get a decoder for {@link BlockType#ENCODED_DATA} blocks from this file. */
    HFileBlockDecodingContext getBlockDecodingContext();

    /** Get the default decoder for blocks from this file. */
    HFileBlockDecodingContext getDefaultBlockDecodingContext();

    void setIncludesMemStoreTS(boolean includesMemstoreTS);
    void setDataBlockEncoder(HFileDataBlockEncoder encoder);

    /**
     * To close the stream's socket. Note: This can be concurrently called from multiple threads and
     * implementation should take care of thread safety.
     */
    void unbufferStream();
  }

  /**
   * Data-structure to use caching the header of the NEXT block. Only works if next read
   * that comes in here is next in sequence in this block.
   *
   * When we read, we read current block and the next blocks' header. We do this so we have
   * the length of the next block to read if the hfile index is not available (rare, at
   * hfile open only).
   */
  private static class PrefetchedHeader {
    long offset = -1;
    byte[] header = new byte[HConstants.HFILEBLOCK_HEADER_SIZE];
    final ByteBuff buf = new SingleByteBuff(ByteBuffer.wrap(header, 0, header.length));

    @Override
    public String toString() {
      return "offset=" + this.offset + ", header=" + Bytes.toStringBinary(header);
    }
  }

  /**
   * Reads version 2 HFile blocks from the filesystem.
   */
  static class FSReaderImpl implements FSReader {
    /** The file system stream of the underlying {@link HFile} that
     * does or doesn't do checksum validations in the filesystem */
    private FSDataInputStreamWrapper streamWrapper;

    private HFileBlockDecodingContext encodedBlockDecodingCtx;

    /** Default context used when BlockType != {@link BlockType#ENCODED_DATA}. */
    private final HFileBlockDefaultDecodingContext defaultDecodingCtx;

    /**
     * Cache of the NEXT header after this. Check it is indeed next blocks header
     * before using it. TODO: Review. This overread into next block to fetch
     * next blocks header seems unnecessary given we usually get the block size
     * from the hfile index. Review!
     */
    private AtomicReference<PrefetchedHeader> prefetchedHeader =
        new AtomicReference<>(new PrefetchedHeader());

    /** The size of the file we are reading from, or -1 if unknown. */
    private long fileSize;

    /** The size of the header */
    protected final int hdrSize;

    /** The filesystem used to access data */
    private HFileSystem hfs;

    private HFileContext fileContext;
    // Cache the fileName
    private String pathName;

    private final ByteBuffAllocator allocator;

    private final Lock streamLock = new ReentrantLock();

    FSReaderImpl(ReaderContext readerContext, HFileContext fileContext,
                 ByteBuffAllocator allocator) throws IOException {
      this.fileSize = readerContext.getFileSize();
      this.hfs = readerContext.getFileSystem();
      if (readerContext.getFilePath() != null) {
        this.pathName = readerContext.getFilePath().toString();
      }
      this.fileContext = fileContext;
      this.hdrSize = headerSize(fileContext.isUseHBaseChecksum());
      this.allocator = allocator;

      this.streamWrapper = readerContext.getInputStreamWrapper();
      // Older versions of HBase didn't support checksum.
      this.streamWrapper.prepareForBlockReader(!fileContext.isUseHBaseChecksum());
      defaultDecodingCtx = new HFileBlockDefaultDecodingContext(fileContext);
      encodedBlockDecodingCtx = defaultDecodingCtx;
    }

    @Override
    public BlockIterator blockRange(final long startOffset, final long endOffset) {
      final FSReader owner = this; // handle for inner class
      return new BlockIterator() {
        private volatile boolean freed = false;
        // Tracking all read blocks until we call freeBlocks.
        private List<HFileBlock> blockTracker = new ArrayList<>();
        private long offset = startOffset;
        // Cache length of next block. Current block has the length of next block in it.
        private long length = -1;

        @Override
        public HFileBlock nextBlock() throws IOException {
          if (offset >= endOffset) {
            return null;
          }
          HFileBlock b = readBlockData(offset, length, false, false, true);
          offset += b.getOnDiskSizeWithHeader();
          length = b.getNextBlockOnDiskSize();
          HFileBlock uncompressed = b.unpack(fileContext, owner);
          if (uncompressed != b) {
            b.release(); // Need to release the compressed Block now.
          }
          blockTracker.add(uncompressed);
          return uncompressed;
        }

        @Override
        public HFileBlock nextBlockWithBlockType(BlockType blockType) throws IOException {
          HFileBlock blk = nextBlock();
          if (blk.getBlockType() != blockType) {
            throw new IOException(
                "Expected block of type " + blockType + " but found " + blk.getBlockType());
          }
          return blk;
        }

        @Override
        public void freeBlocks() {
          if (freed) {
            return;
          }
          blockTracker.forEach(HFileBlock::release);
          blockTracker = null;
          freed = true;
        }
      };
    }

    /**
     * Does a positional read or a seek and read into the given byte buffer. We need take care that
     * we will call the {@link ByteBuff#release()} for every exit to deallocate the ByteBuffers,
     * otherwise the memory leak may happen.
     * @param dest destination buffer
     * @param size size of read
     * @param peekIntoNextBlock whether to read the next block's on-disk size
     * @param fileOffset position in the stream to read at
     * @param pread whether we should do a positional read
     * @param istream The input source of data
     * @return true to indicate the destination buffer include the next block header, otherwise only
     *         include the current block data without the next block header.
     * @throws IOException if any IO error happen.
     */
    protected boolean readAtOffset(FSDataInputStream istream, ByteBuff dest, int size,
                                   boolean peekIntoNextBlock, long fileOffset, boolean pread) throws IOException {
      if (!pread) {
        // Seek + read. Better for scanning.
        HFileUtil.seekOnMultipleSources(istream, fileOffset);
        long realOffset = istream.getPos();
        if (realOffset != fileOffset) {
          throw new IOException("Tried to seek to " + fileOffset + " to read " + size
              + " bytes, but pos=" + realOffset + " after seek");
        }
        if (!peekIntoNextBlock) {
          BlockIOUtils.readFully(dest, istream, size);
          return false;
        }

        // Try to read the next block header
        if (!BlockIOUtils.readWithExtra(dest, istream, size, hdrSize)) {
          // did not read the next block header.
          return false;
        }
      } else {
        // Positional read. Better for random reads; or when the streamLock is already locked.
        int extraSize = peekIntoNextBlock ? hdrSize : 0;
        if (!BlockIOUtils.preadWithExtra(dest, istream, fileOffset, size, extraSize)) {
          // did not read the next block header.
          return false;
        }
      }
      assert peekIntoNextBlock;
      return true;
    }

    /**
     * Reads a version 2 block (version 1 blocks not supported and not expected). Tries to do as
     * little memory allocation as possible, using the provided on-disk size.
     * @param offset the offset in the stream to read at
     * @param onDiskSizeWithHeaderL the on-disk size of the block, including the header, or -1 if
     *          unknown; i.e. when iterating over blocks reading in the file metadata info.
     * @param pread whether to use a positional read
     * @param updateMetrics whether to update the metrics
     * @param intoHeap allocate ByteBuff of block from heap or off-heap.
     * @see FSReader#readBlockData(long, long, boolean, boolean, boolean) for more details about the
     *      useHeap.
     */
    @Override
    public HFileBlock readBlockData(long offset, long onDiskSizeWithHeaderL, boolean pread,
                                    boolean updateMetrics, boolean intoHeap) throws IOException {
      // Get a copy of the current state of whether to validate
      // hbase checksums or not for this read call. This is not
      // thread-safe but the one constaint is that if we decide
      // to skip hbase checksum verification then we are
      // guaranteed to use hdfs checksum verification.
      boolean doVerificationThruHBaseChecksum = streamWrapper.shouldUseHBaseChecksum();
      FSDataInputStream is = streamWrapper.getStream(doVerificationThruHBaseChecksum);

      HFileBlock blk = readBlockDataInternal(is, offset, onDiskSizeWithHeaderL, pread,
          doVerificationThruHBaseChecksum, updateMetrics, intoHeap);
      if (blk == null) {
        HFile.LOG.warn("HBase checksum verification failed for file " +
            pathName + " at offset " +
            offset + " filesize " + fileSize +
            ". Retrying read with HDFS checksums turned on...");

        if (!doVerificationThruHBaseChecksum) {
          String msg = "HBase checksum verification failed for file " +
              pathName + " at offset " +
              offset + " filesize " + fileSize +
              " but this cannot happen because doVerify is " +
              doVerificationThruHBaseChecksum;
          HFile.LOG.warn(msg);
          throw new IOException(msg); // cannot happen case here
        }
        HFile.CHECKSUM_FAILURES.increment(); // update metrics

        // If we have a checksum failure, we fall back into a mode where
        // the next few reads use HDFS level checksums. We aim to make the
        // next CHECKSUM_VERIFICATION_NUM_IO_THRESHOLD reads avoid
        // hbase checksum verification, but since this value is set without
        // holding any locks, it can so happen that we might actually do
        // a few more than precisely this number.
        is = this.streamWrapper.fallbackToFsChecksum(CHECKSUM_VERIFICATION_NUM_IO_THRESHOLD);
        doVerificationThruHBaseChecksum = false;
        blk = readBlockDataInternal(is, offset, onDiskSizeWithHeaderL, pread,
            doVerificationThruHBaseChecksum, updateMetrics, intoHeap);
        if (blk != null) {
          HFile.LOG.warn("HDFS checksum verification succeeded for file " +
              pathName + " at offset " +
              offset + " filesize " + fileSize);
        }
      }
      if (blk == null && !doVerificationThruHBaseChecksum) {
        String msg = "readBlockData failed, possibly due to " +
            "checksum verification failed for file " + pathName +
            " at offset " + offset + " filesize " + fileSize;
        HFile.LOG.warn(msg);
        throw new IOException(msg);
      }

      // If there is a checksum mismatch earlier, then retry with
      // HBase checksums switched off and use HDFS checksum verification.
      // This triggers HDFS to detect and fix corrupt replicas. The
      // next checksumOffCount read requests will use HDFS checksums.
      // The decrementing of this.checksumOffCount is not thread-safe,
      // but it is harmless because eventually checksumOffCount will be
      // a negative number.
      streamWrapper.checksumOk();
      return blk;
    }

    /**
     * @return Check <code>onDiskSizeWithHeaderL</code> size is healthy and then return it as an int
     */
    private static int checkAndGetSizeAsInt(final long onDiskSizeWithHeaderL, final int hdrSize)
        throws IOException {
      if ((onDiskSizeWithHeaderL < hdrSize && onDiskSizeWithHeaderL != -1)
          || onDiskSizeWithHeaderL >= Integer.MAX_VALUE) {
        throw new IOException("Invalid onDisksize=" + onDiskSizeWithHeaderL
            + ": expected to be at least " + hdrSize
            + " and at most " + Integer.MAX_VALUE + ", or -1");
      }
      return (int)onDiskSizeWithHeaderL;
    }

    /**
     * Verify the passed in onDiskSizeWithHeader aligns with what is in the header else something
     * is not right.
     */
    private void verifyOnDiskSizeMatchesHeader(final int passedIn, final ByteBuff headerBuf,
                                               final long offset, boolean verifyChecksum)
        throws IOException {
      // Assert size provided aligns with what is in the header
      int fromHeader = getOnDiskSizeWithHeader(headerBuf, verifyChecksum);
      if (passedIn != fromHeader) {
        throw new IOException("Passed in onDiskSizeWithHeader=" + passedIn + " != " + fromHeader +
            ", offset=" + offset + ", fileContext=" + this.fileContext);
      }
    }

    /**
     * Check atomic reference cache for this block's header. Cache only good if next
     * read coming through is next in sequence in the block. We read next block's
     * header on the tail of reading the previous block to save a seek. Otherwise,
     * we have to do a seek to read the header before we can pull in the block OR
     * we have to backup the stream because we over-read (the next block's header).
     * @see PrefetchedHeader
     * @return The cached block header or null if not found.
     * @see #cacheNextBlockHeader(long, ByteBuff, int, int)
     */
    private ByteBuff getCachedHeader(final long offset) {
      PrefetchedHeader ph = this.prefetchedHeader.get();
      return ph != null && ph.offset == offset ? ph.buf : null;
    }

    /**
     * Save away the next blocks header in atomic reference.
     * @see #getCachedHeader(long)
     * @see PrefetchedHeader
     */
    private void cacheNextBlockHeader(final long offset,
                                      ByteBuff onDiskBlock, int onDiskSizeWithHeader, int headerLength) {
      PrefetchedHeader ph = new PrefetchedHeader();
      ph.offset = offset;
      onDiskBlock.get(onDiskSizeWithHeader, ph.header, 0, headerLength);
      this.prefetchedHeader.set(ph);
    }

    private int getNextBlockOnDiskSize(boolean readNextHeader, ByteBuff onDiskBlock,
                                       int onDiskSizeWithHeader) {
      int nextBlockOnDiskSize = -1;
      if (readNextHeader) {
        nextBlockOnDiskSize =
            onDiskBlock.getIntAfterPosition(onDiskSizeWithHeader + BlockType.MAGIC_LENGTH)
                + hdrSize;
      }
      return nextBlockOnDiskSize;
    }

    private ByteBuff allocate(int size, boolean intoHeap) {
      return intoHeap ? HEAP.allocate(size) : allocator.allocate(size);
    }

    /**
     * Reads a version 2 block.
     * @param offset the offset in the stream to read at.
     * @param onDiskSizeWithHeaderL the on-disk size of the block, including the header and
     *          checksums if present or -1 if unknown (as a long). Can be -1 if we are doing raw
     *          iteration of blocks as when loading up file metadata; i.e. the first read of a new
     *          file. Usually non-null gotten from the file index.
     * @param pread whether to use a positional read
     * @param verifyChecksum Whether to use HBase checksums. If HBase checksum is switched off, then
     *          use HDFS checksum. Can also flip on/off reading same file if we hit a troublesome
     *          patch in an hfile.
     * @param updateMetrics whether need to update the metrics.
     * @param intoHeap allocate the ByteBuff of block from heap or off-heap.
     * @return the HFileBlock or null if there is a HBase checksum mismatch
     */
    protected HFileBlock readBlockDataInternal(FSDataInputStream is, long offset,
                                               long onDiskSizeWithHeaderL, boolean pread, boolean verifyChecksum, boolean updateMetrics,
                                               boolean intoHeap) throws IOException {
      if (offset < 0) {
        throw new IOException("Invalid offset=" + offset + " trying to read "
            + "block (onDiskSize=" + onDiskSizeWithHeaderL + ")");
      }
      int onDiskSizeWithHeader = checkAndGetSizeAsInt(onDiskSizeWithHeaderL, hdrSize);
      // Try and get cached header. Will serve us in rare case where onDiskSizeWithHeaderL is -1
      // and will save us having to seek the stream backwards to reread the header we
      // read the last time through here.
      ByteBuff headerBuf = getCachedHeader(offset);
      LOG.trace("Reading {} at offset={}, pread={}, verifyChecksum={}, cachedHeader={}, " +
              "onDiskSizeWithHeader={}", this.fileContext.getHFileName(), offset, pread,
          verifyChecksum, headerBuf, onDiskSizeWithHeader);
      // This is NOT same as verifyChecksum. This latter is whether to do hbase
      // checksums. Can change with circumstances. The below flag is whether the
      // file has support for checksums (version 2+).
      boolean checksumSupport = this.fileContext.isUseHBaseChecksum();
      long startTime = System.currentTimeMillis();
      if (onDiskSizeWithHeader <= 0) {
        // We were not passed the block size. Need to get it from the header. If header was
        // not cached (see getCachedHeader above), need to seek to pull it in. This is costly
        // and should happen very rarely. Currently happens on open of a hfile reader where we
        // read the trailer blocks to pull in the indices. Otherwise, we are reading block sizes
        // out of the hfile index. To check, enable TRACE in this file and you'll get an exception
        // in a LOG every time we seek. See HBASE-17072 for more detail.
        if (headerBuf == null) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Extra see to get block size!", new RuntimeException());
          }
          headerBuf = HEAP.allocate(hdrSize);
          readAtOffset(is, headerBuf, hdrSize, false, offset, pread);
          headerBuf.rewind();
        }
        onDiskSizeWithHeader = getOnDiskSizeWithHeader(headerBuf, checksumSupport);
      }
      int preReadHeaderSize = headerBuf == null? 0 : hdrSize;
      // Allocate enough space to fit the next block's header too; saves a seek next time through.
      // onDiskBlock is whole block + header + checksums then extra hdrSize to read next header;
      // onDiskSizeWithHeader is header, body, and any checksums if present. preReadHeaderSize
      // says where to start reading. If we have the header cached, then we don't need to read
      // it again and we can likely read from last place we left off w/o need to backup and reread
      // the header we read last time through here.
      ByteBuff onDiskBlock = this.allocate(onDiskSizeWithHeader + hdrSize, intoHeap);
      boolean initHFileBlockSuccess = false;
      try {
        if (headerBuf != null) {
          onDiskBlock.put(0, headerBuf, 0, hdrSize).position(hdrSize);
        }
        boolean readNextHeader = readAtOffset(is, onDiskBlock,
            onDiskSizeWithHeader - preReadHeaderSize, true, offset + preReadHeaderSize, pread);
        onDiskBlock.rewind(); // in case of moving position when copying a cached header
        int nextBlockOnDiskSize =
            getNextBlockOnDiskSize(readNextHeader, onDiskBlock, onDiskSizeWithHeader);
        if (headerBuf == null) {
          headerBuf = onDiskBlock.duplicate().position(0).limit(hdrSize);
        }
        // Do a few checks before we go instantiate HFileBlock.
        assert onDiskSizeWithHeader > this.hdrSize;
        verifyOnDiskSizeMatchesHeader(onDiskSizeWithHeader, headerBuf, offset, checksumSupport);
        ByteBuff curBlock = onDiskBlock.duplicate().position(0).limit(onDiskSizeWithHeader);
        // Verify checksum of the data before using it for building HFileBlock.
        if (verifyChecksum && !validateChecksum(offset, curBlock, hdrSize)) {
          return null;
        }
        long duration = System.currentTimeMillis() - startTime;
        if (updateMetrics) {
          HFile.updateReadLatency(duration, pread);
        }
        // The onDiskBlock will become the headerAndDataBuffer for this block.
        // If nextBlockOnDiskSizeWithHeader is not zero, the onDiskBlock already
        // contains the header of next block, so no need to set next block's header in it.
        HFileBlock hFileBlock = createFromBuff(curBlock, checksumSupport, offset,
            nextBlockOnDiskSize, fileContext, intoHeap ? HEAP : allocator);
        // Run check on uncompressed sizings.
        if (!fileContext.isCompressedOrEncrypted()) {
          hFileBlock.sanityCheckUncompressed();
        }
        LOG.trace("Read {} in {} ns", hFileBlock, duration);
        // Cache next block header if we read it for the next time through here.
        if (nextBlockOnDiskSize != -1) {
          cacheNextBlockHeader(offset + hFileBlock.getOnDiskSizeWithHeader(), onDiskBlock,
              onDiskSizeWithHeader, hdrSize);
        }
        initHFileBlockSuccess = true;
        return hFileBlock;
      } finally {
        if (!initHFileBlockSuccess) {
          onDiskBlock.release();
        }
      }
    }

    @Override
    public void setIncludesMemStoreTS(boolean includesMemstoreTS) {
      this.fileContext = new HFileContextBuilder(this.fileContext)
          .withIncludesMvcc(includesMemstoreTS).build();
    }

    @Override
    public void setDataBlockEncoder(HFileDataBlockEncoder encoder) {
      encodedBlockDecodingCtx = encoder.newDataBlockDecodingContext(this.fileContext);
    }

    @Override
    public HFileBlockDecodingContext getBlockDecodingContext() {
      return this.encodedBlockDecodingCtx;
    }

    @Override
    public HFileBlockDecodingContext getDefaultBlockDecodingContext() {
      return this.defaultDecodingCtx;
    }

    /**
     * Generates the checksum for the header as well as the data and then validates it.
     * If the block doesn't uses checksum, returns false.
     * @return True if checksum matches, else false.
     */
    private boolean validateChecksum(long offset, ByteBuff data, int hdrSize) {
      // If this is an older version of the block that does not have checksums, then return false
      // indicating that checksum verification did not succeed. Actually, this method should never
      // be called when the minorVersion is 0, thus this is a defensive check for a cannot-happen
      // case. Since this is a cannot-happen case, it is better to return false to indicate a
      // checksum validation failure.
      if (!fileContext.isUseHBaseChecksum()) {
        return false;
      }
      return ChecksumUtil.validateChecksum(data, pathName, offset, hdrSize);
    }

    @Override
    public void closeStreams() throws IOException {
      streamWrapper.close();
    }

    @Override
    public void unbufferStream() {
      // To handle concurrent reads, ensure that no other client is accessing the streams while we
      // unbuffer it.
      if (streamLock.tryLock()) {
        try {
          this.streamWrapper.unbuffer();
        } finally {
          streamLock.unlock();
        }
      }
    }

    @Override
    public String toString() {
      return "hfs=" + hfs + ", path=" + pathName + ", fileContext=" + fileContext;
    }
  }

  /** An additional sanity-check in case no compression or encryption is being used. */
  void sanityCheckUncompressed() throws IOException {
    if (onDiskSizeWithoutHeader != uncompressedSizeWithoutHeader +
        totalChecksumBytes()) {
      throw new IOException("Using no compression but "
          + "onDiskSizeWithoutHeader=" + onDiskSizeWithoutHeader + ", "
          + "uncompressedSizeWithoutHeader=" + uncompressedSizeWithoutHeader
          + ", numChecksumbytes=" + totalChecksumBytes());
    }
  }

  // Cacheable implementation
  @Override
  public int getSerializedLength() {
    if (buf != null) {
      // Include extra bytes for block metadata.
      return this.buf.limit() + BLOCK_METADATA_SPACE;
    }
    return 0;
  }

  // Cacheable implementation
  @Override
  public void serialize(ByteBuffer destination, boolean includeNextBlockMetadata) {
    this.buf.get(destination, 0, getSerializedLength() - BLOCK_METADATA_SPACE);
    destination = addMetaData(destination, includeNextBlockMetadata);

    // Make it ready for reading. flip sets position to zero and limit to current position which
    // is what we want if we do not want to serialize the block plus checksums if present plus
    // metadata.
    destination.flip();
  }

  /**
   * For use by bucketcache. This exposes internals.
   */
  public ByteBuffer getMetaData() {
    ByteBuffer bb = ByteBuffer.allocate(BLOCK_METADATA_SPACE);
    bb = addMetaData(bb, true);
    bb.flip();
    return bb;
  }

  /**
   * Adds metadata at current position (position is moved forward). Does not flip or reset.
   * @return The passed <code>destination</code> with metadata added.
   */
  private ByteBuffer addMetaData(final ByteBuffer destination, boolean includeNextBlockMetadata) {
    destination.put(this.fileContext.isUseHBaseChecksum() ? (byte) 1 : (byte) 0);
    destination.putLong(this.offset);
    if (includeNextBlockMetadata) {
      destination.putInt(this.nextBlockOnDiskSize);
    }
    return destination;
  }

  // Cacheable implementation
  @Override
  public CacheableDeserializer<Cacheable> getDeserializer() {
    return HFileBlock.BLOCK_DESERIALIZER;
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = result * 31 + blockType.hashCode();
    result = result * 31 + nextBlockOnDiskSize;
    result = result * 31 + (int) (offset ^ (offset >>> 32));
    result = result * 31 + onDiskSizeWithoutHeader;
    result = result * 31 + (int) (prevBlockOffset ^ (prevBlockOffset >>> 32));
    result = result * 31 + uncompressedSizeWithoutHeader;
    result = result * 31 + buf.hashCode();
    return result;
  }

  @Override
  public boolean equals(Object comparison) {
    if (this == comparison) {
      return true;
    }
    if (comparison == null) {
      return false;
    }
    if (!(comparison instanceof HFileBlock)) {
      return false;
    }

    HFileBlock castedComparison = (HFileBlock) comparison;

    if (castedComparison.blockType != this.blockType) {
      return false;
    }
    if (castedComparison.nextBlockOnDiskSize != this.nextBlockOnDiskSize) {
      return false;
    }
    // Offset is important. Needed when we have to remake cachekey when block is returned to cache.
    if (castedComparison.offset != this.offset) {
      return false;
    }
    if (castedComparison.onDiskSizeWithoutHeader != this.onDiskSizeWithoutHeader) {
      return false;
    }
    if (castedComparison.prevBlockOffset != this.prevBlockOffset) {
      return false;
    }
    if (castedComparison.uncompressedSizeWithoutHeader != this.uncompressedSizeWithoutHeader) {
      return false;
    }
    if (ByteBuff.compareTo(this.buf, 0, this.buf.limit(), castedComparison.buf, 0,
        castedComparison.buf.limit()) != 0) {
      return false;
    }
    return true;
  }

  DataBlockEncoding getDataBlockEncoding() {
    if (blockType == BlockType.ENCODED_DATA) {
      return DataBlockEncoding.getEncodingById(getDataBlockEncodingId());
    }
    return DataBlockEncoding.NONE;
  }

  byte getChecksumType() {
    return this.fileContext.getChecksumType().getCode();
  }

  int getBytesPerChecksum() {
    return this.fileContext.getBytesPerChecksum();
  }

  /** @return the size of data on disk + header. Excludes checksum. */
  int getOnDiskDataSizeWithHeader() {
    return this.onDiskDataSizeWithHeader;
  }

  /**
   * Calculate the number of bytes required to store all the checksums
   * for this block. Each checksum value is a 4 byte integer.
   */
  int totalChecksumBytes() {
    // If the hfile block has minorVersion 0, then there are no checksum
    // data to validate. Similarly, a zero value in this.bytesPerChecksum
    // indicates that cached blocks do not have checksum data because
    // checksums were already validated when the block was read from disk.
    if (!fileContext.isUseHBaseChecksum() || this.fileContext.getBytesPerChecksum() == 0) {
      return 0;
    }
    return (int) ChecksumUtil.numBytes(onDiskDataSizeWithHeader,
        this.fileContext.getBytesPerChecksum());
  }

  /**
   * Returns the size of this block header.
   */
  public int headerSize() {
    return headerSize(this.fileContext.isUseHBaseChecksum());
  }

  /**
   * Maps a minor version to the size of the header.
   */
  public static int headerSize(boolean usesHBaseChecksum) {
    return usesHBaseChecksum?
        HConstants.HFILEBLOCK_HEADER_SIZE: HConstants.HFILEBLOCK_HEADER_SIZE_NO_CHECKSUM;
  }

  /**
   * Return the appropriate DUMMY_HEADER for the minor version
   */
  // TODO: Why is this in here?
  byte[] getDummyHeaderForVersion() {
    return getDummyHeaderForVersion(this.fileContext.isUseHBaseChecksum());
  }

  /**
   * Return the appropriate DUMMY_HEADER for the minor version
   */
  static private byte[] getDummyHeaderForVersion(boolean usesHBaseChecksum) {
    return usesHBaseChecksum? HConstants.HFILEBLOCK_DUMMY_HEADER: DUMMY_HEADER_NO_CHECKSUM;
  }

  /**
   * @return This HFileBlocks fileContext which will a derivative of the
   *   fileContext for the file from which this block's data was originally read.
   */
  public HFileContext getHFileContext() {
    return this.fileContext;
  }

  /**
   * Convert the contents of the block header into a human readable string.
   * This is mostly helpful for debugging. This assumes that the block
   * has minor version > 0.
   */
  static String toStringHeader(ByteBuff buf) throws IOException {
    byte[] magicBuf = new byte[Math.min(buf.limit() - buf.position(), BlockType.MAGIC_LENGTH)];
    buf.get(magicBuf);
    BlockType bt = BlockType.parse(magicBuf, 0, BlockType.MAGIC_LENGTH);
    int compressedBlockSizeNoHeader = buf.getInt();
    int uncompressedBlockSizeNoHeader = buf.getInt();
    long prevBlockOffset = buf.getLong();
    byte cksumtype = buf.get();
    long bytesPerChecksum = buf.getInt();
    long onDiskDataSizeWithHeader = buf.getInt();
    return " Header dump: magic: " + Bytes.toString(magicBuf) +
        " blockType " + bt +
        " compressedBlockSizeNoHeader " +
        compressedBlockSizeNoHeader +
        " uncompressedBlockSizeNoHeader " +
        uncompressedBlockSizeNoHeader +
        " prevBlockOffset " + prevBlockOffset +
        " checksumType " + ChecksumType.codeToType(cksumtype) +
        " bytesPerChecksum " + bytesPerChecksum +
        " onDiskDataSizeWithHeader " + onDiskDataSizeWithHeader;
  }

  private static HFileBlockBuilder createBuilder(HFileBlock blk){
    return new HFileBlockBuilder()
        .withBlockType(blk.blockType)
        .withOnDiskSizeWithoutHeader(blk.onDiskSizeWithoutHeader)
        .withUncompressedSizeWithoutHeader(blk.uncompressedSizeWithoutHeader)
        .withPrevBlockOffset(blk.prevBlockOffset)
        .withByteBuff(blk.buf.duplicate()) // Duplicate the buffer.
        .withOffset(blk.offset)
        .withOnDiskDataSizeWithHeader(blk.onDiskDataSizeWithHeader)
        .withNextBlockOnDiskSize(blk.nextBlockOnDiskSize)
        .withHFileContext(blk.fileContext)
        .withByteBuffAllocator(blk.allocator)
        .withShared(blk.isSharedMem());
  }

  static HFileBlock shallowClone(HFileBlock blk) {
    return createBuilder(blk).build();
  }

  static HFileBlock deepCloneOnHeap(HFileBlock blk) {
    ByteBuff deepCloned = ByteBuff.wrap(ByteBuffer.wrap(blk.buf.toBytes(0, blk.buf.limit())));
    return createBuilder(blk).withByteBuff(deepCloned).withShared(false).build();
  }
}
