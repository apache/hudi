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

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hudi.hbase.ByteBufferExtendedCell;
import org.apache.hudi.hbase.Cell;
import org.apache.hudi.hbase.CellComparator;
import org.apache.hudi.hbase.CellUtil;
import org.apache.hudi.hbase.HConstants;
import org.apache.hudi.hbase.KeyValueUtil;
import org.apache.hudi.hbase.MetaCellComparator;
import org.apache.hudi.hbase.PrivateCellUtil;
import org.apache.hudi.hbase.io.compress.Compression;
import org.apache.hudi.hbase.io.crypto.Encryption;
import org.apache.hudi.hbase.io.encoding.DataBlockEncoding;
import org.apache.hudi.hbase.io.hfile.HFileBlock.BlockWritable;
import org.apache.hudi.hbase.security.EncryptionUtil;
import org.apache.hudi.hbase.security.User;
import org.apache.hudi.hbase.util.BloomFilterWriter;
import org.apache.hudi.hbase.util.ByteBufferUtils;
import org.apache.hudi.hbase.util.Bytes;
import org.apache.hudi.hbase.util.CommonFSUtils;
import org.apache.hudi.hbase.util.FSUtils;
import org.apache.hadoop.io.Writable;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common functionality needed by all versions of {@link HFile} writers.
 */
@InterfaceAudience.Private
public class HFileWriterImpl implements HFile.Writer {
  private static final Logger LOG = LoggerFactory.getLogger(HFileWriterImpl.class);

  private static final long UNSET = -1;

  /** if this feature is enabled, preCalculate encoded data size before real encoding happens*/
  public static final String UNIFIED_ENCODED_BLOCKSIZE_RATIO =
      "hbase.writer.unified.encoded.blocksize.ratio";

  /** Block size limit after encoding, used to unify encoded block Cache entry size*/
  private final int encodedBlockSizeLimit;

  /** The Cell previously appended. Becomes the last cell in the file.*/
  protected Cell lastCell = null;

  /** FileSystem stream to write into. */
  protected FSDataOutputStream outputStream;

  /** True if we opened the <code>outputStream</code> (and so will close it). */
  protected final boolean closeOutputStream;

  /** A "file info" block: a key-value map of file-wide metadata. */
  protected HFileInfo fileInfo = new HFileInfo();

  /** Total # of key/value entries, i.e. how many times add() was called. */
  protected long entryCount = 0;

  /** Used for calculating the average key length. */
  protected long totalKeyLength = 0;

  /** Used for calculating the average value length. */
  protected long totalValueLength = 0;

  /** Total uncompressed bytes, maybe calculate a compression ratio later. */
  protected long totalUncompressedBytes = 0;

  /** Meta block names. */
  protected List<byte[]> metaNames = new ArrayList<>();

  /** {@link Writable}s representing meta block data. */
  protected List<Writable> metaData = new ArrayList<>();

  /**
   * First cell in a block.
   * This reference should be short-lived since we write hfiles in a burst.
   */
  protected Cell firstCellInBlock = null;


  /** May be null if we were passed a stream. */
  protected final Path path;

  /** Cache configuration for caching data on write. */
  protected final CacheConfig cacheConf;

  /**
   * Name for this object used when logging or in toString. Is either
   * the result of a toString on stream or else name of passed file Path.
   */
  protected final String name;

  /**
   * The data block encoding which will be used.
   * {@link NoOpDataBlockEncoder#INSTANCE} if there is no encoding.
   */
  protected final HFileDataBlockEncoder blockEncoder;

  protected final HFileContext hFileContext;

  private int maxTagsLength = 0;

  /** KeyValue version in FileInfo */
  public static final byte [] KEY_VALUE_VERSION = Bytes.toBytes("KEY_VALUE_VERSION");

  /** Version for KeyValue which includes memstore timestamp */
  public static final int KEY_VALUE_VER_WITH_MEMSTORE = 1;

  /** Inline block writers for multi-level block index and compound Blooms. */
  private List<InlineBlockWriter> inlineBlockWriters = new ArrayList<>();

  /** block writer */
  protected HFileBlock.Writer blockWriter;

  private HFileBlockIndex.BlockIndexWriter dataBlockIndexWriter;
  private HFileBlockIndex.BlockIndexWriter metaBlockIndexWriter;

  /** The offset of the first data block or -1 if the file is empty. */
  private long firstDataBlockOffset = UNSET;

  /** The offset of the last data block or 0 if the file is empty. */
  protected long lastDataBlockOffset = UNSET;

  /**
   * The last(stop) Cell of the previous data block.
   * This reference should be short-lived since we write hfiles in a burst.
   */
  private Cell lastCellOfPreviousBlock = null;

  /** Additional data items to be written to the "load-on-open" section. */
  private List<BlockWritable> additionalLoadOnOpenData = new ArrayList<>();

  protected long maxMemstoreTS = 0;

  public HFileWriterImpl(final Configuration conf, CacheConfig cacheConf, Path path,
                         FSDataOutputStream outputStream, HFileContext fileContext) {
    this.outputStream = outputStream;
    this.path = path;
    this.name = path != null ? path.getName() : outputStream.toString();
    this.hFileContext = fileContext;
    DataBlockEncoding encoding = hFileContext.getDataBlockEncoding();
    if (encoding != DataBlockEncoding.NONE) {
      this.blockEncoder = new HFileDataBlockEncoderImpl(encoding);
    } else {
      this.blockEncoder = NoOpDataBlockEncoder.INSTANCE;
    }
    closeOutputStream = path != null;
    this.cacheConf = cacheConf;
    float encodeBlockSizeRatio = conf.getFloat(UNIFIED_ENCODED_BLOCKSIZE_RATIO, 1f);
    this.encodedBlockSizeLimit = (int)(hFileContext.getBlocksize() * encodeBlockSizeRatio);
    finishInit(conf);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Writer" + (path != null ? " for " + path : "") +
          " initialized with cacheConf: " + cacheConf +
          " fileContext: " + fileContext);
    }
  }

  /**
   * Add to the file info. All added key/value pairs can be obtained using
   * {@link HFile.Reader#getHFileInfo()}.
   *
   * @param k Key
   * @param v Value
   * @throws IOException in case the key or the value are invalid
   */
  @Override
  public void appendFileInfo(final byte[] k, final byte[] v)
      throws IOException {
    fileInfo.append(k, v, true);
  }

  /**
   * Sets the file info offset in the trailer, finishes up populating fields in
   * the file info, and writes the file info into the given data output. The
   * reason the data output is not always {@link #outputStream} is that we store
   * file info as a block in version 2.
   *
   * @param trailer fixed file trailer
   * @param out the data output to write the file info to
   */
  protected final void writeFileInfo(FixedFileTrailer trailer, DataOutputStream out)
      throws IOException {
    trailer.setFileInfoOffset(outputStream.getPos());
    finishFileInfo();
    long startTime = System.currentTimeMillis();
    fileInfo.write(out);
    HFile.updateWriteLatency(System.currentTimeMillis() - startTime);
  }

  /**
   * Checks that the given Cell's key does not violate the key order.
   *
   * @param cell Cell whose key to check.
   * @return true if the key is duplicate
   * @throws IOException if the key or the key order is wrong
   */
  protected boolean checkKey(final Cell cell) throws IOException {
    boolean isDuplicateKey = false;

    if (cell == null) {
      throw new IOException("Key cannot be null or empty");
    }
    if (lastCell != null) {
      int keyComp = PrivateCellUtil.compareKeyIgnoresMvcc(this.hFileContext.getCellComparator(),
          lastCell, cell);
      if (keyComp > 0) {
        String message = getLexicalErrorMessage(cell);
        throw new IOException(message);
      } else if (keyComp == 0) {
        isDuplicateKey = true;
      }
    }
    return isDuplicateKey;
  }

  private String getLexicalErrorMessage(Cell cell) {
    StringBuilder sb = new StringBuilder();
    sb.append("Added a key not lexically larger than previous. Current cell = ");
    sb.append(cell);
    sb.append(", lastCell = ");
    sb.append(lastCell);
    //file context includes HFile path and optionally table and CF of file being written
    sb.append("fileContext=");
    sb.append(hFileContext);
    return sb.toString();
  }

  /** Checks the given value for validity. */
  protected void checkValue(final byte[] value, final int offset,
                            final int length) throws IOException {
    if (value == null) {
      throw new IOException("Value cannot be null");
    }
  }

  /**
   * @return Path or null if we were passed a stream rather than a Path.
   */
  @Override
  public Path getPath() {
    return path;
  }

  @Override
  public String toString() {
    return "writer=" + (path != null ? path.toString() : null) + ", name="
        + name + ", compression=" + hFileContext.getCompression().getName();
  }

  public static Compression.Algorithm compressionByName(String algoName) {
    if (algoName == null) {
      return HFile.DEFAULT_COMPRESSION_ALGORITHM;
    }
    return Compression.getCompressionAlgorithmByName(algoName);
  }

  /** A helper method to create HFile output streams in constructors */
  protected static FSDataOutputStream createOutputStream(Configuration conf,
                                                         FileSystem fs, Path path, InetSocketAddress[] favoredNodes) throws IOException {
    FsPermission perms = CommonFSUtils.getFilePermissions(fs, conf,
        HConstants.DATA_FILE_UMASK_KEY);
    return FSUtils.create(conf, fs, path, perms, favoredNodes);
  }

  /** Additional initialization steps */
  protected void finishInit(final Configuration conf) {
    if (blockWriter != null) {
      throw new IllegalStateException("finishInit called twice");
    }
    blockWriter = new HFileBlock.Writer(blockEncoder, hFileContext,
        cacheConf.getByteBuffAllocator());
    // Data block index writer
    boolean cacheIndexesOnWrite = cacheConf.shouldCacheIndexesOnWrite();
    dataBlockIndexWriter = new HFileBlockIndex.BlockIndexWriter(blockWriter,
        cacheIndexesOnWrite ? cacheConf : null,
        cacheIndexesOnWrite ? name : null);
    dataBlockIndexWriter.setMaxChunkSize(
        HFileBlockIndex.getMaxChunkSize(conf));
    dataBlockIndexWriter.setMinIndexNumEntries(
        HFileBlockIndex.getMinIndexNumEntries(conf));
    inlineBlockWriters.add(dataBlockIndexWriter);

    // Meta data block index writer
    metaBlockIndexWriter = new HFileBlockIndex.BlockIndexWriter();
    LOG.trace("Initialized with {}", cacheConf);
  }

  /**
   * At a block boundary, write all the inline blocks and opens new block.
   */
  protected void checkBlockBoundary() throws IOException {
    // For encoder like prefixTree, encoded size is not available, so we have to compare both
    // encoded size and unencoded size to blocksize limit.
    if (blockWriter.encodedBlockSizeWritten() >= encodedBlockSizeLimit
        || blockWriter.blockSizeWritten() >= hFileContext.getBlocksize()) {
      finishBlock();
      writeInlineBlocks(false);
      newBlock();
    }
  }

  /** Clean up the data block that is currently being written.*/
  private void finishBlock() throws IOException {
    if (!blockWriter.isWriting() || blockWriter.blockSizeWritten() == 0) {
      return;
    }

    // Update the first data block offset if UNSET; used scanning.
    if (firstDataBlockOffset == UNSET) {
      firstDataBlockOffset = outputStream.getPos();
    }
    // Update the last data block offset each time through here.
    lastDataBlockOffset = outputStream.getPos();
    blockWriter.writeHeaderAndData(outputStream);
    int onDiskSize = blockWriter.getOnDiskSizeWithHeader();
    Cell indexEntry =
        getMidpoint(this.hFileContext.getCellComparator(), lastCellOfPreviousBlock, firstCellInBlock);
    dataBlockIndexWriter.addEntry(PrivateCellUtil.getCellKeySerializedAsKeyValueKey(indexEntry),
        lastDataBlockOffset, onDiskSize);
    totalUncompressedBytes += blockWriter.getUncompressedSizeWithHeader();
    if (cacheConf.shouldCacheDataOnWrite()) {
      doCacheOnWrite(lastDataBlockOffset);
    }
  }

  /**
   * Try to return a Cell that falls between <code>left</code> and
   * <code>right</code> but that is shorter; i.e. takes up less space. This
   * trick is used building HFile block index. Its an optimization. It does not
   * always work. In this case we'll just return the <code>right</code> cell.
   * @return A cell that sorts between <code>left</code> and <code>right</code>.
   */
  public static Cell getMidpoint(final CellComparator comparator, final Cell left,
                                 final Cell right) {
    // TODO: Redo so only a single pass over the arrays rather than one to
    // compare and then a second composing midpoint.
    if (right == null) {
      throw new IllegalArgumentException("right cell can not be null");
    }
    if (left == null) {
      return right;
    }
    // If Cells from meta table, don't mess around. meta table Cells have schema
    // (table,startrow,hash) so can't be treated as plain byte arrays. Just skip
    // out without trying to do this optimization.
    if (comparator instanceof MetaCellComparator) {
      return right;
    }
    int diff = comparator.compareRows(left, right);
    if (diff > 0) {
      throw new IllegalArgumentException("Left row sorts after right row; left="
          + CellUtil.getCellKeyAsString(left) + ", right=" + CellUtil.getCellKeyAsString(right));
    }
    byte[] midRow;
    boolean bufferBacked = left instanceof ByteBufferExtendedCell
        && right instanceof ByteBufferExtendedCell;
    if (diff < 0) {
      // Left row is < right row.
      if (bufferBacked) {
        midRow = getMinimumMidpointArray(((ByteBufferExtendedCell) left).getRowByteBuffer(),
            ((ByteBufferExtendedCell) left).getRowPosition(), left.getRowLength(),
            ((ByteBufferExtendedCell) right).getRowByteBuffer(),
            ((ByteBufferExtendedCell) right).getRowPosition(), right.getRowLength());
      } else {
        midRow = getMinimumMidpointArray(left.getRowArray(), left.getRowOffset(),
            left.getRowLength(), right.getRowArray(), right.getRowOffset(), right.getRowLength());
      }
      // If midRow is null, just return 'right'. Can't do optimization.
      if (midRow == null) {
        return right;
      }
      return PrivateCellUtil.createFirstOnRow(midRow);
    }
    // Rows are same. Compare on families.
    diff = comparator.compareFamilies(left, right);
    if (diff > 0) {
      throw new IllegalArgumentException("Left family sorts after right family; left="
          + CellUtil.getCellKeyAsString(left) + ", right=" + CellUtil.getCellKeyAsString(right));
    }
    if (diff < 0) {
      if (bufferBacked) {
        midRow = getMinimumMidpointArray(((ByteBufferExtendedCell) left).getFamilyByteBuffer(),
            ((ByteBufferExtendedCell) left).getFamilyPosition(), left.getFamilyLength(),
            ((ByteBufferExtendedCell) right).getFamilyByteBuffer(),
            ((ByteBufferExtendedCell) right).getFamilyPosition(), right.getFamilyLength());
      } else {
        midRow = getMinimumMidpointArray(left.getFamilyArray(), left.getFamilyOffset(),
            left.getFamilyLength(), right.getFamilyArray(), right.getFamilyOffset(),
            right.getFamilyLength());
      }
      // If midRow is null, just return 'right'. Can't do optimization.
      if (midRow == null) {
        return right;
      }
      // Return new Cell where we use right row and then a mid sort family.
      return PrivateCellUtil.createFirstOnRowFamily(right, midRow, 0, midRow.length);
    }
    // Families are same. Compare on qualifiers.
    diff = comparator.compareQualifiers(left, right);
    if (diff > 0) {
      throw new IllegalArgumentException("Left qualifier sorts after right qualifier; left="
          + CellUtil.getCellKeyAsString(left) + ", right=" + CellUtil.getCellKeyAsString(right));
    }
    if (diff < 0) {
      if (bufferBacked) {
        midRow = getMinimumMidpointArray(((ByteBufferExtendedCell) left).getQualifierByteBuffer(),
            ((ByteBufferExtendedCell) left).getQualifierPosition(), left.getQualifierLength(),
            ((ByteBufferExtendedCell) right).getQualifierByteBuffer(),
            ((ByteBufferExtendedCell) right).getQualifierPosition(), right.getQualifierLength());
      } else {
        midRow = getMinimumMidpointArray(left.getQualifierArray(), left.getQualifierOffset(),
            left.getQualifierLength(), right.getQualifierArray(), right.getQualifierOffset(),
            right.getQualifierLength());
      }
      // If midRow is null, just return 'right'. Can't do optimization.
      if (midRow == null) {
        return right;
      }
      // Return new Cell where we use right row and family and then a mid sort qualifier.
      return PrivateCellUtil.createFirstOnRowCol(right, midRow, 0, midRow.length);
    }
    // No opportunity for optimization. Just return right key.
    return right;
  }

  /**
   * @return Return a new array that is between left and right and minimally
   *         sized else just return null as indicator that we could not create a
   *         mid point.
   */
  private static byte[] getMinimumMidpointArray(final byte[] leftArray, final int leftOffset,
                                                final int leftLength, final byte[] rightArray, final int rightOffset, final int rightLength) {
    // rows are different
    int minLength = leftLength < rightLength ? leftLength : rightLength;
    int diffIdx = 0;
    while (diffIdx < minLength
        && leftArray[leftOffset + diffIdx] == rightArray[rightOffset + diffIdx]) {
      diffIdx++;
    }
    byte[] minimumMidpointArray = null;
    if (diffIdx >= minLength) {
      // leftKey's row is prefix of rightKey's.
      minimumMidpointArray = new byte[diffIdx + 1];
      System.arraycopy(rightArray, rightOffset, minimumMidpointArray, 0, diffIdx + 1);
    } else {
      int diffByte = leftArray[leftOffset + diffIdx];
      if ((0xff & diffByte) < 0xff && (diffByte + 1) < (rightArray[rightOffset + diffIdx] & 0xff)) {
        minimumMidpointArray = new byte[diffIdx + 1];
        System.arraycopy(leftArray, leftOffset, minimumMidpointArray, 0, diffIdx);
        minimumMidpointArray[diffIdx] = (byte) (diffByte + 1);
      } else {
        minimumMidpointArray = new byte[diffIdx + 1];
        System.arraycopy(rightArray, rightOffset, minimumMidpointArray, 0, diffIdx + 1);
      }
    }
    return minimumMidpointArray;
  }

  private static byte[] getMinimumMidpointArray(ByteBuffer left, int leftOffset, int leftLength,
                                                ByteBuffer right, int rightOffset, int rightLength) {
    // rows are different
    int minLength = leftLength < rightLength ? leftLength : rightLength;
    int diffIdx = 0;
    while (diffIdx < minLength && ByteBufferUtils.toByte(left,
        leftOffset + diffIdx) == ByteBufferUtils.toByte(right, rightOffset + diffIdx)) {
      diffIdx++;
    }
    byte[] minMidpoint = null;
    if (diffIdx >= minLength) {
      // leftKey's row is prefix of rightKey's.
      minMidpoint = new byte[diffIdx + 1];
      ByteBufferUtils.copyFromBufferToArray(minMidpoint, right, rightOffset, 0, diffIdx + 1);
    } else {
      int diffByte = ByteBufferUtils.toByte(left, leftOffset + diffIdx);
      if ((0xff & diffByte) < 0xff
          && (diffByte + 1) < (ByteBufferUtils.toByte(right, rightOffset + diffIdx) & 0xff)) {
        minMidpoint = new byte[diffIdx + 1];
        ByteBufferUtils.copyFromBufferToArray(minMidpoint, left, leftOffset, 0, diffIdx);
        minMidpoint[diffIdx] = (byte) (diffByte + 1);
      } else {
        minMidpoint = new byte[diffIdx + 1];
        ByteBufferUtils.copyFromBufferToArray(minMidpoint, right, rightOffset, 0, diffIdx + 1);
      }
    }
    return minMidpoint;
  }

  /** Gives inline block writers an opportunity to contribute blocks. */
  private void writeInlineBlocks(boolean closing) throws IOException {
    for (InlineBlockWriter ibw : inlineBlockWriters) {
      while (ibw.shouldWriteBlock(closing)) {
        long offset = outputStream.getPos();
        boolean cacheThisBlock = ibw.getCacheOnWrite();
        ibw.writeInlineBlock(blockWriter.startWriting(
            ibw.getInlineBlockType()));
        blockWriter.writeHeaderAndData(outputStream);
        ibw.blockWritten(offset, blockWriter.getOnDiskSizeWithHeader(),
            blockWriter.getUncompressedSizeWithoutHeader());
        totalUncompressedBytes += blockWriter.getUncompressedSizeWithHeader();

        if (cacheThisBlock) {
          doCacheOnWrite(offset);
        }
      }
    }
  }

  /**
   * Caches the last written HFile block.
   * @param offset the offset of the block we want to cache. Used to determine
   *          the cache key.
   */
  private void doCacheOnWrite(long offset) {
    cacheConf.getBlockCache().ifPresent(cache -> {
      HFileBlock cacheFormatBlock = blockWriter.getBlockForCaching(cacheConf);
      try {
        cache.cacheBlock(new BlockCacheKey(name, offset, true, cacheFormatBlock.getBlockType()),
            cacheFormatBlock);
      } finally {
        // refCnt will auto increase when block add to Cache, see RAMCache#putIfAbsent
        cacheFormatBlock.release();
      }
    });
  }

  /**
   * Ready a new block for writing.
   */
  protected void newBlock() throws IOException {
    // This is where the next block begins.
    blockWriter.startWriting(BlockType.DATA);
    firstCellInBlock = null;
    if (lastCell != null) {
      lastCellOfPreviousBlock = lastCell;
    }
  }

  /**
   * Add a meta block to the end of the file. Call before close(). Metadata
   * blocks are expensive. Fill one with a bunch of serialized data rather than
   * do a metadata block per metadata instance. If metadata is small, consider
   * adding to file info using {@link #appendFileInfo(byte[], byte[])}
   *
   * @param metaBlockName
   *          name of the block
   * @param content
   *          will call readFields to get data later (DO NOT REUSE)
   */
  @Override
  public void appendMetaBlock(String metaBlockName, Writable content) {
    byte[] key = Bytes.toBytes(metaBlockName);
    int i;
    for (i = 0; i < metaNames.size(); ++i) {
      // stop when the current key is greater than our own
      byte[] cur = metaNames.get(i);
      if (Bytes.BYTES_RAWCOMPARATOR.compare(cur, 0, cur.length, key, 0,
          key.length) > 0) {
        break;
      }
    }
    metaNames.add(i, key);
    metaData.add(i, content);
  }

  @Override
  public void close() throws IOException {
    if (outputStream == null) {
      return;
    }
    // Save data block encoder metadata in the file info.
    blockEncoder.saveMetadata(this);
    // Write out the end of the data blocks, then write meta data blocks.
    // followed by fileinfo, data block index and meta block index.

    finishBlock();
    writeInlineBlocks(true);

    FixedFileTrailer trailer = new FixedFileTrailer(getMajorVersion(), getMinorVersion());

    // Write out the metadata blocks if any.
    if (!metaNames.isEmpty()) {
      for (int i = 0; i < metaNames.size(); ++i) {
        // store the beginning offset
        long offset = outputStream.getPos();
        // write the metadata content
        DataOutputStream dos = blockWriter.startWriting(BlockType.META);
        metaData.get(i).write(dos);

        blockWriter.writeHeaderAndData(outputStream);
        totalUncompressedBytes += blockWriter.getUncompressedSizeWithHeader();

        // Add the new meta block to the meta index.
        metaBlockIndexWriter.addEntry(metaNames.get(i), offset,
            blockWriter.getOnDiskSizeWithHeader());
      }
    }

    // Load-on-open section.

    // Data block index.
    //
    // In version 2, this section of the file starts with the root level data
    // block index. We call a function that writes intermediate-level blocks
    // first, then root level, and returns the offset of the root level block
    // index.

    long rootIndexOffset = dataBlockIndexWriter.writeIndexBlocks(outputStream);
    trailer.setLoadOnOpenOffset(rootIndexOffset);

    // Meta block index.
    metaBlockIndexWriter.writeSingleLevelIndex(blockWriter.startWriting(
        BlockType.ROOT_INDEX), "meta");
    blockWriter.writeHeaderAndData(outputStream);
    totalUncompressedBytes += blockWriter.getUncompressedSizeWithHeader();

    if (this.hFileContext.isIncludesMvcc()) {
      appendFileInfo(MAX_MEMSTORE_TS_KEY, Bytes.toBytes(maxMemstoreTS));
      appendFileInfo(KEY_VALUE_VERSION, Bytes.toBytes(KEY_VALUE_VER_WITH_MEMSTORE));
    }

    // File info
    writeFileInfo(trailer, blockWriter.startWriting(BlockType.FILE_INFO));
    blockWriter.writeHeaderAndData(outputStream);
    totalUncompressedBytes += blockWriter.getUncompressedSizeWithHeader();

    // Load-on-open data supplied by higher levels, e.g. Bloom filters.
    for (BlockWritable w : additionalLoadOnOpenData){
      blockWriter.writeBlock(w, outputStream);
      totalUncompressedBytes += blockWriter.getUncompressedSizeWithHeader();
    }

    // Now finish off the trailer.
    trailer.setNumDataIndexLevels(dataBlockIndexWriter.getNumLevels());
    trailer.setUncompressedDataIndexSize(
        dataBlockIndexWriter.getTotalUncompressedSize());
    trailer.setFirstDataBlockOffset(firstDataBlockOffset);
    trailer.setLastDataBlockOffset(lastDataBlockOffset);
    trailer.setComparatorClass(this.hFileContext.getCellComparator().getClass());
    trailer.setDataIndexCount(dataBlockIndexWriter.getNumRootEntries());


    finishClose(trailer);

    blockWriter.release();
  }

  @Override
  public void addInlineBlockWriter(InlineBlockWriter ibw) {
    inlineBlockWriters.add(ibw);
  }

  @Override
  public void addGeneralBloomFilter(final BloomFilterWriter bfw) {
    this.addBloomFilter(bfw, BlockType.GENERAL_BLOOM_META);
  }

  @Override
  public void addDeleteFamilyBloomFilter(final BloomFilterWriter bfw) {
    this.addBloomFilter(bfw, BlockType.DELETE_FAMILY_BLOOM_META);
  }

  private void addBloomFilter(final BloomFilterWriter bfw,
                              final BlockType blockType) {
    if (bfw.getKeyCount() <= 0) {
      return;
    }

    if (blockType != BlockType.GENERAL_BLOOM_META &&
        blockType != BlockType.DELETE_FAMILY_BLOOM_META) {
      throw new RuntimeException("Block Type: " + blockType.toString() +
          "is not supported");
    }
    additionalLoadOnOpenData.add(new BlockWritable() {
      @Override
      public BlockType getBlockType() {
        return blockType;
      }

      @Override
      public void writeToBlock(DataOutput out) throws IOException {
        bfw.getMetaWriter().write(out);
        Writable dataWriter = bfw.getDataWriter();
        if (dataWriter != null) {
          dataWriter.write(out);
        }
      }
    });
  }

  @Override
  public HFileContext getFileContext() {
    return hFileContext;
  }

  /**
   * Add key/value to file. Keys must be added in an order that agrees with the
   * Comparator passed on construction.
   *
   * @param cell
   *          Cell to add. Cannot be empty nor null.
   */
  @Override
  public void append(final Cell cell) throws IOException {
    // checkKey uses comparator to check we are writing in order.
    boolean dupKey = checkKey(cell);
    if (!dupKey) {
      checkBlockBoundary();
    }

    if (!blockWriter.isWriting()) {
      newBlock();
    }

    blockWriter.write(cell);

    totalKeyLength += PrivateCellUtil.estimatedSerializedSizeOfKey(cell);
    totalValueLength += cell.getValueLength();

    // Are we the first key in this block?
    if (firstCellInBlock == null) {
      // If cell is big, block will be closed and this firstCellInBlock reference will only last
      // a short while.
      firstCellInBlock = cell;
    }

    // TODO: What if cell is 10MB and we write infrequently? We hold on to cell here indefinitely?
    lastCell = cell;
    entryCount++;
    this.maxMemstoreTS = Math.max(this.maxMemstoreTS, cell.getSequenceId());
    int tagsLength = cell.getTagsLength();
    if (tagsLength > this.maxTagsLength) {
      this.maxTagsLength = tagsLength;
    }
  }

  @Override
  public void beforeShipped() throws IOException {
    this.blockWriter.beforeShipped();
    // Add clone methods for every cell
    if (this.lastCell != null) {
      this.lastCell = KeyValueUtil.toNewKeyCell(this.lastCell);
    }
    if (this.firstCellInBlock != null) {
      this.firstCellInBlock = KeyValueUtil.toNewKeyCell(this.firstCellInBlock);
    }
    if (this.lastCellOfPreviousBlock != null) {
      this.lastCellOfPreviousBlock = KeyValueUtil.toNewKeyCell(this.lastCellOfPreviousBlock);
    }
  }

  public Cell getLastCell() {
    return lastCell;
  }

  protected void finishFileInfo() throws IOException {
    if (lastCell != null) {
      // Make a copy. The copy is stuffed into our fileinfo map. Needs a clean
      // byte buffer. Won't take a tuple.
      byte [] lastKey = PrivateCellUtil.getCellKeySerializedAsKeyValueKey(this.lastCell);
      fileInfo.append(HFileInfo.LASTKEY, lastKey, false);
    }

    // Average key length.
    int avgKeyLen =
        entryCount == 0 ? 0 : (int) (totalKeyLength / entryCount);
    fileInfo.append(HFileInfo.AVG_KEY_LEN, Bytes.toBytes(avgKeyLen), false);
    fileInfo.append(HFileInfo.CREATE_TIME_TS, Bytes.toBytes(hFileContext.getFileCreateTime()),
        false);

    // Average value length.
    int avgValueLen =
        entryCount == 0 ? 0 : (int) (totalValueLength / entryCount);
    fileInfo.append(HFileInfo.AVG_VALUE_LEN, Bytes.toBytes(avgValueLen), false);
    if (hFileContext.isIncludesTags()) {
      // When tags are not being written in this file, MAX_TAGS_LEN is excluded
      // from the FileInfo
      fileInfo.append(HFileInfo.MAX_TAGS_LEN, Bytes.toBytes(this.maxTagsLength), false);
      boolean tagsCompressed = (hFileContext.getDataBlockEncoding() != DataBlockEncoding.NONE)
          && hFileContext.isCompressTags();
      fileInfo.append(HFileInfo.TAGS_COMPRESSED, Bytes.toBytes(tagsCompressed), false);
    }
  }

  protected int getMajorVersion() {
    return 3;
  }

  protected int getMinorVersion() {
    return HFileReaderImpl.MAX_MINOR_VERSION;
  }

  protected void finishClose(FixedFileTrailer trailer) throws IOException {
    // Write out encryption metadata before finalizing if we have a valid crypto context
    Encryption.Context cryptoContext = hFileContext.getEncryptionContext();
    if (cryptoContext != Encryption.Context.NONE) {
      // Wrap the context's key and write it as the encryption metadata, the wrapper includes
      // all information needed for decryption
      trailer.setEncryptionKey(EncryptionUtil.wrapKey(cryptoContext.getConf(),
          cryptoContext.getConf().get(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY,
              User.getCurrent().getShortName()),
          cryptoContext.getKey()));
    }
    // Now we can finish the close
    trailer.setMetaIndexCount(metaNames.size());
    trailer.setTotalUncompressedBytes(totalUncompressedBytes+ trailer.getTrailerSize());
    trailer.setEntryCount(entryCount);
    trailer.setCompressionCodec(hFileContext.getCompression());

    long startTime = System.currentTimeMillis();
    trailer.serialize(outputStream);
    HFile.updateWriteLatency(System.currentTimeMillis() - startTime);

    if (closeOutputStream) {
      outputStream.close();
      outputStream = null;
    }
  }
}
