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

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Optional;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.hbase.ByteBufferKeyOnlyKeyValue;
import org.apache.hudi.hbase.Cell;
import org.apache.hudi.hbase.CellComparator;
import org.apache.hudi.hbase.CellUtil;
import org.apache.hudi.hbase.HConstants;
import org.apache.hudi.hbase.KeyValue;
import org.apache.hudi.hbase.PrivateCellUtil;
import org.apache.hudi.hbase.SizeCachedByteBufferKeyValue;
import org.apache.hudi.hbase.SizeCachedKeyValue;
import org.apache.hudi.hbase.SizeCachedNoTagsByteBufferKeyValue;
import org.apache.hudi.hbase.SizeCachedNoTagsKeyValue;
import org.apache.hudi.hbase.io.compress.Compression;
import org.apache.hudi.hbase.io.encoding.DataBlockEncoder;
import org.apache.hudi.hbase.io.encoding.DataBlockEncoding;
import org.apache.hudi.hbase.io.encoding.HFileBlockDecodingContext;
import org.apache.hudi.hbase.nio.ByteBuff;
import org.apache.hudi.hbase.regionserver.KeyValueScanner;
import org.apache.hudi.hbase.trace.TraceUtil;
import org.apache.hudi.hbase.util.ByteBufferUtils;
import org.apache.hudi.hbase.util.Bytes;
import org.apache.hudi.hbase.util.IdLock;
import org.apache.hudi.hbase.util.ObjectIntPair;
import org.apache.hadoop.io.WritableUtils;
import org.apache.htrace.core.TraceScope;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation that can handle all hfile versions of {@link HFile.Reader}.
 */
@InterfaceAudience.Private
public abstract class HFileReaderImpl implements HFile.Reader, Configurable {
  // This class is HFileReaderV3 + HFileReaderV2 + AbstractHFileReader all squashed together into
  // one file.  Ditto for all the HFileReader.ScannerV? implementations. I was running up against
  // the MaxInlineLevel limit because too many tiers involved reading from an hfile. Was also hard
  // to navigate the source code when so many classes participating in read.
  private static final Logger LOG = LoggerFactory.getLogger(HFileReaderImpl.class);

  /** Data block index reader keeping the root data index in memory */
  protected HFileBlockIndex.CellBasedKeyBlockIndexReader dataBlockIndexReader;

  /** Meta block index reader -- always single level */
  protected HFileBlockIndex.ByteArrayKeyBlockIndexReader metaBlockIndexReader;

  protected FixedFileTrailer trailer;

  private final boolean primaryReplicaReader;

  /**
   * What kind of data block encoding should be used while reading, writing,
   * and handling cache.
   */
  protected HFileDataBlockEncoder dataBlockEncoder = NoOpDataBlockEncoder.INSTANCE;

  /** Block cache configuration. */
  protected final CacheConfig cacheConf;

  protected ReaderContext context;

  protected final HFileInfo fileInfo;

  /** Path of file */
  protected final Path path;

  /** File name to be used for block names */
  protected final String name;

  private Configuration conf;

  protected HFileContext hfileContext;

  /** Filesystem-level block reader. */
  protected HFileBlock.FSReader fsBlockReader;

  /**
   * A "sparse lock" implementation allowing to lock on a particular block
   * identified by offset. The purpose of this is to avoid two clients loading
   * the same block, and have all but one client wait to get the block from the
   * cache.
   */
  private IdLock offsetLock = new IdLock();

  /** Minimum minor version supported by this HFile format */
  static final int MIN_MINOR_VERSION = 0;

  /** Maximum minor version supported by this HFile format */
  // We went to version 2 when we moved to pb'ing fileinfo and the trailer on
  // the file. This version can read Writables version 1.
  static final int MAX_MINOR_VERSION = 3;

  /** Minor versions starting with this number have faked index key */
  static final int MINOR_VERSION_WITH_FAKED_KEY = 3;

  /**
   * Opens a HFile.
   * @param context Reader context info
   * @param fileInfo HFile info
   * @param cacheConf Cache configuration.
   * @param conf Configuration
   */
  public HFileReaderImpl(ReaderContext context, HFileInfo fileInfo, CacheConfig cacheConf,
                         Configuration conf) throws IOException {
    this.cacheConf = cacheConf;
    this.context = context;
    this.path = context.getFilePath();
    this.name = path.getName();
    this.conf = conf;
    this.primaryReplicaReader = context.isPrimaryReplicaReader();
    this.fileInfo = fileInfo;
    this.trailer = fileInfo.getTrailer();
    this.hfileContext = fileInfo.getHFileContext();
    this.fsBlockReader = new HFileBlock.FSReaderImpl(context, hfileContext,
        cacheConf.getByteBuffAllocator());
    this.dataBlockEncoder = HFileDataBlockEncoderImpl.createFromFileInfo(fileInfo);
    fsBlockReader.setDataBlockEncoder(dataBlockEncoder);
    dataBlockIndexReader = fileInfo.getDataBlockIndexReader();
    metaBlockIndexReader = fileInfo.getMetaBlockIndexReader();
  }

  @SuppressWarnings("serial")
  public static class BlockIndexNotLoadedException extends IllegalStateException {
    public BlockIndexNotLoadedException(Path path) {
      // Add a message in case anyone relies on it as opposed to class name.
      super(path + " block index not loaded");
    }
  }

  private Optional<String> toStringFirstKey() {
    return getFirstKey().map(CellUtil::getCellKeyAsString);
  }

  private Optional<String> toStringLastKey() {
    return getLastKey().map(CellUtil::getCellKeyAsString);
  }

  @Override
  public String toString() {
    return "reader=" + path.toString() +
        (!isFileInfoLoaded()? "":
            ", compression=" + trailer.getCompressionCodec().getName() +
                ", cacheConf=" + cacheConf +
                ", firstKey=" + toStringFirstKey() +
                ", lastKey=" + toStringLastKey()) +
        ", avgKeyLen=" + fileInfo.getAvgKeyLen() +
        ", avgValueLen=" + fileInfo.getAvgValueLen() +
        ", entries=" + trailer.getEntryCount() +
        ", length=" + context.getFileSize();
  }

  @Override
  public long length() {
    return context.getFileSize();
  }

  /**
   * @return the first key in the file. May be null if file has no entries. Note
   *         that this is not the first row key, but rather the byte form of the
   *         first KeyValue.
   */
  @Override
  public Optional<Cell> getFirstKey() {
    if (dataBlockIndexReader == null) {
      throw new BlockIndexNotLoadedException(path);
    }
    return dataBlockIndexReader.isEmpty() ? Optional.empty()
        : Optional.of(dataBlockIndexReader.getRootBlockKey(0));
  }

  /**
   * TODO left from {@link HFile} version 1: move this to StoreFile after Ryan's
   * patch goes in to eliminate {@link KeyValue} here.
   *
   * @return the first row key, or null if the file is empty.
   */
  @Override
  public Optional<byte[]> getFirstRowKey() {
    // We have to copy the row part to form the row key alone
    return getFirstKey().map(CellUtil::cloneRow);
  }

  /**
   * TODO left from {@link HFile} version 1: move this to StoreFile after
   * Ryan's patch goes in to eliminate {@link KeyValue} here.
   *
   * @return the last row key, or null if the file is empty.
   */
  @Override
  public Optional<byte[]> getLastRowKey() {
    // We have to copy the row part to form the row key alone
    return getLastKey().map(CellUtil::cloneRow);
  }

  /** @return number of KV entries in this HFile */
  @Override
  public long getEntries() {
    return trailer.getEntryCount();
  }

  /** @return comparator */
  @Override
  public CellComparator getComparator() {
    return this.hfileContext.getCellComparator();
  }

  public Compression.Algorithm getCompressionAlgorithm() {
    return trailer.getCompressionCodec();
  }

  /**
   * @return the total heap size of data and meta block indexes in bytes. Does
   *         not take into account non-root blocks of a multilevel data index.
   */
  @Override
  public long indexSize() {
    return (dataBlockIndexReader != null ? dataBlockIndexReader.heapSize() : 0)
        + ((metaBlockIndexReader != null) ? metaBlockIndexReader.heapSize()
        : 0);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void setDataBlockEncoder(HFileDataBlockEncoder dataBlockEncoder) {
    this.dataBlockEncoder = dataBlockEncoder;
    this.fsBlockReader.setDataBlockEncoder(dataBlockEncoder);
  }

  @Override
  public void setDataBlockIndexReader(HFileBlockIndex.CellBasedKeyBlockIndexReader reader) {
    this.dataBlockIndexReader = reader;
  }

  @Override
  public HFileBlockIndex.CellBasedKeyBlockIndexReader getDataBlockIndexReader() {
    return dataBlockIndexReader;
  }

  @Override
  public void setMetaBlockIndexReader(HFileBlockIndex.ByteArrayKeyBlockIndexReader reader) {
    this.metaBlockIndexReader = reader;
  }

  @Override
  public HFileBlockIndex.ByteArrayKeyBlockIndexReader getMetaBlockIndexReader() {
    return metaBlockIndexReader;
  }

  @Override
  public FixedFileTrailer getTrailer() {
    return trailer;
  }

  @Override
  public ReaderContext getContext() {
    return this.context;
  }

  @Override
  public HFileInfo getHFileInfo() {
    return this.fileInfo;
  }

  @Override
  public boolean isPrimaryReplicaReader() {
    return primaryReplicaReader;
  }

  /**
   * An exception thrown when an operation requiring a scanner to be seeked
   * is invoked on a scanner that is not seeked.
   */
  @SuppressWarnings("serial")
  public static class NotSeekedException extends IllegalStateException {
    public NotSeekedException(Path path) {
      super(path + " not seeked to a key/value");
    }
  }

  protected static class HFileScannerImpl implements HFileScanner {
    private ByteBuff blockBuffer;
    protected final boolean cacheBlocks;
    protected final boolean pread;
    protected final boolean isCompaction;
    private int currKeyLen;
    private int currValueLen;
    private int currMemstoreTSLen;
    private long currMemstoreTS;
    protected final HFile.Reader reader;
    private int currTagsLen;
    private short rowLen;
    // buffer backed keyonlyKV
    private ByteBufferKeyOnlyKeyValue bufBackedKeyOnlyKv = new ByteBufferKeyOnlyKeyValue();
    // A pair for reusing in blockSeek() so that we don't garbage lot of objects
    final ObjectIntPair<ByteBuffer> pair = new ObjectIntPair<>();

    /**
     * The next indexed key is to keep track of the indexed key of the next data block.
     * If the nextIndexedKey is HConstants.NO_NEXT_INDEXED_KEY, it means that the
     * current data block is the last data block.
     *
     * If the nextIndexedKey is null, it means the nextIndexedKey has not been loaded yet.
     */
    protected Cell nextIndexedKey;
    // Current block being used. NOTICE: DON't release curBlock separately except in shipped() or
    // close() methods. Because the shipped() or close() will do the release finally, even if any
    // exception occur the curBlock will be released by the close() method (see
    // RegionScannerImpl#handleException). Call the releaseIfNotCurBlock() to release the
    // unreferenced block please.
    protected HFileBlock curBlock;
    // Previous blocks that were used in the course of the read
    protected final ArrayList<HFileBlock> prevBlocks = new ArrayList<>();

    public HFileScannerImpl(final HFile.Reader reader, final boolean cacheBlocks,
                            final boolean pread, final boolean isCompaction) {
      this.reader = reader;
      this.cacheBlocks = cacheBlocks;
      this.pread = pread;
      this.isCompaction = isCompaction;
    }

    void updateCurrBlockRef(HFileBlock block) {
      if (block != null && curBlock != null && block.getOffset() == curBlock.getOffset()) {
        return;
      }
      if (this.curBlock != null && this.curBlock.isSharedMem()) {
        prevBlocks.add(this.curBlock);
      }
      this.curBlock = block;
    }

    void reset() {
      // We don't have to keep ref to heap block
      if (this.curBlock != null && this.curBlock.isSharedMem()) {
        this.prevBlocks.add(this.curBlock);
      }
      this.curBlock = null;
    }

    private void returnBlocks(boolean returnAll) {
      this.prevBlocks.forEach(HFileBlock::release);
      this.prevBlocks.clear();
      if (returnAll && this.curBlock != null) {
        this.curBlock.release();
        this.curBlock = null;
      }
    }

    @Override
    public boolean isSeeked(){
      return blockBuffer != null;
    }

    @Override
    public String toString() {
      return "HFileScanner for reader " + String.valueOf(getReader());
    }

    protected void assertSeeked() {
      if (!isSeeked()) {
        throw new NotSeekedException(reader.getPath());
      }
    }

    @Override
    public HFile.Reader getReader() {
      return reader;
    }

    // From non encoded HFiles, we always read back KeyValue or its descendant.(Note: When HFile
    // block is in DBB, it will be OffheapKV). So all parts of the Cell is in a contiguous
    // array/buffer. How many bytes we should wrap to make the KV is what this method returns.
    private int getKVBufSize() {
      int kvBufSize = KEY_VALUE_LEN_SIZE + currKeyLen + currValueLen;
      if (currTagsLen > 0) {
        kvBufSize += Bytes.SIZEOF_SHORT + currTagsLen;
      }
      return kvBufSize;
    }

    @Override
    public void close() {
      if (!pread) {
        // For seek + pread stream socket should be closed when the scanner is closed. HBASE-9393
        reader.unbufferStream();
      }
      this.returnBlocks(true);
    }

    // Returns the #bytes in HFile for the current cell. Used to skip these many bytes in current
    // HFile block's buffer so as to position to the next cell.
    private int getCurCellSerializedSize() {
      int curCellSize =  KEY_VALUE_LEN_SIZE + currKeyLen + currValueLen
          + currMemstoreTSLen;
      if (this.reader.getFileContext().isIncludesTags()) {
        curCellSize += Bytes.SIZEOF_SHORT + currTagsLen;
      }
      return curCellSize;
    }

    protected void readKeyValueLen() {
      // This is a hot method. We go out of our way to make this method short so it can be
      // inlined and is not too big to compile. We also manage position in ByteBuffer ourselves
      // because it is faster than going via range-checked ByteBuffer methods or going through a
      // byte buffer array a byte at a time.
      // Get a long at a time rather than read two individual ints. In micro-benchmarking, even
      // with the extra bit-fiddling, this is order-of-magnitude faster than getting two ints.
      // Trying to imitate what was done - need to profile if this is better or
      // earlier way is better by doing mark and reset?
      // But ensure that you read long instead of two ints
      long ll = blockBuffer.getLongAfterPosition(0);
      // Read top half as an int of key length and bottom int as value length
      this.currKeyLen = (int)(ll >> Integer.SIZE);
      this.currValueLen = (int)(Bytes.MASK_FOR_LOWER_INT_IN_LONG ^ ll);
      checkKeyValueLen();
      this.rowLen = blockBuffer.getShortAfterPosition(Bytes.SIZEOF_LONG);
      // Move position past the key and value lengths and then beyond the key and value
      int p = (Bytes.SIZEOF_LONG + currKeyLen + currValueLen);
      if (reader.getFileContext().isIncludesTags()) {
        // Tags length is a short.
        this.currTagsLen = blockBuffer.getShortAfterPosition(p);
        checkTagsLen();
        p += (Bytes.SIZEOF_SHORT + currTagsLen);
      }
      readMvccVersion(p);
    }

    private final void checkTagsLen() {
      if (checkLen(this.currTagsLen)) {
        throw new IllegalStateException("Invalid currTagsLen " + this.currTagsLen +
            ". Block offset: " + curBlock.getOffset() + ", block length: " +
            this.blockBuffer.limit() +
            ", position: " + this.blockBuffer.position() + " (without header)." +
            " path=" + reader.getPath());
      }
    }

    /**
     * Read mvcc. Does checks to see if we even need to read the mvcc at all.
     */
    protected void readMvccVersion(final int offsetFromPos) {
      // See if we even need to decode mvcc.
      if (!this.reader.getHFileInfo().shouldIncludeMemStoreTS()) {
        return;
      }
      if (!this.reader.getHFileInfo().isDecodeMemstoreTS()) {
        currMemstoreTS = 0;
        currMemstoreTSLen = 1;
        return;
      }
      _readMvccVersion(offsetFromPos);
    }

    /**
     * Actually do the mvcc read. Does no checks.
     */
    private void _readMvccVersion(int offsetFromPos) {
      // This is Bytes#bytesToVint inlined so can save a few instructions in this hot method; i.e.
      // previous if one-byte vint, we'd redo the vint call to find int size.
      // Also the method is kept small so can be inlined.
      byte firstByte = blockBuffer.getByteAfterPosition(offsetFromPos);
      int len = WritableUtils.decodeVIntSize(firstByte);
      if (len == 1) {
        this.currMemstoreTS = firstByte;
      } else {
        int remaining = len -1;
        long i = 0;
        offsetFromPos++;
        if (remaining >= Bytes.SIZEOF_INT) {
          // The int read has to be converted to unsigned long so the & op
          i = (blockBuffer.getIntAfterPosition(offsetFromPos) & 0x00000000ffffffffL);
          remaining -= Bytes.SIZEOF_INT;
          offsetFromPos += Bytes.SIZEOF_INT;
        }
        if (remaining >= Bytes.SIZEOF_SHORT) {
          short s = blockBuffer.getShortAfterPosition(offsetFromPos);
          i = i << 16;
          i = i | (s & 0xFFFF);
          remaining -= Bytes.SIZEOF_SHORT;
          offsetFromPos += Bytes.SIZEOF_SHORT;
        }
        for (int idx = 0; idx < remaining; idx++) {
          byte b = blockBuffer.getByteAfterPosition(offsetFromPos + idx);
          i = i << 8;
          i = i | (b & 0xFF);
        }
        currMemstoreTS = (WritableUtils.isNegativeVInt(firstByte) ? ~i : i);
      }
      this.currMemstoreTSLen = len;
    }

    /**
     * Within a loaded block, seek looking for the last key that is smaller than
     * (or equal to?) the key we are interested in.
     * A note on the seekBefore: if you have seekBefore = true, AND the first
     * key in the block = key, then you'll get thrown exceptions. The caller has
     * to check for that case and load the previous block as appropriate.
     * @param key
     *          the key to find
     * @param seekBefore
     *          find the key before the given key in case of exact match.
     * @return 0 in case of an exact key match, 1 in case of an inexact match,
     *         -2 in case of an inexact match and furthermore, the input key
     *         less than the first key of current block(e.g. using a faked index
     *         key)
     */
    protected int blockSeek(Cell key, boolean seekBefore) {
      int klen, vlen, tlen = 0;
      int lastKeyValueSize = -1;
      int offsetFromPos;
      do {
        offsetFromPos = 0;
        // Better to ensure that we use the BB Utils here
        long ll = blockBuffer.getLongAfterPosition(offsetFromPos);
        klen = (int)(ll >> Integer.SIZE);
        vlen = (int)(Bytes.MASK_FOR_LOWER_INT_IN_LONG ^ ll);
        if (checkKeyLen(klen) || checkLen(vlen)) {
          throw new IllegalStateException("Invalid klen " + klen + " or vlen "
              + vlen + ". Block offset: "
              + curBlock.getOffset() + ", block length: " + blockBuffer.limit() + ", position: "
              + blockBuffer.position() + " (without header)."
              + " path=" + reader.getPath());
        }
        offsetFromPos += Bytes.SIZEOF_LONG;
        this.rowLen = blockBuffer.getShortAfterPosition(offsetFromPos);
        blockBuffer.asSubByteBuffer(blockBuffer.position() + offsetFromPos, klen, pair);
        bufBackedKeyOnlyKv.setKey(pair.getFirst(), pair.getSecond(), klen, rowLen);
        int comp =
            PrivateCellUtil.compareKeyIgnoresMvcc(reader.getComparator(), key, bufBackedKeyOnlyKv);
        offsetFromPos += klen + vlen;
        if (this.reader.getFileContext().isIncludesTags()) {
          // Read short as unsigned, high byte first
          tlen = ((blockBuffer.getByteAfterPosition(offsetFromPos) & 0xff) << 8)
              ^ (blockBuffer.getByteAfterPosition(offsetFromPos + 1) & 0xff);
          if (checkLen(tlen)) {
            throw new IllegalStateException("Invalid tlen " + tlen + ". Block offset: "
                + curBlock.getOffset() + ", block length: " + blockBuffer.limit() + ", position: "
                + blockBuffer.position() + " (without header)."
                + " path=" + reader.getPath());
          }
          // add the two bytes read for the tags.
          offsetFromPos += tlen + (Bytes.SIZEOF_SHORT);
        }
        if (this.reader.getHFileInfo().shouldIncludeMemStoreTS()) {
          // Directly read the mvcc based on current position
          readMvccVersion(offsetFromPos);
        }
        if (comp == 0) {
          if (seekBefore) {
            if (lastKeyValueSize < 0) {
              throw new IllegalStateException("blockSeek with seekBefore "
                  + "at the first key of the block: key=" + CellUtil.getCellKeyAsString(key)
                  + ", blockOffset=" + curBlock.getOffset() + ", onDiskSize="
                  + curBlock.getOnDiskSizeWithHeader()
                  + ", path=" + reader.getPath());
            }
            blockBuffer.moveBack(lastKeyValueSize);
            readKeyValueLen();
            return 1; // non exact match.
          }
          currKeyLen = klen;
          currValueLen = vlen;
          currTagsLen = tlen;
          return 0; // indicate exact match
        } else if (comp < 0) {
          if (lastKeyValueSize > 0) {
            blockBuffer.moveBack(lastKeyValueSize);
          }
          readKeyValueLen();
          if (lastKeyValueSize == -1 && blockBuffer.position() == 0) {
            return HConstants.INDEX_KEY_MAGIC;
          }
          return 1;
        }
        // The size of this key/value tuple, including key/value length fields.
        lastKeyValueSize = klen + vlen + currMemstoreTSLen + KEY_VALUE_LEN_SIZE;
        // include tag length also if tags included with KV
        if (reader.getFileContext().isIncludesTags()) {
          lastKeyValueSize += tlen + Bytes.SIZEOF_SHORT;
        }
        blockBuffer.skip(lastKeyValueSize);
      } while (blockBuffer.hasRemaining());

      // Seek to the last key we successfully read. This will happen if this is
      // the last key/value pair in the file, in which case the following call
      // to next() has to return false.
      blockBuffer.moveBack(lastKeyValueSize);
      readKeyValueLen();
      return 1; // didn't exactly find it.
    }

    @Override
    public Cell getNextIndexedKey() {
      return nextIndexedKey;
    }

    @Override
    public int seekTo(Cell key) throws IOException {
      return seekTo(key, true);
    }

    @Override
    public int reseekTo(Cell key) throws IOException {
      int compared;
      if (isSeeked()) {
        compared = compareKey(reader.getComparator(), key);
        if (compared < 1) {
          // If the required key is less than or equal to current key, then
          // don't do anything.
          return compared;
        } else {
          // The comparison with no_next_index_key has to be checked
          if (this.nextIndexedKey != null &&
              (this.nextIndexedKey == KeyValueScanner.NO_NEXT_INDEXED_KEY || PrivateCellUtil
                  .compareKeyIgnoresMvcc(reader.getComparator(), key, nextIndexedKey) < 0)) {
            // The reader shall continue to scan the current data block instead
            // of querying the
            // block index as long as it knows the target key is strictly
            // smaller than
            // the next indexed key or the current data block is the last data
            // block.
            return loadBlockAndSeekToKey(this.curBlock, nextIndexedKey, false, key,
                false);
          }
        }
      }
      // Don't rewind on a reseek operation, because reseek implies that we are
      // always going forward in the file.
      return seekTo(key, false);
    }

    /**
     * An internal API function. Seek to the given key, optionally rewinding to
     * the first key of the block before doing the seek.
     *
     * @param key - a cell representing the key that we need to fetch
     * @param rewind whether to rewind to the first key of the block before
     *        doing the seek. If this is false, we are assuming we never go
     *        back, otherwise the result is undefined.
     * @return -1 if the key is earlier than the first key of the file,
     *         0 if we are at the given key, 1 if we are past the given key
     *         -2 if the key is earlier than the first key of the file while
     *         using a faked index key
     */
    public int seekTo(Cell key, boolean rewind) throws IOException {
      HFileBlockIndex.BlockIndexReader indexReader = reader.getDataBlockIndexReader();
      BlockWithScanInfo blockWithScanInfo = indexReader.loadDataBlockWithScanInfo(key, curBlock,
          cacheBlocks, pread, isCompaction, getEffectiveDataBlockEncoding(), reader);
      if (blockWithScanInfo == null || blockWithScanInfo.getHFileBlock() == null) {
        // This happens if the key e.g. falls before the beginning of the file.
        return -1;
      }
      return loadBlockAndSeekToKey(blockWithScanInfo.getHFileBlock(),
          blockWithScanInfo.getNextIndexedKey(), rewind, key, false);
    }

    @Override
    public boolean seekBefore(Cell key) throws IOException {
      HFileBlock seekToBlock = reader.getDataBlockIndexReader().seekToDataBlock(key, curBlock,
          cacheBlocks, pread, isCompaction, reader.getEffectiveEncodingInCache(isCompaction),
          reader);
      if (seekToBlock == null) {
        return false;
      }
      Cell firstKey = getFirstKeyCellInBlock(seekToBlock);
      if (PrivateCellUtil.compareKeyIgnoresMvcc(reader.getComparator(), firstKey, key) >= 0) {
        long previousBlockOffset = seekToBlock.getPrevBlockOffset();
        // The key we are interested in
        if (previousBlockOffset == -1) {
          // we have a 'problem', the key we want is the first of the file.
          releaseIfNotCurBlock(seekToBlock);
          return false;
        }

        // The first key in the current block 'seekToBlock' is greater than the given
        // seekBefore key. We will go ahead by reading the next block that satisfies the
        // given key. Return the current block before reading the next one.
        releaseIfNotCurBlock(seekToBlock);
        // It is important that we compute and pass onDiskSize to the block
        // reader so that it does not have to read the header separately to
        // figure out the size. Currently, we do not have a way to do this
        // correctly in the general case however.
        // TODO: See https://issues.apache.org/jira/browse/HBASE-14576
        int prevBlockSize = -1;
        seekToBlock = reader.readBlock(previousBlockOffset, prevBlockSize, cacheBlocks, pread,
            isCompaction, true, BlockType.DATA, getEffectiveDataBlockEncoding());
        // TODO shortcut: seek forward in this block to the last key of the
        // block.
      }
      loadBlockAndSeekToKey(seekToBlock, firstKey, true, key, true);
      return true;
    }

    /**
     * The curBlock will be released by shipping or close method, so only need to consider releasing
     * the block, which was read from HFile before and not referenced by curBlock.
     */
    protected void releaseIfNotCurBlock(HFileBlock block) {
      if (curBlock != block) {
        block.release();
      }
    }

    /**
     * Scans blocks in the "scanned" section of the {@link HFile} until the next
     * data block is found.
     *
     * @return the next block, or null if there are no more data blocks
     */
    protected HFileBlock readNextDataBlock() throws IOException {
      long lastDataBlockOffset = reader.getTrailer().getLastDataBlockOffset();
      if (curBlock == null) {
        return null;
      }
      HFileBlock block = this.curBlock;
      do {
        if (block.getOffset() >= lastDataBlockOffset) {
          releaseIfNotCurBlock(block);
          return null;
        }
        if (block.getOffset() < 0) {
          releaseIfNotCurBlock(block);
          throw new IOException("Invalid block offset=" + block + ", path=" + reader.getPath());
        }
        // We are reading the next block without block type validation, because
        // it might turn out to be a non-data block.
        block = reader.readBlock(block.getOffset() + block.getOnDiskSizeWithHeader(),
            block.getNextBlockOnDiskSize(), cacheBlocks, pread, isCompaction, true, null,
            getEffectiveDataBlockEncoding());
        if (block != null && !block.getBlockType().isData()) {
          // Whatever block we read we will be returning it unless
          // it is a datablock. Just in case the blocks are non data blocks
          block.release();
        }
      } while (!block.getBlockType().isData());
      return block;
    }

    public DataBlockEncoding getEffectiveDataBlockEncoding() {
      return this.reader.getEffectiveEncodingInCache(isCompaction);
    }

    @Override
    public Cell getCell() {
      if (!isSeeked()) {
        return null;
      }

      Cell ret;
      int cellBufSize = getKVBufSize();
      long seqId = 0L;
      if (this.reader.getHFileInfo().shouldIncludeMemStoreTS()) {
        seqId = currMemstoreTS;
      }
      if (blockBuffer.hasArray()) {
        // TODO : reduce the varieties of KV here. Check if based on a boolean
        // we can handle the 'no tags' case.
        if (currTagsLen > 0) {
          ret = new SizeCachedKeyValue(blockBuffer.array(),
              blockBuffer.arrayOffset() + blockBuffer.position(), cellBufSize, seqId, currKeyLen,
              rowLen);
        } else {
          ret = new SizeCachedNoTagsKeyValue(blockBuffer.array(),
              blockBuffer.arrayOffset() + blockBuffer.position(), cellBufSize, seqId, currKeyLen,
              rowLen);
        }
      } else {
        ByteBuffer buf = blockBuffer.asSubByteBuffer(cellBufSize);
        if (buf.isDirect()) {
          ret = currTagsLen > 0
              ? new SizeCachedByteBufferKeyValue(buf, buf.position(), cellBufSize, seqId,
              currKeyLen, rowLen)
              : new SizeCachedNoTagsByteBufferKeyValue(buf, buf.position(), cellBufSize, seqId,
              currKeyLen, rowLen);
        } else {
          if (currTagsLen > 0) {
            ret = new SizeCachedKeyValue(buf.array(), buf.arrayOffset() + buf.position(),
                cellBufSize, seqId, currKeyLen, rowLen);
          } else {
            ret = new SizeCachedNoTagsKeyValue(buf.array(), buf.arrayOffset() + buf.position(),
                cellBufSize, seqId, currKeyLen, rowLen);
          }
        }
      }
      return ret;
    }

    @Override
    public Cell getKey() {
      assertSeeked();
      // Create a new object so that this getKey is cached as firstKey, lastKey
      ObjectIntPair<ByteBuffer> keyPair = new ObjectIntPair<>();
      blockBuffer.asSubByteBuffer(blockBuffer.position() + KEY_VALUE_LEN_SIZE, currKeyLen, keyPair);
      ByteBuffer keyBuf = keyPair.getFirst();
      if (keyBuf.hasArray()) {
        return new KeyValue.KeyOnlyKeyValue(keyBuf.array(), keyBuf.arrayOffset()
            + keyPair.getSecond(), currKeyLen);
      } else {
        // Better to do a copy here instead of holding on to this BB so that
        // we could release the blocks referring to this key. This key is specifically used
        // in HalfStoreFileReader to get the firstkey and lastkey by creating a new scanner
        // every time. So holding onto the BB (incase of DBB) is not advised here.
        byte[] key = new byte[currKeyLen];
        ByteBufferUtils.copyFromBufferToArray(key, keyBuf, keyPair.getSecond(), 0, currKeyLen);
        return new KeyValue.KeyOnlyKeyValue(key, 0, currKeyLen);
      }
    }

    @Override
    public ByteBuffer getValue() {
      assertSeeked();
      // Okie to create new Pair. Not used in hot path
      ObjectIntPair<ByteBuffer> valuePair = new ObjectIntPair<>();
      this.blockBuffer.asSubByteBuffer(blockBuffer.position() + KEY_VALUE_LEN_SIZE + currKeyLen,
          currValueLen, valuePair);
      ByteBuffer valBuf = valuePair.getFirst().duplicate();
      valBuf.position(valuePair.getSecond());
      valBuf.limit(currValueLen + valuePair.getSecond());
      return valBuf.slice();
    }

    protected void setNonSeekedState() {
      reset();
      blockBuffer = null;
      currKeyLen = 0;
      currValueLen = 0;
      currMemstoreTS = 0;
      currMemstoreTSLen = 0;
      currTagsLen = 0;
    }

    /**
     * Set the position on current backing blockBuffer.
     */
    private void positionThisBlockBuffer() {
      try {
        blockBuffer.skip(getCurCellSerializedSize());
      } catch (IllegalArgumentException e) {
        LOG.error("Current pos = " + blockBuffer.position()
            + "; currKeyLen = " + currKeyLen + "; currValLen = "
            + currValueLen + "; block limit = " + blockBuffer.limit()
            + "; currBlock currBlockOffset = " + this.curBlock.getOffset()
            + "; path=" + reader.getPath());
        throw e;
      }
    }

    /**
     * Set our selves up for the next 'next' invocation, set up next block.
     * @return True is more to read else false if at the end.
     */
    private boolean positionForNextBlock() throws IOException {
      // Methods are small so they get inlined because they are 'hot'.
      long lastDataBlockOffset = reader.getTrailer().getLastDataBlockOffset();
      if (this.curBlock.getOffset() >= lastDataBlockOffset) {
        setNonSeekedState();
        return false;
      }
      return isNextBlock();
    }


    private boolean isNextBlock() throws IOException {
      // Methods are small so they get inlined because they are 'hot'.
      HFileBlock nextBlock = readNextDataBlock();
      if (nextBlock == null) {
        setNonSeekedState();
        return false;
      }
      updateCurrentBlock(nextBlock);
      return true;
    }

    private final boolean _next() throws IOException {
      // Small method so can be inlined. It is a hot one.
      if (blockBuffer.remaining() <= 0) {
        return positionForNextBlock();
      }

      // We are still in the same block.
      readKeyValueLen();
      return true;
    }

    /**
     * Go to the next key/value in the block section. Loads the next block if
     * necessary. If successful, {@link #getKey()} and {@link #getValue()} can
     * be called.
     *
     * @return true if successfully navigated to the next key/value
     */
    @Override
    public boolean next() throws IOException {
      // This is a hot method so extreme measures taken to ensure it is small and inlineable.
      // Checked by setting: -XX:+UnlockDiagnosticVMOptions -XX:+PrintInlining -XX:+PrintCompilation
      assertSeeked();
      positionThisBlockBuffer();
      return _next();
    }

    /**
     * Positions this scanner at the start of the file.
     *
     * @return false if empty file; i.e. a call to next would return false and
     *         the current key and value are undefined.
     */
    @Override
    public boolean seekTo() throws IOException {
      if (reader == null) {
        return false;
      }

      if (reader.getTrailer().getEntryCount() == 0) {
        // No data blocks.
        return false;
      }

      long firstDataBlockOffset = reader.getTrailer().getFirstDataBlockOffset();
      if (curBlock != null && curBlock.getOffset() == firstDataBlockOffset) {
        return processFirstDataBlock();
      }

      readAndUpdateNewBlock(firstDataBlockOffset);
      return true;
    }

    protected boolean processFirstDataBlock() throws IOException{
      blockBuffer.rewind();
      readKeyValueLen();
      return true;
    }

    protected void readAndUpdateNewBlock(long firstDataBlockOffset) throws IOException {
      HFileBlock newBlock = reader.readBlock(firstDataBlockOffset, -1, cacheBlocks, pread,
          isCompaction, true, BlockType.DATA, getEffectiveDataBlockEncoding());
      if (newBlock.getOffset() < 0) {
        releaseIfNotCurBlock(newBlock);
        throw new IOException("Invalid offset=" + newBlock.getOffset() +
            ", path=" + reader.getPath());
      }
      updateCurrentBlock(newBlock);
    }

    protected int loadBlockAndSeekToKey(HFileBlock seekToBlock, Cell nextIndexedKey, boolean rewind,
                                        Cell key, boolean seekBefore) throws IOException {
      if (this.curBlock == null || this.curBlock.getOffset() != seekToBlock.getOffset()) {
        updateCurrentBlock(seekToBlock);
      } else if (rewind) {
        blockBuffer.rewind();
      }
      // Update the nextIndexedKey
      this.nextIndexedKey = nextIndexedKey;
      return blockSeek(key, seekBefore);
    }

    /**
     * @return True if v &lt;= 0 or v &gt; current block buffer limit.
     */
    protected final boolean checkKeyLen(final int v) {
      return v <= 0 || v > this.blockBuffer.limit();
    }

    /**
     * @return True if v &lt; 0 or v &gt; current block buffer limit.
     */
    protected final boolean checkLen(final int v) {
      return v < 0 || v > this.blockBuffer.limit();
    }

    /**
     * Check key and value lengths are wholesome.
     */
    protected final void checkKeyValueLen() {
      if (checkKeyLen(this.currKeyLen) || checkLen(this.currValueLen)) {
        throw new IllegalStateException("Invalid currKeyLen " + this.currKeyLen
            + " or currValueLen " + this.currValueLen + ". Block offset: "
            + this.curBlock.getOffset() + ", block length: "
            + this.blockBuffer.limit() + ", position: " + this.blockBuffer.position()
            + " (without header)." + ", path=" + reader.getPath());
      }
    }

    /**
     * Updates the current block to be the given {@link HFileBlock}. Seeks to the the first
     * key/value pair.
     * @param newBlock the block read by {@link HFileReaderImpl#readBlock}, it's a totally new block
     *          with new allocated {@link ByteBuff}, so if no further reference to this block, we
     *          should release it carefully.
     */
    protected void updateCurrentBlock(HFileBlock newBlock) throws IOException {
      try {
        if (newBlock.getBlockType() != BlockType.DATA) {
          throw new IllegalStateException(
              "ScannerV2 works only on data blocks, got " + newBlock.getBlockType() + "; "
                  + "HFileName=" + reader.getPath() + ", " + "dataBlockEncoder="
                  + reader.getDataBlockEncoding() + ", " + "isCompaction=" + isCompaction);
        }
        updateCurrBlockRef(newBlock);
        blockBuffer = newBlock.getBufferWithoutHeader();
        readKeyValueLen();
      } finally {
        releaseIfNotCurBlock(newBlock);
      }
      // Reset the next indexed key
      this.nextIndexedKey = null;
    }

    protected Cell getFirstKeyCellInBlock(HFileBlock curBlock) {
      ByteBuff buffer = curBlock.getBufferWithoutHeader();
      // It is safe to manipulate this buffer because we own the buffer object.
      buffer.rewind();
      int klen = buffer.getInt();
      buffer.skip(Bytes.SIZEOF_INT);// Skip value len part
      ByteBuffer keyBuff = buffer.asSubByteBuffer(klen);
      if (keyBuff.hasArray()) {
        return new KeyValue.KeyOnlyKeyValue(keyBuff.array(), keyBuff.arrayOffset()
            + keyBuff.position(), klen);
      } else {
        return new ByteBufferKeyOnlyKeyValue(keyBuff, keyBuff.position(), klen);
      }
    }

    @Override
    public String getKeyString() {
      return CellUtil.toString(getKey(), false);
    }

    @Override
    public String getValueString() {
      return ByteBufferUtils.toStringBinary(getValue());
    }

    public int compareKey(CellComparator comparator, Cell key) {
      blockBuffer.asSubByteBuffer(blockBuffer.position() + KEY_VALUE_LEN_SIZE, currKeyLen, pair);
      this.bufBackedKeyOnlyKv.setKey(pair.getFirst(), pair.getSecond(), currKeyLen, rowLen);
      return PrivateCellUtil.compareKeyIgnoresMvcc(comparator, key, this.bufBackedKeyOnlyKv);
    }

    @Override
    public void shipped() throws IOException {
      this.returnBlocks(false);
    }
  }

  @Override
  public Path getPath() {
    return path;
  }

  @Override
  public DataBlockEncoding getDataBlockEncoding() {
    return dataBlockEncoder.getDataBlockEncoding();
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /** Minor versions in HFile starting with this number have hbase checksums */
  public static final int MINOR_VERSION_WITH_CHECKSUM = 1;
  /** In HFile minor version that does not support checksums */
  public static final int MINOR_VERSION_NO_CHECKSUM = 0;

  /** HFile minor version that introduced pbuf filetrailer */
  public static final int PBUF_TRAILER_MINOR_VERSION = 2;

  /**
   * The size of a (key length, value length) tuple that prefixes each entry in
   * a data block.
   */
  public final static int KEY_VALUE_LEN_SIZE = 2 * Bytes.SIZEOF_INT;

  /**
   * Retrieve block from cache. Validates the retrieved block's type vs {@code expectedBlockType}
   * and its encoding vs. {@code expectedDataBlockEncoding}. Unpacks the block as necessary.
   */
  private HFileBlock getCachedBlock(BlockCacheKey cacheKey, boolean cacheBlock, boolean useLock,
                                    boolean isCompaction, boolean updateCacheMetrics, BlockType expectedBlockType,
                                    DataBlockEncoding expectedDataBlockEncoding) throws IOException {
    // Check cache for block. If found return.
    BlockCache cache = cacheConf.getBlockCache().orElse(null);
    if (cache != null) {
      HFileBlock cachedBlock =
          (HFileBlock) cache.getBlock(cacheKey, cacheBlock, useLock, updateCacheMetrics);
      if (cachedBlock != null) {
        if (cacheConf.shouldCacheCompressed(cachedBlock.getBlockType().getCategory())) {
          HFileBlock compressedBlock = cachedBlock;
          cachedBlock = compressedBlock.unpack(hfileContext, fsBlockReader);
          // In case of compressed block after unpacking we can release the compressed block
          if (compressedBlock != cachedBlock) {
            compressedBlock.release();
          }
        }
        try {
          validateBlockType(cachedBlock, expectedBlockType);
        } catch (IOException e) {
          returnAndEvictBlock(cache, cacheKey, cachedBlock);
          throw e;
        }

        if (expectedDataBlockEncoding == null) {
          return cachedBlock;
        }
        DataBlockEncoding actualDataBlockEncoding = cachedBlock.getDataBlockEncoding();
        // Block types other than data blocks always have
        // DataBlockEncoding.NONE. To avoid false negative cache misses, only
        // perform this check if cached block is a data block.
        if (cachedBlock.getBlockType().isData() &&
            !actualDataBlockEncoding.equals(expectedDataBlockEncoding)) {
          // This mismatch may happen if a Scanner, which is used for say a
          // compaction, tries to read an encoded block from the block cache.
          // The reverse might happen when an EncodedScanner tries to read
          // un-encoded blocks which were cached earlier.
          //
          // Because returning a data block with an implicit BlockType mismatch
          // will cause the requesting scanner to throw a disk read should be
          // forced here. This will potentially cause a significant number of
          // cache misses, so update so we should keep track of this as it might
          // justify the work on a CompoundScanner.
          if (!expectedDataBlockEncoding.equals(DataBlockEncoding.NONE) &&
              !actualDataBlockEncoding.equals(DataBlockEncoding.NONE)) {
            // If the block is encoded but the encoding does not match the
            // expected encoding it is likely the encoding was changed but the
            // block was not yet evicted. Evictions on file close happen async
            // so blocks with the old encoding still linger in cache for some
            // period of time. This event should be rare as it only happens on
            // schema definition change.
            LOG.info("Evicting cached block with key {} because data block encoding mismatch; " +
                    "expected {}, actual {}, path={}", cacheKey, actualDataBlockEncoding,
                expectedDataBlockEncoding, path);
            // This is an error scenario. so here we need to release the block.
            returnAndEvictBlock(cache, cacheKey, cachedBlock);
          }
          return null;
        }
        return cachedBlock;
      }
    }
    return null;
  }

  private void returnAndEvictBlock(BlockCache cache, BlockCacheKey cacheKey, Cacheable block) {
    block.release();
    cache.evictBlock(cacheKey);
  }

  /**
   * @param cacheBlock Add block to cache, if found
   * @return block wrapped in a ByteBuffer, with header skipped
   */
  @Override
  public HFileBlock getMetaBlock(String metaBlockName, boolean cacheBlock)
      throws IOException {
    if (trailer.getMetaIndexCount() == 0) {
      return null; // there are no meta blocks
    }
    if (metaBlockIndexReader == null) {
      throw new IOException(path + " meta index not loaded");
    }

    byte[] mbname = Bytes.toBytes(metaBlockName);
    int block = metaBlockIndexReader.rootBlockContainingKey(mbname,
        0, mbname.length);
    if (block == -1) {
      return null;
    }
    long blockSize = metaBlockIndexReader.getRootBlockDataSize(block);

    // Per meta key from any given file, synchronize reads for said block. This
    // is OK to do for meta blocks because the meta block index is always
    // single-level.
    synchronized (metaBlockIndexReader.getRootBlockKey(block)) {
      // Check cache for block. If found return.
      long metaBlockOffset = metaBlockIndexReader.getRootBlockOffset(block);
      BlockCacheKey cacheKey =
          new BlockCacheKey(name, metaBlockOffset, this.isPrimaryReplicaReader(), BlockType.META);

      cacheBlock &= cacheConf.shouldCacheBlockOnRead(BlockType.META.getCategory());
      HFileBlock cachedBlock =
          getCachedBlock(cacheKey, cacheBlock, false, true, true, BlockType.META, null);
      if (cachedBlock != null) {
        assert cachedBlock.isUnpacked() : "Packed block leak.";
        // Return a distinct 'shallow copy' of the block,
        // so pos does not get messed by the scanner
        return cachedBlock;
      }
      // Cache Miss, please load.

      HFileBlock compressedBlock =
          fsBlockReader.readBlockData(metaBlockOffset, blockSize, true, false, true);
      HFileBlock uncompressedBlock = compressedBlock.unpack(hfileContext, fsBlockReader);
      if (compressedBlock != uncompressedBlock) {
        compressedBlock.release();
      }

      // Cache the block
      if (cacheBlock) {
        cacheConf.getBlockCache().ifPresent(
            cache -> cache.cacheBlock(cacheKey, uncompressedBlock, cacheConf.isInMemory()));
      }
      return uncompressedBlock;
    }
  }

  /**
   * If expected block is data block, we'll allocate the ByteBuff of block from
   * {@link org.apache.hudi.hbase.io.ByteBuffAllocator} and it's usually an off-heap one,
   * otherwise it will allocate from heap.
   * @see org.apache.hudi.hbase.io.hfile.HFileBlock.FSReader#readBlockData(long, long, boolean,
   *      boolean, boolean)
   */
  private boolean shouldUseHeap(BlockType expectedBlockType) {
    if (!cacheConf.getBlockCache().isPresent()) {
      return false;
    } else if (!cacheConf.isCombinedBlockCache()) {
      // Block to cache in LruBlockCache must be an heap one. So just allocate block memory from
      // heap for saving an extra off-heap to heap copying.
      return true;
    }
    return expectedBlockType != null && !expectedBlockType.isData();
  }

  @Override
  public HFileBlock readBlock(long dataBlockOffset, long onDiskBlockSize,
                              final boolean cacheBlock, boolean pread, final boolean isCompaction,
                              boolean updateCacheMetrics, BlockType expectedBlockType,
                              DataBlockEncoding expectedDataBlockEncoding)
      throws IOException {
    if (dataBlockIndexReader == null) {
      throw new IOException(path + " block index not loaded");
    }
    long trailerOffset = trailer.getLoadOnOpenDataOffset();
    if (dataBlockOffset < 0 || dataBlockOffset >= trailerOffset) {
      throw new IOException("Requested block is out of range: " + dataBlockOffset +
          ", lastDataBlockOffset: " + trailer.getLastDataBlockOffset() +
          ", trailer.getLoadOnOpenDataOffset: " + trailerOffset +
          ", path=" + path);
    }
    // For any given block from any given file, synchronize reads for said
    // block.
    // Without a cache, this synchronizing is needless overhead, but really
    // the other choice is to duplicate work (which the cache would prevent you
    // from doing).

    BlockCacheKey cacheKey = new BlockCacheKey(name, dataBlockOffset,
        this.isPrimaryReplicaReader(), expectedBlockType);

    boolean useLock = false;
    IdLock.Entry lockEntry = null;
    //try (TraceScope traceScope = TraceUtil.createTrace("HFileReaderImpl.readBlock")) {
    try (TraceScope traceScope = null) {
      while (true) {
        // Check cache for block. If found return.
        if (cacheConf.shouldReadBlockFromCache(expectedBlockType)) {
          if (useLock) {
            lockEntry = offsetLock.getLockEntry(dataBlockOffset);
          }
          // Try and get the block from the block cache. If the useLock variable is true then this
          // is the second time through the loop and it should not be counted as a block cache miss.
          HFileBlock cachedBlock = getCachedBlock(cacheKey, cacheBlock, useLock, isCompaction,
              updateCacheMetrics, expectedBlockType, expectedDataBlockEncoding);
          if (cachedBlock != null) {
            if (LOG.isTraceEnabled()) {
              LOG.trace("From Cache " + cachedBlock);
            }
            //TraceUtil.addTimelineAnnotation("blockCacheHit");
            assert cachedBlock.isUnpacked() : "Packed block leak.";
            if (cachedBlock.getBlockType().isData()) {
              if (updateCacheMetrics) {
                HFile.DATABLOCK_READ_COUNT.increment();
              }
              // Validate encoding type for data blocks. We include encoding
              // type in the cache key, and we expect it to match on a cache hit.
              if (cachedBlock.getDataBlockEncoding() != dataBlockEncoder.getDataBlockEncoding()) {
                // Remember to release the block when in exceptional path.
                cacheConf.getBlockCache().ifPresent(cache -> {
                  returnAndEvictBlock(cache, cacheKey, cachedBlock);
                });
                throw new IOException("Cached block under key " + cacheKey + " "
                    + "has wrong encoding: " + cachedBlock.getDataBlockEncoding() + " (expected: "
                    + dataBlockEncoder.getDataBlockEncoding() + "), path=" + path);
              }
            }
            // Cache-hit. Return!
            return cachedBlock;
          }

          if (!useLock && cacheBlock && cacheConf.shouldLockOnCacheMiss(expectedBlockType)) {
            // check cache again with lock
            useLock = true;
            continue;
          }
          // Carry on, please load.
        }

        //TraceUtil.addTimelineAnnotation("blockCacheMiss");
        // Load block from filesystem.
        HFileBlock hfileBlock = fsBlockReader.readBlockData(dataBlockOffset, onDiskBlockSize, pread,
            !isCompaction, shouldUseHeap(expectedBlockType));
        validateBlockType(hfileBlock, expectedBlockType);
        HFileBlock unpacked = hfileBlock.unpack(hfileContext, fsBlockReader);
        BlockType.BlockCategory category = hfileBlock.getBlockType().getCategory();

        // Cache the block if necessary
        cacheConf.getBlockCache().ifPresent(cache -> {
          if (cacheBlock && cacheConf.shouldCacheBlockOnRead(category)) {
            cache.cacheBlock(cacheKey,
                cacheConf.shouldCacheCompressed(category) ? hfileBlock : unpacked,
                cacheConf.isInMemory());
          }
        });
        if (unpacked != hfileBlock) {
          // End of life here if hfileBlock is an independent block.
          hfileBlock.release();
        }
        if (updateCacheMetrics && hfileBlock.getBlockType().isData()) {
          HFile.DATABLOCK_READ_COUNT.increment();
        }

        return unpacked;
      }
    } finally {
      if (lockEntry != null) {
        offsetLock.releaseLockEntry(lockEntry);
      }
    }
  }

  @Override
  public boolean hasMVCCInfo() {
    return fileInfo.shouldIncludeMemStoreTS() && fileInfo.isDecodeMemstoreTS();
  }

  /**
   * Compares the actual type of a block retrieved from cache or disk with its
   * expected type and throws an exception in case of a mismatch. Expected
   * block type of {@link BlockType#DATA} is considered to match the actual
   * block type [@link {@link BlockType#ENCODED_DATA} as well.
   * @param block a block retrieved from cache or disk
   * @param expectedBlockType the expected block type, or null to skip the
   *          check
   */
  private void validateBlockType(HFileBlock block,
                                 BlockType expectedBlockType) throws IOException {
    if (expectedBlockType == null) {
      return;
    }
    BlockType actualBlockType = block.getBlockType();
    if (expectedBlockType.isData() && actualBlockType.isData()) {
      // We consider DATA to match ENCODED_DATA for the purpose of this
      // verification.
      return;
    }
    if (actualBlockType != expectedBlockType) {
      throw new IOException("Expected block type " + expectedBlockType + ", " +
          "but got " + actualBlockType + ": " + block + ", path=" + path);
    }
  }

  /**
   * @return Last key as cell in the file. May be null if file has no entries. Note that
   *         this is not the last row key, but it is the Cell representation of the last
   *         key
   */
  @Override
  public Optional<Cell> getLastKey() {
    return dataBlockIndexReader.isEmpty() ? Optional.empty() :
        Optional.of(fileInfo.getLastKeyCell());
  }

  /**
   * @return Midkey for this file. We work with block boundaries only so
   *         returned midkey is an approximation only.
   */
  @Override
  public Optional<Cell> midKey() throws IOException {
    return Optional.ofNullable(dataBlockIndexReader.midkey(this));
  }

  @Override
  public void close() throws IOException {
    close(cacheConf.shouldEvictOnClose());
  }

  @Override
  public DataBlockEncoding getEffectiveEncodingInCache(boolean isCompaction) {
    return dataBlockEncoder.getEffectiveEncodingInCache(isCompaction);
  }

  /** For testing */
  @Override
  public HFileBlock.FSReader getUncachedBlockReader() {
    return fsBlockReader;
  }

  /**
   * Scanner that operates on encoded data blocks.
   */
  protected static class EncodedScanner extends HFileScannerImpl {
    private final HFileBlockDecodingContext decodingCtx;
    private final DataBlockEncoder.EncodedSeeker seeker;
    private final DataBlockEncoder dataBlockEncoder;

    public EncodedScanner(HFile.Reader reader, boolean cacheBlocks,
                          boolean pread, boolean isCompaction, HFileContext meta) {
      super(reader, cacheBlocks, pread, isCompaction);
      DataBlockEncoding encoding = reader.getDataBlockEncoding();
      dataBlockEncoder = encoding.getEncoder();
      decodingCtx = dataBlockEncoder.newDataBlockDecodingContext(meta);
      seeker = dataBlockEncoder.createSeeker(decodingCtx);
    }

    @Override
    public boolean isSeeked(){
      return curBlock != null;
    }

    @Override
    public void setNonSeekedState() {
      reset();
    }

    /**
     * Updates the current block to be the given {@link HFileBlock}. Seeks to the the first
     * key/value pair.
     * @param newBlock the block to make current, and read by {@link HFileReaderImpl#readBlock},
     *          it's a totally new block with new allocated {@link ByteBuff}, so if no further
     *          reference to this block, we should release it carefully.
     */
    @Override
    protected void updateCurrentBlock(HFileBlock newBlock) throws CorruptHFileException {
      try {
        // sanity checks
        if (newBlock.getBlockType() != BlockType.ENCODED_DATA) {
          throw new IllegalStateException("EncodedScanner works only on encoded data blocks");
        }
        short dataBlockEncoderId = newBlock.getDataBlockEncodingId();
        if (!DataBlockEncoding.isCorrectEncoder(dataBlockEncoder, dataBlockEncoderId)) {
          String encoderCls = dataBlockEncoder.getClass().getName();
          throw new CorruptHFileException("Encoder " + encoderCls +
              " doesn't support data block encoding " +
              DataBlockEncoding.getNameFromId(dataBlockEncoderId) + ",path=" + reader.getPath());
        }
        updateCurrBlockRef(newBlock);
        ByteBuff encodedBuffer = getEncodedBuffer(newBlock);
        seeker.setCurrentBuffer(encodedBuffer);
      } finally {
        releaseIfNotCurBlock(newBlock);
      }
      // Reset the next indexed key
      this.nextIndexedKey = null;
    }

    private ByteBuff getEncodedBuffer(HFileBlock newBlock) {
      ByteBuff origBlock = newBlock.getBufferReadOnly();
      int pos = newBlock.headerSize() + DataBlockEncoding.ID_SIZE;
      origBlock.position(pos);
      origBlock
          .limit(pos + newBlock.getUncompressedSizeWithoutHeader() - DataBlockEncoding.ID_SIZE);
      return origBlock.slice();
    }

    @Override
    protected boolean processFirstDataBlock() throws IOException {
      seeker.rewind();
      return true;
    }

    @Override
    public boolean next() throws IOException {
      boolean isValid = seeker.next();
      if (!isValid) {
        HFileBlock newBlock = readNextDataBlock();
        isValid = newBlock != null;
        if (isValid) {
          updateCurrentBlock(newBlock);
        } else {
          setNonSeekedState();
        }
      }
      return isValid;
    }

    @Override
    public Cell getKey() {
      assertValidSeek();
      return seeker.getKey();
    }

    @Override
    public ByteBuffer getValue() {
      assertValidSeek();
      return seeker.getValueShallowCopy();
    }

    @Override
    public Cell getCell() {
      if (this.curBlock == null) {
        return null;
      }
      return seeker.getCell();
    }

    @Override
    public String getKeyString() {
      return CellUtil.toString(getKey(), true);
    }

    @Override
    public String getValueString() {
      ByteBuffer valueBuffer = getValue();
      return ByteBufferUtils.toStringBinary(valueBuffer);
    }

    private void assertValidSeek() {
      if (this.curBlock == null) {
        throw new NotSeekedException(reader.getPath());
      }
    }

    @Override
    protected Cell getFirstKeyCellInBlock(HFileBlock curBlock) {
      return dataBlockEncoder.getFirstKeyCellInBlock(getEncodedBuffer(curBlock));
    }

    @Override
    protected int loadBlockAndSeekToKey(HFileBlock seekToBlock, Cell nextIndexedKey,
                                        boolean rewind, Cell key, boolean seekBefore) throws IOException {
      if (this.curBlock == null || this.curBlock.getOffset() != seekToBlock.getOffset()) {
        updateCurrentBlock(seekToBlock);
      } else if (rewind) {
        seeker.rewind();
      }
      this.nextIndexedKey = nextIndexedKey;
      return seeker.seekToKeyInBlock(key, seekBefore);
    }

    @Override
    public int compareKey(CellComparator comparator, Cell key) {
      return seeker.compareKey(comparator, key);
    }
  }

  /**
   * Returns a buffer with the Bloom filter metadata. The caller takes
   * ownership of the buffer.
   */
  @Override
  public DataInput getGeneralBloomFilterMetadata() throws IOException {
    return this.getBloomFilterMetadata(BlockType.GENERAL_BLOOM_META);
  }

  @Override
  public DataInput getDeleteBloomFilterMetadata() throws IOException {
    return this.getBloomFilterMetadata(BlockType.DELETE_FAMILY_BLOOM_META);
  }

  private DataInput getBloomFilterMetadata(BlockType blockType)
      throws IOException {
    if (blockType != BlockType.GENERAL_BLOOM_META &&
        blockType != BlockType.DELETE_FAMILY_BLOOM_META) {
      throw new RuntimeException("Block Type: " + blockType.toString() +
          " is not supported, path=" + path) ;
    }

    for (HFileBlock b : fileInfo.getLoadOnOpenBlocks()) {
      if (b.getBlockType() == blockType) {
        return b.getByteStream();
      }
    }
    return null;
  }

  public boolean isFileInfoLoaded() {
    return true; // We load file info in constructor in version 2.
  }

  @Override
  public HFileContext getFileContext() {
    return hfileContext;
  }

  /**
   * Returns false if block prefetching was requested for this file and has
   * not completed, true otherwise
   */
  @Override
  public boolean prefetchComplete() {
    return PrefetchExecutor.isCompleted(path);
  }

  /**
   * Create a Scanner on this file. No seeks or reads are done on creation. Call
   * {@link HFileScanner#seekTo(Cell)} to position an start the read. There is
   * nothing to clean up in a Scanner. Letting go of your references to the
   * scanner is sufficient. NOTE: Do not use this overload of getScanner for
   * compactions. See {@link #getScanner(boolean, boolean, boolean)}
   *
   * @param cacheBlocks True if we should cache blocks read in by this scanner.
   * @param pread Use positional read rather than seek+read if true (pread is
   *          better for random reads, seek+read is better scanning).
   * @return Scanner on this file.
   */
  @Override
  public HFileScanner getScanner(boolean cacheBlocks, final boolean pread) {
    return getScanner(cacheBlocks, pread, false);
  }

  /**
   * Create a Scanner on this file. No seeks or reads are done on creation. Call
   * {@link HFileScanner#seekTo(Cell)} to position an start the read. There is
   * nothing to clean up in a Scanner. Letting go of your references to the
   * scanner is sufficient.
   * @param cacheBlocks
   *          True if we should cache blocks read in by this scanner.
   * @param pread
   *          Use positional read rather than seek+read if true (pread is better
   *          for random reads, seek+read is better scanning).
   * @param isCompaction
   *          is scanner being used for a compaction?
   * @return Scanner on this file.
   */
  @Override
  public HFileScanner getScanner(boolean cacheBlocks, final boolean pread,
                                 final boolean isCompaction) {
    if (dataBlockEncoder.useEncodedScanner()) {
      return new EncodedScanner(this, cacheBlocks, pread, isCompaction, this.hfileContext);
    }
    return new HFileScannerImpl(this, cacheBlocks, pread, isCompaction);
  }

  public int getMajorVersion() {
    return 3;
  }

  @Override
  public void unbufferStream() {
    fsBlockReader.unbufferStream();
  }
}
