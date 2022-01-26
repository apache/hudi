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
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hudi.hbase.CellComparator;
import org.apache.hudi.hbase.CellComparatorImpl;
import org.apache.hudi.hbase.KeyValue;
import org.apache.hudi.hbase.MetaCellComparator;
import org.apache.hudi.hbase.io.compress.Compression;
import org.apache.hudi.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.hudi.hbase.shaded.protobuf.generated.HFileProtos;

/**
 * The {@link HFile} has a fixed trailer which contains offsets to other
 * variable parts of the file. Also includes basic metadata on this file. The
 * trailer size is fixed within a given {@link HFile} format version only, but
 * we always store the version number as the last four-byte integer of the file.
 * The version number itself is split into two portions, a major
 * version and a minor version. The last three bytes of a file are the major
 * version and a single preceding byte is the minor number. The major version
 * determines which readers/writers to use to read/write a hfile while a minor
 * version determines smaller changes in hfile format that do not need a new
 * reader/writer type.
 */
@InterfaceAudience.Private
public class FixedFileTrailer {
  private static final Logger LOG = LoggerFactory.getLogger(FixedFileTrailer.class);

  /**
   * We store the comparator class name as a fixed-length field in the trailer.
   */
  private static final int MAX_COMPARATOR_NAME_LENGTH = 128;

  /**
   * Offset to the fileinfo data, a small block of vitals. Necessary in v1 but
   * only potentially useful for pretty-printing in v2.
   */
  private long fileInfoOffset;

  /**
   * In version 1, the offset to the data block index. Starting from version 2,
   * the meaning of this field is the offset to the section of the file that
   * should be loaded at the time the file is being opened: i.e. on open we load
   * the root index, file info, etc. See http://hbase.apache.org/book.html#_hfile_format_2
   * in the reference guide.
   */
  private long loadOnOpenDataOffset;

  /**
   * The number of entries in the root data index.
   */
  private int dataIndexCount;

  /**
   * Total uncompressed size of all blocks of the data index
   */
  private long uncompressedDataIndexSize;

  /**
   * The number of entries in the meta index
   */
  private int metaIndexCount;

  /**
   * The total uncompressed size of keys/values stored in the file.
   */
  private long totalUncompressedBytes;

  /**
   * The number of key/value pairs in the file. This field was int in version 1,
   * but is now long.
   */
  private long entryCount;

  /**
   * The compression codec used for all blocks.
   */
  private Compression.Algorithm compressionCodec = Compression.Algorithm.NONE;

  /**
   * The number of levels in the potentially multi-level data index. Used from
   * version 2 onwards.
   */
  private int numDataIndexLevels;

  /**
   * The offset of the first data block.
   */
  private long firstDataBlockOffset;

  /**
   * It is guaranteed that no key/value data blocks start after this offset in
   * the file.
   */
  private long lastDataBlockOffset;

  /**
   * Raw key comparator class name in version 3
   */
  // We could write the actual class name from 2.0 onwards and handle BC
  private String comparatorClassName = CellComparator.getInstance().getClass().getName();

  /**
   * The encryption key
   */
  private byte[] encryptionKey;

  /**
   * The {@link HFile} format major version.
   */
  private final int majorVersion;

  /**
   * The {@link HFile} format minor version.
   */
  private final int minorVersion;

  FixedFileTrailer(int majorVersion, int minorVersion) {
    this.majorVersion = majorVersion;
    this.minorVersion = minorVersion;
    HFile.checkFormatVersion(majorVersion);
  }

  private static int[] computeTrailerSizeByVersion() {
    int[] versionToSize = new int[HFile.MAX_FORMAT_VERSION + 1];
    // We support only 2 major versions now. ie. V2, V3
    versionToSize[2] = 212;
    for (int version = 3; version <= HFile.MAX_FORMAT_VERSION; version++) {
      // Max FFT size for V3 and above is taken as 4KB for future enhancements
      // if any.
      // Unless the trailer size exceeds 4K this can continue
      versionToSize[version] = 1024 * 4;
    }
    return versionToSize;
  }

  private static int getMaxTrailerSize() {
    int maxSize = 0;
    for (int version = HFile.MIN_FORMAT_VERSION; version <= HFile.MAX_FORMAT_VERSION; ++version) {
      maxSize = Math.max(getTrailerSize(version), maxSize);
    }
    return maxSize;
  }

  private static final int[] TRAILER_SIZE = computeTrailerSizeByVersion();
  private static final int MAX_TRAILER_SIZE = getMaxTrailerSize();

  private static final int NOT_PB_SIZE = BlockType.MAGIC_LENGTH + Bytes.SIZEOF_INT;

  static int getTrailerSize(int version) {
    return TRAILER_SIZE[version];
  }

  public int getTrailerSize() {
    return getTrailerSize(majorVersion);
  }

  /**
   * Write the trailer to a data stream. We support writing version 1 for
   * testing and for determining version 1 trailer size. It is also easy to see
   * what fields changed in version 2.
   */
  void serialize(DataOutputStream outputStream) throws IOException {
    HFile.checkFormatVersion(majorVersion);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream baosDos = new DataOutputStream(baos);

    BlockType.TRAILER.write(baosDos);
    serializeAsPB(baosDos);

    // The last 4 bytes of the file encode the major and minor version universally
    baosDos.writeInt(materializeVersion(majorVersion, minorVersion));

    baos.writeTo(outputStream);
  }

  HFileProtos.FileTrailerProto toProtobuf() {
    HFileProtos.FileTrailerProto.Builder builder = HFileProtos.FileTrailerProto.newBuilder()
        .setFileInfoOffset(fileInfoOffset)
        .setLoadOnOpenDataOffset(loadOnOpenDataOffset)
        .setUncompressedDataIndexSize(uncompressedDataIndexSize)
        .setTotalUncompressedBytes(totalUncompressedBytes)
        .setDataIndexCount(dataIndexCount)
        .setMetaIndexCount(metaIndexCount)
        .setEntryCount(entryCount)
        .setNumDataIndexLevels(numDataIndexLevels)
        .setFirstDataBlockOffset(firstDataBlockOffset)
        .setLastDataBlockOffset(lastDataBlockOffset)
        .setComparatorClassName(getHBase1CompatibleName(comparatorClassName))
        .setCompressionCodec(compressionCodec.ordinal());
    if (encryptionKey != null) {
      builder.setEncryptionKey(UnsafeByteOperations.unsafeWrap(encryptionKey));
    }
    return builder.build();
  }

  /**
   * Write trailer data as protobuf.
   * NOTE: we run a translation on the comparator name and will serialize the old hbase-1.x where
   * it makes sense. See {@link #getHBase1CompatibleName(String)}.
   */
  void serializeAsPB(DataOutputStream output) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    // We need this extra copy unfortunately to determine the final size of the
    // delimited output, see use of baos.size() below.
    toProtobuf().writeDelimitedTo(baos);
    baos.writeTo(output);
    // Pad to make up the difference between variable PB encoding length and the
    // length when encoded as writable under earlier V2 formats. Failure to pad
    // properly or if the PB encoding is too big would mean the trailer wont be read
    // in properly by HFile.
    int padding = getTrailerSize() - NOT_PB_SIZE - baos.size();
    if (padding < 0) {
      throw new IOException("Pbuf encoding size exceeded fixed trailer size limit");
    }
    for (int i = 0; i < padding; i++) {
      output.write(0);
    }
  }

  /**
   * Deserialize the fixed file trailer from the given stream. The version needs
   * to already be specified. Make sure this is consistent with
   * {@link #serialize(DataOutputStream)}.
   */
  void deserialize(DataInputStream inputStream) throws IOException {
    HFile.checkFormatVersion(majorVersion);

    BlockType.TRAILER.readAndCheck(inputStream);

    if (majorVersion > 2
        || (majorVersion == 2 && minorVersion >= HFileReaderImpl.PBUF_TRAILER_MINOR_VERSION)) {
      deserializeFromPB(inputStream);
    } else {
      deserializeFromWritable(inputStream);
    }

    // The last 4 bytes of the file encode the major and minor version universally
    int version = inputStream.readInt();
    expectMajorVersion(extractMajorVersion(version));
    expectMinorVersion(extractMinorVersion(version));
  }

  /**
   * Deserialize the file trailer as protobuf
   */
  void deserializeFromPB(DataInputStream inputStream) throws IOException {
    // read PB and skip padding
    int start = inputStream.available();
    HFileProtos.FileTrailerProto trailerProto =
        HFileProtos.FileTrailerProto.PARSER.parseDelimitedFrom(inputStream);
    int size = start - inputStream.available();
    inputStream.skip(getTrailerSize() - NOT_PB_SIZE - size);

    // process the PB
    if (trailerProto.hasFileInfoOffset()) {
      fileInfoOffset = trailerProto.getFileInfoOffset();
    }
    if (trailerProto.hasLoadOnOpenDataOffset()) {
      loadOnOpenDataOffset = trailerProto.getLoadOnOpenDataOffset();
    }
    if (trailerProto.hasUncompressedDataIndexSize()) {
      uncompressedDataIndexSize = trailerProto.getUncompressedDataIndexSize();
    }
    if (trailerProto.hasTotalUncompressedBytes()) {
      totalUncompressedBytes = trailerProto.getTotalUncompressedBytes();
    }
    if (trailerProto.hasDataIndexCount()) {
      dataIndexCount = trailerProto.getDataIndexCount();
    }
    if (trailerProto.hasMetaIndexCount()) {
      metaIndexCount = trailerProto.getMetaIndexCount();
    }
    if (trailerProto.hasEntryCount()) {
      entryCount = trailerProto.getEntryCount();
    }
    if (trailerProto.hasNumDataIndexLevels()) {
      numDataIndexLevels = trailerProto.getNumDataIndexLevels();
    }
    if (trailerProto.hasFirstDataBlockOffset()) {
      firstDataBlockOffset = trailerProto.getFirstDataBlockOffset();
    }
    if (trailerProto.hasLastDataBlockOffset()) {
      lastDataBlockOffset = trailerProto.getLastDataBlockOffset();
    }
    if (trailerProto.hasComparatorClassName()) {
      setComparatorClass(getComparatorClass(trailerProto.getComparatorClassName()));
    }
    if (trailerProto.hasCompressionCodec()) {
      compressionCodec = Compression.Algorithm.values()[trailerProto.getCompressionCodec()];
    } else {
      compressionCodec = Compression.Algorithm.NONE;
    }
    if (trailerProto.hasEncryptionKey()) {
      encryptionKey = trailerProto.getEncryptionKey().toByteArray();
    }
  }

  /**
   * Deserialize the file trailer as writable data
   */
  void deserializeFromWritable(DataInput input) throws IOException {
    fileInfoOffset = input.readLong();
    loadOnOpenDataOffset = input.readLong();
    dataIndexCount = input.readInt();
    uncompressedDataIndexSize = input.readLong();
    metaIndexCount = input.readInt();

    totalUncompressedBytes = input.readLong();
    entryCount = input.readLong();
    compressionCodec = Compression.Algorithm.values()[input.readInt()];
    numDataIndexLevels = input.readInt();
    firstDataBlockOffset = input.readLong();
    lastDataBlockOffset = input.readLong();
    // TODO this is a classname encoded into an  HFile's trailer. We are going to need to have
    // some compat code here.
    setComparatorClass(getComparatorClass(Bytes.readStringFixedSize(input,
        MAX_COMPARATOR_NAME_LENGTH)));
  }

  private void append(StringBuilder sb, String s) {
    if (sb.length() > 0) {
      sb.append(", ");
    }
    sb.append(s);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    append(sb, "fileinfoOffset=" + fileInfoOffset);
    append(sb, "loadOnOpenDataOffset=" + loadOnOpenDataOffset);
    append(sb, "dataIndexCount=" + dataIndexCount);
    append(sb, "metaIndexCount=" + metaIndexCount);
    append(sb, "totalUncomressedBytes=" + totalUncompressedBytes);
    append(sb, "entryCount=" + entryCount);
    append(sb, "compressionCodec=" + compressionCodec);
    append(sb, "uncompressedDataIndexSize=" + uncompressedDataIndexSize);
    append(sb, "numDataIndexLevels=" + numDataIndexLevels);
    append(sb, "firstDataBlockOffset=" + firstDataBlockOffset);
    append(sb, "lastDataBlockOffset=" + lastDataBlockOffset);
    append(sb, "comparatorClassName=" + comparatorClassName);
    if (majorVersion >= 3) {
      append(sb, "encryptionKey=" + (encryptionKey != null ? "PRESENT" : "NONE"));
    }
    append(sb, "majorVersion=" + majorVersion);
    append(sb, "minorVersion=" + minorVersion);

    return sb.toString();
  }

  /**
   * Reads a file trailer from the given file.
   *
   * @param istream  the input stream with the ability to seek. Does not have to
   *                 be buffered, as only one read operation is made.
   * @param fileSize the file size. Can be obtained using
   *                 {@link org.apache.hadoop.fs.FileSystem#getFileStatus(
   *org.apache.hadoop.fs.Path)}.
   * @return the fixed file trailer read
   * @throws IOException if failed to read from the underlying stream, or the
   *                     trailer is corrupted, or the version of the trailer is
   *                     unsupported
   */
  public static FixedFileTrailer readFromStream(FSDataInputStream istream,
                                                long fileSize) throws IOException {
    int bufferSize = MAX_TRAILER_SIZE;
    long seekPoint = fileSize - bufferSize;
    if (seekPoint < 0) {
      // It is hard to imagine such a small HFile.
      seekPoint = 0;
      bufferSize = (int) fileSize;
    }

    HFileUtil.seekOnMultipleSources(istream, seekPoint);

    ByteBuffer buf = ByteBuffer.allocate(bufferSize);
    istream.readFully(buf.array(), buf.arrayOffset(),
        buf.arrayOffset() + buf.limit());

    // Read the version from the last int of the file.
    buf.position(buf.limit() - Bytes.SIZEOF_INT);
    int version = buf.getInt();

    // Extract the major and minor versions.
    int majorVersion = extractMajorVersion(version);
    int minorVersion = extractMinorVersion(version);

    HFile.checkFormatVersion(majorVersion); // throws IAE if invalid

    int trailerSize = getTrailerSize(majorVersion);

    FixedFileTrailer fft = new FixedFileTrailer(majorVersion, minorVersion);
    fft.deserialize(new DataInputStream(new ByteArrayInputStream(buf.array(),
        buf.arrayOffset() + bufferSize - trailerSize, trailerSize)));
    return fft;
  }

  public void expectMajorVersion(int expected) {
    if (majorVersion != expected) {
      throw new IllegalArgumentException("Invalid HFile major version: "
          + majorVersion
          + " (expected: " + expected + ")");
    }
  }

  public void expectMinorVersion(int expected) {
    if (minorVersion != expected) {
      throw new IllegalArgumentException("Invalid HFile minor version: "
          + minorVersion + " (expected: " + expected + ")");
    }
  }

  public void expectAtLeastMajorVersion(int lowerBound) {
    if (majorVersion < lowerBound) {
      throw new IllegalArgumentException("Invalid HFile major version: "
          + majorVersion
          + " (expected: " + lowerBound + " or higher).");
    }
  }

  public long getFileInfoOffset() {
    return fileInfoOffset;
  }

  public void setFileInfoOffset(long fileInfoOffset) {
    this.fileInfoOffset = fileInfoOffset;
  }

  public long getLoadOnOpenDataOffset() {
    return loadOnOpenDataOffset;
  }

  public void setLoadOnOpenOffset(long loadOnOpenDataOffset) {
    this.loadOnOpenDataOffset = loadOnOpenDataOffset;
  }

  public int getDataIndexCount() {
    return dataIndexCount;
  }

  public void setDataIndexCount(int dataIndexCount) {
    this.dataIndexCount = dataIndexCount;
  }

  public int getMetaIndexCount() {
    return metaIndexCount;
  }

  public void setMetaIndexCount(int metaIndexCount) {
    this.metaIndexCount = metaIndexCount;
  }

  public long getTotalUncompressedBytes() {
    return totalUncompressedBytes;
  }

  public void setTotalUncompressedBytes(long totalUncompressedBytes) {
    this.totalUncompressedBytes = totalUncompressedBytes;
  }

  public long getEntryCount() {
    return entryCount;
  }

  public void setEntryCount(long newEntryCount) {
    entryCount = newEntryCount;
  }

  public Compression.Algorithm getCompressionCodec() {
    return compressionCodec;
  }

  public void setCompressionCodec(Compression.Algorithm compressionCodec) {
    this.compressionCodec = compressionCodec;
  }

  public int getNumDataIndexLevels() {
    expectAtLeastMajorVersion(2);
    return numDataIndexLevels;
  }

  public void setNumDataIndexLevels(int numDataIndexLevels) {
    expectAtLeastMajorVersion(2);
    this.numDataIndexLevels = numDataIndexLevels;
  }

  public long getLastDataBlockOffset() {
    expectAtLeastMajorVersion(2);
    return lastDataBlockOffset;
  }

  public void setLastDataBlockOffset(long lastDataBlockOffset) {
    expectAtLeastMajorVersion(2);
    this.lastDataBlockOffset = lastDataBlockOffset;
  }

  public long getFirstDataBlockOffset() {
    expectAtLeastMajorVersion(2);
    return firstDataBlockOffset;
  }

  public void setFirstDataBlockOffset(long firstDataBlockOffset) {
    expectAtLeastMajorVersion(2);
    this.firstDataBlockOffset = firstDataBlockOffset;
  }

  public String getComparatorClassName() {
    return comparatorClassName;
  }

  /**
   * Returns the major version of this HFile format
   */
  public int getMajorVersion() {
    return majorVersion;
  }

  /**
   * Returns the minor version of this HFile format
   */
  public int getMinorVersion() {
    return minorVersion;
  }

  public void setComparatorClass(Class<? extends CellComparator> klass) {
    // Is the comparator instantiable?
    try {
      // If null, it should be the Bytes.BYTES_RAWCOMPARATOR
      if (klass != null) {
        CellComparator comp = klass.getDeclaredConstructor().newInstance();
        // if the name wasn't one of the legacy names, maybe its a legit new
        // kind of comparator.
        this.comparatorClassName = klass.getName();
      }
    } catch (Exception e) {
      throw new RuntimeException("Comparator class " + klass.getName() + " is not instantiable", e);
    }
  }

  /**
   * If a 'standard' Comparator, write the old name for the Comparator when we serialize rather
   * than the new name; writing the new name will make it so newly-written hfiles are not parseable
   * by hbase-1.x, a facility we'd like to preserve across rolling upgrade and hbase-1.x clusters
   * reading hbase-2.x produce.
   * <p>
   * The Comparators in hbase-2.x work the same as they did in hbase-1.x; they compare
   * KeyValues. In hbase-2.x they were renamed making use of the more generic 'Cell'
   * nomenclature to indicate that we intend to move away from KeyValues post hbase-2. A naming
   * change is not reason enough to make it so hbase-1.x cannot read hbase-2.x files given the
   * structure goes unchanged (hfile v3). So, lets write the old names for Comparators into the
   * hfile tails in hbase-2. Here is where we do the translation.
   * {@link #getComparatorClass(String)} does translation going the other way.
   *
   * <p>The translation is done on the serialized Protobuf only.</p>
   *
   * @param comparator String class name of the Comparator used in this hfile.
   * @return What to store in the trailer as our comparator name.
   * @see #getComparatorClass(String)
   * @since hbase-2.0.0.
   * @deprecated Since hbase-2.0.0. Will be removed in hbase-3.0.0.
   */
  @Deprecated
  private String getHBase1CompatibleName(final String comparator) {
    if (comparator.equals(CellComparatorImpl.class.getName())) {
      return KeyValue.COMPARATOR.getClass().getName();
    }
    if (comparator.equals(MetaCellComparator.class.getName())) {
      return KeyValue.META_COMPARATOR.getClass().getName();
    }
    return comparator;
  }

  @SuppressWarnings("unchecked")
  private static Class<? extends CellComparator> getComparatorClass(String comparatorClassName)
      throws IOException {
    Class<? extends CellComparator> comparatorKlass;
    // for BC
    if (comparatorClassName.equals(KeyValue.COMPARATOR.getLegacyKeyComparatorName())
        || comparatorClassName.equals(KeyValue.COMPARATOR.getClass().getName())
        || (comparatorClassName.equals("org.apache.hudi.hbase.CellComparator"))) {
      comparatorKlass = CellComparatorImpl.class;
    } else if (comparatorClassName.equals(KeyValue.META_COMPARATOR.getLegacyKeyComparatorName())
        || comparatorClassName.equals(KeyValue.META_COMPARATOR.getClass().getName())
        || (comparatorClassName.equals("org.apache.hudi.hbase.MetaCellComparator"))) {
      comparatorKlass = MetaCellComparator.class;
    } else if (comparatorClassName.equals("org.apache.hudi.hbase.KeyValue$RawBytesComparator")
        || comparatorClassName.equals("org.apache.hudi.hbase.util.Bytes$ByteArrayComparator")) {
      // When the comparator to be used is Bytes.BYTES_RAWCOMPARATOR, we just return null from here
      // Bytes.BYTES_RAWCOMPARATOR is not a CellComparator
      comparatorKlass = null;
    } else {
      // if the name wasn't one of the legacy names, maybe its a legit new kind of comparator.
      try {
        comparatorKlass = (Class<? extends CellComparator>) Class.forName(comparatorClassName);
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }
    }
    return comparatorKlass;
  }

  static CellComparator createComparator(String comparatorClassName) throws IOException {
    if (comparatorClassName.equals(CellComparatorImpl.COMPARATOR.getClass().getName())) {
      return CellComparatorImpl.COMPARATOR;
    } else if (comparatorClassName.equals(
        MetaCellComparator.META_COMPARATOR.getClass().getName())) {
      return MetaCellComparator.META_COMPARATOR;
    }
    try {
      Class<? extends CellComparator> comparatorClass = getComparatorClass(comparatorClassName);
      if (comparatorClass != null) {
        return comparatorClass.getDeclaredConstructor().newInstance();
      }
      LOG.warn("No Comparator class for " + comparatorClassName + ". Returning Null.");
      return null;
    } catch (Exception e) {
      throw new IOException("Comparator class " + comparatorClassName + " is not instantiable", e);
    }
  }

  CellComparator createComparator() throws IOException {
    expectAtLeastMajorVersion(2);
    return createComparator(comparatorClassName);
  }

  public long getUncompressedDataIndexSize() {
    return uncompressedDataIndexSize;
  }

  public void setUncompressedDataIndexSize(
      long uncompressedDataIndexSize) {
    expectAtLeastMajorVersion(2);
    this.uncompressedDataIndexSize = uncompressedDataIndexSize;
  }

  public byte[] getEncryptionKey() {
    // This is a v3 feature but if reading a v2 file the encryptionKey will just be null which
    // if fine for this feature.
    expectAtLeastMajorVersion(2);
    return encryptionKey;
  }

  public void setEncryptionKey(byte[] keyBytes) {
    this.encryptionKey = keyBytes;
  }

  /**
   * Extracts the major version for a 4-byte serialized version data.
   * The major version is the 3 least significant bytes
   */
  private static int extractMajorVersion(int serializedVersion) {
    return (serializedVersion & 0x00ffffff);
  }

  /**
   * Extracts the minor version for a 4-byte serialized version data.
   * The major version are the 3 the most significant bytes
   */
  private static int extractMinorVersion(int serializedVersion) {
    return (serializedVersion >>> 24);
  }

  /**
   * Create a 4 byte serialized version number by combining the
   * minor and major version numbers.
   */
  static int materializeVersion(int majorVersion, int minorVersion) {
    return ((majorVersion & 0x00ffffff) | (minorVersion << 24));
  }
}
