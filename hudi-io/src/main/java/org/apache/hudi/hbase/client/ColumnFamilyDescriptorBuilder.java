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

package org.apache.hudi.hbase.client;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.hudi.hbase.HConstants;
import org.apache.hudi.hbase.KeepDeletedCells;
import org.apache.hudi.hbase.MemoryCompactionPolicy;
import org.apache.hudi.hbase.exceptions.DeserializationException;
import org.apache.hudi.hbase.exceptions.HBaseException;
import org.apache.hudi.hbase.io.compress.Compression;
import org.apache.hudi.hbase.io.encoding.DataBlockEncoding;
import org.apache.hudi.hbase.regionserver.BloomType;
import org.apache.hudi.hbase.util.Bytes;
import org.apache.hudi.hbase.util.PrettyPrinter;
import org.apache.hudi.hbase.util.PrettyPrinter.Unit;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

import org.apache.hudi.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hudi.hbase.shaded.protobuf.generated.HBaseProtos.ColumnFamilySchema;

/**
 * @since 2.0.0
 */
@InterfaceAudience.Public
public class ColumnFamilyDescriptorBuilder {
  // For future backward compatibility

  // Version  3 was when column names become byte arrays and when we picked up
  // Time-to-live feature.  Version 4 was when we moved to byte arrays, HBASE-82.
  // Version  5 was when bloom filter descriptors were removed.
  // Version  6 adds metadata as a map where keys and values are byte[].
  // Version  7 -- add new compression and hfile blocksize to HColumnDescriptor (HBASE-1217)
  // Version  8 -- reintroduction of bloom filters, changed from boolean to enum
  // Version  9 -- add data block encoding
  // Version 10 -- change metadata to standard type.
  // Version 11 -- add column family level configuration.
  private static final byte COLUMN_DESCRIPTOR_VERSION = (byte) 11;

  @InterfaceAudience.Private
  public static final String IN_MEMORY_COMPACTION = "IN_MEMORY_COMPACTION";
  private static final Bytes IN_MEMORY_COMPACTION_BYTES = new Bytes(Bytes.toBytes(IN_MEMORY_COMPACTION));

  @InterfaceAudience.Private
  public static final String IN_MEMORY = HConstants.IN_MEMORY;
  private static final Bytes IN_MEMORY_BYTES = new Bytes(Bytes.toBytes(IN_MEMORY));

  // These constants are used as FileInfo keys
  @InterfaceAudience.Private
  public static final String COMPRESSION = "COMPRESSION";
  private static final Bytes COMPRESSION_BYTES = new Bytes(Bytes.toBytes(COMPRESSION));
  @InterfaceAudience.Private
  public static final String COMPRESSION_COMPACT = "COMPRESSION_COMPACT";
  private static final Bytes COMPRESSION_COMPACT_BYTES = new Bytes(Bytes.toBytes(COMPRESSION_COMPACT));
  @InterfaceAudience.Private
  public static final String DATA_BLOCK_ENCODING = "DATA_BLOCK_ENCODING";
  private static final Bytes DATA_BLOCK_ENCODING_BYTES = new Bytes(Bytes.toBytes(DATA_BLOCK_ENCODING));
  /**
   * Key for the BLOCKCACHE attribute. A more exact name would be
   * CACHE_DATA_ON_READ because this flag sets whether or not we cache DATA
   * blocks. We always cache INDEX and BLOOM blocks; caching these blocks cannot
   * be disabled.
   */
  @InterfaceAudience.Private
  public static final String BLOCKCACHE = "BLOCKCACHE";
  private static final Bytes BLOCKCACHE_BYTES = new Bytes(Bytes.toBytes(BLOCKCACHE));
  @InterfaceAudience.Private
  public static final String CACHE_DATA_ON_WRITE = "CACHE_DATA_ON_WRITE";
  private static final Bytes CACHE_DATA_ON_WRITE_BYTES = new Bytes(Bytes.toBytes(CACHE_DATA_ON_WRITE));
  @InterfaceAudience.Private
  public static final String CACHE_INDEX_ON_WRITE = "CACHE_INDEX_ON_WRITE";
  private static final Bytes CACHE_INDEX_ON_WRITE_BYTES = new Bytes(Bytes.toBytes(CACHE_INDEX_ON_WRITE));
  @InterfaceAudience.Private
  public static final String CACHE_BLOOMS_ON_WRITE = "CACHE_BLOOMS_ON_WRITE";
  private static final Bytes CACHE_BLOOMS_ON_WRITE_BYTES = new Bytes(Bytes.toBytes(CACHE_BLOOMS_ON_WRITE));
  @InterfaceAudience.Private
  public static final String EVICT_BLOCKS_ON_CLOSE = "EVICT_BLOCKS_ON_CLOSE";
  private static final Bytes EVICT_BLOCKS_ON_CLOSE_BYTES = new Bytes(Bytes.toBytes(EVICT_BLOCKS_ON_CLOSE));

  /**
   * Key for the PREFETCH_BLOCKS_ON_OPEN attribute. If set, all INDEX, BLOOM,
   * and DATA blocks of HFiles belonging to this family will be loaded into the
   * cache as soon as the file is opened. These loads will not count as cache
   * misses.
   */
  @InterfaceAudience.Private
  public static final String PREFETCH_BLOCKS_ON_OPEN = "PREFETCH_BLOCKS_ON_OPEN";
  private static final Bytes PREFETCH_BLOCKS_ON_OPEN_BYTES = new Bytes(Bytes.toBytes(PREFETCH_BLOCKS_ON_OPEN));

  /**
   * Size of storefile/hfile 'blocks'. Default is {@link #DEFAULT_BLOCKSIZE}.
   * Use smaller block sizes for faster random-access at expense of larger
   * indices (more memory consumption). Note that this is a soft limit and that
   * blocks have overhead (metadata, CRCs) so blocks will tend to be the size
   * specified here and then some; i.e. don't expect that setting BLOCKSIZE=4k
   * means hbase data will align with an SSDs 4k page accesses (TODO).
   */
  @InterfaceAudience.Private
  public static final String BLOCKSIZE = "BLOCKSIZE";
  private static final Bytes BLOCKSIZE_BYTES = new Bytes(Bytes.toBytes(BLOCKSIZE));

  @InterfaceAudience.Private
  public static final String TTL = "TTL";
  private static final Bytes TTL_BYTES = new Bytes(Bytes.toBytes(TTL));
  @InterfaceAudience.Private
  public static final String BLOOMFILTER = "BLOOMFILTER";
  private static final Bytes BLOOMFILTER_BYTES = new Bytes(Bytes.toBytes(BLOOMFILTER));
  @InterfaceAudience.Private
  public static final String REPLICATION_SCOPE = "REPLICATION_SCOPE";
  @InterfaceAudience.Private
  public static final String MAX_VERSIONS = HConstants.VERSIONS;
  private static final Bytes MAX_VERSIONS_BYTES = new Bytes(Bytes.toBytes(MAX_VERSIONS));
  @InterfaceAudience.Private
  public static final String MIN_VERSIONS = "MIN_VERSIONS";
  private static final Bytes MIN_VERSIONS_BYTES = new Bytes(Bytes.toBytes(MIN_VERSIONS));
  /**
   * Retain all cells across flushes and compactions even if they fall behind a
   * delete tombstone. To see all retained cells, do a 'raw' scan; see
   * Scan#setRaw or pass RAW =&gt; true attribute in the shell.
   */
  @InterfaceAudience.Private
  public static final String KEEP_DELETED_CELLS = "KEEP_DELETED_CELLS";
  private static final Bytes KEEP_DELETED_CELLS_BYTES = new Bytes(Bytes.toBytes(KEEP_DELETED_CELLS));
  @InterfaceAudience.Private
  public static final String COMPRESS_TAGS = "COMPRESS_TAGS";
  private static final Bytes COMPRESS_TAGS_BYTES = new Bytes(Bytes.toBytes(COMPRESS_TAGS));
  @InterfaceAudience.Private
  public static final String ENCRYPTION = "ENCRYPTION";
  private static final Bytes ENCRYPTION_BYTES = new Bytes(Bytes.toBytes(ENCRYPTION));
  @InterfaceAudience.Private
  public static final String ENCRYPTION_KEY = "ENCRYPTION_KEY";
  private static final Bytes ENCRYPTION_KEY_BYTES = new Bytes(Bytes.toBytes(ENCRYPTION_KEY));

  private static final boolean DEFAULT_MOB = false;
  @InterfaceAudience.Private
  public static final String IS_MOB = "IS_MOB";
  private static final Bytes IS_MOB_BYTES = new Bytes(Bytes.toBytes(IS_MOB));
  @InterfaceAudience.Private
  public static final String MOB_THRESHOLD = "MOB_THRESHOLD";
  private static final Bytes MOB_THRESHOLD_BYTES = new Bytes(Bytes.toBytes(MOB_THRESHOLD));
  public static final long DEFAULT_MOB_THRESHOLD = 100 * 1024; // 100k
  @InterfaceAudience.Private
  public static final String MOB_COMPACT_PARTITION_POLICY = "MOB_COMPACT_PARTITION_POLICY";
  private static final Bytes MOB_COMPACT_PARTITION_POLICY_BYTES = new Bytes(Bytes.toBytes(MOB_COMPACT_PARTITION_POLICY));
  public static final MobCompactPartitionPolicy DEFAULT_MOB_COMPACT_PARTITION_POLICY
      = MobCompactPartitionPolicy.DAILY;
  @InterfaceAudience.Private
  public static final String DFS_REPLICATION = "DFS_REPLICATION";
  private static final Bytes DFS_REPLICATION_BYTES = new Bytes(Bytes.toBytes(DFS_REPLICATION));
  public static final short DEFAULT_DFS_REPLICATION = 0;
  @InterfaceAudience.Private
  public static final String STORAGE_POLICY = "STORAGE_POLICY";
  private static final Bytes STORAGE_POLICY_BYTES = new Bytes(Bytes.toBytes(STORAGE_POLICY));

  public static final String NEW_VERSION_BEHAVIOR = "NEW_VERSION_BEHAVIOR";
  private static final Bytes NEW_VERSION_BEHAVIOR_BYTES = new Bytes(Bytes.toBytes(NEW_VERSION_BEHAVIOR));
  public static final boolean DEFAULT_NEW_VERSION_BEHAVIOR = false;
  /**
   * Default compression type.
   */
  public static final Compression.Algorithm DEFAULT_COMPRESSION = Compression.Algorithm.NONE;

  /**
   * Default data block encoding algorithm.
   */
  public static final DataBlockEncoding DEFAULT_DATA_BLOCK_ENCODING = DataBlockEncoding.NONE;

  /**
   * Default number of versions of a record to keep.
   */
  public static final int DEFAULT_MAX_VERSIONS = 1;

  /**
   * Default is not to keep a minimum of versions.
   */
  public static final int DEFAULT_MIN_VERSIONS = 0;

  /**
   * Default setting for whether to try and serve this column family from memory
   * or not.
   */
  public static final boolean DEFAULT_IN_MEMORY = false;

  /**
   * Default setting for preventing deleted from being collected immediately.
   */
  public static final KeepDeletedCells DEFAULT_KEEP_DELETED = KeepDeletedCells.FALSE;

  /**
   * Default setting for whether to use a block cache or not.
   */
  public static final boolean DEFAULT_BLOCKCACHE = true;

  /**
   * Default setting for whether to cache data blocks on write if block caching
   * is enabled.
   */
  public static final boolean DEFAULT_CACHE_DATA_ON_WRITE = false;

  /**
   * Default setting for whether to cache index blocks on write if block caching
   * is enabled.
   */
  public static final boolean DEFAULT_CACHE_INDEX_ON_WRITE = false;

  /**
   * Default size of blocks in files stored to the filesytem (hfiles).
   */
  public static final int DEFAULT_BLOCKSIZE = HConstants.DEFAULT_BLOCKSIZE;

  /**
   * Default setting for whether or not to use bloomfilters.
   */
  public static final BloomType DEFAULT_BLOOMFILTER = BloomType.ROW;

  /**
   * Default setting for whether to cache bloom filter blocks on write if block
   * caching is enabled.
   */
  public static final boolean DEFAULT_CACHE_BLOOMS_ON_WRITE = false;

  /**
   * Default time to live of cell contents.
   */
  public static final int DEFAULT_TTL = HConstants.FOREVER;

  /**
   * Default scope.
   */
  public static final int DEFAULT_REPLICATION_SCOPE = HConstants.REPLICATION_SCOPE_LOCAL;

  /**
   * Default setting for whether to evict cached blocks from the blockcache on
   * close.
   */
  public static final boolean DEFAULT_EVICT_BLOCKS_ON_CLOSE = false;

  /**
   * Default compress tags along with any type of DataBlockEncoding.
   */
  public static final boolean DEFAULT_COMPRESS_TAGS = true;

  /*
   * Default setting for whether to prefetch blocks into the blockcache on open.
   */
  public static final boolean DEFAULT_PREFETCH_BLOCKS_ON_OPEN = false;

  private final static Map<String, String> DEFAULT_VALUES = new HashMap<>();

  private static Map<Bytes, Bytes> getDefaultValuesBytes() {
    Map<Bytes, Bytes> values = new HashMap<>();
    DEFAULT_VALUES.forEach((k, v) -> values.put(new Bytes(Bytes.toBytes(k)), new Bytes(Bytes.toBytes(v))));
    return values;
  }

  public static Map<String, String> getDefaultValues() {
    return Collections.unmodifiableMap(DEFAULT_VALUES);
  }

  private final static Set<Bytes> RESERVED_KEYWORDS = new HashSet<>();

  static {
    DEFAULT_VALUES.put(BLOOMFILTER, DEFAULT_BLOOMFILTER.name());
    DEFAULT_VALUES.put(REPLICATION_SCOPE, String.valueOf(DEFAULT_REPLICATION_SCOPE));
    DEFAULT_VALUES.put(MAX_VERSIONS, String.valueOf(DEFAULT_MAX_VERSIONS));
    DEFAULT_VALUES.put(MIN_VERSIONS, String.valueOf(DEFAULT_MIN_VERSIONS));
    DEFAULT_VALUES.put(COMPRESSION, DEFAULT_COMPRESSION.name());
    DEFAULT_VALUES.put(TTL, String.valueOf(DEFAULT_TTL));
    DEFAULT_VALUES.put(BLOCKSIZE, String.valueOf(DEFAULT_BLOCKSIZE));
    DEFAULT_VALUES.put(IN_MEMORY, String.valueOf(DEFAULT_IN_MEMORY));
    DEFAULT_VALUES.put(BLOCKCACHE, String.valueOf(DEFAULT_BLOCKCACHE));
    DEFAULT_VALUES.put(KEEP_DELETED_CELLS, String.valueOf(DEFAULT_KEEP_DELETED));
    DEFAULT_VALUES.put(DATA_BLOCK_ENCODING, String.valueOf(DEFAULT_DATA_BLOCK_ENCODING));
    // Do NOT add this key/value by default. NEW_VERSION_BEHAVIOR is NOT defined in hbase1 so
    // it is not possible to make an hbase1 HCD the same as an hbase2 HCD and so the replication
    // compare of schemas will fail. It is OK not adding the below to the initial map because of
    // fetch of this value, we will check for null and if null will return the default.
    // DEFAULT_VALUES.put(NEW_VERSION_BEHAVIOR, String.valueOf(DEFAULT_NEW_VERSION_BEHAVIOR));
    DEFAULT_VALUES.keySet().forEach(s -> RESERVED_KEYWORDS.add(new Bytes(Bytes.toBytes(s))));
    RESERVED_KEYWORDS.add(new Bytes(Bytes.toBytes(ENCRYPTION)));
    RESERVED_KEYWORDS.add(new Bytes(Bytes.toBytes(ENCRYPTION_KEY)));
    RESERVED_KEYWORDS.add(new Bytes(Bytes.toBytes(IS_MOB)));
    RESERVED_KEYWORDS.add(new Bytes(Bytes.toBytes(MOB_THRESHOLD)));
    RESERVED_KEYWORDS.add(new Bytes(Bytes.toBytes(MOB_COMPACT_PARTITION_POLICY)));
  }

  public static Unit getUnit(String key) {
    /* TTL for now, we can add more as we need */
    switch (key) {
      case TTL:
        return Unit.TIME_INTERVAL;
      default:
        return Unit.NONE;
    }
  }

  /**
   * @param b Family name.
   * @return <code>b</code>
   * @throws IllegalArgumentException If not null and not a legitimate family
   * name: i.e. 'printable' and ends in a ':' (Null passes are allowed because
   * <code>b</code> can be null when deserializing). Cannot start with a '.'
   * either. Also Family can not be an empty value or equal "recovered.edits".
   */
  public static byte[] isLegalColumnFamilyName(final byte[] b) {
    if (b == null) {
      return null;
    }
    Preconditions.checkArgument(b.length != 0, "Column Family name can not be empty");
    if (b[0] == '.') {
      throw new IllegalArgumentException("Column Family names cannot start with a "
          + "period: " + Bytes.toString(b));
    }
    for (int i = 0; i < b.length; i++) {
      if (Character.isISOControl(b[i]) || b[i] == ':' || b[i] == '\\' || b[i] == '/') {
        throw new IllegalArgumentException("Illegal character <" + b[i]
            + ">. Column Family names cannot contain control characters or colons: "
            + Bytes.toString(b));
      }
    }
    byte[] recoveredEdit = Bytes.toBytes(HConstants.RECOVERED_EDITS_DIR);
    if (Bytes.equals(recoveredEdit, b)) {
      throw new IllegalArgumentException("Column Family name cannot be: "
          + HConstants.RECOVERED_EDITS_DIR);
    }
    return b;
  }

  private final ModifyableColumnFamilyDescriptor desc;

  public static ColumnFamilyDescriptor parseFrom(final byte[] pbBytes) throws DeserializationException {
    return ModifyableColumnFamilyDescriptor.parseFrom(pbBytes);
  }

  public static ColumnFamilyDescriptorBuilder newBuilder(final byte[] name) {
    return new ColumnFamilyDescriptorBuilder(name);
  }

  public static ColumnFamilyDescriptorBuilder newBuilder(final ColumnFamilyDescriptor desc) {
    return new ColumnFamilyDescriptorBuilder(desc);
  }

  public static ColumnFamilyDescriptor copy(ColumnFamilyDescriptor desc) {
    return new ModifyableColumnFamilyDescriptor(desc);
  }

  public static ColumnFamilyDescriptor of(String name) {
    return of(Bytes.toBytes(name));
  }

  public static ColumnFamilyDescriptor of(byte[] name) {
    return newBuilder(name).build();
  }

  private ColumnFamilyDescriptorBuilder(final byte[] name) {
    this.desc = new ModifyableColumnFamilyDescriptor(name);
  }

  private ColumnFamilyDescriptorBuilder(final ColumnFamilyDescriptor desc) {
    this.desc = new ModifyableColumnFamilyDescriptor(desc);
  }

  /**
   * @param desc The table descriptor to serialize
   * @return This instance serialized with pb with pb magic prefix
   */
  public static byte[] toByteArray(ColumnFamilyDescriptor desc) {
    if (desc instanceof ModifyableColumnFamilyDescriptor) {
      return ((ModifyableColumnFamilyDescriptor) desc).toByteArray();
    }
    return new ModifyableColumnFamilyDescriptor(desc).toByteArray();
  }

  public ColumnFamilyDescriptor build() {
    return new ModifyableColumnFamilyDescriptor(desc);
  }

  public ColumnFamilyDescriptorBuilder removeConfiguration(String key) {
    desc.removeConfiguration(key);
    return this;
  }

  public String getNameAsString() {
    return desc.getNameAsString();
  }

  public ColumnFamilyDescriptorBuilder setBlockCacheEnabled(boolean value) {
    desc.setBlockCacheEnabled(value);
    return this;
  }

  public ColumnFamilyDescriptorBuilder setBlocksize(int value) {
    desc.setBlocksize(value);
    return this;
  }

  public ColumnFamilyDescriptorBuilder setBloomFilterType(final BloomType value) {
    desc.setBloomFilterType(value);
    return this;
  }

  public ColumnFamilyDescriptorBuilder setCacheBloomsOnWrite(boolean value) {
    desc.setCacheBloomsOnWrite(value);
    return this;
  }

  public ColumnFamilyDescriptorBuilder setCacheDataOnWrite(boolean value) {
    desc.setCacheDataOnWrite(value);
    return this;
  }

  public ColumnFamilyDescriptorBuilder setCacheIndexesOnWrite(final boolean value) {
    desc.setCacheIndexesOnWrite(value);
    return this;
  }

  public ColumnFamilyDescriptorBuilder setCompactionCompressionType(Compression.Algorithm value) {
    desc.setCompactionCompressionType(value);
    return this;
  }

  public ColumnFamilyDescriptorBuilder setCompressTags(boolean value) {
    desc.setCompressTags(value);
    return this;
  }

  public ColumnFamilyDescriptorBuilder setCompressionType(Compression.Algorithm value) {
    desc.setCompressionType(value);
    return this;
  }

  public Compression.Algorithm getCompressionType() {
    return desc.getCompressionType();
  }

  public ColumnFamilyDescriptorBuilder setConfiguration(final String key, final String value) {
    desc.setConfiguration(key, value);
    return this;
  }

  public ColumnFamilyDescriptorBuilder setDFSReplication(short value) {
    desc.setDFSReplication(value);
    return this;
  }

  public ColumnFamilyDescriptorBuilder setDataBlockEncoding(DataBlockEncoding value) {
    desc.setDataBlockEncoding(value);
    return this;
  }

  public ColumnFamilyDescriptorBuilder setEncryptionKey(final byte[] value) {
    desc.setEncryptionKey(value);
    return this;
  }

  public ColumnFamilyDescriptorBuilder setEncryptionType(String value) {
    desc.setEncryptionType(value);
    return this;
  }

  public ColumnFamilyDescriptorBuilder setEvictBlocksOnClose(boolean value) {
    desc.setEvictBlocksOnClose(value);
    return this;
  }

  public ColumnFamilyDescriptorBuilder setInMemory(final boolean value) {
    desc.setInMemory(value);
    return this;
  }

  public ColumnFamilyDescriptorBuilder setInMemoryCompaction(final MemoryCompactionPolicy value) {
    desc.setInMemoryCompaction(value);
    return this;
  }

  public ColumnFamilyDescriptorBuilder setKeepDeletedCells(KeepDeletedCells value) {
    desc.setKeepDeletedCells(value);
    return this;
  }

  public ColumnFamilyDescriptorBuilder setMaxVersions(final int value) {
    desc.setMaxVersions(value);
    return this;
  }

  public ColumnFamilyDescriptorBuilder setMinVersions(final int value) {
    desc.setMinVersions(value);
    return this;
  }

  public ColumnFamilyDescriptorBuilder setMobCompactPartitionPolicy(final MobCompactPartitionPolicy value) {
    desc.setMobCompactPartitionPolicy(value);
    return this;
  }

  public ColumnFamilyDescriptorBuilder setMobEnabled(final boolean value) {
    desc.setMobEnabled(value);
    return this;
  }

  public ColumnFamilyDescriptorBuilder setMobThreshold(final long value) {
    desc.setMobThreshold(value);
    return this;
  }

  public ColumnFamilyDescriptorBuilder setPrefetchBlocksOnOpen(final boolean value) {
    desc.setPrefetchBlocksOnOpen(value);
    return this;
  }

  public ColumnFamilyDescriptorBuilder setScope(final int value) {
    desc.setScope(value);
    return this;
  }

  public ColumnFamilyDescriptorBuilder setStoragePolicy(final String value) {
    desc.setStoragePolicy(value);
    return this;
  }

  public ColumnFamilyDescriptorBuilder setTimeToLive(final int value) {
    desc.setTimeToLive(value);
    return this;
  }

  public ColumnFamilyDescriptorBuilder setTimeToLive(final String value) throws HBaseException {
    desc.setTimeToLive(value);
    return this;
  }

  public ColumnFamilyDescriptorBuilder setNewVersionBehavior(final boolean value) {
    desc.setNewVersionBehavior(value);
    return this;
  }

  public ColumnFamilyDescriptorBuilder setValue(final Bytes key, final Bytes value) {
    desc.setValue(key, value);
    return this;
  }

  public ColumnFamilyDescriptorBuilder setValue(final byte[] key, final byte[] value) {
    desc.setValue(key, value);
    return this;
  }

  public ColumnFamilyDescriptorBuilder setValue(final String key, final String value) {
    desc.setValue(key, value);
    return this;
  }

  public ColumnFamilyDescriptorBuilder setVersionsWithTimeToLive(final int retentionInterval,
                                                                 final int versionAfterInterval) {
    desc.setVersionsWithTimeToLive(retentionInterval, versionAfterInterval);
    return this;
  }

  /**
   * An ModifyableFamilyDescriptor contains information about a column family such as the
   * number of versions, compression settings, etc.
   *
   * It is used as input when creating a table or adding a column.
   * TODO: make this package-private after removing the HColumnDescriptor
   */
  @InterfaceAudience.Private
  public static class ModifyableColumnFamilyDescriptor
      implements ColumnFamilyDescriptor, Comparable<ModifyableColumnFamilyDescriptor> {

    // Column family name
    private final byte[] name;

    // Column metadata
    private final Map<Bytes, Bytes> values = new HashMap<>();

    /**
     * A map which holds the configuration specific to the column family. The
     * keys of the map have the same names as config keys and override the
     * defaults with cf-specific settings. Example usage may be for compactions,
     * etc.
     */
    private final Map<String, String> configuration = new HashMap<>();

    /**
     * Construct a column descriptor specifying only the family name The other
     * attributes are defaulted.
     *
     * @param name Column family name. Must be 'printable' -- digit or
     * letter -- and may not contain a <code>:</code>
     * TODO: make this private after the HCD is removed.
     */
    @InterfaceAudience.Private
    public ModifyableColumnFamilyDescriptor(final byte[] name) {
      this(isLegalColumnFamilyName(name), getDefaultValuesBytes(), Collections.emptyMap());
    }

    /**
     * Constructor. Makes a deep copy of the supplied descriptor.
     * TODO: make this private after the HCD is removed.
     * @param desc The descriptor.
     */
    @InterfaceAudience.Private
    public ModifyableColumnFamilyDescriptor(ColumnFamilyDescriptor desc) {
      this(desc.getName(), desc.getValues(), desc.getConfiguration());
    }

    private ModifyableColumnFamilyDescriptor(byte[] name, Map<Bytes, Bytes> values, Map<String, String> config) {
      this.name = name;
      this.values.putAll(values);
      this.configuration.putAll(config);
    }

    @Override
    public byte[] getName() {
      return Bytes.copy(name);
    }

    @Override
    public String getNameAsString() {
      return Bytes.toString(name);
    }

    @Override
    public Bytes getValue(Bytes key) {
      return values.get(key);
    }

    @Override
    public byte[] getValue(byte[] key) {
      Bytes value = values.get(new Bytes(key));
      return value == null ? null : value.get();
    }

    @Override
    public Map<Bytes, Bytes> getValues() {
      return Collections.unmodifiableMap(values);
    }

    /**
     * @param key The key.
     * @param value The value.
     * @return this (for chained invocation)
     */
    public ModifyableColumnFamilyDescriptor setValue(byte[] key, byte[] value) {
      return setValue(toBytesOrNull(key, Function.identity()), toBytesOrNull(value, Function.identity()));
    }

    public ModifyableColumnFamilyDescriptor setValue(String key, String value) {
      return setValue(toBytesOrNull(key, Bytes::toBytes), toBytesOrNull(value, Bytes::toBytes));
    }

    private ModifyableColumnFamilyDescriptor setValue(Bytes key, String value) {
      return setValue(key, toBytesOrNull(value, Bytes::toBytes));
    }
    /**
     * @param key The key.
     * @param value The value.
     * @return this (for chained invocation)
     */
    private ModifyableColumnFamilyDescriptor setValue(Bytes key, Bytes value) {
      if (value == null || value.getLength() == 0) {
        values.remove(key);
      } else {
        values.put(key, value);
      }
      return this;
    }

    /**
     *
     * @param key Key whose key and value we're to remove from HCD parameters.
     * @return this (for chained invocation)
     */
    public ModifyableColumnFamilyDescriptor removeValue(final Bytes key) {
      return setValue(key, (Bytes) null);
    }

    private static <T> Bytes toBytesOrNull(T t, Function<T, byte[]> f) {
      if (t == null) {
        return null;
      } else {
        return new Bytes(f.apply(t));
      }
    }

    private <T> T getStringOrDefault(Bytes key, Function<String, T> function, T defaultValue) {
      return getOrDefault(key, b -> function.apply(Bytes.toString(b)), defaultValue);
    }

    private <T> T getOrDefault(Bytes key, Function<byte[], T> function, T defaultValue) {
      Bytes value = values.get(key);
      if (value == null) {
        return defaultValue;
      } else {
        return function.apply(value.get());
      }
    }

    @Override
    public int getMaxVersions() {
      return getStringOrDefault(MAX_VERSIONS_BYTES, Integer::parseInt, DEFAULT_MAX_VERSIONS);
    }

    /**
     * @param maxVersions maximum number of versions
     * @return this (for chained invocation)
     */
    public ModifyableColumnFamilyDescriptor setMaxVersions(int maxVersions) {
      if (maxVersions <= 0) {
        // TODO: Allow maxVersion of 0 to be the way you say "Keep all versions".
        // Until there is support, consider 0 or < 0 -- a configuration error.
        throw new IllegalArgumentException("Maximum versions must be positive");
      }
      if (maxVersions < this.getMinVersions()) {
        throw new IllegalArgumentException("Set MaxVersion to " + maxVersions
            + " while minVersion is " + this.getMinVersions()
            + ". Maximum versions must be >= minimum versions ");
      }
      setValue(MAX_VERSIONS_BYTES, Integer.toString(maxVersions));
      return this;
    }

    /**
     * Set minimum and maximum versions to keep
     *
     * @param minVersions minimal number of versions
     * @param maxVersions maximum number of versions
     * @return this (for chained invocation)
     */
    public ModifyableColumnFamilyDescriptor setVersions(int minVersions, int maxVersions) {
      if (minVersions <= 0) {
        // TODO: Allow minVersion and maxVersion of 0 to be the way you say "Keep all versions".
        // Until there is support, consider 0 or < 0 -- a configuration error.
        throw new IllegalArgumentException("Minimum versions must be positive");
      }

      if (maxVersions < minVersions) {
        throw new IllegalArgumentException("Unable to set MaxVersion to " + maxVersions
            + " and set MinVersion to " + minVersions
            + ", as maximum versions must be >= minimum versions.");
      }
      setMinVersions(minVersions);
      setMaxVersions(maxVersions);
      return this;
    }


    @Override
    public int getBlocksize() {
      return getStringOrDefault(BLOCKSIZE_BYTES, Integer::valueOf, DEFAULT_BLOCKSIZE);
    }

    /**
     * @param s Blocksize to use when writing out storefiles/hfiles on this
     * column family.
     * @return this (for chained invocation)
     */
    public ModifyableColumnFamilyDescriptor setBlocksize(int s) {
      return setValue(BLOCKSIZE_BYTES, Integer.toString(s));
    }

    @Override
    public Compression.Algorithm getCompressionType() {
      return getStringOrDefault(COMPRESSION_BYTES,
          n -> Compression.Algorithm.valueOf(n.toUpperCase()), DEFAULT_COMPRESSION);
    }

    /**
     * Compression types supported in hbase. LZO is not bundled as part of the
     * hbase distribution. See
     * <a href="http://wiki.apache.org/hadoop/UsingLzoCompression">LZO
     * Compression</a>
     * for how to enable it.
     *
     * @param type Compression type setting.
     * @return this (for chained invocation)
     */
    public ModifyableColumnFamilyDescriptor setCompressionType(Compression.Algorithm type) {
      return setValue(COMPRESSION_BYTES, type.name());
    }

    @Override
    public DataBlockEncoding getDataBlockEncoding() {
      return getStringOrDefault(DATA_BLOCK_ENCODING_BYTES,
          n -> DataBlockEncoding.valueOf(n.toUpperCase()), DataBlockEncoding.NONE);
    }

    /**
     * Set data block encoding algorithm used in block cache.
     *
     * @param type What kind of data block encoding will be used.
     * @return this (for chained invocation)
     */
    public ModifyableColumnFamilyDescriptor setDataBlockEncoding(DataBlockEncoding type) {
      return setValue(DATA_BLOCK_ENCODING_BYTES, type == null ? DataBlockEncoding.NONE.name() : type.name());
    }

    /**
     * Set whether the tags should be compressed along with DataBlockEncoding.
     * When no DataBlockEncoding is been used, this is having no effect.
     *
     * @param compressTags
     * @return this (for chained invocation)
     */
    public ModifyableColumnFamilyDescriptor setCompressTags(boolean compressTags) {
      return setValue(COMPRESS_TAGS_BYTES, String.valueOf(compressTags));
    }

    @Override
    public boolean isCompressTags() {
      return getStringOrDefault(COMPRESS_TAGS_BYTES, Boolean::valueOf,
          DEFAULT_COMPRESS_TAGS);
    }

    @Override
    public Compression.Algorithm getCompactionCompressionType() {
      return getStringOrDefault(COMPRESSION_COMPACT_BYTES,
          n -> Compression.Algorithm.valueOf(n.toUpperCase()), getCompressionType());
    }

    /**
     * Compression types supported in hbase. LZO is not bundled as part of the
     * hbase distribution. See
     * <a href="http://wiki.apache.org/hadoop/UsingLzoCompression">LZO
     * Compression</a>
     * for how to enable it.
     *
     * @param type Compression type setting.
     * @return this (for chained invocation)
     */
    public ModifyableColumnFamilyDescriptor setCompactionCompressionType(
        Compression.Algorithm type) {
      return setValue(COMPRESSION_COMPACT_BYTES, type.name());
    }

    @Override
    public boolean isInMemory() {
      return getStringOrDefault(IN_MEMORY_BYTES, Boolean::valueOf, DEFAULT_IN_MEMORY);
    }

    /**
     * @param inMemory True if we are to favor keeping all values for this
     * column family in the HRegionServer cache
     * @return this (for chained invocation)
     */
    public ModifyableColumnFamilyDescriptor setInMemory(boolean inMemory) {
      return setValue(IN_MEMORY_BYTES, Boolean.toString(inMemory));
    }

    @Override
    public MemoryCompactionPolicy getInMemoryCompaction() {
      return getStringOrDefault(IN_MEMORY_COMPACTION_BYTES,
          n -> MemoryCompactionPolicy.valueOf(n.toUpperCase()), null);
    }

    /**
     * @param inMemoryCompaction the prefered in-memory compaction policy for
     * this column family
     * @return this (for chained invocation)
     */
    public ModifyableColumnFamilyDescriptor setInMemoryCompaction(MemoryCompactionPolicy inMemoryCompaction) {
      return setValue(IN_MEMORY_COMPACTION_BYTES, inMemoryCompaction.name());
    }

    @Override
    public KeepDeletedCells getKeepDeletedCells() {
      return getStringOrDefault(KEEP_DELETED_CELLS_BYTES,
          KeepDeletedCells::getValue, DEFAULT_KEEP_DELETED);
    }

    /**
     * @param keepDeletedCells True if deleted rows should not be collected
     * immediately.
     * @return this (for chained invocation)
     */
    public ModifyableColumnFamilyDescriptor setKeepDeletedCells(KeepDeletedCells keepDeletedCells) {
      return setValue(KEEP_DELETED_CELLS_BYTES, keepDeletedCells.name());
    }

    /**
     * By default, HBase only consider timestamp in versions. So a previous Delete with higher ts
     * will mask a later Put with lower ts. Set this to true to enable new semantics of versions.
     * We will also consider mvcc in versions. See HBASE-15968 for details.
     */
    @Override
    public boolean isNewVersionBehavior() {
      return getStringOrDefault(NEW_VERSION_BEHAVIOR_BYTES,
          Boolean::parseBoolean, DEFAULT_NEW_VERSION_BEHAVIOR);
    }

    public ModifyableColumnFamilyDescriptor setNewVersionBehavior(boolean newVersionBehavior) {
      return setValue(NEW_VERSION_BEHAVIOR_BYTES, Boolean.toString(newVersionBehavior));
    }

    @Override
    public int getTimeToLive() {
      return getStringOrDefault(TTL_BYTES, Integer::parseInt, DEFAULT_TTL);
    }

    /**
     * @param timeToLive Time-to-live of cell contents, in seconds.
     * @return this (for chained invocation)
     */
    public ModifyableColumnFamilyDescriptor setTimeToLive(int timeToLive) {
      return setValue(TTL_BYTES, Integer.toString(timeToLive));
    }

    /**
     * @param timeToLive Time-to-live of cell contents, in seconds.
     * @return this (for chained invocation)
     * @throws org.apache.hadoop.hbase.exceptions.HBaseException
     */
    public ModifyableColumnFamilyDescriptor setTimeToLive(String timeToLive) throws HBaseException {
      return setTimeToLive(Integer.parseInt(PrettyPrinter.valueOf(timeToLive, Unit.TIME_INTERVAL)));
    }

    @Override
    public int getMinVersions() {
      return getStringOrDefault(MIN_VERSIONS_BYTES, Integer::valueOf, DEFAULT_MIN_VERSIONS);
    }

    /**
     * @param minVersions The minimum number of versions to keep. (used when
     * timeToLive is set)
     * @return this (for chained invocation)
     */
    public ModifyableColumnFamilyDescriptor setMinVersions(int minVersions) {
      return setValue(MIN_VERSIONS_BYTES, Integer.toString(minVersions));
    }

    /**
     * Retain all versions for a given TTL(retentionInterval), and then only a specific number
     * of versions(versionAfterInterval) after that interval elapses.
     *
     * @param retentionInterval Retain all versions for this interval
     * @param versionAfterInterval Retain no of versions to retain after retentionInterval
     * @return this (for chained invocation)
     */
    public ModifyableColumnFamilyDescriptor setVersionsWithTimeToLive(
        final int retentionInterval, final int versionAfterInterval) {
      ModifyableColumnFamilyDescriptor modifyableColumnFamilyDescriptor =
          setVersions(versionAfterInterval, Integer.MAX_VALUE);
      modifyableColumnFamilyDescriptor.setTimeToLive(retentionInterval);
      modifyableColumnFamilyDescriptor.setKeepDeletedCells(KeepDeletedCells.TTL);
      return modifyableColumnFamilyDescriptor;
    }

    @Override
    public boolean isBlockCacheEnabled() {
      return getStringOrDefault(BLOCKCACHE_BYTES, Boolean::valueOf, DEFAULT_BLOCKCACHE);
    }

    /**
     * @param blockCacheEnabled True if hfile DATA type blocks should be cached
     * (We always cache INDEX and BLOOM blocks; you cannot turn this off).
     * @return this (for chained invocation)
     */
    public ModifyableColumnFamilyDescriptor setBlockCacheEnabled(boolean blockCacheEnabled) {
      return setValue(BLOCKCACHE_BYTES, Boolean.toString(blockCacheEnabled));
    }

    @Override
    public BloomType getBloomFilterType() {
      return getStringOrDefault(BLOOMFILTER_BYTES, n -> BloomType.valueOf(n.toUpperCase()),
          DEFAULT_BLOOMFILTER);
    }

    public ModifyableColumnFamilyDescriptor setBloomFilterType(final BloomType bt) {
      return setValue(BLOOMFILTER_BYTES, bt.name());
    }

    @Override
    public int getScope() {
      return getStringOrDefault(REPLICATION_SCOPE_BYTES, Integer::valueOf, DEFAULT_REPLICATION_SCOPE);
    }

    /**
     * @param scope the scope tag
     * @return this (for chained invocation)
     */
    public ModifyableColumnFamilyDescriptor setScope(int scope) {
      return setValue(REPLICATION_SCOPE_BYTES, Integer.toString(scope));
    }

    @Override
    public boolean isCacheDataOnWrite() {
      return getStringOrDefault(CACHE_DATA_ON_WRITE_BYTES, Boolean::valueOf, DEFAULT_CACHE_DATA_ON_WRITE);
    }

    /**
     * @param value true if we should cache data blocks on write
     * @return this (for chained invocation)
     */
    public ModifyableColumnFamilyDescriptor setCacheDataOnWrite(boolean value) {
      return setValue(CACHE_DATA_ON_WRITE_BYTES, Boolean.toString(value));
    }

    @Override
    public boolean isCacheIndexesOnWrite() {
      return getStringOrDefault(CACHE_INDEX_ON_WRITE_BYTES, Boolean::valueOf, DEFAULT_CACHE_INDEX_ON_WRITE);
    }

    /**
     * @param value true if we should cache index blocks on write
     * @return this (for chained invocation)
     */
    public ModifyableColumnFamilyDescriptor setCacheIndexesOnWrite(boolean value) {
      return setValue(CACHE_INDEX_ON_WRITE_BYTES, Boolean.toString(value));
    }

    @Override
    public boolean isCacheBloomsOnWrite() {
      return getStringOrDefault(CACHE_BLOOMS_ON_WRITE_BYTES, Boolean::valueOf, DEFAULT_CACHE_BLOOMS_ON_WRITE);
    }

    /**
     * @param value true if we should cache bloomfilter blocks on write
     * @return this (for chained invocation)
     */
    public ModifyableColumnFamilyDescriptor setCacheBloomsOnWrite(boolean value) {
      return setValue(CACHE_BLOOMS_ON_WRITE_BYTES, Boolean.toString(value));
    }

    @Override
    public boolean isEvictBlocksOnClose() {
      return getStringOrDefault(EVICT_BLOCKS_ON_CLOSE_BYTES, Boolean::valueOf, DEFAULT_EVICT_BLOCKS_ON_CLOSE);
    }

    /**
     * @param value true if we should evict cached blocks from the blockcache on
     * close
     * @return this (for chained invocation)
     */
    public ModifyableColumnFamilyDescriptor setEvictBlocksOnClose(boolean value) {
      return setValue(EVICT_BLOCKS_ON_CLOSE_BYTES, Boolean.toString(value));
    }

    @Override
    public boolean isPrefetchBlocksOnOpen() {
      return getStringOrDefault(PREFETCH_BLOCKS_ON_OPEN_BYTES, Boolean::valueOf, DEFAULT_PREFETCH_BLOCKS_ON_OPEN);
    }

    /**
     * @param value true if we should prefetch blocks into the blockcache on
     * open
     * @return this (for chained invocation)
     */
    public ModifyableColumnFamilyDescriptor setPrefetchBlocksOnOpen(boolean value) {
      return setValue(PREFETCH_BLOCKS_ON_OPEN_BYTES, Boolean.toString(value));
    }

    @Override
    public String toString() {
      StringBuilder s = new StringBuilder();
      s.append('{');
      s.append(HConstants.NAME);
      s.append(" => '");
      s.append(getNameAsString());
      s.append("'");
      s.append(getValues(true));
      s.append('}');
      return s.toString();
    }


    @Override
    public String toStringCustomizedValues() {
      StringBuilder s = new StringBuilder();
      s.append('{');
      s.append(HConstants.NAME);
      s.append(" => '");
      s.append(getNameAsString());
      s.append("'");
      s.append(getValues(false));
      s.append('}');
      return s.toString();
    }

    private StringBuilder getValues(boolean printDefaults) {
      StringBuilder s = new StringBuilder();

      boolean hasConfigKeys = false;

      // print all reserved keys first
      for (Map.Entry<Bytes, Bytes> entry : values.entrySet()) {
        if (!RESERVED_KEYWORDS.contains(entry.getKey())) {
          hasConfigKeys = true;
          continue;
        }
        String key = Bytes.toString(entry.getKey().get());
        String value = Bytes.toStringBinary(entry.getValue().get());
        if (printDefaults
            || !DEFAULT_VALUES.containsKey(key)
            || !DEFAULT_VALUES.get(key).equalsIgnoreCase(value)) {
          s.append(", ");
          s.append(key);
          s.append(" => ");
          s.append('\'').append(PrettyPrinter.format(value, getUnit(key))).append('\'');
        }
      }

      // print all non-reserved, advanced config keys as a separate subset
      if (hasConfigKeys) {
        s.append(", ");
        s.append(HConstants.METADATA).append(" => ");
        s.append('{');
        boolean printComma = false;
        for (Map.Entry<Bytes, Bytes> entry : values.entrySet()) {
          Bytes k = entry.getKey();
          if (RESERVED_KEYWORDS.contains(k)) {
            continue;
          }
          String key = Bytes.toString(k.get());
          String value = Bytes.toStringBinary(entry.getValue().get());
          if (printComma) {
            s.append(", ");
          }
          printComma = true;
          s.append('\'').append(key).append('\'');
          s.append(" => ");
          s.append('\'').append(PrettyPrinter.format(value, getUnit(key))).append('\'');
        }
        s.append('}');
      }

      if (!configuration.isEmpty()) {
        s.append(", ");
        s.append(HConstants.CONFIGURATION).append(" => ");
        s.append('{');
        boolean printCommaForConfiguration = false;
        for (Map.Entry<String, String> e : configuration.entrySet()) {
          if (printCommaForConfiguration) {
            s.append(", ");
          }
          printCommaForConfiguration = true;
          s.append('\'').append(e.getKey()).append('\'');
          s.append(" => ");
          s.append('\'').append(PrettyPrinter.format(e.getValue(), getUnit(e.getKey()))).append('\'');
        }
        s.append("}");
      }
      return s;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj instanceof ModifyableColumnFamilyDescriptor) {
        return ColumnFamilyDescriptor.COMPARATOR.compare(this, (ModifyableColumnFamilyDescriptor) obj) == 0;
      }
      return false;
    }

    @Override
    public int hashCode() {
      int result = Bytes.hashCode(name);
      result ^= (int) COLUMN_DESCRIPTOR_VERSION;
      result ^= values.hashCode();
      result ^= configuration.hashCode();
      return result;
    }

    @Override
    public int compareTo(ModifyableColumnFamilyDescriptor other) {
      return COMPARATOR.compare(this, other);
    }

    /**
     * @return This instance serialized with pb with pb magic prefix
     * @see #parseFrom(byte[])
     */
    private byte[] toByteArray() {
      return ProtobufUtil.prependPBMagic(ProtobufUtil.toColumnFamilySchema(this)
          .toByteArray());
    }

    /**
     * @param bytes A pb serialized {@link ModifyableColumnFamilyDescriptor} instance with pb
     * magic prefix
     * @return An instance of {@link ModifyableColumnFamilyDescriptor} made from
     * <code>bytes</code>
     * @throws DeserializationException
     * @see #toByteArray()
     */
    private static ColumnFamilyDescriptor parseFrom(final byte[] bytes) throws DeserializationException {
      if (!ProtobufUtil.isPBMagicPrefix(bytes)) {
        throw new DeserializationException("No magic");
      }
      int pblen = ProtobufUtil.lengthOfPBMagic();
      ColumnFamilySchema.Builder builder = ColumnFamilySchema.newBuilder();
      ColumnFamilySchema cfs = null;
      try {
        ProtobufUtil.mergeFrom(builder, bytes, pblen, bytes.length - pblen);
        cfs = builder.build();
      } catch (IOException e) {
        throw new DeserializationException(e);
      }
      return ProtobufUtil.toColumnFamilyDescriptor(cfs);
    }

    @Override
    public String getConfigurationValue(String key) {
      return configuration.get(key);
    }

    @Override
    public Map<String, String> getConfiguration() {
      // shallow pointer copy
      return Collections.unmodifiableMap(configuration);
    }

    /**
     * Setter for storing a configuration setting in {@link #configuration} map.
     *
     * @param key Config key. Same as XML config key e.g.
     * hbase.something.or.other.
     * @param value String value. If null, removes the configuration.
     * @return this (for chained invocation)
     */
    public ModifyableColumnFamilyDescriptor setConfiguration(String key, String value) {
      if (value == null || value.length() == 0) {
        configuration.remove(key);
      } else {
        configuration.put(key, value);
      }
      return this;
    }

    /**
     * Remove a configuration setting represented by the key from the
     * {@link #configuration} map.
     *
     * @param key
     * @return this (for chained invocation)
     */
    public ModifyableColumnFamilyDescriptor removeConfiguration(final String key) {
      return setConfiguration(key, null);
    }

    @Override
    public String getEncryptionType() {
      return getStringOrDefault(ENCRYPTION_BYTES, Function.identity(), null);
    }

    /**
     * Set the encryption algorithm for use with this family
     *
     * @param algorithm
     * @return this (for chained invocation)
     */
    public ModifyableColumnFamilyDescriptor setEncryptionType(String algorithm) {
      return setValue(ENCRYPTION_BYTES, algorithm);
    }

    @Override
    public byte[] getEncryptionKey() {
      return getOrDefault(ENCRYPTION_KEY_BYTES, Bytes::copy, null);
    }

    /**
     * Set the raw crypto key attribute for the family
     *
     * @param keyBytes
     * @return this (for chained invocation)
     */
    public ModifyableColumnFamilyDescriptor setEncryptionKey(byte[] keyBytes) {
      return setValue(ENCRYPTION_KEY_BYTES, new Bytes(keyBytes));
    }

    @Override
    public long getMobThreshold() {
      return getStringOrDefault(MOB_THRESHOLD_BYTES, Long::valueOf, DEFAULT_MOB_THRESHOLD);
    }

    /**
     * Sets the mob threshold of the family.
     *
     * @param threshold The mob threshold.
     * @return this (for chained invocation)
     */
    public ModifyableColumnFamilyDescriptor setMobThreshold(long threshold) {
      return setValue(MOB_THRESHOLD_BYTES, String.valueOf(threshold));
    }

    @Override
    public boolean isMobEnabled() {
      return getStringOrDefault(IS_MOB_BYTES, Boolean::valueOf, DEFAULT_MOB);
    }

    /**
     * Enables the mob for the family.
     *
     * @param isMobEnabled Whether to enable the mob for the family.
     * @return this (for chained invocation)
     */
    public ModifyableColumnFamilyDescriptor setMobEnabled(boolean isMobEnabled) {
      return setValue(IS_MOB_BYTES, String.valueOf(isMobEnabled));
    }

    @Override
    public MobCompactPartitionPolicy getMobCompactPartitionPolicy() {
      return getStringOrDefault(MOB_COMPACT_PARTITION_POLICY_BYTES,
          n -> MobCompactPartitionPolicy.valueOf(n.toUpperCase()),
          DEFAULT_MOB_COMPACT_PARTITION_POLICY);
    }

    /**
     * Set the mob compact partition policy for the family.
     *
     * @param policy policy type
     * @return this (for chained invocation)
     */
    public ModifyableColumnFamilyDescriptor setMobCompactPartitionPolicy(MobCompactPartitionPolicy policy) {
      return setValue(MOB_COMPACT_PARTITION_POLICY_BYTES, policy.name());
    }

    @Override
    public short getDFSReplication() {
      return getStringOrDefault(DFS_REPLICATION_BYTES,
          Short::valueOf, DEFAULT_DFS_REPLICATION);
    }

    /**
     * Set the replication factor to hfile(s) belonging to this family
     *
     * @param replication number of replicas the blocks(s) belonging to this CF
     * should have, or {@link #DEFAULT_DFS_REPLICATION} for the default
     * replication factor set in the filesystem
     * @return this (for chained invocation)
     */
    public ModifyableColumnFamilyDescriptor setDFSReplication(short replication) {
      if (replication < 1 && replication != DEFAULT_DFS_REPLICATION) {
        throw new IllegalArgumentException(
            "DFS replication factor cannot be less than 1 if explicitly set.");
      }
      return setValue(DFS_REPLICATION_BYTES, Short.toString(replication));
    }

    @Override
    public String getStoragePolicy() {
      return getStringOrDefault(STORAGE_POLICY_BYTES, Function.identity(), null);
    }

    /**
     * Set the storage policy for use with this family
     *
     * @param policy the policy to set, valid setting includes:
     * <i>"LAZY_PERSIST"</i>,
     * <i>"ALL_SSD"</i>, <i>"ONE_SSD"</i>, <i>"HOT"</i>, <i>"WARM"</i>,
     * <i>"COLD"</i>
     * @return this (for chained invocation)
     */
    public ModifyableColumnFamilyDescriptor setStoragePolicy(String policy) {
      return setValue(STORAGE_POLICY_BYTES, policy);
    }

  }
}
