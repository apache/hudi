/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.metadata;

import org.apache.hudi.avro.AvroSchemaCache;
import org.apache.hudi.avro.model.HoodieMetadataBloomFilter;
import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.avro.model.HoodieMetadataFileInfo;
import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.avro.model.HoodieRecordIndexInfo;
import org.apache.hudi.avro.model.HoodieSecondaryIndexInfo;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.hash.ColumnIndexID;
import org.apache.hudi.common.util.hash.FileIndexID;
import org.apache.hudi.common.util.hash.PartitionIndexID;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.index.expression.HoodieExpressionIndex;
import org.apache.hudi.io.storage.HoodieAvroHFileReaderImplBase;
import org.apache.hudi.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.util.Lazy;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.util.StringUtils.EMPTY_STRING;
import static org.apache.hudi.common.util.ValidationUtils.checkArgument;
import static org.apache.hudi.common.util.ValidationUtils.checkState;
import static org.apache.hudi.metadata.HoodieTableMetadata.RECORDKEY_PARTITION_LIST;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getBloomFilterIndexPartitionIdentifier;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getColumnStatsIndexPartitionIdentifier;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getLocationFromRecordIndexInfo;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getPartitionIdentifierForFilesPartition;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getPartitionStatsIndexKey;

/**
 * MetadataTable records are persisted with the schema defined in HoodieMetadata.avsc.
 * This class represents the payload for the MetadataTable.
 * <p>
 * This single metadata payload is shared by all the partitions under the metadata table.
 * The partition specific records are determined by the field "type" saved within the record.
 * The following types are supported:
 * <p>
 * METADATA_TYPE_PARTITION_LIST (1):
 * -- List of all partitions. There is a single such record
 * -- key = @{@link HoodieTableMetadata#RECORDKEY_PARTITION_LIST}
 * <p>
 * METADATA_TYPE_FILE_LIST (2):
 * -- List of all files in a partition. There is one such record for each partition
 * -- key = partition name
 * <p>
 * METADATA_TYPE_COLUMN_STATS (3):
 * -- This is an index for column stats in the table
 * <p>
 * METADATA_TYPE_BLOOM_FILTER (4):
 * -- This is an index for base file bloom filters. This is a map of FileID to its BloomFilter byte[].
 * <p>
 * During compaction on the table, the deletions are merged with additions and hence records are pruned.
 */
public class HoodieMetadataPayload implements HoodieRecordPayload<HoodieMetadataPayload> {
  private static final Schema HOODIE_METADATA_SCHEMA = AvroSchemaCache.intern(HoodieMetadataRecord.getClassSchema());
  /**
   * Field offsets when metadata fields are present
   */
  private static final int KEY_FIELD_OFFSET = HoodieRecord.HOODIE_META_COLUMNS.size();
  private static final int TYPE_FIELD_OFFSET = KEY_FIELD_OFFSET + 1;
  private static final int FILESYSTEM_METADATA_FIELD_OFFSET = TYPE_FIELD_OFFSET + 1;
  private static final int BLOOM_FILTER_METADATA_FIELD_OFFSET = FILESYSTEM_METADATA_FIELD_OFFSET + 1;
  private static final int COLUMN_STATS_METADATA_FIELD_OFFSET = BLOOM_FILTER_METADATA_FIELD_OFFSET + 1;
  private static final int RECORD_INDEX_METADATA_FIELD_OFFSET = COLUMN_STATS_METADATA_FIELD_OFFSET + 1;
  private static final int SECONDARY_INDEX_METADATA_FIELD_OFFSET = RECORD_INDEX_METADATA_FIELD_OFFSET + 1;

  /**
   * HoodieMetadata schema field ids
   */
  public static final String KEY_FIELD_NAME = HoodieAvroHFileReaderImplBase.KEY_FIELD_NAME;
  public static final String SCHEMA_FIELD_NAME_TYPE = "type";
  public static final String SCHEMA_FIELD_NAME_METADATA = "filesystemMetadata";
  public static final String SCHEMA_FIELD_ID_COLUMN_STATS = "ColumnStatsMetadata";
  public static final String SCHEMA_FIELD_ID_BLOOM_FILTER = "BloomFilterMetadata";
  public static final String SCHEMA_FIELD_ID_RECORD_INDEX = "recordIndexMetadata";
  public static final String SCHEMA_FIELD_ID_SECONDARY_INDEX = "SecondaryIndexMetadata";

  /**
   * HoodieMetadata bloom filter payload field ids
   */
  public static final String FIELD_IS_DELETED = "isDeleted";
  public static final String BLOOM_FILTER_FIELD_TYPE = "type";
  public static final String BLOOM_FILTER_FIELD_TIMESTAMP = "timestamp";
  public static final String BLOOM_FILTER_FIELD_BLOOM_FILTER = "bloomFilter";
  public static final String BLOOM_FILTER_FIELD_IS_DELETED = FIELD_IS_DELETED;

  /**
   * HoodieMetadata column stats payload field ids
   */
  public static final String COLUMN_STATS_FIELD_MIN_VALUE = "minValue";
  public static final String COLUMN_STATS_FIELD_MAX_VALUE = "maxValue";
  public static final String COLUMN_STATS_FIELD_NULL_COUNT = "nullCount";
  public static final String COLUMN_STATS_FIELD_VALUE_COUNT = "valueCount";
  public static final String COLUMN_STATS_FIELD_TOTAL_SIZE = "totalSize";
  public static final String COLUMN_STATS_FIELD_FILE_NAME = "fileName";
  public static final String COLUMN_STATS_FIELD_COLUMN_NAME = "columnName";
  public static final String COLUMN_STATS_FIELD_TOTAL_UNCOMPRESSED_SIZE = "totalUncompressedSize";
  public static final String COLUMN_STATS_FIELD_IS_DELETED = FIELD_IS_DELETED;
  public static final String COLUMN_STATS_FIELD_IS_TIGHT_BOUND = "isTightBound";
  public static final String COLUMN_STATS_FIELD_VALUE_TYPE = "valueType";
  public static final String COLUMN_STATS_FIELD_VALUE_TYPE_ORDINAL = "typeOrdinal";
  public static final String COLUMN_STATS_FIELD_VALUE_TYPE_ADDITIONAL_INFO = "additionalInfo";

  /**
   * HoodieMetadata record index payload field ids
   */
  public static final String RECORD_INDEX_FIELD_PARTITION = "partitionName";
  public static final String RECORD_INDEX_FIELD_FILEID_HIGH_BITS = "fileIdHighBits";
  public static final String RECORD_INDEX_FIELD_FILEID_LOW_BITS = "fileIdLowBits";
  public static final String RECORD_INDEX_FIELD_FILE_INDEX = "fileIndex";
  public static final String RECORD_INDEX_FIELD_INSTANT_TIME = "instantTime";
  public static final String RECORD_INDEX_FIELD_FILEID = "fileId";
  public static final String RECORD_INDEX_FIELD_FILEID_ENCODING = "fileIdEncoding";
  public static final int RECORD_INDEX_FIELD_FILEID_ENCODING_UUID = 0;
  public static final int RECORD_INDEX_FIELD_FILEID_ENCODING_RAW_STRING = 1;
  public static final String RECORD_INDEX_FIELD_POSITION = "position";

  /**
   * FileIndex value saved in record index record when the fileId has no index (old format of base filename)
   */
  public static final int RECORD_INDEX_MISSING_FILEINDEX_FALLBACK = -1;

  /**
   * HoodieMetadata secondary index payload field ids
   */
  public static final String SECONDARY_INDEX_RECORD_KEY_ESCAPE_CHAR = "\\";
  public static final char SECONDARY_INDEX_RECORD_KEY_SEPARATOR_CHAR = '$';
  public static final String SECONDARY_INDEX_RECORD_KEY_SEPARATOR = String.valueOf(SECONDARY_INDEX_RECORD_KEY_SEPARATOR_CHAR);
  public static final String SECONDARY_INDEX_FIELD_IS_DELETED = FIELD_IS_DELETED;

  /**
   * NOTE: PLEASE READ CAREFULLY
   * <p>
   * In Avro 1.10 generated builders rely on {@code SpecificData.getForSchema} invocation that in turn
   * does use reflection to load the code-gen'd class corresponding to the Avro record model. This has
   * serious adverse effects in terms of performance when gets executed on the hot-path (both, in terms
   * of runtime and efficiency).
   * <p>
   * To work this around instead of using default code-gen'd builder invoking {@code SpecificData.getForSchema},
   * we instead rely on overloaded ctor accepting another instance of the builder: {@code Builder(Builder)},
   * which bypasses such invocation. Following corresponding builder's stubs are statically initialized
   * to be used exactly for that purpose.
   * <p>
   * You can find more details in HUDI-3834.
   */
  public static final Lazy<HoodieMetadataColumnStats.Builder> METADATA_COLUMN_STATS_BUILDER_STUB = Lazy.lazily(HoodieMetadataColumnStats::newBuilder);
  private static final HoodieMetadataFileInfo DELETE_FILE_METADATA = new HoodieMetadataFileInfo(0L, true);
  protected String key = null;
  protected int type = 0;
  protected Map<String, HoodieMetadataFileInfo> filesystemMetadata = null;
  protected HoodieMetadataBloomFilter bloomFilterMetadata = null;
  protected HoodieMetadataColumnStats columnStatMetadata = null;
  protected HoodieRecordIndexInfo recordIndexMetadata;
  protected HoodieSecondaryIndexInfo secondaryIndexMetadata;
  private boolean isDeletedRecord = false;

  public HoodieMetadataPayload(@Nullable GenericRecord record, Comparable<?> orderingVal) {
    this(Option.ofNullable(record));
  }

  public HoodieMetadataPayload(Option<GenericRecord> recordOpt) {
    if (recordOpt.isPresent()) {
      GenericRecord record = recordOpt.get();
      // This can be simplified using SpecificData.deepcopy once this bug is fixed
      // https://issues.apache.org/jira/browse/AVRO-1811
      //
      // NOTE: {@code HoodieMetadataRecord} has to always carry both "key" and "type" fields
      //       for it to be handled appropriately, therefore these fields have to be reflected
      //       in any (read-)projected schema
      key = record.get(KEY_FIELD_NAME).toString();
      type = (int) record.get(SCHEMA_FIELD_NAME_TYPE);
      MetadataPartitionType.get(type).constructMetadataPayload(this, record);
    } else {
      this.isDeletedRecord = true;
    }
  }

  protected HoodieMetadataPayload(String key, int type, Map<String, HoodieMetadataFileInfo> filesystemMetadata) {
    this(key, type, filesystemMetadata, null, null, null, null, false);
  }

  protected HoodieMetadataPayload(String key, HoodieMetadataBloomFilter metadataBloomFilter) {
    this(key, MetadataPartitionType.BLOOM_FILTERS.getRecordType(), null, metadataBloomFilter, null, null, null, metadataBloomFilter.getIsDeleted());
  }

  protected HoodieMetadataPayload(String key, HoodieMetadataColumnStats columnStats, int recordType) {
    this(key, recordType, null, null, columnStats, null, null, columnStats.getIsDeleted());
  }

  private HoodieMetadataPayload(String key, HoodieRecordIndexInfo recordIndexMetadata) {
    this(key, MetadataPartitionType.RECORD_INDEX.getRecordType(), null, null, null, recordIndexMetadata, null, false);
  }

  protected HoodieMetadataPayload(String key, HoodieSecondaryIndexInfo secondaryIndexMetadata) {
    this(key, MetadataPartitionType.SECONDARY_INDEX.getRecordType(), null, null, null, null, secondaryIndexMetadata, secondaryIndexMetadata.getIsDeleted());
  }

  protected HoodieMetadataPayload(String key, int type,
                                  Map<String, HoodieMetadataFileInfo> filesystemMetadata,
                                  HoodieMetadataBloomFilter metadataBloomFilter,
                                  HoodieMetadataColumnStats columnStats,
                                  HoodieRecordIndexInfo recordIndexMetadata,
                                  HoodieSecondaryIndexInfo secondaryIndexMetadata,
                                  boolean isDeletedRecord) {
    this.key = key;
    this.type = type;
    this.filesystemMetadata = filesystemMetadata;
    this.bloomFilterMetadata = metadataBloomFilter;
    this.columnStatMetadata = columnStats;
    this.recordIndexMetadata = recordIndexMetadata;
    this.secondaryIndexMetadata = secondaryIndexMetadata;
    this.isDeletedRecord = isDeletedRecord;
  }

  /**
   * Create and return a {@code HoodieMetadataPayload} to save list of partitions.
   *
   * @param partitions The list of partitions
   */
  public static HoodieRecord<HoodieMetadataPayload> createPartitionListRecord(Collection<String> partitions) {
    return createPartitionListRecord(partitions, false);
  }

  /**
   * Create and return a {@code HoodieMetadataPayload} to save list of partitions.
   *
   * @param partitions The list of partitions
   */
  public static HoodieRecord<HoodieMetadataPayload> createPartitionListRecord(Collection<String> partitions, boolean isDeleted) {
    Map<String, HoodieMetadataFileInfo> fileInfo = new HashMap<>();
    partitions.forEach(partition -> fileInfo.put(getPartitionIdentifierForFilesPartition(partition), new HoodieMetadataFileInfo(0L, isDeleted)));

    HoodieKey key = new HoodieKey(RECORDKEY_PARTITION_LIST, MetadataPartitionType.ALL_PARTITIONS.getPartitionPath());
    HoodieMetadataPayload payload = new HoodieMetadataPayload(key.getRecordKey(), MetadataPartitionType.ALL_PARTITIONS.getRecordType(), fileInfo);
    return new HoodieAvroRecord<>(key, payload);
  }

  /**
   * Create and return a {@code HoodieMetadataPayload} to save list of files within a partition.
   *
   * @param partition    The name of the partition
   * @param filesAdded   Mapping of files to their sizes for files which have been added to this partition
   * @param filesDeleted List of files which have been deleted from this partition
   */
  public static HoodieRecord<HoodieMetadataPayload> createPartitionFilesRecord(String partition,
                                                                               Map<String, Long> filesAdded,
                                                                               List<String> filesDeleted) {
    return createPartitionFilesRecord(partition, filesAdded, filesDeleted, false);
  }

  public static HoodieRecord<HoodieMetadataPayload> createPartitionFilesRecord(String partition,
                                                                               Map<String, Long> filesAdded,
                                                                               List<String> filesDeleted,
                                                                               boolean isPartitionDeleted) {
    String partitionIdentifier = getPartitionIdentifierForFilesPartition(partition);
    HoodieKey key = new HoodieKey(partitionIdentifier, MetadataPartitionType.FILES.getPartitionPath());
    if (isPartitionDeleted) {
      return new HoodieAvroRecord<>(key, new HoodieMetadataPayload(Option.empty()));
    }

    int size = filesAdded.size() + filesDeleted.size();
    Map<String, HoodieMetadataFileInfo> fileInfo = new HashMap<>(size, 1);
    filesAdded.forEach((fileName, fileSize) -> fileInfo.put(fileName, new HoodieMetadataFileInfo(fileSize, false)));

    filesDeleted.forEach(fileName -> fileInfo.put(fileName, DELETE_FILE_METADATA));

    HoodieMetadataPayload payload = new HoodieMetadataPayload(key.getRecordKey(), MetadataPartitionType.FILES.getRecordType(), fileInfo);
    return new HoodieAvroRecord<>(key, payload);
  }

  public static HoodieRecord<HoodieMetadataPayload> createBloomFilterMetadataRecord(final String partitionName,
                                                                                    final String baseFileName,
                                                                                    final String timestamp,
                                                                                    final String bloomFilterType,
                                                                                    final ByteBuffer bloomFilter,
                                                                                    final boolean isDeleted) {
    return createBloomFilterMetadataRecord(partitionName, baseFileName, timestamp, bloomFilterType, bloomFilter, isDeleted, MetadataPartitionType.BLOOM_FILTERS.getPartitionPath());
  }

  /**
   * Create bloom filter metadata record.
   *
   * @param partitionName - Partition name
   * @param baseFileName  - Base file name for which the bloom filter needs to persisted
   * @param timestamp     - Instant timestamp responsible for this record
   * @param bloomFilter   - Bloom filter for the File
   * @param isDeleted     - Is the bloom filter no more valid
   * @return Metadata payload containing the fileID and its bloom filter record
   */
  public static HoodieRecord<HoodieMetadataPayload> createBloomFilterMetadataRecord(final String partitionName,
                                                                                    final String baseFileName,
                                                                                    final String timestamp,
                                                                                    final String bloomFilterType,
                                                                                    final ByteBuffer bloomFilter,
                                                                                    final boolean isDeleted,
                                                                                    String metadataPartitionName) {
    checkArgument(!baseFileName.contains(StoragePath.SEPARATOR)
            && FSUtils.isBaseFile(new StoragePath(baseFileName)),
        "Invalid base file '" + baseFileName + "' for MetaIndexBloomFilter!");
    final String bloomFilterIndexKey = getBloomFilterRecordKey(partitionName, baseFileName);
    HoodieKey key = new HoodieKey(bloomFilterIndexKey, metadataPartitionName);

    HoodieMetadataBloomFilter metadataBloomFilter =
        new HoodieMetadataBloomFilter(bloomFilterType, timestamp, bloomFilter, isDeleted);
    HoodieMetadataPayload metadataPayload = new HoodieMetadataPayload(key.getRecordKey(), metadataBloomFilter);
    return new HoodieAvroRecord<>(key, metadataPayload);
  }

  @Override
  public HoodieMetadataPayload preCombine(HoodieMetadataPayload previousRecord) {
    if (this.isDeletedRecord) {
      // This happens when a record has been deleted. The previous version of the record should be ignored.
      return this;
    }
    if (previousRecord.isDeletedRecord) {
      // This happens when a record with same key is added after a deletion.
      return this;
    }

    // Validation of record merge scenario. Only records of same type and key can be combined. 
    checkArgument(previousRecord.type == type,
        "Cannot combine " + previousRecord.type + " with " + type);
    checkArgument(previousRecord.key.equals(key),
        "Cannot combine " + previousRecord.key + " with " + key + " as the keys differ");

    return MetadataPartitionType.get(type).combineMetadataPayloads(previousRecord, this);
  }

  private static String getBloomFilterRecordKey(String partitionName, String fileName) {
    return new PartitionIndexID(getBloomFilterIndexPartitionIdentifier(partitionName)).asBase64EncodedString()
        .concat(new FileIndexID(fileName).asBase64EncodedString());
  }

  public static Option<HoodieRecord<HoodieMetadataPayload>> combineSecondaryIndexRecord(
      HoodieRecord<HoodieMetadataPayload> oldRecord,
      HoodieRecord<HoodieMetadataPayload> newRecord) {
    // If the new record is tombstone, we can discard it
    if (newRecord.getData().isDeleted() || newRecord.getData().secondaryIndexMetadata.getIsDeleted()) {
      return Option.empty();
    }

    return Option.of(newRecord);
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord oldRecord, Schema schema, Properties properties) throws IOException {
    HoodieMetadataPayload anotherPayload = new HoodieMetadataPayload(Option.of((GenericRecord) oldRecord));
    HoodieRecordPayload combinedPayload = preCombine(anotherPayload);
    return combinedPayload.getInsertValue(schema, properties);
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord oldRecord, Schema schema) throws IOException {
    return combineAndGetUpdateValue(oldRecord, schema, new Properties());
  }

  @Override
  public Option<IndexedRecord> getInsertValue(Schema schema, Properties propertiesIgnored) throws IOException {
    if (key == null || this.isDeletedRecord) {
      return Option.empty();
    }

    if (schema == null || HOODIE_METADATA_SCHEMA == schema) {
      // If the schema is same or none is provided, we can return the record directly
      HoodieMetadataRecord record = new HoodieMetadataRecord(key, type, filesystemMetadata, bloomFilterMetadata,
          columnStatMetadata, recordIndexMetadata, secondaryIndexMetadata);
      return Option.of(record);
    } else {
      // Otherwise, the assumption is that the schema required contains the metadata fields so we construct a new GenericRecord with these fields
      GenericData.Record record = new GenericData.Record(schema);
      record.put(KEY_FIELD_OFFSET, key);
      record.put(TYPE_FIELD_OFFSET, type);
      if (filesystemMetadata != null) {
        record.put(FILESYSTEM_METADATA_FIELD_OFFSET, filesystemMetadata);
      }
      if (bloomFilterMetadata != null) {
        record.put(BLOOM_FILTER_METADATA_FIELD_OFFSET, bloomFilterMetadata);
      }
      if (columnStatMetadata != null) {
        record.put(COLUMN_STATS_METADATA_FIELD_OFFSET, columnStatMetadata);
      }
      if (recordIndexMetadata != null) {
        record.put(RECORD_INDEX_METADATA_FIELD_OFFSET, recordIndexMetadata);
      }
      if (secondaryIndexMetadata != null) {
        record.put(SECONDARY_INDEX_METADATA_FIELD_OFFSET, secondaryIndexMetadata);
      }
      return Option.of(record);
    }
  }

  @Override
  public Option<IndexedRecord> getInsertValue(Schema schema) throws IOException {
    return getInsertValue(schema, CollectionUtils.emptyProps());
  }

  /**
   * Returns the list of filenames added as part of this record.
   */
  public List<String> getFilenames() {
    return filterFileInfoEntries(false).map(Map.Entry::getKey).sorted().collect(Collectors.toList());
  }

  /**
   * Returns the list of filenames deleted as part of this record.
   */
  public List<String> getDeletions() {
    return filterFileInfoEntries(true).map(Map.Entry::getKey).sorted().collect(Collectors.toList());
  }

  /**
   * Get the bloom filter metadata from this payload.
   */
  public Option<HoodieMetadataBloomFilter> getBloomFilterMetadata() {
    if (bloomFilterMetadata == null) {
      return Option.empty();
    }

    return Option.of(bloomFilterMetadata);
  }

  /**
   * Get the bloom filter metadata from this payload.
   */
  public Option<HoodieMetadataColumnStats> getColumnStatMetadata() {
    if (columnStatMetadata == null) {
      return Option.empty();
    }

    return Option.of(columnStatMetadata);
  }

  /**
   * Returns the files added as part of this record.
   */
  public List<StoragePathInfo> getFileList(HoodieStorage storage, StoragePath partitionPath) {
    long blockSize = storage.getDefaultBlockSize(partitionPath);
    return filterFileInfoEntries(false)
        .map(e -> {
          // NOTE: Since we know that the Metadata Table's Payload is simply a file-name we're
          //       creating Hadoop's Path using more performant unsafe variant
          return new StoragePathInfo(new StoragePath(partitionPath, e.getKey()), e.getValue().getSize(),
              false, (short) 0, blockSize, 0);
        })
        .collect(Collectors.toList());
  }

  private Stream<Map.Entry<String, HoodieMetadataFileInfo>> filterFileInfoEntries(boolean isDeleted) {
    if (filesystemMetadata == null) {
      return Stream.empty();
    }

    return filesystemMetadata.entrySet().stream().filter(e -> e.getValue().getIsDeleted() == isDeleted);
  }

  /**
   * Get bloom filter index key.
   *
   * @param partitionIndexID - Partition index id
   * @param fileIndexID      - File index id
   * @return Bloom filter index key
   */
  public static String getBloomFilterIndexKey(PartitionIndexID partitionIndexID, FileIndexID fileIndexID) {
    return partitionIndexID.asBase64EncodedString()
        .concat(fileIndexID.asBase64EncodedString());
  }

  /**
   * Get column stats index key.
   *
   * @param partitionIndexID - Partition index id
   * @param fileIndexID      - File index id
   * @param columnIndexID    - Column index id
   * @return Column stats index key
   */
  public static String getColumnStatsIndexKey(PartitionIndexID partitionIndexID, FileIndexID fileIndexID, ColumnIndexID columnIndexID) {
    return columnIndexID.asBase64EncodedString()
        .concat(partitionIndexID.asBase64EncodedString())
        .concat(fileIndexID.asBase64EncodedString());
  }

  /**
   * Get column stats index key from the column range metadata.
   *
   * @param partitionName       - Partition name
   * @param columnRangeMetadata -  Column range metadata
   * @return Column stats index key
   */
  public static String getColumnStatsIndexKey(String partitionName, HoodieColumnRangeMetadata<Comparable> columnRangeMetadata) {
    final PartitionIndexID partitionIndexID = new PartitionIndexID(getColumnStatsIndexPartitionIdentifier(partitionName));
    final FileIndexID fileIndexID = new FileIndexID(new StoragePath(columnRangeMetadata.getFilePath()).getName());
    final ColumnIndexID columnIndexID = new ColumnIndexID(columnRangeMetadata.getColumnName());
    return getColumnStatsIndexKey(partitionIndexID, fileIndexID, columnIndexID);
  }

  public static Stream<HoodieRecord> createColumnStatsRecords(String partitionName,
                                                              Collection<HoodieColumnRangeMetadata<Comparable>> columnRangeMetadataList,
                                                              boolean isDeleted) {
    return columnRangeMetadataList.stream().map(
        columnRangeMetadata -> createColumnStatsRecord(partitionName, columnRangeMetadata, isDeleted,
            MetadataPartitionType.COLUMN_STATS.getPartitionPath(), MetadataPartitionType.COLUMN_STATS.getRecordType()));
  }

  public static Stream<HoodieRecord> createColumnStatsRecords(String partitionName,
                                                              Collection<HoodieColumnRangeMetadata<Comparable>> columnRangeMetadataList,
                                                              boolean isDeleted,
                                                              String metadataPartitionName,
                                                              int recordType) {
    return columnRangeMetadataList.stream().map(
        columnRangeMetadata -> createColumnStatsRecord(partitionName, columnRangeMetadata, isDeleted,
            metadataPartitionName, recordType));
  }

  private static HoodieAvroRecord<HoodieMetadataPayload> createColumnStatsRecord(String partitionName,
                                                                                 HoodieColumnRangeMetadata<Comparable> columnRangeMetadata,
                                                                                 boolean isDeleted,
                                                                                 String metadataPartitionName,
                                                                                 int recordType) {
    HoodieKey key = new HoodieKey(getColumnStatsIndexKey(partitionName, columnRangeMetadata), metadataPartitionName);
    HoodieMetadataPayload payload = new HoodieMetadataPayload(
        key.getRecordKey(),
        HoodieMetadataColumnStats.newBuilder()
            .setFileName(new StoragePath(columnRangeMetadata.getFilePath()).getName())
            .setColumnName(columnRangeMetadata.getColumnName())
            .setMinValue(columnRangeMetadata.getMinValueWrapped())
            .setMaxValue(columnRangeMetadata.getMaxValueWrapped())
            .setNullCount(columnRangeMetadata.getNullCount())
            .setValueCount(columnRangeMetadata.getValueCount())
            .setTotalSize(columnRangeMetadata.getTotalSize())
            .setTotalUncompressedSize(columnRangeMetadata.getTotalUncompressedSize())
            .setIsDeleted(isDeleted)
            .setValueType(columnRangeMetadata.getValueMetadata().getValueTypeInfo())
            .build(),
        recordType);

    return new HoodieAvroRecord<>(key, payload);
  }

  public static Stream<HoodieRecord> createPartitionStatsRecords(String partitionPath,
                                                                 Collection<HoodieColumnRangeMetadata<Comparable>> columnRangeMetadataList,
                                                                 boolean isDeleted, boolean isTightBound, Option<String> indexPartitionOpt) {
    return columnRangeMetadataList.stream().map(columnRangeMetadata -> {
      HoodieKey key;
      if (indexPartitionOpt.isPresent()) {
        // For Expression index, index name is provided since expression index partition stat records are stored within expression index partition
        // The partition path for such records is same as expression index
        // The key for such records is a combination of column name, HOODIE_EXPRESSION_INDEX_PARTITION_STAT_PREFIX and partition path
        key = new HoodieKey(getPartitionStatsIndexKey(HoodieExpressionIndex.HOODIE_EXPRESSION_INDEX_PARTITION_STAT_PREFIX, partitionPath, columnRangeMetadata.getColumnName()),
            indexPartitionOpt.get());
      } else {
        key = new HoodieKey(getPartitionStatsIndexKey(partitionPath, columnRangeMetadata.getColumnName()),
            MetadataPartitionType.PARTITION_STATS.getPartitionPath());
      }
      HoodieMetadataPayload payload = new HoodieMetadataPayload(
          key.getRecordKey(),
          HoodieMetadataColumnStats.newBuilder()
              .setFileName(columnRangeMetadata.getFilePath())
              .setColumnName(columnRangeMetadata.getColumnName())
              .setMinValue(columnRangeMetadata.getMinValueWrapped())
              .setMaxValue(columnRangeMetadata.getMaxValueWrapped())
              .setNullCount(columnRangeMetadata.getNullCount())
              .setValueCount(columnRangeMetadata.getValueCount())
              .setTotalSize(columnRangeMetadata.getTotalSize())
              .setTotalUncompressedSize(columnRangeMetadata.getTotalUncompressedSize())
              .setIsDeleted(isDeleted)
              .setIsTightBound(isTightBound)
              .setValueType(columnRangeMetadata.getValueMetadata().getValueTypeInfo())
              .build(),
          MetadataPartitionType.PARTITION_STATS.getRecordType());

      return new HoodieAvroRecord<>(key, payload);
    });
  }

  /**
   * Create and return a {@code HoodieMetadataPayload} to insert or update an entry for the record index.
   * <p>
   * Each entry maps the key of a single record in HUDI to its location.
   *
   * @param recordKey   Key of the record
   * @param partition   Name of the partition which contains the record
   * @param fileId      fileId which contains the record
   * @param instantTime instantTime when the record was added
   */
  public static HoodieRecord<HoodieMetadataPayload> createRecordIndexUpdate(String recordKey, String partition,
                                                                            String fileId, String instantTime, int fileIdEncoding) {

    HoodieKey key = new HoodieKey(recordKey, MetadataPartitionType.RECORD_INDEX.getPartitionPath());
    long instantTimeMillis = -1;
    try {
      instantTimeMillis = TimelineUtils.parseDateFromInstantTime(instantTime).getTime();
    } catch (Exception e) {
      throw new HoodieMetadataException("Failed to create metadata payload for record index. Instant time parsing for " + instantTime + " failed ", e);
    }
    if (fileIdEncoding == 0) {
      // Data file names have a -D suffix to denote the index (D = integer) of the file written
      // In older HUID versions the file index was missing
      final UUID uuid;
      final int fileIndex;
      try {
        if (fileId.length() == 36) {
          uuid = UUID.fromString(fileId);
          fileIndex = RECORD_INDEX_MISSING_FILEINDEX_FALLBACK;
        } else {
          final int index = fileId.lastIndexOf("-");
          uuid = UUID.fromString(fileId.substring(0, index));
          fileIndex = Integer.parseInt(fileId.substring(index + 1));
        }
      } catch (Exception e) {
        throw new HoodieMetadataException(String.format("Invalid UUID or index: fileID=%s, partition=%s, instantTIme=%s",
            fileId, partition, instantTime), e);
      }

      HoodieMetadataPayload payload = new HoodieMetadataPayload(recordKey,
          new HoodieRecordIndexInfo(
              partition,
              uuid.getMostSignificantBits(),
              uuid.getLeastSignificantBits(),
              fileIndex,
              EMPTY_STRING,
              instantTimeMillis,
              0,
              null));
      return new HoodieAvroRecord<>(key, payload);
    } else {
      HoodieMetadataPayload payload = new HoodieMetadataPayload(recordKey,
          new HoodieRecordIndexInfo(
              partition,
              -1L,
              -1L,
              -1,
              fileId,
              instantTimeMillis,
              1,
              null));
      return new HoodieAvroRecord<>(key, payload);
    }
  }

  /**
   * Create and return a {@code HoodieMetadataPayload} to insert or update an entry for the secondary index.
   * <p>
   * Each entry maps the secondary key of a single record in HUDI to its record (or primary) key
   *
   * @param recordKey    Primary key of the record
   * @param secondaryKey Secondary key of the record
   * @param isDeleted    true if this record is deleted
   */
  public static HoodieRecord<HoodieMetadataPayload> createSecondaryIndexRecord(String recordKey, String secondaryKey, String partitionPath, Boolean isDeleted) {
    // the payload key is in the format of "secondaryKey$primaryKey"
    HoodieKey key = new HoodieKey(SecondaryIndexKeyUtils.constructSecondaryIndexKey(secondaryKey, recordKey), partitionPath);
    HoodieMetadataPayload payload = new HoodieMetadataPayload(key.getRecordKey(), new HoodieSecondaryIndexInfo(isDeleted));
    return new HoodieAvroRecord<>(key, payload);
  }

  public boolean isSecondaryIndexDeleted() {
    return secondaryIndexMetadata.getIsDeleted();
  }

  /**
   * Create and return a {@code HoodieMetadataPayload} to delete a record in the Metadata Table's record index.
   *
   * @param recordKey Key of the record to be deleted
   * @param partitionPath of the record to be deleted
   */
  public static HoodieRecord createRecordIndexDelete(String recordKey, String partitionPath, boolean isPartitionedRLI) {
    HoodieKey key = new HoodieKey(recordKey, MetadataPartitionType.RECORD_INDEX.getPartitionPath());
    return new HoodieAvroRecord<>(key, isPartitionedRLI
        ? new EmptyHoodieRecordPayloadWithPartition(partitionPath)
        : new EmptyHoodieRecordPayload());
  }

  /**
   * If this is a record-level index entry, returns the file to which this is mapped.
   */
  public HoodieRecordGlobalLocation getRecordGlobalLocation() {
    return getLocationFromRecordIndexInfo(recordIndexMetadata);
  }

  public String getDataPartition() {
    return recordIndexMetadata.getPartitionName();
  }

  public boolean isDeleted() {
    return isDeletedRecord;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (!(other instanceof HoodieMetadataPayload)) {
      return false;
    }

    HoodieMetadataPayload otherMetadataPayload = (HoodieMetadataPayload) other;

    return this.type == otherMetadataPayload.type
        && Objects.equals(this.key, otherMetadataPayload.key)
        && Objects.equals(this.filesystemMetadata, otherMetadataPayload.filesystemMetadata)
        && Objects.equals(this.bloomFilterMetadata, otherMetadataPayload.bloomFilterMetadata)
        && Objects.equals(this.columnStatMetadata, otherMetadataPayload.columnStatMetadata)
        && Objects.equals(this.recordIndexMetadata, otherMetadataPayload.recordIndexMetadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, type, filesystemMetadata, bloomFilterMetadata, columnStatMetadata);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("HoodieMetadataPayload {");
    sb.append(KEY_FIELD_NAME + "=").append(key).append(", ");
    sb.append(SCHEMA_FIELD_NAME_TYPE + "=").append(type).append(", ");

    if (type == MetadataPartitionType.FILES.getRecordType() || type == MetadataPartitionType.ALL_PARTITIONS.getRecordType()) {
      sb.append("Files: {");
      sb.append("creations=").append(Arrays.toString(getFilenames().toArray())).append(", ");
      sb.append("deletions=").append(Arrays.toString(getDeletions().toArray())).append(", ");
      sb.append("}");
    } else if (type == MetadataPartitionType.BLOOM_FILTERS.getRecordType()) {
      checkState(getBloomFilterMetadata().isPresent());
      sb.append("BloomFilter: {");
      sb.append("bloom size: ").append(getBloomFilterMetadata().get().getBloomFilter().array().length).append(", ");
      sb.append("timestamp: ").append(getBloomFilterMetadata().get().getTimestamp()).append(", ");
      sb.append("deleted: ").append(getBloomFilterMetadata().get().getIsDeleted());
      sb.append("}");
    } else if (type == MetadataPartitionType.COLUMN_STATS.getRecordType()) {
      checkState(getColumnStatMetadata().isPresent());
      sb.append("ColStats: {");
      sb.append(getColumnStatMetadata().get());
      sb.append("}");
    } else if (type == MetadataPartitionType.RECORD_INDEX.getRecordType()) {
      sb.append("RecordIndex: {");
      sb.append("location=").append(getRecordGlobalLocation());
      sb.append("}");
    }
    sb.append('}');
    return sb.toString();
  }
}
