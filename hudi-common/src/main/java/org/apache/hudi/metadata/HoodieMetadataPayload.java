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

import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.model.BooleanWrapper;
import org.apache.hudi.avro.model.BytesWrapper;
import org.apache.hudi.avro.model.DateWrapper;
import org.apache.hudi.avro.model.DecimalWrapper;
import org.apache.hudi.avro.model.DoubleWrapper;
import org.apache.hudi.avro.model.FloatWrapper;
import org.apache.hudi.avro.model.HoodieMetadataBloomFilter;
import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.avro.model.HoodieMetadataFileInfo;
import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.avro.model.IntWrapper;
import org.apache.hudi.avro.model.LongWrapper;
import org.apache.hudi.avro.model.StringWrapper;
import org.apache.hudi.avro.model.TimestampMicrosWrapper;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.hash.ColumnIndexID;
import org.apache.hudi.common.util.hash.FileIndexID;
import org.apache.hudi.common.util.hash.PartitionIndexID;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.io.storage.HoodieHFileReader;
import org.apache.hudi.util.Lazy;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.util.DateTimeUtils.instantToMicros;
import static org.apache.hudi.common.util.DateTimeUtils.microsToInstant;
import static org.apache.hudi.common.util.TypeUtils.unsafeCast;
import static org.apache.hudi.common.util.ValidationUtils.checkArgument;
import static org.apache.hudi.common.util.ValidationUtils.checkState;
import static org.apache.hudi.metadata.HoodieTableMetadata.RECORDKEY_PARTITION_LIST;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getPartitionIdentifier;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.tryUpcastDecimal;

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

  // Type of the record. This can be an enum in the schema but Avro1.8
  // has a bug - https://issues.apache.org/jira/browse/AVRO-1810
  protected static final int METADATA_TYPE_PARTITION_LIST = 1;
  protected static final int METADATA_TYPE_FILE_LIST = 2;
  protected static final int METADATA_TYPE_COLUMN_STATS = 3;
  protected static final int METADATA_TYPE_BLOOM_FILTER = 4;

  // HoodieMetadata schema field ids
  public static final String KEY_FIELD_NAME = HoodieHFileReader.KEY_FIELD_NAME;
  public static final String SCHEMA_FIELD_NAME_TYPE = "type";
  public static final String SCHEMA_FIELD_NAME_METADATA = "filesystemMetadata";
  public static final String SCHEMA_FIELD_ID_COLUMN_STATS = "ColumnStatsMetadata";
  public static final String SCHEMA_FIELD_ID_BLOOM_FILTER = "BloomFilterMetadata";

  // HoodieMetadata bloom filter payload field ids
  private static final String FIELD_IS_DELETED = "isDeleted";
  private static final String BLOOM_FILTER_FIELD_TYPE = "type";
  private static final String BLOOM_FILTER_FIELD_TIMESTAMP = "timestamp";
  private static final String BLOOM_FILTER_FIELD_BLOOM_FILTER = "bloomFilter";
  private static final String BLOOM_FILTER_FIELD_IS_DELETED = FIELD_IS_DELETED;

  // HoodieMetadata column stats payload field ids
  public static final String COLUMN_STATS_FIELD_MIN_VALUE = "minValue";
  public static final String COLUMN_STATS_FIELD_MAX_VALUE = "maxValue";
  public static final String COLUMN_STATS_FIELD_NULL_COUNT = "nullCount";
  public static final String COLUMN_STATS_FIELD_VALUE_COUNT = "valueCount";
  public static final String COLUMN_STATS_FIELD_TOTAL_SIZE = "totalSize";
  public static final String COLUMN_STATS_FIELD_FILE_NAME = "fileName";
  public static final String COLUMN_STATS_FIELD_COLUMN_NAME = "columnName";
  public static final String COLUMN_STATS_FIELD_TOTAL_UNCOMPRESSED_SIZE = "totalUncompressedSize";
  public static final String COLUMN_STATS_FIELD_IS_DELETED = FIELD_IS_DELETED;

  private static final Conversions.DecimalConversion AVRO_DECIMAL_CONVERSION = new Conversions.DecimalConversion();

  // NOTE: PLEASE READ CAREFULLY
  //
  // In Avro 1.10 generated builders rely on {@code SpecificData.getForSchema} invocation that in turn
  // does use reflection to load the code-gen'd class corresponding to the Avro record model. This has
  // serious adverse effects in terms of performance when gets executed on the hot-path (both, in terms
  // of runtime and efficiency).
  //
  // To work this around instead of using default code-gen'd builder invoking {@code SpecificData.getForSchema},
  // we instead rely on overloaded ctor accepting another instance of the builder: {@code Builder(Builder)},
  // which bypasses such invocation. Following corresponding builder's stubs are statically initialized
  // to be used exactly for that purpose.
  //
  // You can find more details in HUDI-3834
  private static final Lazy<HoodieMetadataColumnStats.Builder> METADATA_COLUMN_STATS_BUILDER_STUB = Lazy.lazily(HoodieMetadataColumnStats::newBuilder);
  private static final Lazy<StringWrapper.Builder> STRING_WRAPPER_BUILDER_STUB = Lazy.lazily(StringWrapper::newBuilder);
  private static final Lazy<BytesWrapper.Builder> BYTES_WRAPPER_BUILDER_STUB = Lazy.lazily(BytesWrapper::newBuilder);
  private static final Lazy<DoubleWrapper.Builder> DOUBLE_WRAPPER_BUILDER_STUB = Lazy.lazily(DoubleWrapper::newBuilder);
  private static final Lazy<FloatWrapper.Builder> FLOAT_WRAPPER_BUILDER_STUB = Lazy.lazily(FloatWrapper::newBuilder);
  private static final Lazy<LongWrapper.Builder> LONG_WRAPPER_BUILDER_STUB = Lazy.lazily(LongWrapper::newBuilder);
  private static final Lazy<IntWrapper.Builder> INT_WRAPPER_BUILDER_STUB = Lazy.lazily(IntWrapper::newBuilder);
  private static final Lazy<BooleanWrapper.Builder> BOOLEAN_WRAPPER_BUILDER_STUB = Lazy.lazily(BooleanWrapper::newBuilder);
  private static final Lazy<TimestampMicrosWrapper.Builder> TIMESTAMP_MICROS_WRAPPER_BUILDER_STUB = Lazy.lazily(TimestampMicrosWrapper::newBuilder);
  private static final Lazy<DecimalWrapper.Builder> DECIMAL_WRAPPER_BUILDER_STUB = Lazy.lazily(DecimalWrapper::newBuilder);
  private static final Lazy<DateWrapper.Builder> DATE_WRAPPER_BUILDER_STUB = Lazy.lazily(DateWrapper::newBuilder);

  private String key = null;
  private int type = 0;
  private Map<String, HoodieMetadataFileInfo> filesystemMetadata = null;
  private HoodieMetadataBloomFilter bloomFilterMetadata = null;
  private HoodieMetadataColumnStats columnStatMetadata = null;

  public HoodieMetadataPayload(GenericRecord record, Comparable<?> orderingVal) {
    this(Option.of(record));
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

      Map<String, HoodieMetadataFileInfo> metadata = getNestedFieldValue(record, SCHEMA_FIELD_NAME_METADATA);
      if (metadata != null) {
        filesystemMetadata = metadata;
        filesystemMetadata.keySet().forEach(k -> {
          GenericRecord v = filesystemMetadata.get(k);
          filesystemMetadata.put(k, new HoodieMetadataFileInfo((Long) v.get("size"), (Boolean) v.get("isDeleted")));
        });
      }

      if (type == METADATA_TYPE_BLOOM_FILTER) {
        GenericRecord bloomFilterRecord = getNestedFieldValue(record, SCHEMA_FIELD_ID_BLOOM_FILTER);
        // NOTE: Only legitimate reason for {@code BloomFilterMetadata} to not be present is when
        //       it's not been read from the storage (ie it's not been a part of projected schema).
        //       Otherwise, it has to be present or the record would be considered invalid
        if (bloomFilterRecord == null) {
          checkArgument(record.getSchema().getField(SCHEMA_FIELD_ID_BLOOM_FILTER) == null,
              String.format("Valid %s record expected for type: %s", SCHEMA_FIELD_ID_BLOOM_FILTER, METADATA_TYPE_COLUMN_STATS));
        } else {
          bloomFilterMetadata = new HoodieMetadataBloomFilter(
              (String) bloomFilterRecord.get(BLOOM_FILTER_FIELD_TYPE),
              (String) bloomFilterRecord.get(BLOOM_FILTER_FIELD_TIMESTAMP),
              (ByteBuffer) bloomFilterRecord.get(BLOOM_FILTER_FIELD_BLOOM_FILTER),
              (Boolean) bloomFilterRecord.get(BLOOM_FILTER_FIELD_IS_DELETED)
          );
        }
      }

      if (type == METADATA_TYPE_COLUMN_STATS) {
        GenericRecord columnStatsRecord = getNestedFieldValue(record, SCHEMA_FIELD_ID_COLUMN_STATS);
        // NOTE: Only legitimate reason for {@code ColumnStatsMetadata} to not be present is when
        //       it's not been read from the storage (ie it's not been a part of projected schema).
        //       Otherwise, it has to be present or the record would be considered invalid
        if (columnStatsRecord == null) {
          checkArgument(record.getSchema().getField(SCHEMA_FIELD_ID_COLUMN_STATS) == null,
              String.format("Valid %s record expected for type: %s", SCHEMA_FIELD_ID_COLUMN_STATS, METADATA_TYPE_COLUMN_STATS));
        } else {
          columnStatMetadata = HoodieMetadataColumnStats.newBuilder(METADATA_COLUMN_STATS_BUILDER_STUB.get())
              .setFileName((String) columnStatsRecord.get(COLUMN_STATS_FIELD_FILE_NAME))
              .setColumnName((String) columnStatsRecord.get(COLUMN_STATS_FIELD_COLUMN_NAME))
              .setMinValue(columnStatsRecord.get(COLUMN_STATS_FIELD_MIN_VALUE))
              .setMaxValue(columnStatsRecord.get(COLUMN_STATS_FIELD_MAX_VALUE))
              .setValueCount((Long) columnStatsRecord.get(COLUMN_STATS_FIELD_VALUE_COUNT))
              .setNullCount((Long) columnStatsRecord.get(COLUMN_STATS_FIELD_NULL_COUNT))
              .setTotalSize((Long) columnStatsRecord.get(COLUMN_STATS_FIELD_TOTAL_SIZE))
              .setTotalUncompressedSize((Long) columnStatsRecord.get(COLUMN_STATS_FIELD_TOTAL_UNCOMPRESSED_SIZE))
              .setIsDeleted((Boolean) columnStatsRecord.get(COLUMN_STATS_FIELD_IS_DELETED))
          .build();
        }
      }
    }
  }

  private HoodieMetadataPayload(String key, int type, Map<String, HoodieMetadataFileInfo> filesystemMetadata) {
    this(key, type, filesystemMetadata, null, null);
  }

  private HoodieMetadataPayload(String key, HoodieMetadataBloomFilter metadataBloomFilter) {
    this(key, METADATA_TYPE_BLOOM_FILTER, null, metadataBloomFilter, null);
  }

  private HoodieMetadataPayload(String key, HoodieMetadataColumnStats columnStats) {
    this(key, METADATA_TYPE_COLUMN_STATS, null, null, columnStats);
  }

  protected HoodieMetadataPayload(String key, int type,
                                  Map<String, HoodieMetadataFileInfo> filesystemMetadata,
                                  HoodieMetadataBloomFilter metadataBloomFilter,
                                  HoodieMetadataColumnStats columnStats) {
    this.key = key;
    this.type = type;
    this.filesystemMetadata = filesystemMetadata;
    this.bloomFilterMetadata = metadataBloomFilter;
    this.columnStatMetadata = columnStats;
  }

  /**
   * Create and return a {@code HoodieMetadataPayload} to save list of partitions.
   *
   * @param partitions The list of partitions
   */
  public static HoodieRecord<HoodieMetadataPayload> createPartitionListRecord(List<String> partitions) {
    return createPartitionListRecord(partitions, false);
  }

  /**
   * Create and return a {@code HoodieMetadataPayload} to save list of partitions.
   *
   * @param partitions The list of partitions
   */
  public static HoodieRecord<HoodieMetadataPayload> createPartitionListRecord(List<String> partitions, boolean isDeleted) {
    Map<String, HoodieMetadataFileInfo> fileInfo = new HashMap<>();
    partitions.forEach(partition -> fileInfo.put(getPartitionIdentifier(partition), new HoodieMetadataFileInfo(0L, isDeleted)));

    HoodieKey key = new HoodieKey(RECORDKEY_PARTITION_LIST, MetadataPartitionType.FILES.getPartitionPath());
    HoodieMetadataPayload payload = new HoodieMetadataPayload(key.getRecordKey(), METADATA_TYPE_PARTITION_LIST,
        fileInfo);
    return new HoodieAvroRecord<>(key, payload);
  }

  /**
   * Create and return a {@code HoodieMetadataPayload} to save list of partitions.
   *
   * @param partitionsAdded   The list of added partitions
   * @param partitionsDeleted The list of deleted partitions
   */
  public static HoodieRecord<HoodieMetadataPayload> createPartitionListRecord(List<String> partitionsAdded, List<String> partitionsDeleted) {
    Map<String, HoodieMetadataFileInfo> fileInfo = new HashMap<>();
    partitionsAdded.forEach(partition -> fileInfo.put(partition, new HoodieMetadataFileInfo(0L, false)));
    partitionsDeleted.forEach(partition -> fileInfo.put(partition, new HoodieMetadataFileInfo(0L, true)));

    HoodieKey key = new HoodieKey(RECORDKEY_PARTITION_LIST, MetadataPartitionType.FILES.getPartitionPath());
    HoodieMetadataPayload payload = new HoodieMetadataPayload(key.getRecordKey(), METADATA_TYPE_PARTITION_LIST,
        fileInfo);
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
                                                                               Option<Map<String, Long>> filesAdded,
                                                                               Option<List<String>> filesDeleted) {
    Map<String, HoodieMetadataFileInfo> fileInfo = new HashMap<>();
    filesAdded.ifPresent(filesMap ->
        fileInfo.putAll(
            filesMap.entrySet().stream().collect(
                Collectors.toMap(Map.Entry::getKey, (entry) -> {
                  long fileSize = entry.getValue();
                  // Assert that the file-size of the file being added is positive, since Hudi
                  // should not be creating empty files
                  checkState(fileSize > 0);
                  return new HoodieMetadataFileInfo(fileSize, false);
                })))
    );
    filesDeleted.ifPresent(filesList ->
        fileInfo.putAll(
            filesList.stream().collect(
                Collectors.toMap(Function.identity(), (ignored) -> new HoodieMetadataFileInfo(0L, true))))
    );

    HoodieKey key = new HoodieKey(partition, MetadataPartitionType.FILES.getPartitionPath());
    HoodieMetadataPayload payload = new HoodieMetadataPayload(key.getRecordKey(), METADATA_TYPE_FILE_LIST, fileInfo);
    return new HoodieAvroRecord<>(key, payload);
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
                                                                                    final boolean isDeleted) {
    checkArgument(!baseFileName.contains(Path.SEPARATOR)
            && FSUtils.isBaseFile(new Path(baseFileName)),
        "Invalid base file '" + baseFileName + "' for MetaIndexBloomFilter!");
    final String bloomFilterIndexKey = new PartitionIndexID(partitionName).asBase64EncodedString()
        .concat(new FileIndexID(baseFileName).asBase64EncodedString());
    HoodieKey key = new HoodieKey(bloomFilterIndexKey, MetadataPartitionType.BLOOM_FILTERS.getPartitionPath());

    HoodieMetadataBloomFilter metadataBloomFilter =
        new HoodieMetadataBloomFilter(bloomFilterType, timestamp, bloomFilter, isDeleted);
    HoodieMetadataPayload metadataPayload = new HoodieMetadataPayload(key.getRecordKey(), metadataBloomFilter);
    return new HoodieAvroRecord<>(key, metadataPayload);
  }

  @Override
  public HoodieMetadataPayload preCombine(HoodieMetadataPayload previousRecord) {
    checkArgument(previousRecord.type == type,
        "Cannot combine " + previousRecord.type + " with " + type);

    switch (type) {
      case METADATA_TYPE_PARTITION_LIST:
      case METADATA_TYPE_FILE_LIST:
        Map<String, HoodieMetadataFileInfo> combinedFileInfo = combineFileSystemMetadata(previousRecord);
        return new HoodieMetadataPayload(key, type, combinedFileInfo);
      case METADATA_TYPE_BLOOM_FILTER:
        HoodieMetadataBloomFilter combineBloomFilterMetadata = combineBloomFilterMetadata(previousRecord);
        return new HoodieMetadataPayload(key, combineBloomFilterMetadata);
      case METADATA_TYPE_COLUMN_STATS:
        return new HoodieMetadataPayload(key, combineColumnStatsMetadata(previousRecord));
      default:
        throw new HoodieMetadataException("Unknown type of HoodieMetadataPayload: " + type);
    }
  }

  private HoodieMetadataBloomFilter combineBloomFilterMetadata(HoodieMetadataPayload previousRecord) {
    // Bloom filters are always additive. No need to merge with previous bloom filter
    return this.bloomFilterMetadata;
  }

  private HoodieMetadataColumnStats combineColumnStatsMetadata(HoodieMetadataPayload previousRecord) {
    checkArgument(previousRecord.getColumnStatMetadata().isPresent());
    checkArgument(getColumnStatMetadata().isPresent());

    HoodieMetadataColumnStats previousColStatsRecord = previousRecord.getColumnStatMetadata().get();
    HoodieMetadataColumnStats newColumnStatsRecord = getColumnStatMetadata().get();

    return mergeColumnStatsRecords(previousColStatsRecord, newColumnStatsRecord);
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
  public Option<IndexedRecord> getInsertValue(Schema schemaIgnored, Properties propertiesIgnored) throws IOException {
    if (key == null) {
      return Option.empty();
    }

    HoodieMetadataRecord record = new HoodieMetadataRecord(key, type, filesystemMetadata, bloomFilterMetadata,
        columnStatMetadata);
    return Option.of(record);
  }

  @Override
  public Option<IndexedRecord> getInsertValue(Schema schema) throws IOException {
    return getInsertValue(schema, new Properties());
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
  public FileStatus[] getFileStatuses(Configuration hadoopConf, Path partitionPath) throws IOException {
    FileSystem fs = partitionPath.getFileSystem(hadoopConf);
    long blockSize = fs.getDefaultBlockSize(partitionPath);
    return filterFileInfoEntries(false)
        .map(e -> new FileStatus(e.getValue().getSize(), false, 0, blockSize, 0, 0,
            null, null, null, new Path(partitionPath, e.getKey())))
        .toArray(FileStatus[]::new);
  }

  private Stream<Map.Entry<String, HoodieMetadataFileInfo>> filterFileInfoEntries(boolean isDeleted) {
    if (filesystemMetadata == null) {
      return Stream.empty();
    }

    return filesystemMetadata.entrySet().stream().filter(e -> e.getValue().getIsDeleted() == isDeleted);
  }

  private Map<String, HoodieMetadataFileInfo> combineFileSystemMetadata(HoodieMetadataPayload previousRecord) {
    Map<String, HoodieMetadataFileInfo> combinedFileInfo = new HashMap<>();

    // First, add all files listed in the previous record
    if (previousRecord.filesystemMetadata != null) {
      combinedFileInfo.putAll(previousRecord.filesystemMetadata);
    }

    // Second, merge in the files listed in the new record
    if (filesystemMetadata != null) {
      validatePayload(type, filesystemMetadata);

      filesystemMetadata.forEach((key, fileInfo) -> {
        combinedFileInfo.merge(key, fileInfo,
            // Combine previous record w/ the new one, new records taking precedence over
            // the old one
            //
            // NOTE: That if previous listing contains the file that is being deleted by the tombstone
            //       record (`IsDeleted` = true) in the new one, we simply delete the file from the resulting
            //       listing as well as drop the tombstone itself.
            //       However, if file is not present in the previous record we have to persist tombstone
            //       record in the listing to make sure we carry forward information that this file
            //       was deleted. This special case could occur since the merging flow is 2-stage:
            //          - First we merge records from all of the delta log-files
            //          - Then we merge records from base-files with the delta ones (coming as a result
            //          of the previous step)
            (oldFileInfo, newFileInfo) ->
                // NOTE: We can’t assume that MT update records will be ordered the same way as actual
                //       FS operations (since they are not atomic), therefore MT record merging should be a
                //       _commutative_ & _associative_ operation (ie one that would work even in case records
                //       will get re-ordered), which is
                //          - Possible for file-sizes (since file-sizes will ever grow, we can simply
                //          take max of the old and new records)
                //          - Not possible for is-deleted flags*
                //
                //       *However, we’re assuming that the case of concurrent write and deletion of the same
                //       file is _impossible_ -- it would only be possible with concurrent upsert and
                //       rollback operation (affecting the same log-file), which is implausible, b/c either
                //       of the following have to be true:
                //          - We’re appending to failed log-file (then the other writer is trying to
                //          rollback it concurrently, before it’s own write)
                //          - Rollback (of completed instant) is running concurrently with append (meaning
                //          that restore is running concurrently with a write, which is also nut supported
                //          currently)
                newFileInfo.getIsDeleted()
                    ? null
                    : new HoodieMetadataFileInfo(Math.max(newFileInfo.getSize(), oldFileInfo.getSize()), false));
      });
    }

    return combinedFileInfo;
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
    final PartitionIndexID partitionIndexID = new PartitionIndexID(partitionName);
    final FileIndexID fileIndexID = new FileIndexID(new Path(columnRangeMetadata.getFilePath()).getName());
    final ColumnIndexID columnIndexID = new ColumnIndexID(columnRangeMetadata.getColumnName());
    return getColumnStatsIndexKey(partitionIndexID, fileIndexID, columnIndexID);
  }

  public static Stream<HoodieRecord> createColumnStatsRecords(String partitionName,
                                                              Collection<HoodieColumnRangeMetadata<Comparable>> columnRangeMetadataList,
                                                              boolean isDeleted) {
    return columnRangeMetadataList.stream().map(columnRangeMetadata -> {
      HoodieKey key = new HoodieKey(getColumnStatsIndexKey(partitionName, columnRangeMetadata),
          MetadataPartitionType.COLUMN_STATS.getPartitionPath());

      HoodieMetadataPayload payload = new HoodieMetadataPayload(key.getRecordKey(),
          HoodieMetadataColumnStats.newBuilder()
              .setFileName(new Path(columnRangeMetadata.getFilePath()).getName())
              .setColumnName(columnRangeMetadata.getColumnName())
              .setMinValue(wrapStatisticValue(columnRangeMetadata.getMinValue()))
              .setMaxValue(wrapStatisticValue(columnRangeMetadata.getMaxValue()))
              .setNullCount(columnRangeMetadata.getNullCount())
              .setValueCount(columnRangeMetadata.getValueCount())
              .setTotalSize(columnRangeMetadata.getTotalSize())
              .setTotalUncompressedSize(columnRangeMetadata.getTotalUncompressedSize())
              .setIsDeleted(isDeleted)
              .build());

      return new HoodieAvroRecord<>(key, payload);
    });
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static HoodieMetadataColumnStats mergeColumnStatsRecords(HoodieMetadataColumnStats prevColumnStats,
                                                                   HoodieMetadataColumnStats newColumnStats) {
    checkArgument(Objects.equals(prevColumnStats.getFileName(), newColumnStats.getFileName()));
    checkArgument(Objects.equals(prevColumnStats.getColumnName(), newColumnStats.getColumnName()));

    if (newColumnStats.getIsDeleted()) {
      return newColumnStats;
    }

    Comparable minValue =
        (Comparable) Stream.of(
            (Comparable) unwrapStatisticValueWrapper(prevColumnStats.getMinValue()),
            (Comparable) unwrapStatisticValueWrapper(newColumnStats.getMinValue()))
        .filter(Objects::nonNull)
        .min(Comparator.naturalOrder())
        .orElse(null);

    Comparable maxValue =
        (Comparable) Stream.of(
            (Comparable) unwrapStatisticValueWrapper(prevColumnStats.getMinValue()),
            (Comparable) unwrapStatisticValueWrapper(newColumnStats.getMinValue()))
        .filter(Objects::nonNull)
        .max(Comparator.naturalOrder())
        .orElse(null);

    return HoodieMetadataColumnStats.newBuilder(METADATA_COLUMN_STATS_BUILDER_STUB.get())
        .setFileName(newColumnStats.getFileName())
        .setColumnName(newColumnStats.getColumnName())
        .setMinValue(wrapStatisticValue(minValue))
        .setMaxValue(wrapStatisticValue(maxValue))
        .setValueCount(prevColumnStats.getValueCount() + newColumnStats.getValueCount())
        .setNullCount(prevColumnStats.getNullCount() + newColumnStats.getNullCount())
        .setTotalSize(prevColumnStats.getTotalSize() + newColumnStats.getTotalSize())
        .setTotalUncompressedSize(prevColumnStats.getTotalUncompressedSize() + newColumnStats.getTotalUncompressedSize())
        .setIsDeleted(newColumnStats.getIsDeleted())
        .build();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("HoodieMetadataPayload {");
    sb.append(KEY_FIELD_NAME + "=").append(key).append(", ");
    sb.append(SCHEMA_FIELD_NAME_TYPE + "=").append(type).append(", ");
    sb.append("creations=").append(Arrays.toString(getFilenames().toArray())).append(", ");
    sb.append("deletions=").append(Arrays.toString(getDeletions().toArray())).append(", ");
    if (type == METADATA_TYPE_BLOOM_FILTER) {
      checkState(getBloomFilterMetadata().isPresent());
      sb.append("BloomFilter: {");
      sb.append("bloom size: ").append(getBloomFilterMetadata().get().getBloomFilter().array().length).append(", ");
      sb.append("timestamp: ").append(getBloomFilterMetadata().get().getTimestamp()).append(", ");
      sb.append("deleted: ").append(getBloomFilterMetadata().get().getIsDeleted());
      sb.append("}");
    }
    if (type == METADATA_TYPE_COLUMN_STATS) {
      checkState(getColumnStatMetadata().isPresent());
      sb.append("ColStats: {");
      sb.append(getColumnStatMetadata().get());
      sb.append("}");
    }
    sb.append('}');
    return sb.toString();
  }

  private static Object wrapStatisticValue(Comparable<?> statValue) {
    if (statValue == null) {
      return null;
    } else if (statValue instanceof Date || statValue instanceof LocalDate) {
      // NOTE: Due to breaking changes in code-gen b/w Avro 1.8.2 and 1.10, we can't
      //       rely on logical types to do proper encoding of the native Java types,
      //       and hereby have to encode statistic manually
      LocalDate localDate = statValue instanceof LocalDate
          ? (LocalDate) statValue
          : ((Date) statValue).toLocalDate();
      return DateWrapper.newBuilder(DATE_WRAPPER_BUILDER_STUB.get())
          .setValue((int) localDate.toEpochDay())
          .build();
    } else if (statValue instanceof BigDecimal) {
      Schema valueSchema = DecimalWrapper.SCHEMA$.getField("value").schema();
      BigDecimal upcastDecimal = tryUpcastDecimal((BigDecimal) statValue, (LogicalTypes.Decimal) valueSchema.getLogicalType());
      return DecimalWrapper.newBuilder(DECIMAL_WRAPPER_BUILDER_STUB.get())
          .setValue(AVRO_DECIMAL_CONVERSION.toBytes(upcastDecimal, valueSchema, valueSchema.getLogicalType()))
          .build();
    } else if (statValue instanceof Timestamp) {
      // NOTE: Due to breaking changes in code-gen b/w Avro 1.8.2 and 1.10, we can't
      //       rely on logical types to do proper encoding of the native Java types,
      //       and hereby have to encode statistic manually
      Instant instant = ((Timestamp) statValue).toInstant();
      return TimestampMicrosWrapper.newBuilder(TIMESTAMP_MICROS_WRAPPER_BUILDER_STUB.get())
          .setValue(instantToMicros(instant))
          .build();
    } else if (statValue instanceof Boolean) {
      return BooleanWrapper.newBuilder(BOOLEAN_WRAPPER_BUILDER_STUB.get()).setValue((Boolean) statValue).build();
    } else if (statValue instanceof Integer) {
      return IntWrapper.newBuilder(INT_WRAPPER_BUILDER_STUB.get()).setValue((Integer) statValue).build();
    } else if (statValue instanceof Long) {
      return LongWrapper.newBuilder(LONG_WRAPPER_BUILDER_STUB.get()).setValue((Long) statValue).build();
    } else if (statValue instanceof Float) {
      return FloatWrapper.newBuilder(FLOAT_WRAPPER_BUILDER_STUB.get()).setValue((Float) statValue).build();
    } else if (statValue instanceof Double) {
      return DoubleWrapper.newBuilder(DOUBLE_WRAPPER_BUILDER_STUB.get()).setValue((Double) statValue).build();
    } else if (statValue instanceof ByteBuffer) {
      return BytesWrapper.newBuilder(BYTES_WRAPPER_BUILDER_STUB.get()).setValue((ByteBuffer) statValue).build();
    } else if (statValue instanceof String || statValue instanceof Utf8) {
      return StringWrapper.newBuilder(STRING_WRAPPER_BUILDER_STUB.get()).setValue(statValue.toString()).build();
    } else {
      throw new UnsupportedOperationException(String.format("Unsupported type of the statistic (%s)", statValue.getClass()));
    }
  }

  public static Comparable<?> unwrapStatisticValueWrapper(Object statValueWrapper) {
    if (statValueWrapper == null) {
      return null;
    } else if (statValueWrapper instanceof DateWrapper) {
      return LocalDate.ofEpochDay(((DateWrapper) statValueWrapper).getValue());
    } else if (statValueWrapper instanceof DecimalWrapper) {
      Schema valueSchema = DecimalWrapper.SCHEMA$.getField("value").schema();
      return AVRO_DECIMAL_CONVERSION.fromBytes(((DecimalWrapper) statValueWrapper).getValue(), valueSchema, valueSchema.getLogicalType());
    } else if (statValueWrapper instanceof TimestampMicrosWrapper) {
      return microsToInstant(((TimestampMicrosWrapper) statValueWrapper).getValue());
    } else if (statValueWrapper instanceof BooleanWrapper) {
      return ((BooleanWrapper) statValueWrapper).getValue();
    } else if (statValueWrapper instanceof IntWrapper) {
      return ((IntWrapper) statValueWrapper).getValue();
    } else if (statValueWrapper instanceof LongWrapper) {
      return ((LongWrapper) statValueWrapper).getValue();
    } else if (statValueWrapper instanceof FloatWrapper) {
      return ((FloatWrapper) statValueWrapper).getValue();
    } else if (statValueWrapper instanceof DoubleWrapper) {
      return ((DoubleWrapper) statValueWrapper).getValue();
    } else if (statValueWrapper instanceof BytesWrapper) {
      return ((BytesWrapper) statValueWrapper).getValue();
    } else if (statValueWrapper instanceof StringWrapper) {
      return ((StringWrapper) statValueWrapper).getValue();
    } else if (statValueWrapper instanceof GenericRecord) {
      // NOTE: This branch could be hit b/c Avro records could be reconstructed
      //       as {@code GenericRecord)
      // TODO add logical type decoding
      GenericRecord record = (GenericRecord) statValueWrapper;
      return (Comparable<?>) record.get("value");
    } else {
      throw new UnsupportedOperationException(String.format("Unsupported type of the statistic (%s)", statValueWrapper.getClass()));
    }
  }

  private static void validatePayload(int type, Map<String, HoodieMetadataFileInfo> filesystemMetadata) {
    if (type == METADATA_TYPE_FILE_LIST) {
      filesystemMetadata.forEach((fileName, fileInfo) -> {
        checkState(fileInfo.getIsDeleted() || fileInfo.getSize() > 0, "Existing files should have size > 0");
      });
    }
  }

  private static <T> T getNestedFieldValue(GenericRecord record, String fieldName) {
    // NOTE: This routine is more lightweight than {@code HoodieAvroUtils.getNestedFieldVal}
    if (record.getSchema().getField(fieldName) == null) {
      return null;
    }

    return unsafeCast(record.get(fieldName));
  }
}
