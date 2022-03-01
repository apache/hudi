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

import org.apache.hudi.avro.model.HoodieMetadataBloomFilter;
import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.avro.model.HoodieMetadataFileInfo;
import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.hash.ColumnIndexID;
import org.apache.hudi.common.util.hash.FileIndexID;
import org.apache.hudi.common.util.hash.PartitionIndexID;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.io.storage.HoodieHFileReader;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.metadata.HoodieTableMetadata.RECORDKEY_PARTITION_LIST;

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
  private static final String SCHEMA_FIELD_ID_COLUMN_STATS = "ColumnStatsMetadata";
  private static final String SCHEMA_FIELD_ID_BLOOM_FILTER = "BloomFilterMetadata";

  // HoodieMetadata bloom filter payload field ids
  private static final String FIELD_IS_DELETED = "isDeleted";
  private static final String BLOOM_FILTER_FIELD_TYPE = "type";
  private static final String BLOOM_FILTER_FIELD_TIMESTAMP = "timestamp";
  private static final String BLOOM_FILTER_FIELD_BLOOM_FILTER = "bloomFilter";
  private static final String BLOOM_FILTER_FIELD_IS_DELETED = FIELD_IS_DELETED;

  // HoodieMetadata column stats payload field ids
  private static final String COLUMN_STATS_FIELD_MIN_VALUE = "minValue";
  private static final String COLUMN_STATS_FIELD_MAX_VALUE = "maxValue";
  private static final String COLUMN_STATS_FIELD_NULL_COUNT = "nullCount";
  private static final String COLUMN_STATS_FIELD_VALUE_COUNT = "valueCount";
  private static final String COLUMN_STATS_FIELD_TOTAL_SIZE = "totalSize";
  private static final String COLUMN_STATS_FIELD_RESOURCE_NAME = "fileName";
  private static final String COLUMN_STATS_FIELD_TOTAL_UNCOMPRESSED_SIZE = "totalUncompressedSize";
  private static final String COLUMN_STATS_FIELD_IS_DELETED = FIELD_IS_DELETED;

  private String key = null;
  private int type = 0;
  private Map<String, HoodieMetadataFileInfo> filesystemMetadata = null;
  private HoodieMetadataBloomFilter bloomFilterMetadata = null;
  private HoodieMetadataColumnStats columnStatMetadata = null;

  public HoodieMetadataPayload(GenericRecord record, Comparable<?> orderingVal) {
    this(Option.of(record));
  }

  public HoodieMetadataPayload(Option<GenericRecord> record) {
    if (record.isPresent()) {
      // This can be simplified using SpecificData.deepcopy once this bug is fixed
      // https://issues.apache.org/jira/browse/AVRO-1811
      key = record.get().get(KEY_FIELD_NAME).toString();
      type = (int) record.get().get(SCHEMA_FIELD_NAME_TYPE);
      if (record.get().get(SCHEMA_FIELD_NAME_METADATA) != null) {
        filesystemMetadata = (Map<String, HoodieMetadataFileInfo>) record.get().get("filesystemMetadata");
        filesystemMetadata.keySet().forEach(k -> {
          GenericRecord v = filesystemMetadata.get(k);
          filesystemMetadata.put(k.toString(), new HoodieMetadataFileInfo((Long) v.get("size"), (Boolean) v.get("isDeleted")));
        });
      }

      if (type == METADATA_TYPE_BLOOM_FILTER) {
        final GenericRecord metadataRecord = (GenericRecord) record.get().get(SCHEMA_FIELD_ID_BLOOM_FILTER);
        if (metadataRecord == null) {
          throw new HoodieMetadataException("Valid " + SCHEMA_FIELD_ID_BLOOM_FILTER + " record expected for type: " + METADATA_TYPE_BLOOM_FILTER);
        }
        bloomFilterMetadata = new HoodieMetadataBloomFilter(
            (String) metadataRecord.get(BLOOM_FILTER_FIELD_TYPE),
            (String) metadataRecord.get(BLOOM_FILTER_FIELD_TIMESTAMP),
            (ByteBuffer) metadataRecord.get(BLOOM_FILTER_FIELD_BLOOM_FILTER),
            (Boolean) metadataRecord.get(BLOOM_FILTER_FIELD_IS_DELETED)
        );
      }

      if (type == METADATA_TYPE_COLUMN_STATS) {
        GenericRecord v = (GenericRecord) record.get().get(SCHEMA_FIELD_ID_COLUMN_STATS);
        if (v == null) {
          throw new HoodieMetadataException("Valid " + SCHEMA_FIELD_ID_COLUMN_STATS + " record expected for type: " + METADATA_TYPE_COLUMN_STATS);
        }
        columnStatMetadata = HoodieMetadataColumnStats.newBuilder()
            .setFileName((String) v.get(COLUMN_STATS_FIELD_RESOURCE_NAME))
            .setMinValue((String) v.get(COLUMN_STATS_FIELD_MIN_VALUE))
            .setMaxValue((String) v.get(COLUMN_STATS_FIELD_MAX_VALUE))
            .setValueCount((Long) v.get(COLUMN_STATS_FIELD_VALUE_COUNT))
            .setNullCount((Long) v.get(COLUMN_STATS_FIELD_NULL_COUNT))
            .setTotalSize((Long) v.get(COLUMN_STATS_FIELD_TOTAL_SIZE))
            .setTotalUncompressedSize((Long) v.get(COLUMN_STATS_FIELD_TOTAL_UNCOMPRESSED_SIZE))
            .setIsDeleted((Boolean) v.get(COLUMN_STATS_FIELD_IS_DELETED))
            .build();
      }
    }
  }

  private HoodieMetadataPayload(String key, int type, Map<String, HoodieMetadataFileInfo> filesystemMetadata) {
    this(key, type, filesystemMetadata, null, null);
  }

  private HoodieMetadataPayload(String key, int type, HoodieMetadataBloomFilter metadataBloomFilter) {
    this(key, type, null, metadataBloomFilter, null);
  }

  private HoodieMetadataPayload(String key, int type, HoodieMetadataColumnStats columnStats) {
    this(key, type, null, null, columnStats);
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
    Map<String, HoodieMetadataFileInfo> fileInfo = new HashMap<>();
    partitions.forEach(partition -> fileInfo.put(partition, new HoodieMetadataFileInfo(0L, false)));

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
    filesAdded.ifPresent(
        m -> m.forEach((filename, size) -> fileInfo.put(filename, new HoodieMetadataFileInfo(size, false))));
    filesDeleted.ifPresent(
        m -> m.forEach(filename -> fileInfo.put(filename, new HoodieMetadataFileInfo(0L, true))));

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
                                                                                    final ByteBuffer bloomFilter,
                                                                                    final boolean isDeleted) {
    ValidationUtils.checkArgument(!baseFileName.contains(Path.SEPARATOR)
            && FSUtils.isBaseFile(new Path(baseFileName)),
        "Invalid base file '" + baseFileName + "' for MetaIndexBloomFilter!");
    final String bloomFilterIndexKey = new PartitionIndexID(partitionName).asBase64EncodedString()
        .concat(new FileIndexID(baseFileName).asBase64EncodedString());
    HoodieKey key = new HoodieKey(bloomFilterIndexKey, MetadataPartitionType.BLOOM_FILTERS.getPartitionPath());

    // TODO: HUDI-3203 Get the bloom filter type from the file
    HoodieMetadataBloomFilter metadataBloomFilter =
        new HoodieMetadataBloomFilter(BloomFilterTypeCode.DYNAMIC_V0.name(),
            timestamp, bloomFilter, isDeleted);
    HoodieMetadataPayload metadataPayload = new HoodieMetadataPayload(key.getRecordKey(),
        HoodieMetadataPayload.METADATA_TYPE_BLOOM_FILTER, metadataBloomFilter);
    return new HoodieAvroRecord<>(key, metadataPayload);
  }

  @Override
  public HoodieMetadataPayload preCombine(HoodieMetadataPayload previousRecord) {
    ValidationUtils.checkArgument(previousRecord.type == type,
        "Cannot combine " + previousRecord.type + " with " + type);

    switch (type) {
      case METADATA_TYPE_PARTITION_LIST:
      case METADATA_TYPE_FILE_LIST:
        Map<String, HoodieMetadataFileInfo> combinedFileInfo = combineFilesystemMetadata(previousRecord);
        return new HoodieMetadataPayload(key, type, combinedFileInfo);
      case METADATA_TYPE_BLOOM_FILTER:
        HoodieMetadataBloomFilter combineBloomFilterMetadata = combineBloomFilterMetadata(previousRecord);
        return new HoodieMetadataPayload(key, type, combineBloomFilterMetadata);
      case METADATA_TYPE_COLUMN_STATS:
        return new HoodieMetadataPayload(key, type, combineColumnStatsMetadata(previousRecord));
      default:
        throw new HoodieMetadataException("Unknown type of HoodieMetadataPayload: " + type);
    }
  }

  private HoodieMetadataBloomFilter combineBloomFilterMetadata(HoodieMetadataPayload previousRecord) {
    return this.bloomFilterMetadata;
  }

  private HoodieMetadataColumnStats combineColumnStatsMetadata(HoodieMetadataPayload previousRecord) {
    return this.columnStatMetadata;
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
  public Option<IndexedRecord> getInsertValue(Schema schema, Properties properties) throws IOException {
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
    return filterFileInfoEntries(false).map(e -> e.getKey()).sorted().collect(Collectors.toList());
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

  private Map<String, HoodieMetadataFileInfo> combineFilesystemMetadata(HoodieMetadataPayload previousRecord) {
    Map<String, HoodieMetadataFileInfo> combinedFileInfo = new HashMap<>();
    if (previousRecord.filesystemMetadata != null) {
      combinedFileInfo.putAll(previousRecord.filesystemMetadata);
    }

    if (filesystemMetadata != null) {
      filesystemMetadata.forEach((filename, fileInfo) -> {
        // If the filename wasnt present then we carry it forward
        if (!combinedFileInfo.containsKey(filename)) {
          combinedFileInfo.put(filename, fileInfo);
        } else {
          if (fileInfo.getIsDeleted()) {
            // file deletion
            combinedFileInfo.remove(filename);
          } else {
            // file appends.
            combinedFileInfo.merge(filename, fileInfo, (oldFileInfo, newFileInfo) -> {
              return new HoodieMetadataFileInfo(oldFileInfo.getSize() + newFileInfo.getSize(), false);
            });
          }
        }
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

  public static Stream<HoodieRecord> createColumnStatsRecords(
      String partitionName, Collection<HoodieColumnRangeMetadata<Comparable>> columnRangeMetadataList, boolean isDeleted) {
    return columnRangeMetadataList.stream().map(columnRangeMetadata -> {
      HoodieKey key = new HoodieKey(getColumnStatsIndexKey(partitionName, columnRangeMetadata),
          MetadataPartitionType.COLUMN_STATS.getPartitionPath());
      HoodieMetadataPayload payload = new HoodieMetadataPayload(key.getRecordKey(), METADATA_TYPE_COLUMN_STATS,
          HoodieMetadataColumnStats.newBuilder()
              .setFileName(new Path(columnRangeMetadata.getFilePath()).getName())
              .setMinValue(columnRangeMetadata.getMinValue() == null ? null :
                  columnRangeMetadata.getMinValue().toString())
              .setMaxValue(columnRangeMetadata.getMaxValue() == null ? null :
                  columnRangeMetadata.getMaxValue().toString())
              .setNullCount(columnRangeMetadata.getNullCount())
              .setValueCount(columnRangeMetadata.getValueCount())
              .setTotalSize(columnRangeMetadata.getTotalSize())
              .setTotalUncompressedSize(columnRangeMetadata.getTotalUncompressedSize())
              .setIsDeleted(isDeleted)
              .build());
      return new HoodieAvroRecord<>(key, payload);
    });


  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("HoodieMetadataPayload {");
    sb.append(KEY_FIELD_NAME + "=").append(key).append(", ");
    sb.append(SCHEMA_FIELD_NAME_TYPE + "=").append(type).append(", ");
    sb.append("creations=").append(Arrays.toString(getFilenames().toArray())).append(", ");
    sb.append("deletions=").append(Arrays.toString(getDeletions().toArray())).append(", ");
    if (type == METADATA_TYPE_BLOOM_FILTER) {
      ValidationUtils.checkState(getBloomFilterMetadata().isPresent());
      sb.append("BloomFilter: {");
      sb.append("bloom size: " + getBloomFilterMetadata().get().getBloomFilter().array().length).append(", ");
      sb.append("timestamp: " + getBloomFilterMetadata().get().getTimestamp()).append(", ");
      sb.append("deleted: " + getBloomFilterMetadata().get().getIsDeleted());
      sb.append("}");
    }
    if (type == METADATA_TYPE_COLUMN_STATS) {
      ValidationUtils.checkState(getColumnStatMetadata().isPresent());
      sb.append("ColStats: {");
      sb.append(getColumnStatMetadata().get());
      sb.append("}");
    }
    sb.append('}');
    return sb.toString();
  }
}
