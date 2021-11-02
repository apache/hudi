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
import org.apache.hudi.avro.model.HoodieMetadataFileInfo;
import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.hash.FileID;

import org.apache.hudi.exception.HoodieMetadataException;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
 * -- key = @{@link HoodieTableMetadata.RECORDKEY_PARTITION_LIST}
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
  //protected static final int METADATA_TYPE_COLUMN_STATS = 3;
  protected static final int METADATA_TYPE_BLOOM_FILTER = 4;

  // Various metadata type record names in the payload
  private static final String METADATA_RECORD_FILESYSTEM = "filesystemMetadata";
  //private static final String METADATA_RECORD_COLUMN_STATS = "ColumnStatsMetadata";
  private static final String METADATA_RECORD_BLOOM_FILTER = "BloomFilterMetadata";

  private String key = null;
  private int type = 0;
  private Map<String, HoodieMetadataFileInfo> filesystemMetadata = null;
  private HoodieMetadataBloomFilter bloomFilterMetadata = null;

  public HoodieMetadataPayload(GenericRecord record, Comparable<?> orderingVal) {
    this(Option.of(record));
  }

  public HoodieMetadataPayload(Option<GenericRecord> record) {
    if (record.isPresent()) {
      // This can be simplified using SpecificData.deepcopy once this bug is fixed
      // https://issues.apache.org/jira/browse/AVRO-1811
      key = record.get().get("key").toString();
      type = (int) record.get().get("type");

      if (record.get().get(METADATA_RECORD_FILESYSTEM) != null) {
        filesystemMetadata = (Map<String, HoodieMetadataFileInfo>) record.get().get(METADATA_RECORD_FILESYSTEM);
        filesystemMetadata.keySet().forEach(k -> {
          GenericRecord v = filesystemMetadata.get(k);
          filesystemMetadata.put(k.toString(), new HoodieMetadataFileInfo((Long) v.get("size"), (Boolean) v.get(
              "isDeleted")));
        });
      }

      if (type == METADATA_TYPE_BLOOM_FILTER) {
        final String FIELD_VERSION = "version";
        final String FIELD_TIMESTAMP = "timestamp";
        final String FIELD_BLOOM_FILTER = "bloomfilter";
        final String FIELD_VALID = "valid";

        final GenericRecord metadataRecord = (GenericRecord) record.get().get(METADATA_RECORD_BLOOM_FILTER);
        if (metadataRecord == null) {
          throw new HoodieMetadataException("Valid " + METADATA_RECORD_BLOOM_FILTER + " record expected for type: " + METADATA_TYPE_BLOOM_FILTER);
        }
        bloomFilterMetadata = new HoodieMetadataBloomFilter(
            (Integer) metadataRecord.get(FIELD_VERSION),
            (String) metadataRecord.get(FIELD_TIMESTAMP),
            (ByteBuffer) metadataRecord.get(FIELD_BLOOM_FILTER),
            (Boolean) metadataRecord.get(FIELD_VALID)
        );
      }
    }
  }

  private HoodieMetadataPayload(String key, int type, Map<String, HoodieMetadataFileInfo> filesystemMetadata) {
    this(key, type, filesystemMetadata, null);
  }

  private HoodieMetadataPayload(String key, int type, HoodieMetadataBloomFilter metadataBloomFilter) {
    this(key, type, null, metadataBloomFilter);
  }

  protected HoodieMetadataPayload(String key, int type,
                                  Map<String, HoodieMetadataFileInfo> filesystemMetadata,
                                  HoodieMetadataBloomFilter metadataBloomFilter) {
    this.key = key;
    this.type = type;
    this.filesystemMetadata = filesystemMetadata;
    this.bloomFilterMetadata = metadataBloomFilter;
  }

  /**
   * Create and return a {@code HoodieMetadataPayload} to save list of partitions.
   *
   * @param partitions The list of partitions
   */
  public static HoodieRecord<HoodieMetadataPayload> createPartitionListRecord(List<String> partitions) {
    Map<String, HoodieMetadataFileInfo> fileInfo = new HashMap<>();
    partitions.forEach(partition -> fileInfo.put(partition, new HoodieMetadataFileInfo(0L, false)));

    HoodieKey key = new HoodieKey(RECORDKEY_PARTITION_LIST, MetadataPartitionType.FILES.partitionPath());
    HoodieMetadataPayload payload = new HoodieMetadataPayload(key.getRecordKey(), METADATA_TYPE_PARTITION_LIST,
        fileInfo, null);
    return new HoodieRecord<>(key, payload);
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

    HoodieKey key = new HoodieKey(partition, MetadataPartitionType.FILES.partitionPath());
    HoodieMetadataPayload payload = new HoodieMetadataPayload(key.getRecordKey(), METADATA_TYPE_FILE_LIST, fileInfo,
        null);
    return new HoodieRecord<>(key, payload);
  }

  /**
   * Create bloom filter metadata record.
   *
   * @param fileID      - FileID for which the bloom filter needs to persisted
   * @param timestamp   - Instant timestamp responsible for this record
   * @param bloomFilter - Bloom filter for the File
   * @param isValid     - Is the valid and the bloom filter valid
   * @return Metadata payload containing the fileID and its bloom filter record
   */
  public static HoodieRecord<HoodieMetadataPayload> createBloomFilterMetadataRecord(final FileID fileID,
                                                                                    final String timestamp,
                                                                                    final ByteBuffer bloomFilter,
                                                                                    final boolean isValid) {
    HoodieKey key = new HoodieKey(fileID.asBase64EncodedString(), MetadataPartitionType.BLOOM_FILTERS.partitionPath());

    HoodieMetadataBloomFilter metadataBloomFilter =
        new HoodieMetadataBloomFilter(HoodieMetadataBloomFilterUtil.VERSION_METADATA_BLOOM_FILTER,
            timestamp, bloomFilter, isValid);
    HoodieMetadataPayload metadataPayload = new HoodieMetadataPayload(key.getRecordKey(),
        HoodieMetadataPayload.METADATA_TYPE_BLOOM_FILTER, metadataBloomFilter);
    return new HoodieRecord<>(key, metadataPayload);
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
      default:
        throw new HoodieMetadataException("Unknown type of HoodieMetadataPayload: " + type);
    }
  }

  private HoodieMetadataBloomFilter combineBloomFilterMetadata(HoodieMetadataPayload previousRecord) {
    return this.bloomFilterMetadata;
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord oldRecord, Schema schema) throws IOException {
    HoodieMetadataPayload anotherPayload = new HoodieMetadataPayload(Option.of((GenericRecord) oldRecord));
    HoodieRecordPayload combinedPayload = preCombine(anotherPayload);
    return combinedPayload.getInsertValue(schema);
  }

  @Override
  public Option<IndexedRecord> getInsertValue(Schema schema) throws IOException {
    if (key == null) {
      return Option.empty();
    }

    HoodieMetadataRecord record = new HoodieMetadataRecord(key, type, filesystemMetadata, bloomFilterMetadata);
    return Option.of(record);
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

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("HoodieMetadataPayload {");
    sb.append("key=").append(key).append(", ");
    sb.append("type=").append(type).append(", ");
    sb.append("creations=").append(Arrays.toString(getFilenames().toArray())).append(", ");
    sb.append("deletions=").append(Arrays.toString(getDeletions().toArray())).append(", ");
    sb.append('}');
    return sb.toString();
  }
}
