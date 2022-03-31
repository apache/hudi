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

import org.apache.hudi.avro.model.HoodieMetadataFileInfo;
import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.avro.model.HoodieRecordIndexInfo;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.metadata.HoodieTableMetadata.RECORDKEY_PARTITION_LIST;

/**
 * This is a payload which saves information about a single entry in the Metadata Table.
 *
 * The type of the entry is determined by the "type" saved within the record. The following types of entries are saved:
 *
 *   1. List of partitions: There is a single such record
 *         key="__all_partitions__"
 *
 *   2. List of files in a Partition: There is one such record for each partition
 *         key=Partition name
 *
 *  During compaction on the table, the deletions are merged with additions and hence pruned.
 *
 * Metadata Table records are saved with the schema defined in HoodieMetadata.avsc. This class encapsulates the
 * HoodieMetadataRecord for ease of operations.
 */
public class HoodieMetadataPayload implements HoodieRecordPayload<HoodieMetadataPayload> {

  // HoodieMetadata schema field ids
  public static final String KEY_FIELD_NAME = "key";
  public static final String SCHEMA_FIELD_NAME_TYPE = "type";
  public static final String SCHEMA_FIELD_NAME_METADATA = "filesystemMetadata";
  public static final String SCHEMA_FIELD_NAME_RECORD_INDEX = "recordIndexMetadata";

  // Type of the record
  // This can be an enum in the schema but Avro 1.8 has a bug - https://issues.apache.org/jira/browse/AVRO-1810
  private static final int PARTITION_LIST = 1;
  private static final int FILE_LIST = 2;
  private static final int RECORD_INDEX = 3;

  // FileIndex value saved when the fileId has no index (old format)
  private static final int MISSING_FILEINDEX = -1;

  private String key = null;
  private int type = 0;
  private Map<String, HoodieMetadataFileInfo> filesystemMetadata;
  private HoodieRecordIndexInfo recordIndexInfo;

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
        filesystemMetadata = (Map<String, HoodieMetadataFileInfo>) record.get().get(SCHEMA_FIELD_NAME_METADATA);
        filesystemMetadata.keySet().forEach(k -> {
          GenericRecord v = filesystemMetadata.get(k);
          filesystemMetadata.put(k.toString(), new HoodieMetadataFileInfo((Long) v.get("size"), (Boolean) v.get("isDeleted")));
        });
      } else if (record.get().get(SCHEMA_FIELD_NAME_RECORD_INDEX) != null) {
        GenericRecord recordIndexMetadata = (GenericRecord) record.get().get(SCHEMA_FIELD_NAME_RECORD_INDEX);
        recordIndexInfo = new HoodieRecordIndexInfo(recordIndexMetadata.get("partition").toString(),
            Long.parseLong(recordIndexMetadata.get("fileIdHighBits").toString()),
            Long.parseLong(recordIndexMetadata.get("fileIdLowBits").toString()),
            Integer.parseInt(recordIndexMetadata.get("fileIndex").toString()),
            Long.parseLong(recordIndexMetadata.get("instantTime").toString()));
      }
    }
  }

  private HoodieMetadataPayload(String key, int type, Map<String, HoodieMetadataFileInfo> filesystemMetadata) {
    this.key = key;
    this.type = type;
    this.filesystemMetadata = filesystemMetadata;
  }

  private HoodieMetadataPayload(String key, HoodieRecordIndexInfo recordIndexInfo) {
    this.key = key;
    this.type = RECORD_INDEX;
    this.recordIndexInfo = recordIndexInfo;
  }

  private HoodieMetadataPayload() {}

  /**
   * Create and return a {@code HoodieMetadataPayload} to save list of partitions.
   *
   * @param partitions The list of partitions
   */
  public static HoodieRecord<HoodieMetadataPayload> createPartitionListRecord(List<String> partitions) {
    Map<String, HoodieMetadataFileInfo> fileInfo = new HashMap<>();
    partitions.forEach(partition -> fileInfo.put(partition, new HoodieMetadataFileInfo(0L,  false)));

    HoodieKey key = new HoodieKey(RECORDKEY_PARTITION_LIST, MetadataPartitionType.FILES.partitionPath());
    HoodieMetadataPayload payload = new HoodieMetadataPayload(key.getRecordKey(), PARTITION_LIST, fileInfo);
    return new HoodieRecord<>(key, payload);
  }

  /**
   * Create and return a {@code HoodieMetadataPayload} to save list of files within a partition.
   *
   * @param partition The name of the partition
   * @param filesAdded Mapping of files to their sizes for files which have been added to this partition
   * @param filesDeleted List of files which have been deleted from this partition
   */
  public static HoodieRecord<HoodieMetadataPayload> createPartitionFilesRecord(String partition,
                                                                               Option<Map<String, Long>> filesAdded, Option<List<String>> filesDeleted) {
    Map<String, HoodieMetadataFileInfo> fileInfo = new HashMap<>();
    filesAdded.ifPresent(
        m -> m.forEach((filename, size) -> fileInfo.put(filename, new HoodieMetadataFileInfo(size, false))));
    filesDeleted.ifPresent(
        m -> m.forEach(filename -> fileInfo.put(filename, new HoodieMetadataFileInfo(0L,  true))));

    HoodieKey key = new HoodieKey(partition, MetadataPartitionType.FILES.partitionPath());
    HoodieMetadataPayload payload = new HoodieMetadataPayload(key.getRecordKey(), FILE_LIST, fileInfo);
    return new HoodieRecord<>(key, payload);
  }

  /**
   * Create and return a {@code HoodieMetadataPayload} to insert or update an entry for the record index.
   *
   * Each entry maps the key of a single record in HUDI to its location.
   *
   * @param recordKey Key of the record
   * @param partition Name of the partition which contains the record
   * @param fileId fileId which contains the record
   * @param instantTime instantTime when the record was added
   */
  public static HoodieRecord<HoodieMetadataPayload> createRecordIndexUpdate(String recordKey, String partition,
      String fileId, String instantTime) {
    HoodieKey key = new HoodieKey(recordKey, MetadataPartitionType.RECORD_INDEX.partitionPath());
    // Data file names have a -D suffix to denote the index (D = integer) of the file written
    // In older HUID versions the file index was missing
    final UUID uuid;
    final int fileIndex;
    try {
      if (fileId.length() == 36) {
        uuid = UUID.fromString(fileId);
        fileIndex = MISSING_FILEINDEX;
      } else {
        final int index = fileId.lastIndexOf("-");
        uuid = UUID.fromString(fileId.substring(0, index));
        fileIndex = Integer.parseInt(fileId.substring(index + 1));
      }
    } catch (Exception e) {
      throw new HoodieMetadataException(String.format("Invalid UUID or index: fileID=%s, partition=%s, instantTIme=%s",
          fileId, partition, instantTime), e);
    }

    // Store instantTime as milliseconds sinch epoch. Using an int here allows dates till year 2038.
    Date instantDate;
    try {
      instantDate = HoodieActiveTimeline.parseDateFromInstantTime(instantTime);
    } catch (Exception e) {
      throw new HoodieMetadataException("Invalid instantTime format: " + instantTime, e);
    }

    HoodieMetadataPayload payload = new HoodieMetadataPayload(recordKey, new HoodieRecordIndexInfo(partition,
        uuid.getMostSignificantBits(), uuid.getLeastSignificantBits(), fileIndex,
        instantDate.getTime()));
    return new HoodieRecord<>(key, payload);
  }

  /**
   * Create and return a {@code HoodieMetadataPayload} to delete a record in the Metadata Table's record index.
   *
   * @param recordKey Key of the record to be deleted
   */
  public static HoodieRecord<HoodieMetadataPayload> createRecordIndexDelete(String recordKey) {
    HoodieKey key = new HoodieKey(recordKey, MetadataPartitionType.RECORD_INDEX.partitionPath());
    HoodieMetadataPayload payload = new HoodieMetadataPayload();
    return new HoodieRecord<>(key, payload);
  }

  @Override
  public HoodieMetadataPayload preCombine(HoodieMetadataPayload previousRecord) {
    if (previousRecord.type != type) {
      throw new HoodieMetadataException("Cannot combine " + previousRecord.type  + " with " + type);
    }
    if (!previousRecord.key.equals(key)) {
      throw new HoodieMetadataException("Cannot combine record with key " + previousRecord.key + " with " + key);
    }

    switch (type) {
      case PARTITION_LIST:
      case FILE_LIST:
        Map<String, HoodieMetadataFileInfo> combinedFileInfo = combineFilesystemMetadata(previousRecord);
        return new HoodieMetadataPayload(key, type, combinedFileInfo);
      case RECORD_INDEX:
        // TODO: does not work with updates
        if (previousRecord.recordIndexInfo.getInstantTime() != recordIndexInfo.getInstantTime()) {
          throw new HoodieMetadataException(String.format("InstantTime for %s should not change from %s to %s", previousRecord.key,
                previousRecord, toString()));
        }
        // TODO: This does not work with clustering
        if (!previousRecord.getRecordGlobalLocation().equals(getRecordGlobalLocation())) {
          throw new HoodieMetadataException(String.format("Location for %s should not change from %s to %s", previousRecord.key,
                previousRecord, toString()));
        }
        return this;
      default:
        throw new HoodieMetadataException("Unknown type of HoodieMetadataPayload: " + type);
    }
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord oldRecord, Schema schema) throws IOException {
    HoodieMetadataPayload anotherPayload = new HoodieMetadataPayload(Option.of((GenericRecord)oldRecord));
    HoodieRecordPayload combinedPayload = preCombine(anotherPayload);
    return combinedPayload.getInsertValue(schema);
  }

  @Override
  public Option<IndexedRecord> getInsertValue(Schema schema) throws IOException {
    if (key == null) {
      return Option.empty();
    }

    HoodieMetadataRecord record = new HoodieMetadataRecord(key, type, filesystemMetadata, recordIndexInfo);
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

  /**
   * If this is a record-level index entry, returns the file to which this is mapped.
   */
  public HoodieRecordGlobalLocation getRecordGlobalLocation() {
    final UUID uuid = new UUID(recordIndexInfo.getFileIdHighBits(), recordIndexInfo.getFileIdLowBits());
    final String partition = recordIndexInfo.getPartition();
    String fileId = uuid.toString();
    if (recordIndexInfo.getFileIndex() != MISSING_FILEINDEX) {
      fileId += "-" + String.valueOf(recordIndexInfo.getFileIndex());
    }
    final Date instantDate = new Date(recordIndexInfo.getInstantTime());
    return new HoodieRecordGlobalLocation(partition, HoodieActiveTimeline.formatDate(instantDate), fileId);
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
    sb.append(KEY_FIELD_NAME + "=").append(key).append(", ");
    sb.append(SCHEMA_FIELD_NAME_TYPE + "=").append(type).append(", ");
    if (type == PARTITION_LIST || type == FILE_LIST) {
      sb.append("creations=").append(Arrays.toString(getFilenames().toArray())).append(", ");
      sb.append("deletions=").append(Arrays.toString(getDeletions().toArray())).append(", ");
    } else if (type == RECORD_INDEX) {
      sb.append("location=").append(getRecordGlobalLocation());
    }
    sb.append('}');
    return sb.toString();
  }
}
