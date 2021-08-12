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
import org.apache.hudi.avro.model.HoodieRangeIndexInfo;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
  // Type of the record
  // This can be an enum in the schema but Avro 1.8 has a bug - https://issues.apache.org/jira/browse/AVRO-1810
  private static final int PARTITION_LIST = 1;
  private static final int FILE_LIST = 2;
  private static final int RANGE_INDEX = 3;

  private String key = null;
  private int type = 0;
  private Map<String, HoodieMetadataFileInfo> filesystemMetadata = null;
  private HoodieRangeIndexInfo rangeIndexMetadata = null;

  public HoodieMetadataPayload(Option<GenericRecord> record) {
    if (record.isPresent()) {
      // This can be simplified using SpecificData.deepcopy once this bug is fixed
      // https://issues.apache.org/jira/browse/AVRO-1811
      key = record.get().get("key").toString();
      type = (int) record.get().get("type");
      if (record.get().get("filesystemMetadata") != null) {
        filesystemMetadata = (Map<String, HoodieMetadataFileInfo>) record.get().get("filesystemMetadata");
        filesystemMetadata.keySet().forEach(k -> {
          GenericRecord v = filesystemMetadata.get(k);
          filesystemMetadata.put(k.toString(), new HoodieMetadataFileInfo((Long)v.get("size"), (Boolean)v.get("isDeleted")));
        });
      }

      if (record.get().get("rangeIndexMetadata") != null) {
        GenericRecord v = (GenericRecord) record.get().get("rangeIndexMetadata");
        rangeIndexMetadata = new HoodieRangeIndexInfo(String.valueOf(v.get("columnName")), String.valueOf(v.get("filePath")),
            String.valueOf(v.get("rangeLow")), String.valueOf(v.get("rangeHigh")), (Boolean) v.get("isDeleted"));
      }
    }
  }

  private HoodieMetadataPayload(String key, int type, Map<String, HoodieMetadataFileInfo> filesystemMetadata) {
    this.key = key;
    this.type = type;
    this.filesystemMetadata = filesystemMetadata;
  }

  private HoodieMetadataPayload(String key, int type,
                                Map<String, HoodieMetadataFileInfo> filesystemMetadata,
                                HoodieRangeIndexInfo rangeIndexInfo) {
    this.key = key;
    this.type = type;
    this.filesystemMetadata = filesystemMetadata;
    this.rangeIndexMetadata = rangeIndexInfo;
  }


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

  @Override
  public HoodieMetadataPayload preCombine(HoodieMetadataPayload previousRecord) {
    ValidationUtils.checkArgument(previousRecord.type == type,
        "Cannot combine " + previousRecord.type  + " with " + type);

    Map<String, HoodieMetadataFileInfo> combinedFileInfo = null;
    HoodieRangeIndexInfo combinedRangeInfo = null;

    switch (type) {
      case PARTITION_LIST:
      case FILE_LIST:
        combinedFileInfo = combineFilesystemMetadata(previousRecord);
        break;
      case RANGE_INDEX:
        combinedRangeInfo = combineRangeMetadata(previousRecord);
        break;
      default:
        throw new HoodieMetadataException("Unknown type of HoodieMetadataPayload: " + type);
    }

    return new HoodieMetadataPayload(key, type, combinedFileInfo, combinedRangeInfo);
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

    HoodieMetadataRecord record = new HoodieMetadataRecord(key, type, filesystemMetadata, rangeIndexMetadata);
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

  private HoodieRangeIndexInfo combineRangeMetadata(HoodieMetadataPayload previousRecord) {
    // files are immutable and range is not expected to change. TODO Figure out deletions
    return previousRecord.rangeIndexMetadata;
  }

  /**
   * Create and return a {@code HoodieMetadataPayload} to save list of ranges for given set of files.
   */
  public static Stream<HoodieRecord<HoodieMetadataPayload>> createRangeRecords(String filePath, Collection<HoodieColumnRangeMetadata<Comparable>> columnRangeInfo) {
    return columnRangeInfo.stream().map(columnRange -> {
      HoodieKey key = new HoodieKey(getRangeRecordKey(columnRange), MetadataPartitionType.RANGE_INDEX.partitionPath());
      
      HoodieMetadataPayload payload = new HoodieMetadataPayload(key.getRecordKey(), RANGE_INDEX, Collections.emptyMap(),
          HoodieRangeIndexInfo.newBuilder()
              //TODO: we are storing range for all columns as string. add support for other primitive types
              // also if min/max is null, we store null for these columns. Should we consider storing String "null" instead?
              .setRangeHigh(columnRange.getMinValue() == null ? null : columnRange.getMaxValue().toString()) 
              .setRangeLow(columnRange.getMinValue() == null ? null : columnRange.getMaxValue().toString()) 
              .setColumnName(columnRange.getColumnName())
              .setFilePath(filePath)
              .setIsDeleted(false)
              .build());

      return new HoodieRecord<>(key, payload);
    });
  }
  
  // get record key from range metadata
  public static String getRangeRecordKey(HoodieColumnRangeMetadata<Comparable> columnRange) {
    return "column||" + columnRange.getColumnName() + ";;path||" + columnRange.getFilePath();
  }
  
  // parse attribute in record key. TODO: find better way to get this attribute instaed of parsing key
  public static String getAttributeFromRecordKey(String recordKey, String attribute) {
    String[] attributeNameValuePairs = recordKey.split(";;");
    return Arrays.stream(attributeNameValuePairs)
        .filter(nameValue -> nameValue.startsWith(attribute))
        .findFirst()
        .map(s -> s.split("\\|\\|")[1]).orElse(null);
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
