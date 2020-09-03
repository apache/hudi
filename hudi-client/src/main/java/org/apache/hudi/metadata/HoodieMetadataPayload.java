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

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This is a payload which saves information about a single entry in the Metadata Table. The type of the entry is
 * determined by the "type" saved within the record. The following types of entries are saved:
 *
 *   1. List of partitions: There is a single such record
 *         key="__all_partitions__"
 *         filenameToSizeMap={"2020/01/01": 0, "2020/01/02": 0, ...}
 *
 *   2. List of files in a Partition: There is one such record for each partition
 *         key=Partition name
 *         filenameToSizeMap={"file1.parquet": 12345, "file2.parquet": 56789, "file1.log": 9876,
 *                            "file0.parquet": -1, ...}
 *
 *      For deleted files, -1 is used as the size.
 *
 *  During compaction on the table, the deletions are merged with additions and hence pruned.
 */
public class HoodieMetadataPayload implements HoodieRecordPayload<HoodieMetadataPayload> {
  private static final Logger LOG = LogManager.getLogger(HoodieMetadataPayload.class);

  // Represents the size stored for a deleted file
  private static final long DELETED_FILE_SIZE = -1;

  // Key and type for the metadata record
  private final String metadataKey;
  private final PayloadType type;

  // Filenames which are part of this record
  // key=filename, value=file size (or DELETED_FILE_SIZE to represent a deleted file)
  private final Map<String, Long> filenameMap = new HashMap<>();

  // Type of the metadata record
  public enum PayloadType {
    PARTITION_LIST(1),        // list of partitions
    PARTITION_FILES(2);       // list of files in a partition

    private final int value;

    PayloadType(final int newValue) {
      value = newValue;
    }

    static PayloadType valueOf(int value) {
      for (PayloadType t : values()) {
        if (t.value == value) {
          return t;
        }
      }
      return null;
    }
  }

  public HoodieMetadataPayload(Option<GenericRecord> record) {
    if (record.isPresent()) {
      // GenericRecord saves strings in UTF8 and we need to convert them back to Java's String
      metadataKey = record.get().get("key").toString();
      type = PayloadType.valueOf((Integer)record.get().get("type"));
      Map<Object, Long> recordFileMap = (Map<Object, Long>)record.get().get("filenameToSizeMap");
      recordFileMap.forEach((utf8filename, size) -> {
        filenameMap.put(utf8filename.toString(), size);
      });
    } else {
      metadataKey = null;
      type = null;
    }
  }

  private HoodieMetadataPayload(String metadataKey, PayloadType type, Map<String, Long> filesAdded,
                                List<String> filesDeleted) {
    this.metadataKey = metadataKey;
    this.type = type;
    filesAdded.forEach((filename, size) -> this.filenameMap.put(filename, size));
    filesDeleted.forEach(filename -> this.filenameMap.put(filename, DELETED_FILE_SIZE));
  }

  private HoodieMetadataPayload(String metadataKey, PayloadType type, Map<String, Long> filesAdded) {
    this(metadataKey, type, filesAdded, Collections.emptyList());
  }

  /**
   * Create and return a {@code HoodieMetadataPayload} to save list of partitions.
   *
   * @param partitions The list of partitions
   */
  public static HoodieRecord<HoodieMetadataPayload> createPartitionListRecord(List<String> partitions) {
    Map<String, Long> fileList = new HashMap<>();
    partitions.forEach(partition -> fileList.put(partition, 0L));

    HoodieKey key = new HoodieKey(HoodieMetadataImpl.RECORDKEY_PARTITION_LIST, HoodieMetadataImpl.METADATA_PARTITION_NAME);
    HoodieMetadataPayload payload = new HoodieMetadataPayload(key.getRecordKey(), PayloadType.PARTITION_LIST, fileList);
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
    HoodieKey key = new HoodieKey(partition, HoodieMetadataImpl.METADATA_PARTITION_NAME);
    HoodieMetadataPayload payload = new HoodieMetadataPayload(partition, PayloadType.PARTITION_FILES,
        filesAdded.orElseGet(() -> Collections.emptyMap()),
        filesDeleted.orElseGet(() -> Collections.emptyList()));
    return new HoodieRecord<>(key, payload);
  }

  @Override
  public HoodieMetadataPayload preCombine(HoodieMetadataPayload previousRecord) {
    if (previousRecord.type != this.type) {
      throw new HoodieMetadataException("Cannot combine " + previousRecord.type + " with " + this.type);
    }

    HashMap<String, Long> combinedFilenameMap = new HashMap<>(previousRecord.filenameMap);
    filenameMap.forEach((filename, newSize) -> {
      // Merge: If the filename does not exist in merge then newSize will be stored as the value. Otherwise the
      //        lambda function will be called with old stored value and the newSize.
      combinedFilenameMap.merge(filename, newSize, (oldSize, newSizeCopy) -> {
        // If the file has been deleted then return null which removes the key mapping from the map. Otherwise
        // update the stored file size.
        return newSize == DELETED_FILE_SIZE ? null : newSize + oldSize;
      });
    });

    return new HoodieMetadataPayload(this.metadataKey, this.type, combinedFilenameMap);
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord oldRecord, Schema schema) throws IOException {
    HoodieMetadataPayload anotherPayload = new HoodieMetadataPayload(Option.of((GenericRecord)oldRecord));
    HoodieRecordPayload combinedPayload = preCombine(anotherPayload);
    return combinedPayload.getInsertValue(schema);
  }

  @Override
  public Option<IndexedRecord> getInsertValue(Schema schema) throws IOException {
    if (metadataKey != null) {
      GenericRecord record = new GenericData.Record(schema);
      record.put("key", metadataKey);
      record.put("type", type.value);
      record.put("filenameToSizeMap", filenameMap);
      return Option.of(record);
    } else {
      return Option.empty();
    }
  }

  /**
   * Returns the list of filenames added as part of this record.
   */
  public List<String> getFilenames() {
    return filenameMap.entrySet().stream().filter(e -> e.getValue() != DELETED_FILE_SIZE).map(e -> e.getKey())
        .collect(Collectors.toList());
  }

  /**
   * Returns the list of filenames deleted as part of this record.
   */
  public List<String> getDeletions() {
    return filenameMap.entrySet().stream().filter(e -> e.getValue() == DELETED_FILE_SIZE).map(e -> e.getKey())
        .collect(Collectors.toList());
  }

  /**
   * Returns the files added as part of this record.
   */
  public FileStatus[] getFileStatuses(Path partitionPath) {
    return filenameMap.entrySet().stream().filter(e -> e.getValue() != DELETED_FILE_SIZE)
        .map(e -> new FileStatus(e.getValue(), false, 0, 0, 0, 0, null, null, null, new Path(partitionPath, e.getKey())))
        .toArray(FileStatus[]::new);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("HoodieMetadataPayload {");
    sb.append("key=").append(metadataKey).append(", ");
    sb.append("type=").append(type.toString()).append(", ");
    sb.append("creations=").append(Arrays.toString(getFilenames().toArray())).append(", ");
    sb.append("deletions=").append(Arrays.toString(getDeletions().toArray())).append(", ");
    sb.append('}');
    return sb.toString();
  }
}
