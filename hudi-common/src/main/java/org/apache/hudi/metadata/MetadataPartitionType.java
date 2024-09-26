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
import org.apache.hudi.avro.model.HoodieRecordIndexInfo;
import org.apache.hudi.avro.model.HoodieSecondaryIndexInfo;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableMetaClient;

import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.avro.HoodieAvroUtils.unwrapAvroValueWrapper;
import static org.apache.hudi.avro.HoodieAvroUtils.wrapValueIntoAvro;
import static org.apache.hudi.common.config.HoodieMetadataConfig.ENABLE;
import static org.apache.hudi.common.config.HoodieMetadataConfig.ENABLE_METADATA_INDEX_BLOOM_FILTER;
import static org.apache.hudi.common.config.HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS;
import static org.apache.hudi.common.config.HoodieMetadataConfig.ENABLE_METADATA_INDEX_PARTITION_STATS;
import static org.apache.hudi.common.config.HoodieMetadataConfig.FUNCTIONAL_INDEX_ENABLE_PROP;
import static org.apache.hudi.common.config.HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP;
import static org.apache.hudi.common.util.ConfigUtils.getBooleanWithAltKeys;
import static org.apache.hudi.common.util.TypeUtils.unsafeCast;
import static org.apache.hudi.common.util.ValidationUtils.checkArgument;
import static org.apache.hudi.common.util.ValidationUtils.checkState;
import static org.apache.hudi.metadata.HoodieMetadataPayload.BLOOM_FILTER_FIELD_BLOOM_FILTER;
import static org.apache.hudi.metadata.HoodieMetadataPayload.BLOOM_FILTER_FIELD_IS_DELETED;
import static org.apache.hudi.metadata.HoodieMetadataPayload.BLOOM_FILTER_FIELD_TIMESTAMP;
import static org.apache.hudi.metadata.HoodieMetadataPayload.BLOOM_FILTER_FIELD_TYPE;
import static org.apache.hudi.metadata.HoodieMetadataPayload.COLUMN_STATS_FIELD_COLUMN_NAME;
import static org.apache.hudi.metadata.HoodieMetadataPayload.COLUMN_STATS_FIELD_FILE_NAME;
import static org.apache.hudi.metadata.HoodieMetadataPayload.COLUMN_STATS_FIELD_IS_DELETED;
import static org.apache.hudi.metadata.HoodieMetadataPayload.COLUMN_STATS_FIELD_MAX_VALUE;
import static org.apache.hudi.metadata.HoodieMetadataPayload.COLUMN_STATS_FIELD_MIN_VALUE;
import static org.apache.hudi.metadata.HoodieMetadataPayload.COLUMN_STATS_FIELD_NULL_COUNT;
import static org.apache.hudi.metadata.HoodieMetadataPayload.COLUMN_STATS_FIELD_TOTAL_SIZE;
import static org.apache.hudi.metadata.HoodieMetadataPayload.COLUMN_STATS_FIELD_TOTAL_UNCOMPRESSED_SIZE;
import static org.apache.hudi.metadata.HoodieMetadataPayload.COLUMN_STATS_FIELD_VALUE_COUNT;
import static org.apache.hudi.metadata.HoodieMetadataPayload.METADATA_COLUMN_STATS_BUILDER_STUB;
import static org.apache.hudi.metadata.HoodieMetadataPayload.RECORD_INDEX_FIELD_FILEID;
import static org.apache.hudi.metadata.HoodieMetadataPayload.RECORD_INDEX_FIELD_FILEID_ENCODING;
import static org.apache.hudi.metadata.HoodieMetadataPayload.RECORD_INDEX_FIELD_FILEID_HIGH_BITS;
import static org.apache.hudi.metadata.HoodieMetadataPayload.RECORD_INDEX_FIELD_FILEID_LOW_BITS;
import static org.apache.hudi.metadata.HoodieMetadataPayload.RECORD_INDEX_FIELD_FILE_INDEX;
import static org.apache.hudi.metadata.HoodieMetadataPayload.RECORD_INDEX_FIELD_INSTANT_TIME;
import static org.apache.hudi.metadata.HoodieMetadataPayload.RECORD_INDEX_FIELD_PARTITION;
import static org.apache.hudi.metadata.HoodieMetadataPayload.RECORD_INDEX_FIELD_POSITION;
import static org.apache.hudi.metadata.HoodieMetadataPayload.SCHEMA_FIELD_ID_BLOOM_FILTER;
import static org.apache.hudi.metadata.HoodieMetadataPayload.SCHEMA_FIELD_ID_COLUMN_STATS;
import static org.apache.hudi.metadata.HoodieMetadataPayload.SCHEMA_FIELD_ID_RECORD_INDEX;
import static org.apache.hudi.metadata.HoodieMetadataPayload.SCHEMA_FIELD_ID_SECONDARY_INDEX;
import static org.apache.hudi.metadata.HoodieMetadataPayload.SCHEMA_FIELD_NAME_METADATA;
import static org.apache.hudi.metadata.HoodieMetadataPayload.SECONDARY_INDEX_FIELD_IS_DELETED;
import static org.apache.hudi.metadata.HoodieMetadataPayload.SECONDARY_INDEX_FIELD_RECORD_KEY;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.combineFileSystemMetadata;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.mergeColumnStatsRecords;

/**
 * Partition types for metadata table.
 */
public enum MetadataPartitionType {
  FILES(HoodieTableMetadataUtil.PARTITION_NAME_FILES, "files-", 2) {
    @Override
    public boolean isMetadataPartitionEnabled(TypedProperties writeConfig) {
      return getBooleanWithAltKeys(writeConfig, ENABLE);
    }

    @Override
    public void constructMetadataPayload(HoodieMetadataPayload payload, GenericRecord record) {
      constructFilesMetadataPayload(payload, record);
    }

    @Override
    public HoodieMetadataPayload combineMetadataPayloads(HoodieMetadataPayload older, HoodieMetadataPayload newer) {
      return new HoodieMetadataPayload(newer.key, newer.type, combineFileSystemMetadata(older, newer));
    }
  },
  COLUMN_STATS(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS, "col-stats-", 3) {
    @Override
    public boolean isMetadataPartitionEnabled(TypedProperties writeConfig) {
      return getBooleanWithAltKeys(writeConfig, ENABLE_METADATA_INDEX_COLUMN_STATS);
    }

    @Override
    public void constructMetadataPayload(HoodieMetadataPayload payload, GenericRecord record) {
      constructColumnStatsMetadataPayload(payload, record);
    }

    @Override
    public HoodieMetadataPayload combineMetadataPayloads(HoodieMetadataPayload older, HoodieMetadataPayload newer) {
      checkArgument(older.getColumnStatMetadata().isPresent());
      checkArgument(newer.getColumnStatMetadata().isPresent());

      HoodieMetadataColumnStats previousColStatsRecord = older.getColumnStatMetadata().get();
      HoodieMetadataColumnStats newColumnStatsRecord = newer.getColumnStatMetadata().get();

      return new HoodieMetadataPayload(newer.key, mergeColumnStatsRecords(previousColStatsRecord, newColumnStatsRecord), getRecordType());
    }
  },
  BLOOM_FILTERS(HoodieTableMetadataUtil.PARTITION_NAME_BLOOM_FILTERS, "bloom-filters-", 4) {
    @Override
    public boolean isMetadataPartitionEnabled(TypedProperties writeConfig) {
      return getBooleanWithAltKeys(writeConfig, ENABLE_METADATA_INDEX_BLOOM_FILTER);
    }

    @Override
    public void constructMetadataPayload(HoodieMetadataPayload payload, GenericRecord record) {
      GenericRecord bloomFilterRecord = getNestedFieldValue(record, SCHEMA_FIELD_ID_BLOOM_FILTER);
      // NOTE: Only legitimate reason for {@code BloomFilterMetadata} to not be present is when
      //       it's not been read from the storage (ie it's not been a part of projected schema).
      //       Otherwise, it has to be present or the record would be considered invalid
      if (bloomFilterRecord == null) {
        checkArgument(record.getSchema().getField(SCHEMA_FIELD_ID_BLOOM_FILTER) == null,
            String.format("Valid %s record expected for type: %s", SCHEMA_FIELD_ID_BLOOM_FILTER, MetadataPartitionType.BLOOM_FILTERS.getRecordType()));
      } else {
        payload.bloomFilterMetadata = new HoodieMetadataBloomFilter(
            (String) bloomFilterRecord.get(BLOOM_FILTER_FIELD_TYPE),
            (String) bloomFilterRecord.get(BLOOM_FILTER_FIELD_TIMESTAMP),
            (ByteBuffer) bloomFilterRecord.get(BLOOM_FILTER_FIELD_BLOOM_FILTER),
            (Boolean) bloomFilterRecord.get(BLOOM_FILTER_FIELD_IS_DELETED)
        );
      }
    }

    @Override
    public HoodieMetadataPayload combineMetadataPayloads(HoodieMetadataPayload older, HoodieMetadataPayload newer) {
      // Bloom filters are always additive. No need to merge with previous bloom filter
      return new HoodieMetadataPayload(newer.key, newer.bloomFilterMetadata);
    }
  },
  RECORD_INDEX(HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX, "record-index-", 5) {
    @Override
    public boolean isMetadataPartitionEnabled(TypedProperties writeConfig) {
      return getBooleanWithAltKeys(writeConfig, RECORD_INDEX_ENABLE_PROP);
    }

    @Override
    public void constructMetadataPayload(HoodieMetadataPayload payload, GenericRecord record) {
      GenericRecord recordIndexRecord = getNestedFieldValue(record, SCHEMA_FIELD_ID_RECORD_INDEX);
      Object recordIndexPosition = recordIndexRecord.get(RECORD_INDEX_FIELD_POSITION);
      payload.recordIndexMetadata = new HoodieRecordIndexInfo(recordIndexRecord.get(RECORD_INDEX_FIELD_PARTITION).toString(),
          Long.parseLong(recordIndexRecord.get(RECORD_INDEX_FIELD_FILEID_HIGH_BITS).toString()),
          Long.parseLong(recordIndexRecord.get(RECORD_INDEX_FIELD_FILEID_LOW_BITS).toString()),
          Integer.parseInt(recordIndexRecord.get(RECORD_INDEX_FIELD_FILE_INDEX).toString()),
          recordIndexRecord.get(RECORD_INDEX_FIELD_FILEID).toString(),
          Long.parseLong(recordIndexRecord.get(RECORD_INDEX_FIELD_INSTANT_TIME).toString()),
          Integer.parseInt(recordIndexRecord.get(RECORD_INDEX_FIELD_FILEID_ENCODING).toString()),
          recordIndexPosition != null ? Long.parseLong(recordIndexPosition.toString()) : null);
    }
  },
  FUNCTIONAL_INDEX(HoodieTableMetadataUtil.PARTITION_NAME_FUNCTIONAL_INDEX_PREFIX, "func-index-", -1) {
    @Override
    public boolean isMetadataPartitionEnabled(TypedProperties writeConfig) {
      return getBooleanWithAltKeys(writeConfig, FUNCTIONAL_INDEX_ENABLE_PROP);
    }

    @Override
    public boolean isMetadataPartitionAvailable(HoodieTableMetaClient metaClient) {
      if (metaClient.getIndexMetadata().isPresent()) {
        return metaClient.getIndexMetadata().get().getIndexDefinitions().values().stream()
            .anyMatch(indexDef -> indexDef.getIndexName().startsWith(HoodieTableMetadataUtil.PARTITION_NAME_FUNCTIONAL_INDEX_PREFIX));
      }
      return false;
    }

    @Override
    public String getPartitionPath(HoodieTableMetaClient metaClient, String indexName) {
      checkArgument(metaClient.getIndexMetadata().isPresent(), "Index definition is not present for index: " + indexName);
      return metaClient.getIndexMetadata().get().getIndexDefinitions().get(indexName).getIndexName();
    }
  },
  SECONDARY_INDEX(HoodieTableMetadataUtil.PARTITION_NAME_SECONDARY_INDEX_PREFIX, "secondary-index-", 7) {
    @Override
    public boolean isMetadataPartitionEnabled(TypedProperties writeConfig) {
      // Secondary index is created via sql and not via write path.
      // HUDI-7662 tracks adding a separate config to enable/disable secondary index.
      return false;
    }

    @Override
    public boolean isMetadataPartitionAvailable(HoodieTableMetaClient metaClient) {
      if (metaClient.getIndexMetadata().isPresent()) {
        return metaClient.getIndexMetadata().get().getIndexDefinitions().values().stream()
            .anyMatch(indexDef -> indexDef.getIndexName().startsWith(HoodieTableMetadataUtil.PARTITION_NAME_SECONDARY_INDEX_PREFIX));
      }
      return false;
    }

    @Override
    public void constructMetadataPayload(HoodieMetadataPayload payload, GenericRecord record) {
      GenericRecord secondaryIndexRecord = getNestedFieldValue(record, SCHEMA_FIELD_ID_SECONDARY_INDEX);
      checkState(secondaryIndexRecord != null, "Valid SecondaryIndexMetadata record expected for type: " + MetadataPartitionType.SECONDARY_INDEX.getRecordType());
      payload.secondaryIndexMetadata = new HoodieSecondaryIndexInfo(
          secondaryIndexRecord.get(SECONDARY_INDEX_FIELD_RECORD_KEY).toString(),
          (Boolean) secondaryIndexRecord.get(SECONDARY_INDEX_FIELD_IS_DELETED));
    }

    @Override
    public String getPartitionPath(HoodieTableMetaClient metaClient, String indexName) {
      checkArgument(metaClient.getIndexMetadata().isPresent(), "Index definition is not present for index: " + indexName);
      return metaClient.getIndexMetadata().get().getIndexDefinitions().get(indexName).getIndexName();
    }
  },
  PARTITION_STATS(HoodieTableMetadataUtil.PARTITION_NAME_PARTITION_STATS, "partition-stats-", 6) {
    @Override
    public boolean isMetadataPartitionEnabled(TypedProperties writeConfig) {
      return getBooleanWithAltKeys(writeConfig, ENABLE_METADATA_INDEX_PARTITION_STATS);
    }

    @Override
    public void constructMetadataPayload(HoodieMetadataPayload payload, GenericRecord record) {
      constructColumnStatsMetadataPayload(payload, record);
    }

    @Override
    public HoodieMetadataPayload combineMetadataPayloads(HoodieMetadataPayload older, HoodieMetadataPayload newer) {
      checkArgument(older.getColumnStatMetadata().isPresent());
      checkArgument(newer.getColumnStatMetadata().isPresent());

      HoodieMetadataColumnStats previousColStatsRecord = older.getColumnStatMetadata().get();
      HoodieMetadataColumnStats newColumnStatsRecord = newer.getColumnStatMetadata().get();

      return new HoodieMetadataPayload(newer.key, mergeColumnStatsRecords(previousColStatsRecord, newColumnStatsRecord), getRecordType());
    }
  },
  // ALL_PARTITIONS is just another record type in FILES partition
  ALL_PARTITIONS(HoodieTableMetadataUtil.PARTITION_NAME_FILES, "files-", 1) {
    @Override
    public boolean isMetadataPartitionEnabled(TypedProperties writeConfig) {
      return getBooleanWithAltKeys(writeConfig, ENABLE);
    }

    @Override
    public void constructMetadataPayload(HoodieMetadataPayload payload, GenericRecord record) {
      MetadataPartitionType.constructFilesMetadataPayload(payload, record);
    }

    @Override
    public HoodieMetadataPayload combineMetadataPayloads(HoodieMetadataPayload older, HoodieMetadataPayload newer) {
      return new HoodieMetadataPayload(newer.key, newer.type, combineFileSystemMetadata(older, newer));
    }
  };

  private static <T> T getNestedFieldValue(GenericRecord record, String fieldName) {
    // NOTE: This routine is more lightweight than {@code HoodieAvroUtils.getNestedFieldVal}
    if (record.getSchema().getField(fieldName) == null) {
      return null;
    }

    return unsafeCast(record.get(fieldName));
  }

  private static void constructFilesMetadataPayload(HoodieMetadataPayload payload, GenericRecord record) {
    Map<String, HoodieMetadataFileInfo> metadata = getNestedFieldValue(record, SCHEMA_FIELD_NAME_METADATA);
    if (metadata != null) {
      payload.filesystemMetadata = metadata;
      payload.filesystemMetadata.keySet().forEach(k -> {
        GenericRecord v = payload.filesystemMetadata.get(k);
        payload.filesystemMetadata.put(k, new HoodieMetadataFileInfo((Long) v.get("size"), (Boolean) v.get("isDeleted")));
      });
    }
  }

  private static void constructColumnStatsMetadataPayload(HoodieMetadataPayload payload, GenericRecord record) {
    GenericRecord columnStatsRecord = getNestedFieldValue(record, SCHEMA_FIELD_ID_COLUMN_STATS);
    // NOTE: Only legitimate reason for {@code ColumnStatsMetadata} to not be present is when
    //       it's not been read from the storage (ie it's not been a part of projected schema).
    //       Otherwise, it has to be present or the record would be considered invalid
    if (columnStatsRecord == null) {
      checkArgument(record.getSchema().getField(SCHEMA_FIELD_ID_COLUMN_STATS) == null,
          String.format("Valid %s record expected for type: %s", SCHEMA_FIELD_ID_COLUMN_STATS, MetadataPartitionType.COLUMN_STATS.getRecordType()));
    } else {
      payload.columnStatMetadata = HoodieMetadataColumnStats.newBuilder(METADATA_COLUMN_STATS_BUILDER_STUB.get())
          .setFileName((String) columnStatsRecord.get(COLUMN_STATS_FIELD_FILE_NAME))
          .setColumnName((String) columnStatsRecord.get(COLUMN_STATS_FIELD_COLUMN_NAME))
          // AVRO-2377 1.9.2 Modified the type of org.apache.avro.Schema#FIELD_RESERVED to Collections.unmodifiableSet.
          // This causes Kryo to fail when deserializing a GenericRecord, See HUDI-5484.
          // We should avoid using GenericRecord and convert GenericRecord into a serializable type.
          .setMinValue(wrapValueIntoAvro(unwrapAvroValueWrapper(columnStatsRecord.get(COLUMN_STATS_FIELD_MIN_VALUE))))
          .setMaxValue(wrapValueIntoAvro(unwrapAvroValueWrapper(columnStatsRecord.get(COLUMN_STATS_FIELD_MAX_VALUE))))
          .setValueCount((Long) columnStatsRecord.get(COLUMN_STATS_FIELD_VALUE_COUNT))
          .setNullCount((Long) columnStatsRecord.get(COLUMN_STATS_FIELD_NULL_COUNT))
          .setTotalSize((Long) columnStatsRecord.get(COLUMN_STATS_FIELD_TOTAL_SIZE))
          .setTotalUncompressedSize((Long) columnStatsRecord.get(COLUMN_STATS_FIELD_TOTAL_UNCOMPRESSED_SIZE))
          .setIsDeleted((Boolean) columnStatsRecord.get(COLUMN_STATS_FIELD_IS_DELETED))
          .build();
    }
  }

  // Partition path in metadata table.
  private final String partitionPath;
  // FileId prefix used for all file groups in this partition.
  private final String fileIdPrefix;
  private final int recordType;

  /**
   * Check if the metadata partition is enabled based on the metadata config.
   */
  public abstract boolean isMetadataPartitionEnabled(TypedProperties writeConfig);

  /**
   * Check if the metadata partition is available based on the table config.
   */
  public boolean isMetadataPartitionAvailable(HoodieTableMetaClient metaClient) {
    return metaClient.getTableConfig().isMetadataPartitionAvailable(this);
  }

  MetadataPartitionType(final String partitionPath, final String fileIdPrefix, final int recordType) {
    this.partitionPath = partitionPath;
    this.fileIdPrefix = fileIdPrefix;
    this.recordType = recordType;
  }

  /**
   * Get the partition name from the metadata partition type.
   * NOTE: For certain types of metadata partition, such as functional index and secondary index,
   * partition path defined enum is just the prefix to denote the type of metadata partition.
   * The actual partition name is contained in the index definition.
   */
  public String getPartitionPath(HoodieTableMetaClient metaClient, String indexName) {
    return partitionPath;
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  public String getFileIdPrefix() {
    return fileIdPrefix;
  }

  public int getRecordType() {
    return recordType;
  }

  /**
   * Construct metadata payload from the given record.
   */
  public void constructMetadataPayload(HoodieMetadataPayload payload, GenericRecord record) {
    throw new UnsupportedOperationException("MetadataPayload construction not supported for partition type: " + this);
  }

  /**
   * Merge old and new metadata payloads. By default, it returns the newer payload.
   * Implementations can override this method to merge the payloads depending on the partition type.
   */
  public HoodieMetadataPayload combineMetadataPayloads(HoodieMetadataPayload older, HoodieMetadataPayload newer) {
    return newer;
  }

  /**
   * Get the metadata partition type for the given record type.
   */
  public static MetadataPartitionType get(int type) {
    for (MetadataPartitionType partitionType : values()) {
      if (partitionType.getRecordType() == type) {
        return partitionType;
      }
    }
    throw new IllegalArgumentException("No MetadataPartitionType for record type: " + type);
  }

  /**
   * Returns the list of metadata table partitions which require WriteStatus to track written records.
   * <p>
   * These partitions need the list of written records so that they can update their metadata.
   */
  public static List<MetadataPartitionType> getMetadataPartitionsNeedingWriteStatusTracking() {
    return Collections.singletonList(MetadataPartitionType.RECORD_INDEX);
  }

  /**
   * Returns the set of all metadata partition names.
   */
  public static Set<String> getAllPartitionPaths() {
    return Arrays.stream(getValidValues())
        .map(MetadataPartitionType::getPartitionPath)
        .collect(Collectors.toSet());
  }

  /**
   * Returns the set of all valid metadata partition types. Prefer using this method over {@link #values()}.
   */
  public static MetadataPartitionType[] getValidValues() {
    // ALL_PARTITIONS is just another record type in FILES partition
    return EnumSet.complementOf(EnumSet.of(
        ALL_PARTITIONS)).toArray(new MetadataPartitionType[0]);
  }

  /**
   * Returns the list of metadata partition types enabled based on the metadata config and table config.
   */
  public static List<MetadataPartitionType> getEnabledPartitions(TypedProperties writeConfig, HoodieTableMetaClient metaClient) {
    if (!getBooleanWithAltKeys(writeConfig, ENABLE)) {
      return Collections.emptyList();
    }
    return Arrays.stream(getValidValues())
        .filter(partitionType -> partitionType.isMetadataPartitionEnabled(writeConfig) || partitionType.isMetadataPartitionAvailable(metaClient))
        .collect(Collectors.toList());
  }

  public static MetadataPartitionType fromPartitionPath(String partitionPath) {
    for (MetadataPartitionType partitionType : getValidValues()) {
      if (partitionPath.equals(partitionType.getPartitionPath()) || partitionPath.startsWith(partitionType.getPartitionPath())) {
        return partitionType;
      }
    }
    throw new IllegalArgumentException("No MetadataPartitionType for partition path: " + partitionPath);
  }

  @Override
  public String toString() {
    return "Metadata partition {"
        + "name: " + getPartitionPath()
        + ", prefix: " + getFileIdPrefix()
        + "}";
  }
}
