/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table.format;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.serialization.DefaultSerializer;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.read.HoodieRecordReader;
import org.apache.hudi.common.table.read.lsm.HoodieLsmFileGroupReader;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.source.ExpressionPredicates;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.util.FlinkClientUtil;
import org.apache.hudi.util.HoodieSchemaConverter;
import org.apache.hudi.util.StreamerUtil;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Utilities for format.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class FormatUtils {

  public static GenericRecord buildAvroRecordBySchema(
      IndexedRecord record,
      HoodieSchema requiredSchema,
      int[] requiredPos,
      GenericRecordBuilder recordBuilder) {
    List<HoodieSchemaField> requiredFields = requiredSchema.getFields();
    assert (requiredFields.size() == requiredPos.length);
    Iterator<Integer> positionIterator = Arrays.stream(requiredPos).iterator();
    requiredFields.forEach(f -> recordBuilder.set(f.getAvroField(), getVal(record, positionIterator.next())));
    return recordBuilder.build();
  }

  private static Object getVal(IndexedRecord record, int pos) {
    return pos == -1 ? null : record.get(pos);
  }

  public static ClosableIterator<RowData> getLanceRecordIterator(
      String path,
      List<String> fieldNames,
      List<DataType> fieldTypes,
      int[] selectedFields,
      Configuration hadoopConf) {
    DataType selectedDataType = DataTypes.ROW(Arrays.stream(selectedFields)
            .mapToObj(i -> DataTypes.FIELD(fieldNames.get(i), fieldTypes.get(i)))
            .toArray(DataTypes.Field[]::new))
        .bridgedTo(RowData.class);
    HoodieSchema requestedSchema = HoodieSchemaConverter.convertToSchema(selectedDataType.getLogicalType());
    HoodieRowDataLanceReader reader = new HoodieRowDataLanceReader(
        new StoragePath(path), StreamerUtil.getLanceReadConfig(hadoopConf));
    try {
      return reader.getRowDataIterator(selectedDataType, requestedSchema);
    } catch (RuntimeException e) {
      reader.close();
      throw new HoodieException("Failed to get iterator from Lance reader: " + path, e);
    }
  }

  public static ExternalSpillableMap<String, byte[]> spillableMap(
      HoodieWriteConfig writeConfig,
      long maxCompactionMemoryInBytes,
      String loggingContext) {
    try {
      return new ExternalSpillableMap<>(
          maxCompactionMemoryInBytes,
          writeConfig.getSpillableMapBasePath(),
          new DefaultSizeEstimator<>(),
          new DefaultSizeEstimator<>(),
          writeConfig.getCommonConfig().getSpillableDiskMapType(),
          new DefaultSerializer<>(),
          writeConfig.getCommonConfig().isBitCaskDiskMapCompressionEnabled(),
          loggingContext);
    } catch (IOException e) {
      throw new HoodieIOException(
          "IOException when creating ExternalSpillableMap at " + writeConfig.getSpillableMapBasePath(), e);
    }
  }

  /**
   * Creates the record reader matching the physical layout of the file slice.
   *
   * <p>Pure native-log file slices in an LSM-tree table use the sorted LSM reader. Mixed file
   * slices containing legacy inline logs continue to use the general file-group reader so table
   * upgrades remain readable.</p>
   */
  public static HoodieRecordReader<RowData> createRecordReader(
      HoodieTableMetaClient metaClient,
      HoodieWriteConfig writeConfig,
      InternalSchemaManager internalSchemaManager,
      FileSlice fileSlice,
      HoodieSchema tableSchema,
      HoodieSchema requiredSchema,
      String latestInstant,
      String mergeType,
      boolean emitDelete,
      List<ExpressionPredicates.Predicate> predicates,
      Option<InstantRange> instantRangeOption) {
    if (!shouldUseLsmReader(metaClient, fileSlice, mergeType)) {
      return createFileGroupReader(metaClient, writeConfig, internalSchemaManager, fileSlice,
          tableSchema, requiredSchema, latestInstant, mergeType, emitDelete, predicates, instantRangeOption);
    }

    final FlinkRowDataReaderContext readerContext =
        new FlinkRowDataReaderContext(
            metaClient.getStorageConf(),
            () -> internalSchemaManager,
            predicates,
            metaClient.getTableConfig(),
            instantRangeOption);

    final TypedProperties typedProps = FlinkClientUtil.getReadProps(metaClient.getTableConfig(), writeConfig);
    typedProps.put(HoodieReaderConfig.MERGE_TYPE.key(), mergeType);

    return HoodieLsmFileGroupReader.<RowData>builder()
        .withReaderContext(readerContext)
        .withHoodieTableMetaClient(metaClient)
        .withLatestCommitTime(latestInstant)
        .withBaseFileOption(fileSlice.getBaseFile())
        .withLogFiles(fileSlice.getLogFiles())
        .withPartitionPath(fileSlice.getPartitionPath())
        .withDataSchema(tableSchema)
        .withRequestedSchema(requiredSchema)
        .withInternalSchemaOpt(Option.ofNullable(internalSchemaManager.getQuerySchema()))
        .withProps(typedProps)
        .withEmitDelete(emitDelete)
        .build();
  }

  static boolean shouldUseLsmReader(HoodieTableMetaClient metaClient, FileSlice fileSlice) {
    return metaClient.getTableConfig().isLSMTreeStorageLayout()
        && fileSlice.getLogFiles().allMatch(logFile -> FSUtils.isNativeLogFile(logFile.getFileName()));
  }

  static boolean shouldUseLsmReader(HoodieTableMetaClient metaClient, FileSlice fileSlice, String mergeType) {
    // The LSM reader collapses all sorted versions of a key. Skip-merge queries intentionally
    // expose those versions independently, so retain the classic unmerged reader for that mode.
    return !HoodieReaderConfig.REALTIME_SKIP_MERGE.equalsIgnoreCase(mergeType)
        && shouldUseLsmReader(metaClient, fileSlice);
  }

  /**
   * Create a {@link HoodieFileGroupReader}.
   *
   * @param metaClient            Hoodie metadata client
   * @param writeConfig           Hoodie write configuration
   * @param internalSchemaManager Internal schema manager
   * @param fileSlice             The file slice
   * @param tableSchema           The schema of table
   * @param requiredSchema        The required query schema
   * @param latestInstant         The latest instant
   * @param mergeType             The type of merging mode
   * @param emitDelete            Flag to emit DELETE record
   * @param predicates            The expression predicates
   * @param instantRangeOption    The instant range used to filter files
   *
   * @return A {@link HoodieFileGroupReader}.
   */
  public static HoodieFileGroupReader<RowData> createFileGroupReader(
      HoodieTableMetaClient metaClient,
      HoodieWriteConfig writeConfig,
      InternalSchemaManager internalSchemaManager,
      FileSlice fileSlice,
      HoodieSchema tableSchema,
      HoodieSchema requiredSchema,
      String latestInstant,
      String mergeType,
      boolean emitDelete,
      List<ExpressionPredicates.Predicate> predicates,
      Option<InstantRange> instantRangeOption) {

    final FlinkRowDataReaderContext readerContext =
        new FlinkRowDataReaderContext(
            metaClient.getStorageConf(),
            () -> internalSchemaManager,
            predicates,
            metaClient.getTableConfig(),
            instantRangeOption);

    final TypedProperties typedProps = FlinkClientUtil.getReadProps(metaClient.getTableConfig(), writeConfig);
    typedProps.put(HoodieReaderConfig.MERGE_TYPE.key(), mergeType);

    return HoodieFileGroupReader.<RowData>builder()
        .withReaderContext(readerContext)
        .withHoodieTableMetaClient(metaClient)
        .withLatestCommitTime(latestInstant)
        .withBaseFileOption(fileSlice.getBaseFile())
        .withLogFiles(fileSlice.getLogFiles())
        .withPartitionPath(fileSlice.getPartitionPath())
        .withDataSchema(tableSchema)
        .withRequestedSchema(requiredSchema)
        .withInternalSchemaOpt(Option.ofNullable(internalSchemaManager.getQuerySchema()))
        .withProps(typedProps)
        .withShouldUseRecordPosition(false)
        .withEmitDelete(emitDelete)
        .build();
  }

  /**
   * Gets the raw value for a {@link ConfigProperty} config from Flink configuration. The key and
   * alternative keys are used to fetch the config.
   *
   * @param flinkConf      Configs in Flink {@link org.apache.flink.configuration.Configuration}.
   * @param configProperty {@link ConfigProperty} config to fetch.
   * @return {@link Option} of value if the config exists; empty {@link Option} otherwise.
   */
  public static Option<String> getRawValueWithAltKeys(org.apache.flink.configuration.Configuration flinkConf,
                                                      ConfigProperty<?> configProperty) {
    if (flinkConf.containsKey(configProperty.key())) {
      return Option.ofNullable(flinkConf.getString(configProperty.key(), ""));
    }
    for (String alternative : configProperty.getAlternatives()) {
      if (flinkConf.containsKey(alternative)) {
        return Option.ofNullable(flinkConf.getString(alternative, ""));
      }
    }
    return Option.empty();
  }

  /**
   * Gets the boolean value for a {@link ConfigProperty} config from Flink configuration. The key and
   * alternative keys are used to fetch the config. The default value of {@link ConfigProperty}
   * config, if exists, is returned if the config is not found in the configuration.
   *
   * @param conf           Configs in Flink {@link Configuration}.
   * @param configProperty {@link ConfigProperty} config to fetch.
   * @return boolean value if the config exists; default boolean value if the config does not exist
   * and there is default value defined in the {@link ConfigProperty} config; {@code false} otherwise.
   */
  public static boolean getBooleanWithAltKeys(org.apache.flink.configuration.Configuration conf,
                                              ConfigProperty<?> configProperty) {
    Option<String> rawValue = getRawValueWithAltKeys(conf, configProperty);
    boolean defaultValue = configProperty.hasDefaultValue() && Boolean.parseBoolean(configProperty.defaultValue().toString());
    return rawValue.map(Boolean::parseBoolean).orElse(defaultValue);
  }
}
