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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.io.storage.row.parquet.ParquetSchemaConverter;
import org.apache.hudi.source.ExpressionPredicates.Predicate;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.inline.InLineFSUtils;
import org.apache.hudi.table.format.cow.ParquetSplitReaderUtil;
import org.apache.hudi.util.RowDataProjection;
import org.apache.hudi.util.RowProjection;
import org.apache.hudi.util.StreamerUtil;

import org.apache.avro.Schema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.filter.UnboundRecordFilter;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.BadConfigurationException;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.ConfigurationUtil;
import org.apache.parquet.hadoop.util.SerializationUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.range;
import static org.apache.parquet.hadoop.ParquetFileReader.readFooter;
import static org.apache.parquet.hadoop.ParquetInputFormat.FILTER_PREDICATE;
import static org.apache.parquet.hadoop.ParquetInputFormat.UNBOUND_RECORD_FILTER;

/**
 * Factory clazz for record iterators.
 */
public abstract class RecordIterators {

  private static final int DEFAULT_BATCH_SIZE = 2048;

  public static ClosableIterator<RowData> getParquetRecordIterator(
      StorageConfiguration<?> conf,
      InternalSchemaManager internalSchemaManager,
      DataType dataType,
      Schema requestedSchema,
      StoragePath path,
      List<Predicate> predicates) throws IOException {
    List<String> fieldNames = ((RowType) dataType.getLogicalType()).getFieldNames();
    List<DataType> fieldTypes = dataType.getChildren();
    int[] selectedFields = requestedSchema.getFields().stream().map(Schema.Field::name)
        .map(fieldNames::indexOf)
        .mapToInt(i -> i)
        .toArray();
    final boolean useUTCTimeStamp = conf.getBoolean(
        FlinkOptions.READ_UTC_TIMEZONE.key(), FlinkOptions.READ_UTC_TIMEZONE.defaultValue());
    LinkedHashMap<String, Object> partitionSpec = getPartitionSpec(conf, path, fieldNames, fieldTypes);
    return RecordIterators.getParquetRecordIterator(
        internalSchemaManager,
        useUTCTimeStamp,
        true,
        conf.unwrapAs(Configuration.class),
        fieldNames.toArray(new String[0]),
        fieldTypes.toArray(new DataType[0]),
        partitionSpec,
        selectedFields,
        DEFAULT_BATCH_SIZE,
        new org.apache.flink.core.fs.Path(path.toUri()),
        0L,
        Long.MAX_VALUE,
        predicates);
  }

  public static ClosableIterator<RowData> getParquetRecordIterator(
      InternalSchemaManager internalSchemaManager,
      boolean utcTimestamp,
      boolean caseSensitive,
      Configuration conf,
      String[] fieldNames,
      DataType[] fieldTypes,
      Map<String, Object> partitionSpec,
      int[] selectedFields,
      int batchSize,
      Path path,
      long splitStart,
      long splitLength,
      List<Predicate> predicates) throws IOException {
    FilterPredicate filterPredicate = getFilterPredicate(conf);
    for (Predicate predicate : predicates) {
      FilterPredicate filter = predicate.filter();
      if (filter != null) {
        filterPredicate = filterPredicate == null ? filter : and(filterPredicate, filter);
      }
    }
    UnboundRecordFilter recordFilter = getUnboundRecordFilterInstance(conf);

    ParquetMetadata footer = readFooter(conf, new org.apache.hadoop.fs.Path(path.toUri()), range(splitStart, splitStart + splitLength));
    // schema on read is disabled
    if (internalSchemaManager.getQuerySchema().isEmptySchema())  {
      RowType fileRowType = ParquetSchemaConverter.convertToRowType(footer.getFileMetaData().getSchema());
      DataType[] mergedFiledTypes = getMergedFieldTypes(fileRowType, fieldTypes, fieldNames);
      RowProjection recordTransform = createRecordTransform(mergedFiledTypes, fieldTypes, fieldNames, selectedFields);
      ClosableIterator<RowData> itr = new ParquetSplitRecordIterator(
          ParquetSplitReaderUtil.genPartColumnarRowReader(
              utcTimestamp,
              caseSensitive,
              conf,
              fieldNames,
              mergedFiledTypes,
              partitionSpec,
              selectedFields,
              batchSize,
              path,
              footer,
              filterPredicate,
              recordFilter));
      return recordTransform == RowProjection.IDENTITY ? itr : new SchemaEvolvedRecordIterator(itr, recordTransform);
    } else  {
      InternalSchema mergeSchema = internalSchemaManager.getMergeSchema(getFileName(path));
      Option<RowDataProjection> castProjection =  Option.empty();
      if (!mergeSchema.isEmptySchema()) {
        CastMap castMap = internalSchemaManager.getCastMap(mergeSchema, fieldNames, fieldTypes, selectedFields);
        castProjection = castMap.toRowDataProjection(selectedFields);
        // the reconciled field names
        fieldNames = internalSchemaManager.getMergeFieldNames(mergeSchema, fieldNames);
        // the reconciled field types
        fieldTypes = castMap.getFileFieldTypes();
      }
      ClosableIterator<RowData> itr = new ParquetSplitRecordIterator(
          ParquetSplitReaderUtil.genPartColumnarRowReader(
              utcTimestamp,
              caseSensitive,
              conf,
              fieldNames,
              fieldTypes,
              partitionSpec,
              selectedFields,
              batchSize,
              path,
              footer,
              filterPredicate,
              recordFilter));
      return castProjection.isPresent() ? new SchemaEvolvedRecordIterator(itr, castProjection.get()) : itr;
    }
  }

  /**
   * Create a record transform to support read file with history schema which is different with current table schema.
   */
  private static RowProjection createRecordTransform(DataType[] fileFieldTypes, DataType[] tableFieldTypes, String[] fieldNames, int[] selectedFields) {
    String[] selectFieldNames = Arrays.stream(selectedFields).mapToObj(idx -> fieldNames[idx]).toArray(String[]::new);
    LogicalType[] selectFileFieldTypes = Arrays.stream(selectedFields).mapToObj(idx -> fileFieldTypes[idx].getLogicalType()).toArray(LogicalType[]::new);
    LogicalType[] selectFieldTypes = Arrays.stream(selectedFields).mapToObj(idx -> tableFieldTypes[idx].getLogicalType()).toArray(LogicalType[]::new);
    return StreamerUtil.createRecordTransform(RowType.of(selectFileFieldTypes, selectFieldNames), RowType.of(selectFieldTypes, selectFieldNames));
  }

  /**
   * Attempts to merge the file and query data types to produce merged data types, prioritising the use of file row types.
   */
  private static DataType[] getMergedFieldTypes(RowType fileRowType, DataType[] fieldTypes, String[] fieldNames) {
    DataType[] mergedTypes = new DataType[fieldTypes.length];
    for (int i = 0; i < fieldTypes.length; i++) {
      int idx = fileRowType.getFieldIndex(fieldNames[i]);
      if (idx > -1) {
        mergedTypes[i] = DataTypes.of(fileRowType.getTypeAt(idx));
      } else {
        mergedTypes[i] = fieldTypes[i];
      }
    }
    return mergedTypes;
  }

  private static String getFileName(Path path) {
    if (InLineFSUtils.SCHEME.equals(path.toUri().getScheme())) {
      return InLineFSUtils.getOuterFilePathFromInlinePath(new StoragePath(path.toUri())).getName();
    }
    return path.getName();
  }

  private static FilterPredicate getFilterPredicate(Configuration configuration) {
    try {
      return SerializationUtil.readObjectFromConfAsBase64(FILTER_PREDICATE, configuration);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static UnboundRecordFilter getUnboundRecordFilterInstance(Configuration configuration) {
    Class<?> clazz = ConfigurationUtil.getClassFromConfig(configuration, UNBOUND_RECORD_FILTER, UnboundRecordFilter.class);
    if (clazz == null) {
      return null;
    }

    try {
      UnboundRecordFilter unboundRecordFilter = (UnboundRecordFilter) clazz.newInstance();

      if (unboundRecordFilter instanceof Configurable) {
        ((Configurable) unboundRecordFilter).setConf(configuration);
      }

      return unboundRecordFilter;
    } catch (InstantiationException | IllegalAccessException e) {
      throw new BadConfigurationException(
          "could not instantiate unbound record filter class", e);
    }
  }

  /**
   * Get partition values from the file path.
   *
   * @param conf       storage configuration
   * @param path       file path
   * @param fieldNames full field names
   * @param fieldTypes full field types
   *
   * @return partition specification values.
   */
  private static LinkedHashMap<String, Object> getPartitionSpec(
      StorageConfiguration<?> conf,
      StoragePath path,
      List<String> fieldNames,
      List<DataType> fieldTypes) {
    if (InLineFSUtils.SCHEME.equals(path.toUri().getScheme())) {
      path = InLineFSUtils.getOuterFilePathFromInlinePath(path);
    }
    return FilePathUtils.generatePartitionSpecs(
        path.toString(),
        fieldNames,
        fieldTypes,
        conf.getString(FlinkOptions.PARTITION_DEFAULT_NAME.key(), FlinkOptions.PARTITION_DEFAULT_NAME.defaultValue()),
        conf.getString(FlinkOptions.PARTITION_PATH_FIELD.key(), FlinkOptions.PARTITION_PATH_FIELD.defaultValue()),
        conf.getBoolean(FlinkOptions.HIVE_STYLE_PARTITIONING.key(), FlinkOptions.HIVE_STYLE_PARTITIONING.defaultValue()));
  }
}
