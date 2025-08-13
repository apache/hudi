/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.hadoop;

import org.apache.hudi.avro.AvroSchemaUtils;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieAvroSchemaException;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeException;
import org.apache.hadoop.hive.serde2.avro.HiveTypeUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.parquet.avro.AvroSchemaConverter;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.config.HoodieReaderConfig.RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY;

/**
 * {@link HoodieReaderContext} for Hive-specific {@link HoodieFileGroupReaderBasedRecordReader}.
 */
public class HiveHoodieReaderContext extends HoodieReaderContext<ArrayWritable> {
  protected final HoodieFileGroupReaderBasedRecordReader.HiveReaderCreator readerCreator;
  private RecordReader<NullWritable, ArrayWritable> firstRecordReader = null;

  private final List<String> partitionCols;
  private final Set<String> partitionColSet;

  protected HiveHoodieReaderContext(HoodieFileGroupReaderBasedRecordReader.HiveReaderCreator readerCreator,
                                    List<String> partitionCols,
                                    StorageConfiguration<?> storageConfiguration,
                                    HoodieTableConfig tableConfig) {
    super(storageConfiguration, tableConfig, Option.empty(), Option.empty(), new HiveRecordContext(tableConfig));
    if (storageConfiguration.getString(AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS).isEmpty()) {
      // Overriding default treatment of repeated groups in Parquet
      storageConfiguration.set(AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS, "false");
    }
    this.readerCreator = readerCreator;
    this.partitionCols = partitionCols;
    this.partitionColSet = new HashSet<>(this.partitionCols);
  }

  private void setSchemas(JobConf jobConf, Schema dataSchema, Schema requiredSchema) {
    List<String> dataColumnNameList = dataSchema.getFields().stream().map(f -> f.name().toLowerCase(Locale.ROOT)).collect(Collectors.toList());
    jobConf.set(serdeConstants.LIST_COLUMNS, String.join(",", dataColumnNameList));
    List<TypeInfo> columnTypes;
    try {
      columnTypes = HiveTypeUtils.generateColumnTypes(dataSchema);
    } catch (AvroSerdeException e) {
      throw new HoodieAvroSchemaException(String.format("Failed to generate hive column types from avro schema: %s, due to %s", dataSchema, e));
    }
    jobConf.set(serdeConstants.LIST_COLUMN_TYPES, columnTypes.stream().map(TypeInfo::getTypeName).collect(Collectors.joining(",")));
    // don't replace `f -> f.name()` with lambda reference
    String readColNames = requiredSchema.getFields().stream().map(f -> f.name()).collect(Collectors.joining(","));
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, readColNames);
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, requiredSchema.getFields()
        .stream().map(f -> String.valueOf(dataSchema.getField(f.name()).pos())).collect(Collectors.joining(",")));
  }

  @Override
  public ClosableIterator<ArrayWritable> getFileRecordIterator(StoragePath filePath, long start, long length, Schema dataSchema,
                                                               Schema requiredSchema, HoodieStorage storage) throws IOException {
    return getFileRecordIterator(filePath, null, start, length, dataSchema, requiredSchema, storage);
  }

  @Override
  public ClosableIterator<ArrayWritable> getFileRecordIterator(
      StoragePathInfo storagePathInfo, long start, long length, Schema dataSchema, Schema requiredSchema, HoodieStorage storage) throws IOException {
    return getFileRecordIterator(storagePathInfo.getPath(), storagePathInfo.getLocations(), start, length, dataSchema, requiredSchema, storage);
  }

  private ClosableIterator<ArrayWritable> getFileRecordIterator(StoragePath filePath, String[] hosts, long start, long length, Schema dataSchema,
                                                                Schema requiredSchema, HoodieStorage storage) throws IOException {
    // mdt file schema irregular and does not work with this logic. Also, log file evolution is handled inside the log block
    boolean isParquetOrOrc = filePath.getFileExtension().equals(HoodieFileFormat.PARQUET.getFileExtension())
        || filePath.getFileExtension().equals(HoodieFileFormat.ORC.getFileExtension());
    Schema avroFileSchema = isParquetOrOrc ? HoodieIOFactory.getIOFactory(storage)
        .getFileFormatUtils(filePath).readAvroSchema(storage, filePath) : dataSchema;
    Schema actualRequiredSchema = isParquetOrOrc ? AvroSchemaUtils.pruneDataSchema(avroFileSchema, requiredSchema, Collections.emptySet()) : requiredSchema;
    JobConf jobConfCopy = new JobConf(storage.getConf().unwrapAs(Configuration.class));
    if (getNeedsBootstrapMerge()) {
      // Hive PPD works at row-group level and only enabled when hive.optimize.index.filter=true;
      // The above config is disabled by default. But when enabled, would cause misalignment between
      // skeleton and bootstrap file. We will disable them specifically when query needs bootstrap and skeleton
      // file to be stitched.
      // This disables row-group filtering
      jobConfCopy.unset(TableScanDesc.FILTER_EXPR_CONF_STR);
      jobConfCopy.unset(ConvertAstToSearchArg.SARG_PUSHDOWN);
    }
    //move the partition cols to the end, because in some cases it has issues if we don't do that
    Schema modifiedDataSchema = HoodieAvroUtils.generateProjectionSchema(avroFileSchema, Stream.concat(avroFileSchema.getFields().stream()
            .map(f -> f.name().toLowerCase(Locale.ROOT)).filter(n -> !partitionColSet.contains(n)),
        partitionCols.stream().filter(c -> avroFileSchema.getField(c) != null)).collect(Collectors.toList()));
    setSchemas(jobConfCopy, modifiedDataSchema, actualRequiredSchema);
    InputSplit inputSplit = new FileSplit(new Path(filePath.toString()), start, length, hosts);
    RecordReader<NullWritable, ArrayWritable> recordReader = readerCreator.getRecordReader(inputSplit, jobConfCopy);
    if (firstRecordReader == null) {
      firstRecordReader = recordReader;
    }
    ClosableIterator<ArrayWritable> recordIterator = new RecordReaderValueIterator<>(recordReader);
    if (AvroSchemaUtils.areSchemasProjectionEquivalent(modifiedDataSchema, requiredSchema)) {
      return recordIterator;
    }
    // record reader puts the required columns in the positions of the data schema and nulls the rest of the columns
    return new CloseableMappingIterator<>(recordIterator, recordContext.projectRecord(modifiedDataSchema, requiredSchema));
  }

  @Override
  public Option<HoodieRecordMerger> getRecordMerger(RecordMergeMode mergeMode, String mergeStrategyId, String mergeImplClasses) {
    // TODO(HUDI-7843):
    // get rid of event time and commit time ordering. Just return Option.empty
    switch (mergeMode) {
      case EVENT_TIME_ORDERING:
        return Option.of(new DefaultHiveRecordMerger());
      case COMMIT_TIME_ORDERING:
        return Option.of(new OverwriteWithLatestHiveRecordMerger());
      case CUSTOM:
      default:
        Option<HoodieRecordMerger> recordMerger = HoodieRecordUtils.createValidRecordMerger(EngineType.JAVA, mergeImplClasses, mergeStrategyId);
        if (recordMerger.isEmpty()) {
          throw new IllegalArgumentException("No valid hive merger implementation set for `"
              + RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY + "`");
        }
        return recordMerger;
    }
  }

  @Override
  public ClosableIterator<ArrayWritable> mergeBootstrapReaders(ClosableIterator<ArrayWritable> skeletonFileIterator,
                                                               Schema skeletonRequiredSchema,
                                                               ClosableIterator<ArrayWritable> dataFileIterator,
                                                               Schema dataRequiredSchema,
                                                               List<Pair<String, Object>> partitionFieldsAndValues) {
    int skeletonLen = skeletonRequiredSchema.getFields().size();
    int dataLen = dataRequiredSchema.getFields().size();
    int[] partitionFieldPositions = partitionFieldsAndValues.stream()
        .map(pair -> dataRequiredSchema.getField(pair.getKey()).pos()).mapToInt(Integer::intValue).toArray();
    Writable[] convertedPartitionValues = partitionFieldsAndValues.stream().map(Pair::getValue).toArray(Writable[]::new);
    return new ClosableIterator<ArrayWritable>() {

      private final ArrayWritable returnWritable = new ArrayWritable(Writable.class);

      @Override
      public boolean hasNext() {
        if (dataFileIterator.hasNext() != skeletonFileIterator.hasNext()) {
          throw new IllegalStateException("bootstrap data file iterator and skeleton file iterator are out of sync");
        }
        return dataFileIterator.hasNext();
      }

      @Override
      public ArrayWritable next() {
        Writable[] skeletonWritable = skeletonFileIterator.next().get();
        Writable[] dataWritable = dataFileIterator.next().get();
        for (int i = 0; i < partitionFieldPositions.length; i++) {
          if (dataWritable[partitionFieldPositions[i]] == null || dataWritable[partitionFieldPositions[i]] instanceof NullWritable) {
            dataWritable[partitionFieldPositions[i]] = convertedPartitionValues[i];
          }
        }
        Writable[] mergedWritable = new Writable[skeletonLen + dataLen];
        System.arraycopy(skeletonWritable, 0, mergedWritable, 0, skeletonLen);
        System.arraycopy(dataWritable, 0, mergedWritable, skeletonLen, dataLen);
        returnWritable.set(mergedWritable);
        return returnWritable;
      }

      @Override
      public void close() {
        skeletonFileIterator.close();
        dataFileIterator.close();
      }
    };
  }

  public long getPos() throws IOException {
    if (firstRecordReader != null) {
      return firstRecordReader.getPos();
    }
    throw new IllegalStateException("getPos() should not be called before a record reader has been initialized");
  }

  public float getProgress() throws IOException {
    if (firstRecordReader != null) {
      return firstRecordReader.getProgress();
    }
    throw new IllegalStateException("getProgress() should not be called before a record reader has been initialized");
  }
}
