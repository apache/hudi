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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.utils.HoodieArrayWritableAvroUtils;
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils;
import org.apache.hudi.hadoop.utils.ObjectInspectorCache;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.model.HoodieRecordMerger.DEFAULT_MERGER_STRATEGY_UUID;
import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.getPartitionFieldNames;

/**
 * {@link HoodieReaderContext} for Hive-specific {@link HoodieFileGroupReaderBasedRecordReader}.
 */
public class HiveHoodieReaderContext extends HoodieReaderContext<ArrayWritable> {
  protected final HoodieFileGroupReaderBasedRecordReader.HiveReaderCreator readerCreator;
  protected final InputSplit split;
  protected final JobConf jobConf;
  protected final Reporter reporter;
  protected final Schema writerSchema;
  protected Map<String, String[]> hosts;
  protected final Map<String, TypeInfo> columnTypeMap;
  private final ObjectInspectorCache objectInspectorCache;
  private RecordReader<NullWritable, ArrayWritable> firstRecordReader = null;

  private final List<String> partitionCols;
  private final Set<String> partitionColSet;

  private final String recordKeyField;

  protected HiveHoodieReaderContext(HoodieFileGroupReaderBasedRecordReader.HiveReaderCreator readerCreator,
                                    InputSplit split,
                                    JobConf jobConf,
                                    Reporter reporter,
                                    Schema writerSchema,
                                    Map<String, String[]> hosts,
                                    HoodieTableMetaClient metaClient) {
    this.readerCreator = readerCreator;
    this.split = split;
    this.jobConf = jobConf;
    this.reporter = reporter;
    this.writerSchema = writerSchema;
    this.hosts = hosts;
    this.partitionCols = getPartitionFieldNames(jobConf).stream().filter(n -> writerSchema.getField(n) != null).collect(Collectors.toList());
    this.partitionColSet = new HashSet<>(this.partitionCols);
    String tableName = metaClient.getTableConfig().getTableName();
    recordKeyField = getRecordKeyField(metaClient);
    this.objectInspectorCache = HoodieArrayWritableAvroUtils.getCacheForTable(tableName, writerSchema, jobConf);
    this.columnTypeMap = objectInspectorCache.getColumnTypeMap();
  }

  /**
   * If populate meta fields is false, then getRecordKeyFields()
   * should return exactly 1 recordkey field.
   */
  private static String getRecordKeyField(HoodieTableMetaClient metaClient) {
    if (metaClient.getTableConfig().populateMetaFields()) {
      return HoodieRecord.RECORD_KEY_METADATA_FIELD;
    }

    Option<String[]> recordKeyFieldsOpt = metaClient.getTableConfig().getRecordKeyFields();
    ValidationUtils.checkArgument(recordKeyFieldsOpt.isPresent(), "No record key field set in table config, but populateMetaFields is disabled");
    ValidationUtils.checkArgument(recordKeyFieldsOpt.get().length == 1, "More than 1 record key set in table config, but populateMetaFields is disabled");
    return recordKeyFieldsOpt.get()[0];
  }

  private void setSchemas(JobConf jobConf, Schema dataSchema, Schema requiredSchema) {
    List<String> dataColumnNameList = dataSchema.getFields().stream().map(f -> f.name().toLowerCase(Locale.ROOT)).collect(Collectors.toList());
    List<TypeInfo> dataColumnTypeList = dataColumnNameList.stream().map(fieldName -> {
      TypeInfo type = columnTypeMap.get(fieldName);
      if (type == null) {
        throw new IllegalArgumentException("Field: " + fieldName + ", does not have a defined type");
      }
      return type;
    }).collect(Collectors.toList());
    jobConf.set(serdeConstants.LIST_COLUMNS, String.join(",", dataColumnNameList));
    jobConf.set(serdeConstants.LIST_COLUMN_TYPES, dataColumnTypeList.stream().map(TypeInfo::getQualifiedName).collect(Collectors.joining(",")));
    // don't replace `f -> f.name()` with lambda reference
    String readColNames = requiredSchema.getFields().stream().map(f -> f.name()).collect(Collectors.joining(","));
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, readColNames);
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, requiredSchema.getFields()
        .stream().map(f -> String.valueOf(dataSchema.getField(f.name()).pos())).collect(Collectors.joining(",")));
  }

  @Override
  public HoodieStorage getStorage(String path, StorageConfiguration<?> conf) {
    return HoodieStorageUtils.getStorage(path, conf);
  }

  @Override
  public ClosableIterator<ArrayWritable> getFileRecordIterator(StoragePath filePath, long start, long length, Schema dataSchema, Schema requiredSchema, HoodieStorage storage) throws IOException {
    JobConf jobConfCopy = new JobConf(jobConf);
    //move the partition cols to the end, because in some cases it has issues if we don't do that
    Schema modifiedDataSchema = HoodieAvroUtils.generateProjectionSchema(dataSchema, Stream.concat(dataSchema.getFields().stream()
            .map(f -> f.name().toLowerCase(Locale.ROOT)).filter(n -> !partitionColSet.contains(n)),
        partitionCols.stream().filter(c -> dataSchema.getField(c) != null)).collect(Collectors.toList()));
    setSchemas(jobConfCopy, modifiedDataSchema, requiredSchema);
    InputSplit inputSplit = new FileSplit(new Path(filePath.toString()), start, length, hosts.get(filePath.toString()));
    RecordReader<NullWritable, ArrayWritable> recordReader = readerCreator.getRecordReader(inputSplit, jobConfCopy, reporter);
    if (firstRecordReader == null) {
      firstRecordReader = recordReader;
    }
    ClosableIterator<ArrayWritable> recordIterator = new RecordReaderValueIterator<>(recordReader);
    if (modifiedDataSchema.equals(requiredSchema)) {
      return recordIterator;
    }
    // record reader puts the required columns in the positions of the data schema and nulls the rest of the columns
    return new CloseableMappingIterator<>(recordIterator, projectRecord(modifiedDataSchema, requiredSchema));
  }

  @Override
  public ArrayWritable convertAvroRecord(IndexedRecord avroRecord) {
    return (ArrayWritable) HoodieRealtimeRecordReaderUtils.avroToArrayWritable(avroRecord, avroRecord.getSchema(), true);
  }

  @Override
  public HoodieRecordMerger getRecordMerger(String mergerStrategy) {
    if (mergerStrategy.equals(DEFAULT_MERGER_STRATEGY_UUID)) {
      return new HoodieHiveRecordMerger();
    }
    throw new HoodieException(String.format("The merger strategy UUID is not supported, Default: %s, Passed: %s", mergerStrategy, DEFAULT_MERGER_STRATEGY_UUID));
  }

  @Override
  public String getRecordKey(ArrayWritable record, Schema schema) {
    return getValue(record, schema, recordKeyField).toString();
  }

  @Override
  public Object getValue(ArrayWritable record, Schema schema, String fieldName) {
    return StringUtils.isNullOrEmpty(fieldName) ? null : objectInspectorCache.getValue(record, schema, fieldName);
  }

  @Override
  public HoodieRecord<ArrayWritable> constructHoodieRecord(Option<ArrayWritable> recordOption, Map<String, Object> metadataMap) {
    if (!recordOption.isPresent()) {
      return new HoodieEmptyRecord<>(new HoodieKey((String) metadataMap.get(INTERNAL_META_RECORD_KEY), (String) metadataMap.get(INTERNAL_META_PARTITION_PATH)), HoodieRecord.HoodieRecordType.HIVE);
    }
    Schema schema = (Schema) metadataMap.get(INTERNAL_META_SCHEMA);
    ArrayWritable writable = recordOption.get();
    return new HoodieHiveRecord(new HoodieKey((String) metadataMap.get(INTERNAL_META_RECORD_KEY), (String) metadataMap.get(INTERNAL_META_PARTITION_PATH)), writable, schema, objectInspectorCache);
  }

  @Override
  public ArrayWritable seal(ArrayWritable record) {
    return new ArrayWritable(Writable.class, Arrays.copyOf(record.get(), record.get().length));
  }

  @Override
  public ClosableIterator<ArrayWritable> mergeBootstrapReaders(ClosableIterator<ArrayWritable> skeletonFileIterator,
                                                               Schema skeletonRequiredSchema,
                                                               ClosableIterator<ArrayWritable> dataFileIterator,
                                                               Schema dataRequiredSchema) {
    int skeletonLen = skeletonRequiredSchema.getFields().size();
    int dataLen = dataRequiredSchema.getFields().size();
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

  @Override
  public UnaryOperator<ArrayWritable> projectRecord(Schema from, Schema to, Map<String, String> renamedColumns) {
    if (!renamedColumns.isEmpty()) {
      throw new IllegalStateException("Schema evolution is not supported in the filegroup reader for Hive currently");
    }
    return HoodieArrayWritableAvroUtils.projectRecord(from, to);
  }

  public UnaryOperator<ArrayWritable> reverseProjectRecord(Schema from, Schema to) {
    return HoodieArrayWritableAvroUtils.reverseProject(from, to);
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
