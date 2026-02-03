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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.hadoop.realtime.RealtimeSplit;
import org.apache.hudi.hadoop.utils.HoodieArrayWritableSchemaUtils;
import org.apache.hudi.hadoop.utils.HoodieRealtimeInputFormatUtils;
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.config.HoodieReaderConfig.MERGE_TYPE;
import static org.apache.hudi.common.config.HoodieReaderConfig.REALTIME_PAYLOAD_COMBINE;
import static org.apache.hudi.common.config.HoodieReaderConfig.REALTIME_SKIP_MERGE;
import static org.apache.hudi.common.fs.FSUtils.getCommitTime;
import static org.apache.hudi.common.fs.FSUtils.getFileId;
import static org.apache.hudi.common.util.ConfigUtils.containsConfigProperty;
import static org.apache.hudi.common.util.StringUtils.EMPTY_STRING;
import static org.apache.hudi.hadoop.fs.HadoopFSUtils.convertToStoragePathInfo;
import static org.apache.hudi.hadoop.fs.HadoopFSUtils.getDeltaCommitTimeFromLogPath;
import static org.apache.hudi.hadoop.fs.HadoopFSUtils.getFileIdFromLogPath;
import static org.apache.hudi.hadoop.fs.HadoopFSUtils.getFs;
import static org.apache.hudi.hadoop.fs.HadoopFSUtils.getRelativePartitionPath;
import static org.apache.hudi.hadoop.fs.HadoopFSUtils.getStorageConf;
import static org.apache.hudi.hadoop.fs.HadoopFSUtils.isLogFile;
import static org.apache.hudi.hadoop.realtime.HoodieRealtimeRecordReader.REALTIME_SKIP_MERGE_PROP;
import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.getPartitionFieldNames;
import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.getTableBasePath;

/**
 * {@link HoodieFileGroupReader} based implementation of Hive's {@link RecordReader} for {@link ArrayWritable}.
 */
public class HoodieFileGroupReaderBasedRecordReader implements RecordReader<NullWritable, ArrayWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieFileGroupReaderBasedRecordReader.class);

  public interface HiveReaderCreator {
    org.apache.hadoop.mapred.RecordReader<NullWritable, ArrayWritable> getRecordReader(
        final org.apache.hadoop.mapred.InputSplit split,
        final org.apache.hadoop.mapred.JobConf job,
        Schema dataSchema
    ) throws IOException;
  }

  private final HiveHoodieReaderContext readerContext;
  private final ClosableIterator<ArrayWritable> recordIterator;
  private final ArrayWritable arrayWritable;
  private final NullWritable nullWritable = NullWritable.get();
  private final InputSplit inputSplit;
  private final JobConf jobConfCopy;
  private final UnaryOperator<ArrayWritable> reverseProjection;
  private final boolean containsBaseFile;

  public HoodieFileGroupReaderBasedRecordReader(HiveReaderCreator readerCreator,
                                                final InputSplit split,
                                                final JobConf jobConf) throws IOException {
    this.jobConfCopy = new JobConf(jobConf);
    HoodieRealtimeInputFormatUtils.cleanProjectionColumnIds(jobConfCopy);
    Set<String> partitionColumns = new HashSet<>(getPartitionFieldNames(jobConfCopy));
    this.inputSplit = split;

    FileSplit fileSplit = (FileSplit) split;
    String tableBasePath = getTableBasePath(split, jobConfCopy);
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(getStorageConf(jobConfCopy))
        .setBasePath(tableBasePath)
        .build();
    String latestCommitTime = getLatestCommitTime(split, metaClient);
    HoodieSchema tableSchema = getLatestTableSchema(metaClient, jobConfCopy, latestCommitTime);
    HoodieSchema requestedSchema = createRequestedSchema(tableSchema, jobConfCopy);
    this.readerContext = new HiveHoodieReaderContext(readerCreator,
        getStoredPartitionFieldNames(jobConfCopy, tableSchema), new HadoopStorageConfiguration(jobConfCopy),
        metaClient.getTableConfig());
    this.arrayWritable = new ArrayWritable(Writable.class, new Writable[requestedSchema.getFields().size()]);
    TypedProperties props = metaClient.getTableConfig().getProps();
    jobConf.forEach(e -> {
      if (e.getKey().startsWith("hoodie")) {
        props.setProperty(e.getKey(), e.getValue());
      }
    });
    if (props.containsKey(REALTIME_SKIP_MERGE_PROP)
        && !containsConfigProperty(props, MERGE_TYPE)) {
      if (props.getString(REALTIME_SKIP_MERGE_PROP).equalsIgnoreCase("true")) {
        props.setProperty(MERGE_TYPE.key(), REALTIME_SKIP_MERGE);
      } else {
        props.setProperty(MERGE_TYPE.key(), REALTIME_PAYLOAD_COMBINE);
      }
    }
    LOG.debug("Creating HoodieFileGroupReaderRecordReader with tableBasePath={}, latestCommitTime={}, fileSplit={}", tableBasePath, latestCommitTime, fileSplit.getPath());
    FileSlice fileSlice = getFileSliceFromSplit(fileSplit, getFs(tableBasePath, jobConfCopy), tableBasePath);
    this.containsBaseFile = fileSlice.getBaseFile().isPresent();
    this.recordIterator = HoodieFileGroupReader.<ArrayWritable>newBuilder()
        .withReaderContext(readerContext)
        .withHoodieTableMetaClient(metaClient)
        .withLatestCommitTime(latestCommitTime)
        .withFileSlice(fileSlice)
        .withDataSchema(tableSchema)
        .withRequestedSchema(requestedSchema)
        .withProps(props)
        .withStart(fileSplit.getStart())
        .withLength(fileSplit.getLength())
        .withShouldUseRecordPosition(false)
        .build()
        .getClosableIterator();
    // it expects the partition columns to be at the end
    HoodieSchema outputSchema = HoodieSchemaUtils.generateProjectionSchema(tableSchema,
        Stream.concat(tableSchema.getFields().stream().map(f -> f.name().toLowerCase(Locale.ROOT)).filter(n -> !partitionColumns.contains(n)),
            partitionColumns.stream()).collect(Collectors.toList()));
    this.reverseProjection = HoodieArrayWritableSchemaUtils.getReverseProjection(requestedSchema, outputSchema);
  }

  @VisibleForTesting
  HoodieFileGroupReaderBasedRecordReader(
      HiveHoodieReaderContext readerContext,
      ClosableIterator<ArrayWritable> recordIterator,
      ArrayWritable arrayWritable,
      InputSplit inputSplit,
      JobConf jobConf,
      UnaryOperator<ArrayWritable> reverseProjection,
      boolean containsBaseFile
  ) {
    this.readerContext = readerContext;
    this.recordIterator = recordIterator;
    this.arrayWritable = arrayWritable;
    this.inputSplit = inputSplit;
    this.jobConfCopy = jobConf;
    this.reverseProjection = reverseProjection;
    this.containsBaseFile = containsBaseFile;
  }

  @Override
  public boolean next(NullWritable key, ArrayWritable value) throws IOException {
    if (!recordIterator.hasNext()) {
      return false;
    }
    value.set(recordIterator.next().get());
    reverseProjection.apply(value);
    return true;
  }

  @Override
  public NullWritable createKey() {
    return nullWritable;
  }

  @Override
  public ArrayWritable createValue() {
    return arrayWritable;
  }

  @Override
  public long getPos() throws IOException {
    if (this.containsBaseFile) {
      return readerContext.getPos();
    }

    // for log only split, we return 0 to make it consistent with non HoodieFileGroupReader based Record Readers
    return 0;
  }

  @Override
  public void close() throws IOException {
    recordIterator.close();
  }

  @Override
  public float getProgress() throws IOException {
    if (this.containsBaseFile) {
      return readerContext.getProgress();
    }

    // for log only split, we return 0 to make it consistent with non HoodieFileGroupReader based Record Readers
    return 0;
  }

  /**
   * List of partition fields that are actually written to the file
   */
  @VisibleForTesting
  static List<String> getStoredPartitionFieldNames(JobConf jobConf, HoodieSchema writerSchema) {
    return getPartitionFieldNames(jobConf).stream().filter(n -> writerSchema.getField(n).isPresent()).collect(Collectors.toList());
  }

  public RealtimeSplit getSplit() {
    return (RealtimeSplit) inputSplit;
  }

  public JobConf getJobConf() {
    return jobConfCopy;
  }

  private static HoodieSchema getLatestTableSchema(HoodieTableMetaClient metaClient, JobConf jobConf, String latestCommitTime) {
    TableSchemaResolver tableSchemaResolver = new TableSchemaResolver(metaClient);
    try {
      HoodieSchema schema = tableSchemaResolver.getTableSchema(latestCommitTime);
      // Add partitioning fields to writer schema for resulting row to contain null values for these fields
      return HoodieRealtimeRecordReaderUtils.addPartitionFields(schema, getPartitionFieldNames(jobConf));
    } catch (Exception e) {
      throw new RuntimeException("Unable to get table schema", e);
    }
  }

  private static String getLatestCommitTime(InputSplit split, HoodieTableMetaClient metaClient) {
    if (split instanceof RealtimeSplit) {
      return ((RealtimeSplit) split).getMaxCommitTime();
    }
    Option<HoodieInstant> lastInstant = metaClient.getCommitsTimeline().lastInstant();
    if (lastInstant.isPresent()) {
      return lastInstant.get().requestedTime();
    } else {
      return EMPTY_STRING;
    }
  }

  /**
   * Convert FileSplit to FileSlice
   */
  private static FileSlice getFileSliceFromSplit(FileSplit split, FileSystem fs, String tableBasePath) throws IOException {
    BaseFile bootstrapBaseFile = createBootstrapBaseFile(split, fs);
    if (split instanceof RealtimeSplit) {
      // MOR
      RealtimeSplit realtimeSplit = (RealtimeSplit) split;
      boolean isLogFile = isLogFile(realtimeSplit.getPath());
      String fileID;
      String commitTime;
      if (isLogFile) {
        fileID = getFileIdFromLogPath(realtimeSplit.getPath());
        commitTime = getDeltaCommitTimeFromLogPath(realtimeSplit.getPath());
      } else {
        fileID = getFileId(realtimeSplit.getPath().getName());
        commitTime = getCommitTime(realtimeSplit.getPath().toString());
      }
      HoodieFileGroupId fileGroupId = new HoodieFileGroupId(getRelativePartitionPath(new Path(realtimeSplit.getBasePath()), realtimeSplit.getPath()), fileID);
      if (isLogFile) {
        return new FileSlice(fileGroupId, commitTime, null, realtimeSplit.getDeltaLogFiles());
      }
      HoodieBaseFile hoodieBaseFile = new HoodieBaseFile(convertToStoragePathInfo(fs.getFileStatus(realtimeSplit.getPath()), realtimeSplit.getLocations()), bootstrapBaseFile);
      return new FileSlice(fileGroupId, commitTime, hoodieBaseFile, realtimeSplit.getDeltaLogFiles());
    }
    // COW
    HoodieFileGroupId fileGroupId = new HoodieFileGroupId(getFileId(split.getPath().getName()), getRelativePartitionPath(new Path(tableBasePath), split.getPath()));
    return new FileSlice(
        fileGroupId,
        getCommitTime(split.getPath().toString()),
        new HoodieBaseFile(convertToStoragePathInfo(fs.getFileStatus(split.getPath()), split.getLocations()), bootstrapBaseFile),
        Collections.emptyList());
  }

  private static BaseFile createBootstrapBaseFile(FileSplit split, FileSystem fs) throws IOException {
    if (split instanceof BootstrapBaseFileSplit) {
      BootstrapBaseFileSplit bootstrapBaseFileSplit = (BootstrapBaseFileSplit) split;
      FileSplit bootstrapFileSplit = bootstrapBaseFileSplit.getBootstrapFileSplit();
      return new BaseFile(convertToStoragePathInfo(fs.getFileStatus(bootstrapFileSplit.getPath()), bootstrapFileSplit.getLocations()));
    }
    return null;
  }

  @VisibleForTesting
  public static HoodieSchema createRequestedSchema(HoodieSchema tableSchema, JobConf jobConf) {
    String readCols = jobConf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR);
    if (StringUtils.isNullOrEmpty(readCols)) {
      HoodieSchema emptySchema = HoodieSchema.createRecord(tableSchema.getName(), tableSchema.getDoc().orElse(null),
          tableSchema.getNamespace().orElse(null), tableSchema.isError(), Collections.emptyList());
      return emptySchema;
    }
    // hive will handle the partition cols
    String partitionColString = jobConf.get(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS);
    Set<String> partitionColumns;
    if (partitionColString == null) {
      partitionColumns = Collections.emptySet();
    } else {
      partitionColumns = Arrays.stream(partitionColString.split(",")).collect(Collectors.toSet());
    }
    // if they are actually written to the file, then it is ok to read them from the file
    tableSchema.getFields().forEach(f -> partitionColumns.remove(f.name().toLowerCase(Locale.ROOT)));
    return HoodieSchemaUtils.generateProjectionSchema(tableSchema,
        // The READ_COLUMN_NAMES_CONF_STR includes all columns from the query, including those used in the WHERE clause,
        // so any column referenced in the filter (non-partition) will appear twice if already present in the project schema,
        // here distinct() is used here to deduplicate the read columns.
        Arrays.stream(jobConf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR).split(","))
            .filter(c -> !partitionColumns.contains(c))
            .distinct()
            .collect(Collectors.toList()));
  }
}
