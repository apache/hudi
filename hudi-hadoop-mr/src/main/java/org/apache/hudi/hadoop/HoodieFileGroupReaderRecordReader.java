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
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.TablePathUtils;
import org.apache.hudi.hadoop.realtime.HoodieRealtimeRecordReader;
import org.apache.hudi.hadoop.realtime.RealtimeSplit;
import org.apache.hudi.hadoop.utils.HoodieRealtimeInputFormatUtils;
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils;

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
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HoodieFileGroupReaderRecordReader implements RecordReader<NullWritable, ArrayWritable>  {

  public interface HiveReaderCreator {
    org.apache.hadoop.mapred.RecordReader<NullWritable, ArrayWritable> getRecordReader(
        final org.apache.hadoop.mapred.InputSplit split,
        final org.apache.hadoop.mapred.JobConf job,
        final org.apache.hadoop.mapred.Reporter reporter
    ) throws IOException;
  }

  private final HiveHoodieReaderContext readerContext;
  private final HoodieFileGroupReader<ArrayWritable> fileGroupReader;
  private final ArrayWritable arrayWritable;
  private final NullWritable nullWritable = NullWritable.get();
  private final InputSplit inputSplit;
  private final JobConf jobConf;
  private final UnaryOperator<ArrayWritable> reverseProjection;

  public HoodieFileGroupReaderRecordReader(HiveReaderCreator readerCreator,
                                           final InputSplit split,
                                           final JobConf jobConf,
                                           final Reporter reporter) throws IOException {
    HoodieRealtimeInputFormatUtils.cleanProjectionColumnIds(jobConf);
    Set<String> partitionColumns = new HashSet<>(getPartitionFieldNames(jobConf));
    this.inputSplit = split;
    this.jobConf = jobConf;
    FileSplit fileSplit = (FileSplit) split;
    String tableBasePath = getTableBasePath(split, jobConf);
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(jobConf)
        .setBasePath(tableBasePath)
        .build();
    String latestCommitTime = getLatestCommitTime(split, metaClient);
    Schema tableSchema = getLatestTableSchema(metaClient, jobConf, latestCommitTime);
    Schema requestedSchema = createRequestedSchema(tableSchema, jobConf);
    Map<String, String[]> hosts = new HashMap<>();
    this.readerContext = new HiveHoodieReaderContext(readerCreator, split, jobConf, reporter, tableSchema, hosts, metaClient);
    this.arrayWritable = new ArrayWritable(Writable.class, new Writable[requestedSchema.getFields().size()]);
    this.fileGroupReader = new HoodieFileGroupReader<>(readerContext, jobConf, tableBasePath,
        latestCommitTime, getFileSliceFromSplit(fileSplit, hosts, readerContext.getFs(tableBasePath, jobConf), tableBasePath),
        tableSchema, requestedSchema, metaClient.getTableConfig().getProps(), metaClient.getTableConfig(), fileSplit.getStart(),
        fileSplit.getLength(), false);
    this.fileGroupReader.initRecordIterators();
    Schema outputSchema = HoodieAvroUtils.generateProjectionSchema(tableSchema,
        Stream.concat(tableSchema.getFields().stream().map(f -> f.name().toLowerCase(Locale.ROOT)).filter(n -> !partitionColumns.contains(n)),
            partitionColumns.stream()).collect(Collectors.toList()));
    this.reverseProjection = readerContext.reverseProjectRecord(requestedSchema, outputSchema);
  }

  @Override
  public boolean next(NullWritable key, ArrayWritable value) throws IOException {
    if (!fileGroupReader.hasNext()) {
      return false;
    }
    value.set(fileGroupReader.next().get());
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
    return readerContext.getPos();
  }

  @Override
  public void close() throws IOException {
    fileGroupReader.close();
  }

  @Override
  public float getProgress() throws IOException {
    return readerContext.getProgress();
  }

  public RealtimeSplit getSplit() {
    return (RealtimeSplit) inputSplit;
  }

  public JobConf getJobConf() {
    return jobConf;
  }

  private static List<String> getPartitionFieldNames(JobConf jobConf) {
    String partitionFields = jobConf.get(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "");
    return partitionFields.length() > 0 ? Arrays.stream(partitionFields.split("/")).collect(Collectors.toList())
        : new ArrayList<>();
  }

  private static Schema getLatestTableSchema(HoodieTableMetaClient metaClient, JobConf jobConf, String latestCommitTime) {
    TableSchemaResolver tableSchemaResolver = new TableSchemaResolver(metaClient);
    try {
      Schema schema = tableSchemaResolver.getTableAvroSchema(latestCommitTime);
      // Add partitioning fields to writer schema for resulting row to contain null values for these fields
      return HoodieRealtimeRecordReaderUtils.addPartitionFields(schema, getPartitionFieldNames(jobConf));
    } catch (Exception e) {
      throw new RuntimeException("Unable to get table schema", e);
    }
  }

  public static String getTableBasePath(InputSplit split, JobConf jobConf) throws IOException {
    if (split instanceof RealtimeSplit) {
      RealtimeSplit realtimeSplit = (RealtimeSplit) split;
      return realtimeSplit.getBasePath();
    } else {
      Path inputPath = ((FileSplit)split).getPath();
      FileSystem fs =  inputPath.getFileSystem(jobConf);
      Option<Path> tablePath = TablePathUtils.getTablePath(fs, inputPath);
      return tablePath.get().toString();
    }
  }

  private static String getLatestCommitTime(InputSplit split, HoodieTableMetaClient metaClient) {
    if (split instanceof RealtimeSplit) {
      return ((RealtimeSplit) split).getMaxCommitTime();
    }
    Option<HoodieInstant> lastInstant = metaClient.getCommitsTimeline().lastInstant();
    if (lastInstant.isPresent()) {
      return lastInstant.get().getTimestamp();
    } else {
      return "";
    }
  }

  /**
   * Convert FileSplit to FileSlice, but save the locations in 'hosts' because that data is otherwise lost.
   */
  private static FileSlice getFileSliceFromSplit(FileSplit split, Map<String, String[]> hosts, FileSystem fs, String tableBasePath) throws IOException {
    BaseFile bootstrapBaseFile = createBootstrapBaseFile(split, hosts, fs);
    if (split instanceof RealtimeSplit) {
      //mor
      RealtimeSplit realtimeSplit = (RealtimeSplit) split;
      boolean isLogFile = FSUtils.isLogFile(realtimeSplit.getPath());
      String fileID;
      String commitTime;
      if (isLogFile) {
        fileID = FSUtils.getFileIdFromLogPath(realtimeSplit.getPath());
        commitTime = FSUtils.getDeltaCommitTimeFromLogPath(realtimeSplit.getPath());
      } else {
        fileID = FSUtils.getFileId(realtimeSplit.getPath().getName());
        commitTime = FSUtils.getCommitTime(realtimeSplit.getPath().toString());
      }
      HoodieFileGroupId fileGroupId = new HoodieFileGroupId(FSUtils.getPartitionPath(realtimeSplit.getBasePath(),
          realtimeSplit.getPath().getParent().toString()).toString(), fileID);
      if (isLogFile) {
        return new FileSlice(fileGroupId, commitTime, null, realtimeSplit.getDeltaLogFiles());
      }
      hosts.put(realtimeSplit.getPath().toString(), realtimeSplit.getLocations());
      HoodieBaseFile hoodieBaseFile = new HoodieBaseFile(fs.getFileStatus(realtimeSplit.getPath()), bootstrapBaseFile);
      return new FileSlice(fileGroupId, commitTime, hoodieBaseFile, realtimeSplit.getDeltaLogFiles());
    }
    //cow
    HoodieFileGroupId fileGroupId = new HoodieFileGroupId(FSUtils.getFileId(split.getPath().getName()),
        FSUtils.getPartitionPath(tableBasePath, split.getPath().getParent().toString()).toString());
    hosts.put(split.getPath().toString(), split.getLocations());
    return new FileSlice(fileGroupId, FSUtils.getCommitTime(split.getPath().toString()), new HoodieBaseFile(fs.getFileStatus(split.getPath()), bootstrapBaseFile), Collections.emptyList());
  }

  private static BaseFile createBootstrapBaseFile(FileSplit split, Map<String, String[]> hosts, FileSystem fs) throws IOException {
    if (split instanceof BootstrapBaseFileSplit) {
      BootstrapBaseFileSplit bootstrapBaseFileSplit = (BootstrapBaseFileSplit) split;
      FileSplit bootstrapFileSplit = bootstrapBaseFileSplit.getBootstrapFileSplit();
      hosts.put(bootstrapFileSplit.getPath().toString(), bootstrapFileSplit.getLocations());
      return new BaseFile(fs.getFileStatus(bootstrapFileSplit.getPath()));
    }
    return null;
  }

  private static Schema createRequestedSchema(Schema tableSchema, JobConf jobConf) {
    //hive will handle the partition cols
    String partitionColString = jobConf.get(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS);
    Set<String> partitionColumns;
    if (partitionColString == null) {
      partitionColumns = Collections.emptySet();
    } else {
      partitionColumns = Arrays.stream(partitionColString.split(",")).collect(Collectors.toSet());
    }
    //if they are actually written to the file, then it is ok to read them from the file
    tableSchema.getFields().forEach(f -> partitionColumns.remove(f.name().toLowerCase(Locale.ROOT)));
    return HoodieAvroUtils.generateProjectionSchema(tableSchema,
        Arrays.stream(jobConf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR).split(",")).filter(c -> !partitionColumns.contains(c)).collect(Collectors.toList()));
  }

  /**
   * `schema.on.read` and skip merge not implemented
   */
  public static boolean useFilegroupReader(final JobConf jobConf) {
    return jobConf.getBoolean(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key(), HoodieReaderConfig.FILE_GROUP_READER_ENABLED.defaultValue())
        && !jobConf.getBoolean(HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key(), HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.defaultValue())
        && !jobConf.getBoolean(HoodieRealtimeRecordReader.REALTIME_SKIP_MERGE_PROP, false);
  }
}
