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

package org.apache.hudi.hadoop;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieTableQueryType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.realtime.HoodieVirtualKeyInfo;
import org.apache.hudi.hadoop.utils.HoodieHiveUtils;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.ValidationUtils.checkState;

/**
 * Base implementation of the Hive's {@link FileInputFormat} allowing for reading of Hudi's
 * Copy-on-Write (COW) tables in various configurations:
 *
 * <ul>
 *   <li>Snapshot mode: reading table's state as of particular timestamp (or instant, in Hudi's terms)</li>
 *   <li>Incremental mode: reading table's state as of particular timestamp (or instant, in Hudi's terms)</li>
 *   <li>External mode: reading non-Hudi partitions</li>
 * </ul>
 *
 * NOTE: This class is invariant of the underlying file-format of the files being read
 */
public class HoodieCopyOnWriteTableInputFormat extends HoodieTableInputFormat {

  @Override
  protected boolean isSplitable(FileSystem fs, Path filename) {
    return !(filename instanceof PathWithBootstrapFileStatus);
  }

  @Override
  protected FileSplit makeSplit(Path file, long start, long length,
                                String[] hosts) {
    FileSplit split = new FileSplit(file, start, length, hosts);

    if (file instanceof PathWithBootstrapFileStatus) {
      return makeExternalFileSplit((PathWithBootstrapFileStatus)file, split);
    }
    return split;
  }

  @Override
  protected FileSplit makeSplit(Path file, long start, long length,
                                String[] hosts, String[] inMemoryHosts) {
    FileSplit split = new FileSplit(file, start, length, hosts, inMemoryHosts);
    if (file instanceof PathWithBootstrapFileStatus) {
      return makeExternalFileSplit((PathWithBootstrapFileStatus)file, split);
    }
    return split;
  }

  @Override
  public FileStatus[] listStatus(JobConf job) throws IOException {
    // Segregate inputPaths[] to incremental, snapshot and non hoodie paths
    List<String> incrementalTables = HoodieHiveUtils.getIncrementalTableNames(Job.getInstance(job));
    InputPathHandler inputPathHandler = new InputPathHandler(conf, getInputPaths(job), incrementalTables);
    List<FileStatus> returns = new ArrayList<>();

    Map<String, HoodieTableMetaClient> tableMetaClientMap = inputPathHandler.getTableMetaClientMap();
    // process incremental pulls first
    for (String table : incrementalTables) {
      HoodieTableMetaClient metaClient = tableMetaClientMap.get(table);
      if (metaClient == null) {
        /* This can happen when the INCREMENTAL mode is set for a table but there were no InputPaths
         * in the jobConf
         */
        continue;
      }
      List<Path> inputPaths = inputPathHandler.getGroupedIncrementalPaths().get(metaClient);
      List<FileStatus> result = listStatusForIncrementalMode(job, metaClient, inputPaths, table);
      if (result != null) {
        returns.addAll(result);
      }
    }

    // process non hoodie Paths next.
    List<Path> nonHoodiePaths = inputPathHandler.getNonHoodieInputPaths();
    if (nonHoodiePaths.size() > 0) {
      setInputPaths(job, nonHoodiePaths.toArray(new Path[nonHoodiePaths.size()]));
      FileStatus[] fileStatuses = doListStatus(job);
      returns.addAll(Arrays.asList(fileStatuses));
    }

    // process snapshot queries next.
    List<Path> snapshotPaths = inputPathHandler.getSnapshotPaths();
    if (snapshotPaths.size() > 0) {
      returns.addAll(listStatusForSnapshotMode(job, tableMetaClientMap, snapshotPaths));
    }
    return returns.toArray(new FileStatus[0]);
  }

  @Override
  public RecordReader<NullWritable, ArrayWritable> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
    throw new UnsupportedEncodingException("not implemented");
  }

  /**
   * Abstracts and exposes {@link FileInputFormat#listStatus(JobConf)} operation to subclasses that
   * lists files (returning an array of {@link FileStatus}) corresponding to the input paths specified
   * as part of provided {@link JobConf}
   */
  protected final FileStatus[] doListStatus(JobConf job) throws IOException {
    return super.listStatus(job);
  }

  /**
   * Achieves listStatus functionality for an incrementally queried table. Instead of listing all
   * partitions and then filtering based on the commits of interest, this logic first extracts the
   * partitions touched by the desired commits and then lists only those partitions.
   */
  protected List<FileStatus> listStatusForIncrementalMode(JobConf job,
                                                          HoodieTableMetaClient tableMetaClient,
                                                          List<Path> inputPaths,
                                                          String incrementalTable) throws IOException {
    Job jobContext = Job.getInstance(job);
    Option<HoodieTimeline> timeline = HoodieInputFormatUtils.getFilteredCommitsTimeline(jobContext, tableMetaClient);
    if (!timeline.isPresent()) {
      return null;
    }
    Option<List<HoodieInstant>> commitsToCheck = HoodieInputFormatUtils.getCommitsForIncrementalQuery(jobContext, incrementalTable, timeline.get());
    if (!commitsToCheck.isPresent()) {
      return null;
    }
    Option<String> incrementalInputPaths = HoodieInputFormatUtils.getAffectedPartitions(commitsToCheck.get(), tableMetaClient, timeline.get(), inputPaths);
    // Mutate the JobConf to set the input paths to only partitions touched by incremental pull.
    if (!incrementalInputPaths.isPresent()) {
      return null;
    }
    setInputPaths(job, incrementalInputPaths.get());
    FileStatus[] fileStatuses = doListStatus(job);
    return HoodieInputFormatUtils.filterIncrementalFileStatus(jobContext, tableMetaClient, timeline.get(), fileStatuses, commitsToCheck.get());
  }

  protected FileStatus createFileStatusUnchecked(FileSlice fileSlice, HiveHoodieTableFileIndex fileIndex, Option<HoodieVirtualKeyInfo> virtualKeyInfoOpt) {
    Option<HoodieBaseFile> baseFileOpt = fileSlice.getBaseFile();

    if (baseFileOpt.isPresent()) {
      return getFileStatusUnchecked(baseFileOpt.get());
    } else {
      throw new IllegalStateException("Invalid state: base-file has to be present");
    }
  }

  private BootstrapBaseFileSplit makeExternalFileSplit(PathWithBootstrapFileStatus file, FileSplit split) {
    try {
      LOG.info("Making external data split for " + file);
      FileStatus externalFileStatus = file.getBootstrapFileStatus();
      FileSplit externalFileSplit = makeSplit(externalFileStatus.getPath(), 0, externalFileStatus.getLen(),
          new String[0], new String[0]);
      return new BootstrapBaseFileSplit(split, externalFileSplit);
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage(), e);
    }
  }

  @Nonnull
  private List<FileStatus> listStatusForSnapshotMode(JobConf job,
                                                     Map<String, HoodieTableMetaClient> tableMetaClientMap,
                                                     List<Path> snapshotPaths) throws IOException {
    HoodieLocalEngineContext engineContext = new HoodieLocalEngineContext(job);
    List<FileStatus> targetFiles = new ArrayList<>();

    TypedProperties props = new TypedProperties(new Properties());

    Map<HoodieTableMetaClient, List<Path>> groupedPaths =
        HoodieInputFormatUtils.groupSnapshotPathsByMetaClient(tableMetaClientMap.values(), snapshotPaths);

    for (Map.Entry<HoodieTableMetaClient, List<Path>> entry : groupedPaths.entrySet()) {
      HoodieTableMetaClient tableMetaClient = entry.getKey();
      List<Path> partitionPaths = entry.getValue();

      // Hive job might specify a max commit instant up to which table's state
      // should be examined. We simply pass it as query's instant to the file-index
      Option<String> queryCommitInstant =
          HoodieHiveUtils.getMaxCommit(job, tableMetaClient.getTableConfig().getTableName());

      boolean shouldIncludePendingCommits =
          HoodieHiveUtils.shouldIncludePendingCommits(job, tableMetaClient.getTableConfig().getTableName());

      HiveHoodieTableFileIndex fileIndex =
          new HiveHoodieTableFileIndex(
              engineContext,
              tableMetaClient,
              props,
              HoodieTableQueryType.SNAPSHOT,
              partitionPaths,
              queryCommitInstant,
              shouldIncludePendingCommits);

      Map<String, List<FileSlice>> partitionedFileSlices = fileIndex.listFileSlices();

      Option<HoodieVirtualKeyInfo> virtualKeyInfoOpt = getHoodieVirtualKeyInfo(tableMetaClient);

      targetFiles.addAll(
          partitionedFileSlices.values()
              .stream()
              .flatMap(Collection::stream)
              .map(fileSlice -> createFileStatusUnchecked(fileSlice, fileIndex, virtualKeyInfoOpt))
              .collect(Collectors.toList())
      );
    }

    return targetFiles;
  }

  private void validate(List<FileStatus> targetFiles, List<FileStatus> legacyFileStatuses) {
    List<FileStatus> diff = CollectionUtils.diff(targetFiles, legacyFileStatuses);
    checkState(diff.isEmpty(), "Should be empty");
  }

  @Nonnull
  protected static FileStatus getFileStatusUnchecked(HoodieBaseFile baseFile) {
    try {
      return HoodieInputFormatUtils.getFileStatus(baseFile);
    } catch (IOException ioe) {
      throw new HoodieIOException("Failed to get file-status", ioe);
    }
  }

  protected static Option<HoodieVirtualKeyInfo> getHoodieVirtualKeyInfo(HoodieTableMetaClient metaClient) {
    HoodieTableConfig tableConfig = metaClient.getTableConfig();
    if (tableConfig.populateMetaFields()) {
      return Option.empty();
    }

    TableSchemaResolver tableSchemaResolver = new TableSchemaResolver(metaClient);
    try {
      Schema schema = tableSchemaResolver.getTableAvroSchema();
      return Option.of(
          new HoodieVirtualKeyInfo(
              tableConfig.getRecordKeyFieldProp(),
              tableConfig.getPartitionFieldProp(),
              schema.getField(tableConfig.getRecordKeyFieldProp()).pos(),
              schema.getField(tableConfig.getPartitionFieldProp()).pos()));
    } catch (Exception exception) {
      throw new HoodieException("Fetching table schema failed with exception ", exception);
    }
  }
}
