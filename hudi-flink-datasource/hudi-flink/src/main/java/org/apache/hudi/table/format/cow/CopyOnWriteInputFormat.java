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

package org.apache.hudi.table.format.cow;

import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaCache;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.source.ExpressionPredicates.Predicate;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.format.FlinkRowDataReaderContext;
import org.apache.hudi.table.format.InternalSchemaManager;
import org.apache.hudi.util.FlinkClientUtil;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.StreamerUtil;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.io.GlobFilePathFilter;
import org.apache.flink.api.common.io.compression.InflaterInputStreamFactory;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * An implementation of {@link FileInputFormat} to read {@link RowData} records
 * from base files using {@link HoodieFileGroupReader}.
 *
 * <p>Note: Override the {@link #createInputSplits} method from parent to rewrite the logic creating the FileSystem,
 * use {@link HadoopFSUtils#getFs} to get a plugin filesystem.
 */
@Slf4j
public class CopyOnWriteInputFormat extends FileInputFormat<RowData> {
  private static final long serialVersionUID = 1L;

  private final org.apache.flink.configuration.Configuration flinkConf;
  private final String tableSchema;
  private final String requiredSchema;
  private final List<Predicate> predicates;
  private final long limit;

  private transient HoodieTableMetaClient metaClient;
  private transient HoodieSchema dataSchema;
  private transient HoodieSchema requestedSchema;
  private transient FlinkRowDataReaderContext readerContext;
  private transient TypedProperties readProps;
  private transient ClosableIterator<RowData> itr;
  private transient HoodieFileGroupReader<RowData> fileGroupReader;
  private transient long currentReadCount;

  /**
   * Files filter for determining what files/directories should be included.
   */
  private FilePathFilter localFilesFilter = new GlobFilePathFilter();

  private final InternalSchemaManager internalSchemaManager;

  public CopyOnWriteInputFormat(
      Path[] paths,
      List<Predicate> predicates,
      long limit,
      org.apache.flink.configuration.Configuration flinkConf,
      String tableSchema,
      String requiredSchema,
      InternalSchemaManager internalSchemaManager) {
    super.setFilePaths(paths);
    this.predicates = predicates;
    this.limit = limit;
    this.flinkConf = flinkConf;
    this.tableSchema = tableSchema;
    this.requiredSchema = requiredSchema;
    this.internalSchemaManager = internalSchemaManager;
  }

  @Override
  public void openInputFormat() throws IOException {
    super.openInputFormat();
    Configuration hadoopConf = getParquetHadoopConf();
    this.metaClient = StreamerUtil.metaClientForReader(this.flinkConf, hadoopConf);
    this.dataSchema = HoodieSchemaCache.intern(HoodieSchema.parse(this.tableSchema));
    this.requestedSchema = HoodieSchemaCache.intern(HoodieSchema.parse(this.requiredSchema));
    this.readerContext = new FlinkRowDataReaderContext(
        metaClient.getStorageConf(),
        () -> internalSchemaManager,
        predicates,
        metaClient.getTableConfig(),
        Option.empty());
    HoodieWriteConfig writeConfig = FlinkWriteClients.getHoodieClientConfig(this.flinkConf);
    this.readProps = FlinkClientUtil.getReadProps(metaClient.getTableConfig(), writeConfig);
    this.readProps.put(HoodieReaderConfig.MERGE_TYPE.key(), this.flinkConf.get(FlinkOptions.MERGE_TYPE));
  }

  @Override
  public void open(FileInputSplit fileSplit) throws IOException {
    HoodieBaseFile baseFile = new HoodieBaseFile(fileSplit.getPath().toUri().toString());
    String partitionPath = FSUtils.getRelativePartitionPath(
        metaClient.getBasePath(),
        new StoragePath(fileSplit.getPath().toUri()).getParent());

    // close previous reader if necessary
    closeReader();

    // create new file group reader
    this.fileGroupReader = HoodieFileGroupReader.<RowData>newBuilder()
        .withReaderContext(readerContext)
        .withHoodieTableMetaClient(metaClient)
        .withLatestCommitTime(baseFile.getCommitTime())
        .withBaseFileOption(Option.of(baseFile))
        .withLogFiles(Stream.empty())
        .withPartitionPath(partitionPath)
        .withDataSchema(dataSchema)
        .withRequestedSchema(requestedSchema)
        .withInternalSchema(Option.ofNullable(internalSchemaManager.getQuerySchema()))
        .withProps(readProps)
        .withStart(fileSplit.getStart())
        .withLength(fileSplit.getLength())
        .withShouldUseRecordPosition(false)
        .withEmitDelete(false)
        .build();
    this.itr = this.fileGroupReader.getClosableIterator();
    this.currentReadCount = 0L;
  }

  @Override
  public FileInputSplit[] createInputSplits(int minNumSplits) throws IOException {
    if (minNumSplits < 1) {
      throw new IllegalArgumentException("Number of input splits has to be at least 1.");
    }

    // take the desired number of splits into account
    minNumSplits = Math.max(minNumSplits, this.numSplits);

    final List<FileInputSplit> inputSplits = new ArrayList<>(minNumSplits);

    // get all the files that are involved in the splits
    List<FileStatus> files = new ArrayList<>();
    long totalLength = 0;
    final Configuration hadoopConf = getParquetHadoopConf();

    for (Path path : getFilePaths()) {
      final org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(path.toUri());
      final FileSystem fs = HadoopFSUtils.getFs(hadoopPath.toString(), hadoopConf);
      final FileStatus pathFile = fs.getFileStatus(hadoopPath);

      if (pathFile.isDirectory()) {
        totalLength += addFilesInDir(hadoopPath, files, true, hadoopConf);
      } else {
        testForUnsplittable(pathFile);

        files.add(pathFile);
        totalLength += pathFile.getLen();
      }
    }

    // returns if unsplittable
    if (unsplittable) {
      int splitNum = 0;
      for (final FileStatus file : files) {
        final FileSystem fs = HadoopFSUtils.getFs(file.getPath().toString(), hadoopConf);
        final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, file.getLen());
        Set<String> hosts = new HashSet<>();
        for (BlockLocation block : blocks) {
          hosts.addAll(Arrays.asList(block.getHosts()));
        }
        long len = file.getLen();
        if (testForUnsplittable(file)) {
          len = READ_WHOLE_SPLIT_FLAG;
        }
        FileInputSplit fis = new FileInputSplit(splitNum++, new Path(file.getPath().toUri()), 0, len,
            hosts.toArray(new String[0]));
        inputSplits.add(fis);
      }
      return inputSplits.toArray(new FileInputSplit[0]);
    }


    final long maxSplitSize = totalLength / minNumSplits + (totalLength % minNumSplits == 0 ? 0 : 1);

    // now that we have the files, generate the splits
    int splitNum = 0;
    for (final FileStatus file : files) {

      final FileSystem fs = HadoopFSUtils.getFs(file.getPath().toString(), hadoopConf);
      final long len = file.getLen();
      final long blockSize = file.getBlockSize();

      final long minSplitSize;
      if (this.minSplitSize <= blockSize) {
        minSplitSize = this.minSplitSize;
      } else {
        log.warn("Minimal split size of {} is larger than the block size of {}."
            + " Decreasing minimal split size to block size.", this.minSplitSize, blockSize);
        minSplitSize = blockSize;
      }

      final long splitSize = Math.max(minSplitSize, Math.min(maxSplitSize, blockSize));
      final long halfSplit = splitSize >>> 1;

      final long maxBytesForLastSplit = (long) (splitSize * 1.1f);

      if (len > 0) {

        // get the block locations and make sure they are in order with respect to their offset
        final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, len);
        Arrays.sort(blocks, Comparator.comparingLong(BlockLocation::getOffset));

        long bytesUnassigned = len;
        long position = 0;

        int blockIndex = 0;

        while (bytesUnassigned > maxBytesForLastSplit) {
          // get the block containing the majority of the data
          blockIndex = getBlockIndexForPosition(blocks, position, halfSplit, blockIndex);
          // create a new split
          FileInputSplit fis = new FileInputSplit(splitNum++, new Path(file.getPath().toUri()), position, splitSize,
              blocks[blockIndex].getHosts());
          inputSplits.add(fis);

          // adjust the positions
          position += splitSize;
          bytesUnassigned -= splitSize;
        }

        // assign the last split
        if (bytesUnassigned > 0) {
          blockIndex = getBlockIndexForPosition(blocks, position, halfSplit, blockIndex);
          final FileInputSplit fis = new FileInputSplit(splitNum++, new Path(file.getPath().toUri()), position,
              bytesUnassigned, blocks[blockIndex].getHosts());
          inputSplits.add(fis);
        }
      } else {
        // special case with a file of zero bytes size
        final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, 0);
        String[] hosts;
        if (blocks.length > 0) {
          hosts = blocks[0].getHosts();
        } else {
          hosts = new String[0];
        }
        final FileInputSplit fis = new FileInputSplit(splitNum++, new Path(file.getPath().toUri()), 0, 0, hosts);
        inputSplits.add(fis);
      }
    }

    return inputSplits.toArray(new FileInputSplit[0]);
  }

  public boolean supportsMultiPaths() {
    return true;
  }

  @Override
  public boolean reachedEnd() {
    if (currentReadCount >= limit) {
      return true;
    } else {
      return !itr.hasNext();
    }
  }

  @Override
  public RowData nextRecord(RowData reuse) {
    currentReadCount++;
    return itr.next();
  }

  @Override
  public void close() throws IOException {
    closeReader();
  }

  private void closeReader() throws IOException {
    if (itr != null) {
      this.itr.close();
    }
    if (fileGroupReader != null) {
      this.fileGroupReader.close();
    }
    this.itr = null;
    this.fileGroupReader = null;
  }

  /**
   * Enumerate all files in the directory and recursive if enumerateNestedFiles is true.
   *
   * @return the total length of accepted files.
   */
  private long addFilesInDir(
      org.apache.hadoop.fs.Path path,
      List<FileStatus> files,
      boolean logExcludedFiles,
      Configuration hadoopConf)
      throws IOException {
    final org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(path.toUri());
    final FileSystem fs = HadoopFSUtils.getFs(hadoopPath.toString(), hadoopConf);

    long length = 0;

    for (FileStatus dir : fs.listStatus(hadoopPath)) {
      if (dir.isDirectory()) {
        if (acceptFile(dir) && enumerateNestedFiles) {
          length += addFilesInDir(dir.getPath(), files, logExcludedFiles, hadoopConf);
        } else {
          if (logExcludedFiles) {
            log.debug("Directory {} did not pass the file-filter and is excluded.", dir.getPath());
          }
        }
      } else {
        if (acceptFile(dir)) {
          files.add(dir);
          length += dir.getLen();
          testForUnsplittable(dir);
        } else {
          if (logExcludedFiles) {
            log.debug("Directory {} did not pass the file-filter and is excluded.", dir.getPath());
          }
        }
      }
    }
    return length;
  }

  @Override
  public void setFilesFilter(FilePathFilter filesFilter) {
    this.localFilesFilter = filesFilter;
    super.setFilesFilter(filesFilter);
  }

  /**
   * A simple hook to filter files and directories from the input.
   * The method may be overridden. Hadoop's FileInputFormat has a similar mechanism and applies the
   * same filters by default.
   *
   * @param fileStatus The file status to check.
   * @return true, if the given file or directory is accepted
   */
  public boolean acceptFile(FileStatus fileStatus) {
    final String name = fileStatus.getPath().getName();
    return !name.startsWith("_")
        && !name.startsWith(".")
        && !localFilesFilter.filterPath(new Path(fileStatus.getPath().toUri()));
  }

  /**
   * Retrieves the index of the <tt>BlockLocation</tt> that contains the part of the file described by the given
   * offset.
   *
   * @param blocks     The different blocks of the file. Must be ordered by their offset.
   * @param offset     The offset of the position in the file.
   * @param startIndex The earliest index to look at.
   * @return The index of the block containing the given position.
   */
  private int getBlockIndexForPosition(BlockLocation[] blocks, long offset, long halfSplitSize, int startIndex) {
    // go over all indexes after the startIndex
    for (int i = startIndex; i < blocks.length; i++) {
      long blockStart = blocks[i].getOffset();
      long blockEnd = blockStart + blocks[i].getLength();

      if (offset >= blockStart && offset < blockEnd) {
        // got the block where the split starts
        // check if the next block contains more than this one does
        if (i < blocks.length - 1 && blockEnd - offset < halfSplitSize) {
          return i + 1;
        } else {
          return i;
        }
      }
    }
    throw new IllegalArgumentException("The given offset is not contained in the any block.");
  }

  private boolean testForUnsplittable(FileStatus pathFile) {
    if (getInflaterInputStreamFactory(pathFile.getPath()) != null) {
      unsplittable = true;
      return true;
    }
    return false;
  }

  private InflaterInputStreamFactory<?> getInflaterInputStreamFactory(org.apache.hadoop.fs.Path path) {
    String fileExtension = extractFileExtension(path.getName());
    if (fileExtension != null) {
      return getInflaterInputStreamFactory(fileExtension);
    } else {
      return null;
    }
  }

  private Configuration getParquetHadoopConf() {
    return HadoopConfigurations.getParquetConf(flinkConf, HadoopConfigurations.getHadoopConf(flinkConf));
  }
}
