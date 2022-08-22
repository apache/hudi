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

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.CustomizedThreadFactory;
import org.apache.hudi.common.util.FutureUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.format.cow.vector.reader.ParquetColumnarRowSplitReader;
import org.apache.hudi.util.DataTypeUtils;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.io.GlobFilePathFilter;
import org.apache.flink.api.common.io.compression.InflaterInputStreamFactory;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.utils.SerializableConfiguration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.PartitionPathUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An implementation of {@link FileInputFormat} to read {@link RowData} records
 * from Parquet files.
 *
 * <p>Note: Reference Flink release 1.11.2
 * {@code org.apache.flink.formats.parquet.ParquetFileSystemFormatFactory.ParquetInputFormat}
 * to support TIMESTAMP_MILLIS.
 *
 * <p>Note: Override the {@link #createInputSplits} method from parent to rewrite the logic creating the FileSystem,
 * use {@link FSUtils#getFs} to get a plugin filesystem.
 *
 * @see ParquetSplitReaderUtil
 */
public class CopyOnWriteInputFormat extends FileInputFormat<RowData> {
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(CopyOnWriteInputFormat.class);

  private final String[] fullFieldNames;
  private final DataType[] fullFieldTypes;
  private final int[] selectedFields;
  private final String partDefaultName;
  private final boolean utcTimestamp;
  private final SerializableConfiguration conf;
  private final org.apache.flink.configuration.Configuration flinkConf;
  private final boolean createInputSplitAsync;
  private final long limit;

  private transient ParquetColumnarRowSplitReader reader;
  private transient long currentReadCount;
  private transient ExecutorService createInputSplitPool;

  /**
   * Files filter for determining what files/directories should be included.
   */
  private FilePathFilter localFilesFilter = new GlobFilePathFilter();

  public CopyOnWriteInputFormat(
      Path[] paths,
      String[] fullFieldNames,
      DataType[] fullFieldTypes,
      int[] selectedFields,
      org.apache.flink.configuration.Configuration flinkConf,
      long limit,
      Configuration conf) {
    super.setFilePaths(paths);
    this.limit = limit;
    this.flinkConf = flinkConf;
    this.partDefaultName = this.flinkConf.getString(FlinkOptions.PARTITION_DEFAULT_NAME);
    this.fullFieldNames = fullFieldNames;
    this.fullFieldTypes = fullFieldTypes;
    this.selectedFields = selectedFields;
    this.conf = new SerializableConfiguration(conf);
    this.utcTimestamp = this.flinkConf.getBoolean(FlinkOptions.UTC_TIMEZONE);
    this.createInputSplitAsync = this.flinkConf.getBoolean(FlinkOptions.READ_COW_CREATE_SPLIT_ASYNC_ENABLED);
  }

  @Override
  public void open(FileInputSplit fileSplit) throws IOException {
    // generate partition specs.
    List<String> fieldNameList = Arrays.asList(fullFieldNames);
    LinkedHashMap<String, String> partSpec = PartitionPathUtils.extractPartitionSpecFromPath(
        fileSplit.getPath());
    LinkedHashMap<String, Object> partObjects = new LinkedHashMap<>();
    partSpec.forEach((k, v) -> {
      final int idx = fieldNameList.indexOf(k);
      if (idx == -1) {
        // for any rare cases that the partition field does not exist in schema,
        // fallback to file read
        return;
      }
      DataType fieldType = fullFieldTypes[idx];
      if (!DataTypeUtils.isDatetimeType(fieldType)) {
        // date time type partition field is formatted specifically,
        // read directly from the data file to avoid format mismatch or precision loss
        partObjects.put(k, DataTypeUtils.resolvePartition(partDefaultName.equals(v) ? null : v, fieldType));
      }
    });

    this.reader = ParquetSplitReaderUtil.genPartColumnarRowReader(
        utcTimestamp,
        true,
        conf.conf(),
        fullFieldNames,
        fullFieldTypes,
        partObjects,
        selectedFields,
        2048,
        fileSplit.getPath(),
        fileSplit.getStart(),
        fileSplit.getLength());
    this.currentReadCount = 0L;
  }

  @Override
  public FileInputSplit[] createInputSplits(int minNumSplits) throws IOException {
    if (minNumSplits < 1) {
      throw new IllegalArgumentException("Number of input splits has to be at least 1.");
    }
    HoodieTimer timer = new HoodieTimer().startTimer();
    // take the desired number of splits into account
    minNumSplits = Math.max(minNumSplits, this.numSplits);
    final List<FileInputSplit> inputSplits = new ArrayList<>(minNumSplits);

    // get all the files that are involved in the splits
    timer.startTimer();
    FileStatusSpec fileStatusSpec = getTotalFileStatusSpec(getFilePaths());
    List<FileStatus> files = fileStatusSpec.getFileStatuses();
    long totalLength = fileStatusSpec.getFileLength();
    unsplittable = fileStatusSpec.getUnsplitable();
    LOG.info("getFileStatusSpec cost: {} mills, files size: {}, totalLength: {}, unsplittable: {}", timer.endTimer(), files.size(), totalLength, unsplittable);

    timer.startTimer();
    List<FileBlockSpec> fileBlockSpecs = getTotalFileBlockSpec(files);
    Map<FileStatus, BlockLocation[]> blockLocationsMap = fileBlockSpecs.stream().collect(Collectors.toMap(FileBlockSpec::getFileStatus, FileBlockSpec::getBlockLocations));
    LOG.info("getFileBlockSpec cost: {} mills, blockLocationsMap size: {}", timer.endTimer(), blockLocationsMap.size());

    // returns if unsplittable
    if (unsplittable) {
      int splitNum = 0;
      for (final FileStatus file : files) {
        final BlockLocation[] blocks = blockLocationsMap.get(file);
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
      final long len = file.getLen();
      final long blockSize = file.getBlockSize();

      final long minSplitSize;
      if (this.minSplitSize <= blockSize) {
        minSplitSize = this.minSplitSize;
      } else {
        if (LOG.isWarnEnabled()) {
          LOG.warn("Minimal split size of " + this.minSplitSize + " is larger than the block size of "
              + blockSize + ". Decreasing minimal split size to block size.");
        }
        minSplitSize = blockSize;
      }

      final long splitSize = Math.max(minSplitSize, Math.min(maxSplitSize, blockSize));
      final long halfSplit = splitSize >>> 1;

      final long maxBytesForLastSplit = (long) (splitSize * 1.1f);

      if (len > 0) {

        // get the block locations and make sure they are in order with respect to their offset
        final BlockLocation[] blocks = blockLocationsMap.get(file);
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
        LOG.warn("ignore 0 size file: {}, fileLength: {}", file.getPath().toString(), file.getLen());
      }
    }
    LOG.info("create input splits cost: {} mills, inputSplits size: {}", timer.endTimer(), inputSplits.size());

    return inputSplits.toArray(new FileInputSplit[0]);
  }

  private synchronized ExecutorService getCreateInputSplitPool() {
    // initialize it here but not in open function because this is invoked ahead of that
    if (this.createInputSplitPool == null) {
      int threadNum = calculateCreateInputSplitParallelism();
      this.createInputSplitPool = Executors.newFixedThreadPool(
          threadNum,
          new CustomizedThreadFactory("hudi_create_splits", true));
    }

    return this.createInputSplitPool;
  }

  private int calculateCreateInputSplitParallelism() {
    int filePathCnt = getFilePaths().length;
    int minCreateSplitParallelism = this.flinkConf.getInteger(FlinkOptions.READ_COW_CREATE_SPLIT_ASYNC_MIN_PARALLELISM);
    int maxCreateSplitParallelism = this.flinkConf.getInteger(FlinkOptions.READ_COW_CREATE_SPLIT_ASYNC_MAX_PARALLELISM);
    int threadNum = Math.max(minCreateSplitParallelism, Math.min(maxCreateSplitParallelism, filePathCnt));
    LOG.info("create splits thread num: {}", threadNum);

    return threadNum;
  }

  @Override
  public boolean supportsMultiPaths() {
    return true;
  }

  @Override
  public boolean reachedEnd() throws IOException {
    if (currentReadCount >= limit) {
      return true;
    } else {
      return reader.reachedEnd();
    }
  }

  @Override
  public RowData nextRecord(RowData reuse) {
    currentReadCount++;
    return reader.nextRecord();
  }

  @Override
  public void close() throws IOException {
    if (reader != null) {
      this.reader.close();
    }
    this.reader = null;

    if (this.createInputSplitPool != null) {
      this.createInputSplitPool.shutdown();
    }
    this.createInputSplitPool = null;
  }

  /**
   * Enumerate all files in the directory and recursive if enumerateNestedFiles is true.
   *
   * @return the total length of accepted files.
   */
  private long addFilesInDir(org.apache.hadoop.fs.Path path, List<FileStatus> files, boolean logExcludedFiles)
      throws IOException {
    final org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(path.toUri());
    final FileSystem fs = FSUtils.getFs(hadoopPath.toString(), this.conf.conf());

    long length = 0;

    for (FileStatus dir : fs.listStatus(hadoopPath)) {
      if (dir.isDirectory()) {
        if (acceptFile(dir) && enumerateNestedFiles) {
          length += addFilesInDir(dir.getPath(), files, logExcludedFiles);
        } else {
          if (logExcludedFiles && LOG.isDebugEnabled()) {
            LOG.debug("Directory " + dir.getPath().toString() + " did not pass the file-filter and is excluded.");
          }
        }
      } else {
        if (acceptFile(dir)) {
          files.add(dir);
          length += dir.getLen();
          testForUnsplittable(dir);
        } else {
          if (logExcludedFiles && LOG.isDebugEnabled()) {
            LOG.debug("Directory " + dir.getPath().toString() + " did not pass the file-filter and is excluded.");
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

  @VisibleForTesting
  public FileStatusSpec getTotalFileStatusSpec(Path[] fileInputPaths) {
    final List<CompletableFuture<FileStatusSpec>> fileStatusSpecFutures = Arrays.stream(fileInputPaths).map(
        path -> this.createInputSplitAsync
            ? CompletableFuture.supplyAsync(() -> this.getFileStatusSpec(path, this.conf.conf()), getCreateInputSplitPool())
            : CompletableFuture.completedFuture(this.getFileStatusSpec(path, this.conf.conf()))
    ).collect(Collectors.toList());

    final CompletableFuture<FileStatusSpec> fileStatusSpecFuture =
        FutureUtils.allOf(fileStatusSpecFutures)
            .thenApply((fileStatusSpecs) -> fileStatusSpecs.stream().reduce((specs1, specs2) -> {
              List<FileStatus> fileStatuses = Stream.of(specs1.getFileStatuses(), specs2.getFileStatuses()).flatMap(Collection::stream).collect(Collectors.toList());
              long length = Stream.of(specs1.getFileLength(), specs2.getFileLength()).mapToLong(Long::longValue).sum();
              boolean unsplitable = Stream.of(specs1.getUnsplitable(), specs2.getUnsplitable()).anyMatch(x -> x);
              return new FileStatusSpec(fileStatuses, length, unsplitable);
            }).orElseThrow(() -> new HoodieException("failed to merge file status specs")));

    return FutureUtils.get(fileStatusSpecFuture);
  }

  private FileStatusSpec getFileStatusSpec(Path path, Configuration conf) {
    HoodieTimer timer = new HoodieTimer().startTimer();
    long length = 0;
    List<FileStatus> fileStatuses = new ArrayList<>();
    boolean unsplitable0 = false;
    try {
      final org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(path.toUri());
      final FileSystem fs = FSUtils.getFs(hadoopPath.toString(), conf);
      final FileStatus pathFile = fs.getFileStatus(hadoopPath);

      if (pathFile.isDirectory()) {
        length += addFilesInDir(hadoopPath, fileStatuses, true);
      } else {
        unsplitable0 = testForUnsplittable(pathFile);
        fileStatuses.add(pathFile);
        length += pathFile.getLen();
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("thread: {} getFileStatus cost: {} mills, path: {}, fileStatuses size: {}, length: {}, unsplitable0: {}", Thread.currentThread().getName(), timer.endTimer(), path,
            fileStatuses.size(), length, unsplitable0);
      }
    } catch (IOException e) {
      throw new HoodieIOException("get file status specs failed", e);
    }
    return new FileStatusSpec(fileStatuses, length, unsplitable0);
  }

  @VisibleForTesting
  public List<FileBlockSpec> getTotalFileBlockSpec(List<FileStatus> files) {
    final List<CompletableFuture<FileBlockSpec>> fileBlocksFutures = files.stream().map(file ->
        this.createInputSplitAsync
            ? CompletableFuture.supplyAsync(() -> this.getFileBlockSpec(file, this.conf.conf()), getCreateInputSplitPool())
            : CompletableFuture.completedFuture(this.getFileBlockSpec(file, this.conf.conf()))
    ).collect(Collectors.toList());

    final CompletableFuture<List<FileBlockSpec>> fileBlockListFuture = FutureUtils.allOf(fileBlocksFutures);

    return FutureUtils.get(fileBlockListFuture);
  }

  private FileBlockSpec getFileBlockSpec(FileStatus file, Configuration conf) {
    HoodieTimer timer = new HoodieTimer().startTimer();
    try {
      final FileSystem fs = FSUtils.getFs(file.getPath().toString(), conf);
      final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, file.getLen());

      if (LOG.isDebugEnabled()) {
        LOG.debug("thread: {} getFileBlock cost: {} mills, file: {}", Thread.currentThread().getName(), timer.endTimer(), file);
      }
      return new FileBlockSpec(file, blocks);
    } catch (IOException e) {
      throw new HoodieIOException("get file blocks failed", e);
    }
  }

  static class FileStatusSpec {
    private List<FileStatus> fileStatuses;
    private long fileLength;
    private boolean unsplitable;

    public FileStatusSpec() {
    }

    public FileStatusSpec(List<FileStatus> fileStatuses, long fileLength, boolean unsplitable) {
      this.fileStatuses = fileStatuses;
      this.fileLength = fileLength;
      this.unsplitable = unsplitable;
    }

    public List<FileStatus> getFileStatuses() {
      return fileStatuses;
    }

    public void setFileStatuses(List<FileStatus> fileStatuses) {
      this.fileStatuses = fileStatuses;
    }

    public long getFileLength() {
      return fileLength;
    }

    public void setFileLength(long fileLength) {
      this.fileLength = fileLength;
    }

    public boolean getUnsplitable() {
      return unsplitable;
    }

    public void setUnsplitable(boolean unsplitable) {
      this.unsplitable = unsplitable;
    }
  }

  static class FileBlockSpec {
    FileStatus fileStatus;
    BlockLocation[] blockLocations;

    public FileBlockSpec() {
    }

    public FileBlockSpec(FileStatus filePath, BlockLocation[] blockLocations) {
      this.fileStatus = filePath;
      this.blockLocations = blockLocations;
    }

    public FileStatus getFileStatus() {
      return fileStatus;
    }

    public void setFileStatus(FileStatus fileStatus) {
      this.fileStatus = fileStatus;
    }

    public BlockLocation[] getBlockLocations() {
      return blockLocations;
    }

    public void setBlockLocations(BlockLocation[] blockLocations) {
      this.blockLocations = blockLocations;
    }
  }
}
