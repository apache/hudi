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
import org.apache.hudi.table.format.cow.vector.reader.ParquetColumnarRowSplitReader;
import org.apache.hudi.util.DataTypeUtils;

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
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

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
  private final long limit;

  private transient ParquetColumnarRowSplitReader reader;
  private transient long currentReadCount;

  /**
   * Files filter for determining what files/directories should be included.
   */
  private FilePathFilter localFilesFilter = new GlobFilePathFilter();

  public CopyOnWriteInputFormat(
      Path[] paths,
      String[] fullFieldNames,
      DataType[] fullFieldTypes,
      int[] selectedFields,
      String partDefaultName,
      long limit,
      Configuration conf,
      boolean utcTimestamp) {
    super.setFilePaths(paths);
    this.limit = limit;
    this.partDefaultName = partDefaultName;
    this.fullFieldNames = fullFieldNames;
    this.fullFieldTypes = fullFieldTypes;
    this.selectedFields = selectedFields;
    this.conf = new SerializableConfiguration(conf);
    this.utcTimestamp = utcTimestamp;
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

    // take the desired number of splits into account
    minNumSplits = Math.max(minNumSplits, this.numSplits);

    final List<FileInputSplit> inputSplits = new ArrayList<>(minNumSplits);

    // get all the files that are involved in the splits
    List<FileStatus> files = new ArrayList<>();
    long totalLength = 0;

    for (Path path : getFilePaths()) {
      final org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(path.toUri());
      final FileSystem fs = FSUtils.getFs(hadoopPath.toString(), this.conf.conf());
      final FileStatus pathFile = fs.getFileStatus(hadoopPath);

      if (pathFile.isDirectory()) {
        totalLength += addFilesInDir(hadoopPath, files, true);
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
        final FileSystem fs = FSUtils.getFs(file.getPath().toString(), this.conf.conf());
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

      final FileSystem fs = FSUtils.getFs(file.getPath().toString(), this.conf.conf());
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
        final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, len);
        Arrays.sort(blocks, new Comparator<BlockLocation>() {
          @Override
          public int compare(BlockLocation o1, BlockLocation o2) {
            long diff = o1.getLength() - o2.getOffset();
            return Long.compare(diff, 0L);
          }
        });

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
}
