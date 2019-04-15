/*
 * Copyright (c) 2019 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this path except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.io.storage;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.util.HadoopStreams;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

/**
 * HoodieOutputFile implements OutputFile such that the internal file system is HoodieWrapperFileSystem. It is very
 * similar to org.apache.parquet.hadoop.util.HadoopOutputFile
 */
public class HoodieOutputFile implements OutputFile {

  private static final int DFS_BUFFER_SIZE_DEFAULT = 4096;
  private static final Set<String> BLOCK_FS_SCHEMES = new HashSet<String>();

  static {
    BLOCK_FS_SCHEMES.add("hoodie-hdfs");
    BLOCK_FS_SCHEMES.add("hoodie-webhdfs");
    BLOCK_FS_SCHEMES.add("hoodie-viewfs");
  }

  private final HoodieWrapperFileSystem fs;
  private final Path path;
  private final Configuration conf;

  private HoodieOutputFile(FileSystem fs, Path path, Configuration conf) {
    this.fs = (HoodieWrapperFileSystem) fs;
    this.path = path;
    this.conf = conf;
  }

  public static HoodieOutputFile fromPath(Path path, Configuration conf) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    return new HoodieOutputFile(fs, fs.makeQualified(path), conf);
  }

  private static boolean supportsBlockSize(FileSystem fs) {
    return BLOCK_FS_SCHEMES.contains(fs.getUri().getScheme());
  }

  public HoodieWrapperFileSystem getFileSystem() {
    return fs;
  }

  @Override
  public PositionOutputStream create(long blockSizeHint) throws IOException {
    return HadoopStreams.wrap(fs.create(path, false, DFS_BUFFER_SIZE_DEFAULT, fs.getDefaultReplication(path),
        Math.max(fs.getDefaultBlockSize(path), blockSizeHint)));
  }

  @Override
  public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
    return HadoopStreams.wrap(fs.create(path, true, DFS_BUFFER_SIZE_DEFAULT, fs.getDefaultReplication(path),
        Math.max(fs.getDefaultBlockSize(path), blockSizeHint)));
  }

  @Override
  public boolean supportsBlockSize() {
    return supportsBlockSize(fs);
  }

  @Override
  public long defaultBlockSize() {
    return fs.getDefaultBlockSize(path);
  }

}
