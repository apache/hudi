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

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.hudi.utilities.sources.helpers.DFSPathSelector.Config.ROOT_INPUT_PATH_PROP;

/**
 * This class provides various clean-up strategies for DeltaStreamer when reading from DFS file sources.
 * Each <code>*DFSSource</code> may invoke this to clean up/archive files after each successful commit.
 *
 */
public abstract class FileSourceCleaner {
  private static final Logger LOG = LogManager.getLogger(FileSourceCleaner.class);

  /**
   * Configs supported.
   */
  public static class Config {
    private Config() {}

    public static final String FILE_SOURCE_CLEAN_MODE_OPT_KEY = "hoodie.deltastreamer.source.dfs.clean.mode";
    public static final String DEFAULT_FILE_SOURCE_CLEAN_MODE_OPT_KEY = CleanMode.OFF.name();

    public static final String FILE_SOURCE_CLEAN_NUM_THREADS_OPT_KEY = "hoodie.deltastreamer.source.dfs.clean.numThreads";
    public static final int DEFAULT_FILE_SOURCE_CLEAN_NUM_THREADS_OPT_VAL = 1;

    public static final String FILE_SOURCE_ARCHIVE_DIR_KEY = "hoodie.deltastreamer.source.dfs.clean.archiveDir";
  }

  private enum CleanMode {
    /**
     * Remove source files after each successfully commit.
     */
    DELETE,
    /**
     * Move source files to specified archive directory after each successful commit.
     * Used in conjunction with <code>hoodie.deltastreamer.source.dfs.clean.archiveDir</code>
     */
    ARCHIVE,
    /**
     * Default option. Do not clean up source files.
     */
    OFF
  }

  protected final FileSystem fs;
  private final Option<ExecutorService> cleanerPool;

  protected FileSourceCleaner(TypedProperties props, FileSystem fs) {
    this.fs = fs;
    int numCleanerThreads = props.getInteger(Config.FILE_SOURCE_CLEAN_NUM_THREADS_OPT_KEY,
        Config.DEFAULT_FILE_SOURCE_CLEAN_NUM_THREADS_OPT_VAL);
    cleanerPool = (numCleanerThreads > 0) ? Option.of(Executors.newFixedThreadPool(numCleanerThreads)) : Option.empty();
  }

  /**
   * Factory method to create FileSourceCleaner based on properties.
   */
  public static FileSourceCleaner create(TypedProperties props, FileSystem fs) {
    final String cleanMode = props.getString(Config.FILE_SOURCE_CLEAN_MODE_OPT_KEY, Config.DEFAULT_FILE_SOURCE_CLEAN_MODE_OPT_KEY);
    switch (CleanMode.valueOf(cleanMode.toUpperCase())) {
      case DELETE:
        return new FileSourceRemover(props, fs);
      case ARCHIVE:
        return new FileSourceArchiver(props, fs);
      case OFF:
        return new FileSourceNoOpCleaner(props);
      default:
        throw new IllegalArgumentException(String.format("Unknown option %s for %s. Available options are: "
            + "delete, archive, off(default)", cleanMode, Config.FILE_SOURCE_CLEAN_MODE_OPT_KEY));
    }
  }

  /**
   * Clean up a file that has been ingested successfully.
   */
  public void clean(String file) {
    if (cleanerPool.isPresent()) {
      cleanerPool.get().submit(() -> cleanTask(file));
    } else {
      cleanTask(file);
    }
  }

  abstract void cleanTask(String file);

  private static class FileSourceRemover extends FileSourceCleaner {
    public FileSourceRemover(TypedProperties props, FileSystem fs) {
      super(props, fs);
    }

    @Override
    void cleanTask(String file) {

      LOG.info(String.format("Removing %s...", file));
      try {
        if (fs.delete(new Path(file), false)) {
          LOG.info(String.format("Successfully remove up %s", file));
        } else {
          LOG.warn(String.format("Failed to remove %s", file));
        }
      } catch (IOException e) {
        LOG.error(String.format("Failed to remove %s", file), e);
      }
    }
  }

  private static class FileSourceArchiver extends FileSourceCleaner {
    private final Path archiveRootDir;
    private final Path sourceRootDir;

    public FileSourceArchiver(TypedProperties props, FileSystem fs) {
      super(props, fs);
      this.archiveRootDir = new Path(props.getString(Config.FILE_SOURCE_ARCHIVE_DIR_KEY));
      this.sourceRootDir = new Path(props.getString(ROOT_INPUT_PATH_PROP));
      ValidationUtils.checkArgument(!isSubDir(archiveRootDir, sourceRootDir),
          String.format("%s must not be child of %s", Config.FILE_SOURCE_ARCHIVE_DIR_KEY, ROOT_INPUT_PATH_PROP));
    }

    private boolean isSubDir(Path childDir, Path parentDir) {
      while (childDir != null) {
        if (childDir.equals(parentDir)) {
          return true;
        }
        childDir = childDir.getParent();
      }
      return false;
    }

    @Override
    void cleanTask(String file) {
      try {
        final Path srcFile = new Path(file);
        FileStatus status = fs.getFileStatus(srcFile);
        if (status.isDirectory()) {
          LOG.warn(String.format("%s is a directory. Skip archiving", file));
          return;
        }
        final Path fileDir = srcFile.getParent();
        Path relativeDir = getRelativeDir(fileDir, sourceRootDir);
        final Path archiveDir = new Path(archiveRootDir, relativeDir);
        if (!fs.exists(archiveDir)) {
          LOG.info("Creating archive directory: " + archiveDir.toString());
          fs.mkdirs(archiveDir);
        }

        final Path dstFile = new Path(archiveDir, srcFile.getName());
        LOG.info(String.format("Renaming: %s to %s", srcFile.toString(), dstFile));
        if (fs.rename(srcFile, dstFile)) {
          LOG.info(String.format("Successfully archive %s", file));
        } else {
          LOG.warn(String.format("Failed to archive %s", file));
        }
      } catch (IOException e) {
        LOG.error(String.format("Failed to archive %s", file), e);
      }
    }

    /**
     * Get the relative Path of a sub-directory relative to its parent
     *
     * E.g. getRelativeDir('/foo/bar/x', '/foo') => 'bar'
     */
    private Path getRelativeDir(Path childPath, Path parentPath) {
      LinkedList<String> paths = new LinkedList<>();
      while (childPath != null && !childPath.equals(parentPath)) {
        paths.addFirst(childPath.getName());
        childPath = childPath.getParent();
      }
      // return '.' if childPath == parentPath
      return new Path(paths.isEmpty() ? "." : String.join("/", paths));
    }
  }

  private static class FileSourceNoOpCleaner extends FileSourceCleaner {
    protected FileSourceNoOpCleaner(TypedProperties props) {
      super(props, null);
    }

    @Override
    void cleanTask(String file) {
      LOG.info("No hoodie.deltastreamer.source.dfs.clean was specified. Leaving source unchanged.");
    }
  }
}
