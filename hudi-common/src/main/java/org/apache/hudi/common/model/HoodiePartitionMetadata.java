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

package org.apache.hudi.common.model;

import org.apache.hudi.exception.HoodieException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * The metadata that goes into the meta file in each partition.
 */
public class HoodiePartitionMetadata {

  public static final String HOODIE_PARTITION_METAFILE = ".hoodie_partition_metadata";
  public static final String PARTITION_DEPTH_KEY = "partitionDepth";
  public static final String COMMIT_TIME_KEY = "commitTime";

  /**
   * Contents of the metadata.
   */
  private final Properties props;

  /**
   * Path to the partition, about which we have the metadata.
   */
  private final Path partitionPath;

  private final FileSystem fs;

  private static final Logger LOG = LoggerFactory.getLogger(HoodiePartitionMetadata.class);

  /**
   * Construct metadata from existing partition.
   */
  public HoodiePartitionMetadata(FileSystem fs, Path partitionPath) {
    this.fs = fs;
    this.props = new Properties();
    this.partitionPath = partitionPath;
  }

  /**
   * Construct metadata object to be written out.
   */
  public HoodiePartitionMetadata(FileSystem fs, String commitTime, Path basePath, Path partitionPath) {
    this(fs, partitionPath);
    props.setProperty(COMMIT_TIME_KEY, commitTime);
    props.setProperty(PARTITION_DEPTH_KEY, String.valueOf(partitionPath.depth() - basePath.depth()));
  }

  public int getPartitionDepth() {
    if (!props.containsKey(PARTITION_DEPTH_KEY)) {
      throw new HoodieException("Could not find partitionDepth in partition metafile");
    }
    return Integer.parseInt(props.getProperty(PARTITION_DEPTH_KEY));
  }

  /**
   * Write the metadata safely into partition atomically.
   */
  public void trySave(int taskPartitionId) {
    Path tmpMetaPath =
        new Path(partitionPath, HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE + "_" + taskPartitionId);
    Path metaPath = new Path(partitionPath, HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE);
    boolean metafileExists = false;

    try {
      metafileExists = fs.exists(metaPath);
      if (!metafileExists) {
        // write to temporary file
        FSDataOutputStream os = fs.create(tmpMetaPath, true);
        props.store(os, "partition metadata");
        os.hsync();
        os.hflush();
        os.close();

        // move to actual path
        fs.rename(tmpMetaPath, metaPath);
      }
    } catch (IOException ioe) {
      LOG.warn("Error trying to save partition metadata (this is okay, as long as atleast 1 of these succced), {}", partitionPath, ioe);
    } finally {
      if (!metafileExists) {
        try {
          // clean up tmp file, if still lying around
          if (fs.exists(tmpMetaPath)) {
            fs.delete(tmpMetaPath, false);
          }
        } catch (IOException ioe) {
          LOG.warn("Error trying to clean up temporary files for {}", partitionPath, ioe);
        }
      }
    }
  }

  /**
   * Read out the metadata for this partition.
   */
  public void readFromFS() throws IOException {
    FSDataInputStream is = null;
    try {
      Path metaFile = new Path(partitionPath, HOODIE_PARTITION_METAFILE);
      is = fs.open(metaFile);
      props.load(is);
    } catch (IOException ioe) {
      throw new HoodieException("Error reading Hoodie partition metadata for " + partitionPath, ioe);
    } finally {
      if (is != null) {
        is.close();
      }
    }
  }

  // methods related to partition meta data
  public static boolean hasPartitionMetadata(FileSystem fs, Path partitionPath) {
    try {
      return fs.exists(new Path(partitionPath, HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE));
    } catch (IOException ioe) {
      throw new HoodieException("Error checking Hoodie partition metadata for " + partitionPath, ioe);
    }
  }
}
