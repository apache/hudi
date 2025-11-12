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

import org.apache.hudi.common.util.FileFormatUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.RetryHelper;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.HoodieInstantWriter;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.HoodieTableMetaClient.COMMIT_TIME_KEY;

/**
 * The metadata that goes into the meta file in each partition.
 */
public class HoodiePartitionMetadata {

  public static final String HOODIE_PARTITION_METAFILE_PREFIX = ".hoodie_partition_metadata";
  private static final String PARTITION_DEPTH_KEY = "partitionDepth";
  private static final Logger LOG = LoggerFactory.getLogger(HoodiePartitionMetadata.class);

  /**
   * Contents of the metadata.
   */
  private final Properties props;

  /**
   * Path to the partition, about which we have the metadata.
   */
  private final StoragePath partitionPath;

  private final HoodieStorage storage;

  // The format in which to write the partition metadata
  private Option<HoodieFileFormat> format;

  /**
   * Construct metadata from existing partition.
   */
  public HoodiePartitionMetadata(HoodieStorage storage, StoragePath partitionPath) {
    this.storage = storage;
    this.props = new Properties();
    this.partitionPath = partitionPath;
    this.format = Option.empty();
  }

  /**
   * Construct metadata object to be written out.
   */
  public HoodiePartitionMetadata(HoodieStorage storage, String instantTime, StoragePath basePath, StoragePath partitionPath, Option<HoodieFileFormat> format) {
    this(storage, partitionPath);
    this.format = format;
    props.setProperty(COMMIT_TIME_KEY, instantTime);
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
  public void trySave() throws HoodieIOException {
    StoragePath metaPath = new StoragePath(
        partitionPath, HOODIE_PARTITION_METAFILE_PREFIX + getMetafileExtension());

    // This retry mechanism enables an exit-fast in metaPath exists check, which avoid the
    // tasks failures when there are two or more tasks trying to create the same metaPath.
    RetryHelper<Void, HoodieIOException>  retryHelper = new RetryHelper(1000, 3, 1000, HoodieIOException.class.getName())
        .tryWith(() -> {
          if (!storage.exists(metaPath)) {
            if (format.isPresent()) {
              writeMetafileInFormat(metaPath, format.get());
            } else {
              // Backwards compatible properties file format
              try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
                props.store(os, "partition metadata");
                Option<byte []> content = Option.of(os.toByteArray());
                storage.createImmutableFileInPath(metaPath, content.map(HoodieInstantWriter::convertByteArrayToWriter));
              }
            }
          }
          return null;
        });
    retryHelper.start();
  }

  private String getMetafileExtension() {
    // To be backwards compatible, there is no extension to the properties file base partition metafile
    return format.isPresent() ? format.get().getFileExtension() : StringUtils.EMPTY_STRING;
  }

  /**
   * Write the partition metadata in the correct format in the given file path.
   *
   * @param filePath Path of the file to write
   * @param format Hoodie table file format
   * @throws IOException
   */
  private void writeMetafileInFormat(StoragePath filePath, HoodieFileFormat format) throws IOException {
    StoragePath tmpPath = new StoragePath(partitionPath,
        HOODIE_PARTITION_METAFILE_PREFIX + "_" + UUID.randomUUID() + getMetafileExtension());
    try {
      // write to temporary file
      HoodieIOFactory.getIOFactory(storage).getFileFormatUtils(format)
          .writeMetaFile(storage, tmpPath, props);
      // move to actual path
      storage.rename(tmpPath, filePath);
    } finally {
      try {
        // clean up tmp file, if still lying around
        if (storage.exists(tmpPath)) {
          storage.deleteFile(tmpPath);
        }
      } catch (IOException ioe) {
        LOG.info("Error trying to clean up temporary files for {}", partitionPath, ioe);
      }
    }
  }

  /**
   * Read out the metadata for this partition.
   */
  public void readFromFS() throws IOException {
    // first try reading the text format (legacy, currently widespread)
    boolean readFile = readTextFormatMetaFile();
    if (!readFile) {
      // now try reading the base file formats.
      readFile = readBaseFormatMetaFile();
    }

    // throw exception.
    if (!readFile) {
      throw new HoodieException("Unable to read any partition meta file to locate the table timeline.");
    }
  }

  private boolean readTextFormatMetaFile() {
    // Properties file format
    StoragePath metafilePath = textFormatMetaFilePath(partitionPath);
    try (InputStream is = storage.open(metafilePath)) {
      props.load(is);
      format = Option.empty();
      return true;
    } catch (Throwable t) {
      LOG.debug("Unable to read partition meta properties file for partition {}", partitionPath);
      return false;
    }
  }

  private boolean readBaseFormatMetaFile() {
    for (StoragePath metafilePath : baseFormatMetaFilePaths(partitionPath)) {
      try {
        FileFormatUtils reader = HoodieIOFactory.getIOFactory(storage)
            .getFileFormatUtils(metafilePath);
        // Data file format
        Map<String, String> metadata = reader.readFooter(
            storage, true, metafilePath, PARTITION_DEPTH_KEY, COMMIT_TIME_KEY);
        props.clear();
        props.putAll(metadata);
        format = Option.of(reader.getFormat());
        return true;
      } catch (Throwable t) {
        LOG.debug("Unable to read partition metadata {} for partition {}", metafilePath.getName(), partitionPath);
      }
    }
    return false;
  }

  /**
   * Read out the COMMIT_TIME_KEY metadata for this partition.
   */
  public Option<String> readPartitionCreatedCommitTime() {
    try {
      if (!props.containsKey(COMMIT_TIME_KEY)) {
        readFromFS();
      }
      return Option.of(props.getProperty(COMMIT_TIME_KEY));
    } catch (IOException ioe) {
      LOG.warn("Error fetch Hoodie partition metadata for {}", partitionPath, ioe);
      return Option.empty();
    }
  }

  public static boolean hasPartitionMetadata(HoodieStorage storage, StoragePath partitionPath) {
    try {
      return textFormatMetaPathIfExists(storage, partitionPath).isPresent()
          || baseFormatMetaPathIfExists(storage, partitionPath).isPresent();
    } catch (IOException ioe) {
      throw new HoodieIOException("Error checking presence of partition meta file for " + partitionPath, ioe);
    }
  }

  /**
   * Returns the name of the partition metadata.
   *
   * @return Name of the partition metafile or empty option
   */
  public static Option<StoragePath> getPartitionMetafilePath(HoodieStorage storage, StoragePath partitionPath) {
    // The partition listing is a costly operation so instead we are searching for existence of the files instead.
    // This is in expected order as properties file based partition metafiles should be the most common.
    try {
      Option<StoragePath> textFormatPath = textFormatMetaPathIfExists(storage, partitionPath);
      if (textFormatPath.isPresent()) {
        return textFormatPath;
      } else {
        return baseFormatMetaPathIfExists(storage, partitionPath);
      }
    } catch (IOException ioe) {
      throw new HoodieException("Error checking Hoodie partition metadata for " + partitionPath, ioe);
    }
  }

  public static Option<StoragePath> baseFormatMetaPathIfExists(HoodieStorage storage, StoragePath partitionPath) throws IOException {
    // Parquet should be more common than ORC so check it first
    for (StoragePath metafilePath : baseFormatMetaFilePaths(partitionPath)) {
      if (storage.exists(metafilePath)) {
        return Option.of(metafilePath);
      }
    }
    return Option.empty();
  }

  public static Option<StoragePath> textFormatMetaPathIfExists(HoodieStorage storage, StoragePath partitionPath) throws IOException {
    StoragePath path = textFormatMetaFilePath(partitionPath);
    return Option.ofNullable(storage.exists(path) ? path : null);
  }

  static StoragePath textFormatMetaFilePath(StoragePath partitionPath) {
    return new StoragePath(partitionPath, HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX);
  }

  static List<StoragePath> baseFormatMetaFilePaths(StoragePath partitionPath) {
    return Stream.of(HoodieFileFormat.PARQUET.getFileExtension(), HoodieFileFormat.ORC.getFileExtension())
        .map(ext -> new StoragePath(partitionPath, HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX + ext))
        .collect(Collectors.toList());
  }
}
