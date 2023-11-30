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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.HoodieAvroWriteSupport;
import org.apache.hudi.common.util.AvroOrcUtils;
import org.apache.hudi.common.util.BaseFileUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieLocation;
import org.apache.hudi.storage.HoodieStorage;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.Writer;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The metadata that goes into the meta file in each partition.
 */
public class HoodiePartitionMetadata {

  public static final String HOODIE_PARTITION_METAFILE_PREFIX = ".hoodie_partition_metadata";
  public static final String COMMIT_TIME_KEY = "commitTime";
  private static final String PARTITION_DEPTH_KEY = "partitionDepth";
  private static final Logger LOG = LoggerFactory.getLogger(HoodiePartitionMetadata.class);

  /**
   * Contents of the metadata.
   */
  private final Properties props;

  /**
   * Path to the partition, about which we have the metadata.
   */
  private final HoodieLocation partitionPath;

  private final HoodieStorage storage;

  // The format in which to write the partition metadata
  private Option<HoodieFileFormat> format;

  /**
   * Construct metadata from existing partition.
   */
  public HoodiePartitionMetadata(HoodieStorage storage, HoodieLocation partitionPath) {
    this.storage = storage;
    this.props = new Properties();
    this.partitionPath = partitionPath;
    this.format = Option.empty();
  }

  /**
   * Construct metadata object to be written out.
   */
  public HoodiePartitionMetadata(HoodieStorage storage, String instantTime, HoodieLocation basePath, HoodieLocation partitionPath, Option<HoodieFileFormat> format) {
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
  public void trySave(int taskPartitionId) {
    String extension = getMetafileExtension();
    HoodieLocation tmpMetaPath =
        new HoodieLocation(partitionPath, HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX + "_" + taskPartitionId + extension);
    HoodieLocation metaPath = new HoodieLocation(partitionPath, HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX + extension);
    boolean metafileExists = false;

    try {
      metafileExists = storage.exists(metaPath);
      if (!metafileExists) {
        // write to temporary file
        writeMetafile(tmpMetaPath);
        // move to actual path
        storage.rename(tmpMetaPath, metaPath);
      }
    } catch (IOException ioe) {
      LOG.warn("Error trying to save partition metadata (this is okay, as long as at least 1 of these succeeded), "
          + partitionPath, ioe);
    } finally {
      if (!metafileExists) {
        try {
          // clean up tmp file, if still lying around
          if (storage.exists(tmpMetaPath)) {
            storage.delete(tmpMetaPath, false);
          }
        } catch (IOException ioe) {
          LOG.warn("Error trying to clean up temporary files for " + partitionPath, ioe);
        }
      }
    }
  }

  private String getMetafileExtension() {
    // To be backwards compatible, there is no extension to the properties file base partition metafile
    return format.isPresent() ? format.get().getFileExtension() : StringUtils.EMPTY_STRING;
  }

  /**
   * Write the partition metadata in the correct format in the given file path.
   *
   * @param filePath Path of the file to write
   * @throws IOException
   */
  private void writeMetafile(HoodieLocation filePath) throws IOException {
    if (format.isPresent()) {
      Schema schema = HoodieAvroUtils.getRecordKeySchema();

      switch (format.get()) {
        case PARQUET:
          // Since we are only interested in saving metadata to the footer, the schema, blocksizes and other
          // parameters are not important.
          MessageType type = Types.buildMessage().optional(PrimitiveTypeName.INT64).named("dummyint").named("dummy");
          HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(type, schema, Option.empty(), new Properties());
          try (ParquetWriter writer = new ParquetWriter(new Path(filePath.toString()), writeSupport, CompressionCodecName.UNCOMPRESSED, 1024, 1024)) {
            for (String key : props.stringPropertyNames()) {
              writeSupport.addFooterMetadata(key, props.getProperty(key));
            }
          }
          break;
        case ORC:
          // Since we are only interested in saving metadata to the footer, the schema, blocksizes and other
          // parameters are not important.
          FileSystem fs = (FileSystem) storage.getFileSystem();
          OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(fs.getConf()).fileSystem(fs)
              .setSchema(AvroOrcUtils.createOrcSchema(schema));
          try (Writer writer = OrcFile.createWriter(new Path(filePath.toString()), writerOptions)) {
            for (String key : props.stringPropertyNames()) {
              writer.addUserMetadata(key, ByteBuffer.wrap(props.getProperty(key).getBytes()));
            }
          }
          break;
        default:
          throw new HoodieException("Unsupported format for partition metafiles: " + format.get());
      }
    } else {
      // Backwards compatible properties file format
      FileSystem fs = (FileSystem) storage.getFileSystem();
      FSDataOutputStream os = fs.create(new Path(filePath.toString()), true);
      props.store(os, "partition metadata");
      os.hsync();
      os.hflush();
      os.close();
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
    HoodieLocation metafilePath = textFormatMetaFileLocation(partitionPath);
    try (InputStream is = storage.open(metafilePath)) {
      props.load(is);
      format = Option.empty();
      return true;
    } catch (Throwable t) {
      LOG.debug("Unable to read partition meta properties file for partition " + partitionPath);
      return false;
    }
  }

  private boolean readBaseFormatMetaFile() {
    for (HoodieLocation metafilePath : baseFormatMetaFileLocations(partitionPath)) {
      try {
        BaseFileUtils reader = BaseFileUtils.getInstance(metafilePath.toString());
        FileSystem fs = (FileSystem) storage.getFileSystem();
        // Data file format
        Map<String, String> metadata = reader.readFooter(fs.getConf(), true, new Path(metafilePath.toString()), PARTITION_DEPTH_KEY, COMMIT_TIME_KEY);
        props.clear();
        props.putAll(metadata);
        format = Option.of(reader.getFormat());
        return true;
      } catch (Throwable t) {
        LOG.debug("Unable to read partition metadata " + metafilePath.getName() + " for partition " + partitionPath);
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
      LOG.warn("Error fetch Hoodie partition metadata for " + partitionPath, ioe);
      return Option.empty();
    }
  }

  // methods related to partition meta data
  public static boolean hasPartitionMetadata(FileSystem fs, Path partitionPath) {
    try {
      return textFormatMetaPathIfExists(fs, partitionPath).isPresent()
          || baseFormatMetaPathIfExists(fs, partitionPath).isPresent();
    } catch (IOException ioe) {
      throw new HoodieIOException("Error checking presence of partition meta file for " + partitionPath, ioe);
    }
  }

  public static boolean hasPartitionMetadata(HoodieStorage storage, HoodieLocation partitionPath) {
    try {
      return textFormatMetaLocationIfExists(storage, partitionPath).isPresent()
          || baseFormatMetaLocationIfExists(storage, partitionPath).isPresent();
    } catch (IOException ioe) {
      throw new HoodieIOException("Error checking presence of partition meta file for " + partitionPath, ioe);
    }
  }

  /**
   * Returns the name of the partition metadata.
   *
   * @return Name of the partition metafile or empty option
   */
  public static Option<Path> getPartitionMetafilePath(FileSystem fs, Path partitionPath) {
    // The partition listing is a costly operation so instead we are searching for existence of the files instead.
    // This is in expected order as properties file based partition metafiles should be the most common.
    try {
      Option<Path> textFormatPath = textFormatMetaPathIfExists(fs, partitionPath);
      if (textFormatPath.isPresent()) {
        return textFormatPath;
      } else {
        return baseFormatMetaPathIfExists(fs, partitionPath);
      }
    } catch (IOException ioe) {
      throw new HoodieException("Error checking Hoodie partition metadata for " + partitionPath, ioe);
    }
  }

  public static Option<Path> baseFormatMetaPathIfExists(FileSystem fs, Path partitionPath) throws IOException {
    // Parquet should be more common than ORC so check it first
    for (Path metafilePath : baseFormatMetaFilePaths(partitionPath)) {
      if (fs.exists(metafilePath)) {
        return Option.of(metafilePath);
      }
    }
    return Option.empty();
  }

  public static Option<Path> textFormatMetaPathIfExists(FileSystem fs, Path partitionPath) throws IOException {
    Path path = textFormatMetaFilePath(partitionPath);
    return Option.ofNullable(fs.exists(path) ? path : null);
  }

  static Path textFormatMetaFilePath(Path partitionPath) {
    return new Path(partitionPath, HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX);
  }

  static List<Path> baseFormatMetaFilePaths(Path partitionPath) {
    return Stream.of(HoodieFileFormat.PARQUET.getFileExtension(), HoodieFileFormat.ORC.getFileExtension())
        .map(ext -> new Path(partitionPath, HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX + ext))
        .collect(Collectors.toList());
  }

  public static Option<HoodieLocation> baseFormatMetaLocationIfExists(HoodieStorage storage, HoodieLocation partitionPath) throws IOException {
    // Parquet should be more common than ORC so check it first
    for (HoodieLocation metafilePath : baseFormatMetaFileLocations(partitionPath)) {
      if (storage.exists(metafilePath)) {
        return Option.of(metafilePath);
      }
    }
    return Option.empty();
  }

  public static Option<HoodieLocation> textFormatMetaLocationIfExists(HoodieStorage storage, HoodieLocation partitionPath) throws IOException {
    HoodieLocation path = textFormatMetaFileLocation(partitionPath);
    return Option.ofNullable(storage.exists(path) ? path : null);
  }

  static HoodieLocation textFormatMetaFileLocation(HoodieLocation partitionPath) {
    return new HoodieLocation(partitionPath, HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX);
  }

  static List<HoodieLocation> baseFormatMetaFileLocations(HoodieLocation partitionPath) {
    return Stream.of(HoodieFileFormat.PARQUET.getFileExtension(), HoodieFileFormat.ORC.getFileExtension())
        .map(ext -> new HoodieLocation(partitionPath, HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX + ext))
        .collect(Collectors.toList());
  }
}
