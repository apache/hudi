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
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.orc.OrcFile;
import org.apache.orc.Writer;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * The metadata that goes into the meta file in each partition.
 */
public class HoodiePartitionMetadata {

  public static final String HOODIE_PARTITION_METAFILE_PREFIX = ".hoodie_partition_metadata";
  private static final String PARTITION_DEPTH_KEY = "partitionDepth";
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

  private static final Logger LOG = LogManager.getLogger(HoodiePartitionMetadata.class);

  // The format in which to write the partition metadata
  private Option<HoodieFileFormat> format;

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
  public HoodiePartitionMetadata(FileSystem fs, String instantTime, Path basePath, Path partitionPath, Option<HoodieFileFormat> format) {
    this(fs, partitionPath);
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
    Path tmpMetaPath =
        new Path(partitionPath, HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX + "_" + taskPartitionId + extension);
    Path metaPath = new Path(partitionPath, HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX + extension);
    boolean metafileExists = false;

    try {
      metafileExists = fs.exists(metaPath);
      if (!metafileExists) {
        // write to temporary file
        writeMetafile(tmpMetaPath);

        // move to actual path
        fs.rename(tmpMetaPath, metaPath);
      }
    } catch (IOException ioe) {
      LOG.warn("Error trying to save partition metadata (this is okay, as long as atleast 1 of these succced), "
          + partitionPath, ioe);
    } finally {
      if (!metafileExists) {
        try {
          // clean up tmp file, if still lying around
          if (fs.exists(tmpMetaPath)) {
            fs.delete(tmpMetaPath, false);
          }
        } catch (IOException ioe) {
          LOG.warn("Error trying to clean up temporary files for " + partitionPath, ioe);
        }
      }
    }
  }

  private String getMetafileExtension() {
    // To be backwards compatible, there is no extension to the properties file base partition metafile
    return format.isPresent() ? format.get().getFileExtension() : "";
  }

  /**
   * Write the partition metadata in the correct format in the given file path.
   *
   * @param filePath Path of the file to write
   * @throws IOException
   */
  private void writeMetafile(Path filePath) throws IOException {
    if (format.isPresent()) {
      Schema schema = HoodieAvroUtils.getRecordKeySchema();

      switch (format.get()) {
        case PARQUET:
          // Since we are only interested in saving metadata to the footer, the schema, blocksizes and other
          // parameters are not important.
          MessageType type = Types.buildMessage().optional(PrimitiveTypeName.INT64).named("dummyint").named("dummy");
          HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(type, schema, Option.empty());
          try (ParquetWriter writer = new ParquetWriter(filePath, writeSupport, CompressionCodecName.UNCOMPRESSED, 1024, 1024)) {
            for (String key : props.stringPropertyNames()) {
              writeSupport.addFooterMetadata(key, props.getProperty(key));
            }
          }
          break;
        case ORC:
          // Since we are only interested in saving metadata to the footer, the schema, blocksizes and other
          // parameters are not important.
          OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(fs.getConf()).fileSystem(fs)
              .setSchema(AvroOrcUtils.createOrcSchema(schema));
          try (Writer writer = OrcFile.createWriter(filePath, writerOptions)) {
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
      FSDataOutputStream os = fs.create(filePath, true);
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
    Option<Path> metafilePathOption = getPartitionMetafilePath(fs, partitionPath);
    if (!metafilePathOption.isPresent()) {
      throw new HoodieException("Partition metafile not found in path " + partitionPath);
    }

    final Path metafilePath = metafilePathOption.get();
    BaseFileUtils reader = null;
    try {
      reader = BaseFileUtils.getInstance(metafilePath.toString());
    } catch (UnsupportedOperationException e) {
      // This exception implies the metafile was not in a data file format
      ValidationUtils.checkArgument(metafilePath.toString().endsWith(HOODIE_PARTITION_METAFILE_PREFIX),
          "Partition metafile is not in properties file format: " + metafilePath);
    }

    if (reader != null) {
      // Data file format
      Map<String, String> metadata = reader.readFooter(fs.getConf(), true, metafilePath, PARTITION_DEPTH_KEY, COMMIT_TIME_KEY);
      props.clear();
      metadata.forEach((k, v) -> props.put(k, v));
    } else {
      // Properties file format
      FSDataInputStream is = null;
      try {
        is = fs.open(metafilePath);
        props.load(is);
      } catch (IOException ioe) {
        throw new HoodieException("Error reading Hoodie partition metadata from " + metafilePath, ioe);
      } finally {
        if (is != null) {
          is.close();
        }
      }
    }
  }

  /**
   * Read out the COMMIT_TIME_KEY metadata for this partition.
   */
  public Option<String> readPartitionCreatedCommitTime() {
    try {
      if (props.containsKey(COMMIT_TIME_KEY)) {
        return Option.of(props.getProperty(COMMIT_TIME_KEY));
      } else {
        readFromFS();
        return Option.of(props.getProperty(COMMIT_TIME_KEY));
      }
    } catch (IOException ioe) {
      LOG.warn("Error fetch Hoodie partition metadata for " + partitionPath, ioe);
      return Option.empty();
    }
  }

  // methods related to partition meta data
  public static boolean hasPartitionMetadata(FileSystem fs, Path partitionPath) {
    return getPartitionMetafilePath(fs, partitionPath).isPresent();
  }

  /**
   * Returns the name of the partition metadata.
   *
   * @param fs
   * @param partitionPath
   * @return Name of the partition metafile or empty option
   */
  public static Option<Path> getPartitionMetafilePath(FileSystem fs, Path partitionPath) {
    // The partition listing is a costly operation so instead we are searching for existence of the files instead.
    // This is in expected order as properties file based partition metafiles should be the most common.
    try {
      Path metafilePath = new Path(partitionPath, HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX);
      if (fs.exists(metafilePath)) {
        return Option.of(metafilePath);
      }
      // Parquet should be more common than ORC so check it first
      for (String extension : Arrays.asList(HoodieFileFormat.PARQUET.getFileExtension(),
          HoodieFileFormat.ORC.getFileExtension())) {
        metafilePath = new Path(partitionPath, HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX + extension);
        if (fs.exists(metafilePath)) {
          return Option.of(metafilePath);
        }
      }
    } catch (IOException ioe) {
      throw new HoodieException("Error checking Hoodie partition metadata for " + partitionPath, ioe);
    }

    return Option.empty();
  }
}
