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

package org.apache.hudi.parquet.io;

import org.apache.hudi.util.HoodieFileMetadataMerger;
import org.apache.hudi.io.storage.HoodieFileBinaryCopier;
import org.apache.hudi.storage.StoragePath;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.Preconditions;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.util.CompressionConverter;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * HoodieParquetFileBinaryCopier is a high-performance utility designed for efficient merging of Parquet files at the binary level.
 * Unlike conventional Parquet writers, it bypasses costly data processing operations through a block-based approach:
 *
 * Core Capabilities:
 * 1. Zero-Processing Merge
 *    Directly concatenates raw Parquet data blocks (row groups) from input files
 *    Avoids:
 *      1) Data serialization/deserialization
 *      2) Compression/decompression cycles
 *      3) Record-level reprocessing
 *
 * 2. Metadata Reconstruction
 *    Dynamically rebuilds file metadata:
 *    1) New footer with merged statistics
 *    2) Updated row group offsets
 *    3) Validated schema consistency
 */
public class HoodieParquetFileBinaryCopier extends HoodieParquetBinaryCopyBase implements HoodieFileBinaryCopier {
  
  private static final Logger LOG = LoggerFactory.getLogger(HoodieParquetFileBinaryCopier.class);
  private final CompressionCodecName codecName;

  // Reader and relevant states of the in-processing input file
  private Queue<CompressionConverter.TransParquetFileReader> inputFiles = new LinkedList<>();

  private Map<String, String> extraMetaData = new HashMap<>();

  // The reader for the current input file
  private CompressionConverter.TransParquetFileReader reader = null;

  private HoodieFileMetadataMerger metadataMerger;

  public HoodieParquetFileBinaryCopier(
      Configuration conf,
      CompressionCodecName codecName,
      HoodieFileMetadataMerger metadataMerger) {
    super(conf);
    this.metadataMerger = metadataMerger;
    this.codecName = codecName;
  }

  @Override
  protected Map<String, String> finalizeMetadata() {
    return this.extraMetaData;
  }

  /**
   * Merge all inputFilePaths to outputFilePath at block level
   */
  @Override
  public long binaryCopy(List<StoragePath> inputFilePaths,
                         List<StoragePath> outputFilePath,
                         MessageType writeSchema,
                         boolean schemaEvolutionEnabled) throws IOException {
    // Set schema evolution enabled flag
    setSchemaEvolutionEnabled(schemaEvolutionEnabled);
    
    openInputFiles(inputFilePaths, conf);
    initFileWriter(new Path(outputFilePath.get(0).toUri()), codecName, writeSchema);
    initNextReader();

    Set<String> allOriginalCreatedBys = new HashSet<>();
    while (reader != null) {
      List<BlockMetaData> rowGroups = reader.getRowGroups();
      FileMetaData fileMetaData = reader.getFooter().getFileMetaData();
      String createdBy = fileMetaData.getCreatedBy();
      allOriginalCreatedBys.add(createdBy);
      Map<String, String> metaMap = fileMetaData.getKeyValueMetaData();
      metadataMerger.mergeMetaData(metaMap);

      for (BlockMetaData block : rowGroups) {
        processBlocksFromReader(reader, reader.readNextRowGroup(), block, createdBy);
      }
      initNextReader();
    }
    extraMetaData.putAll(metadataMerger.getMergedMetaData());
    extraMetaData.put(ORIGINAL_CREATED_BY_KEY, String.join("\n", allOriginalCreatedBys));
    return totalRecordsWritten;
  }

  // Open all input files to validate their schemas are compatible to merge
  private void openInputFiles(List<StoragePath> inputFiles, Configuration conf) {
    Preconditions.checkArgument(inputFiles != null && !inputFiles.isEmpty(), "No input files");

    for (StoragePath inputFile : inputFiles) {
      try {
        CompressionConverter.TransParquetFileReader reader = new CompressionConverter.TransParquetFileReader(
            HadoopInputFile.fromPath(new Path(inputFile.toUri()), conf),
            HadoopReadOptions.builder(conf).build());
        this.inputFiles.add(reader);
      } catch (IOException e) {
        throw new IllegalArgumentException("Failed to open input file: " + inputFile, e);
      }
    }
  }

  // Routines to get reader of next input file and set up relevant states
  private void initNextReader() throws IOException {
    if (reader != null) {
      reader.close();
      LOG.info("Finish binary copy input file: {}", reader.getFile());
    }

    if (inputFiles.isEmpty()) {
      reader = null;
      return;
    }

    reader = inputFiles.poll();
    LOG.info("Merging input file: {}, remaining files: {}", reader.getFile(), inputFiles.size());
  }
}
