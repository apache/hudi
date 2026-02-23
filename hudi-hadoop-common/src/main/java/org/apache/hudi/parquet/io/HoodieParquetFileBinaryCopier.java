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

import org.apache.hudi.io.storage.HoodieFileBinaryCopier;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.util.HoodieFileMetadataMerger;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.Preconditions;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.util.CompressionConverter;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * HoodieParquetFileBinaryCopier is a high-performance utility designed for efficient merging of Parquet files at the binary level.
 * Unlike conventional Parquet writers, it bypasses costly data processing operations through a block-based approach:
 * <p>
 * Core Capabilities:
 * 1. Zero-Processing Merge
 * Directly concatenates raw Parquet data blocks (row groups) from input files
 * Avoids:
 * 1) Data serialization/deserialization
 * 2) Compression/decompression cycles
 * 3) Record-level reprocessing
 * <p>
 * 2. Metadata Reconstruction
 * Dynamically rebuilds file metadata:
 * 1) New footer with merged statistics
 * 2) Updated row group offsets
 * 3) Validated schema consistency
 */
@Slf4j
public class HoodieParquetFileBinaryCopier extends HoodieParquetBinaryCopyBase implements HoodieFileBinaryCopier {

  private final CompressionCodecName codecName;

  // Queue of input files to be processed
  private Queue<StoragePath> inputFiles = new LinkedList<>();

  private Map<String, String> extraMetaData = new HashMap<>();

  // The reader for the current input file
  private CompressionConverter.TransParquetFileReader reader = null;

  private HoodieFileMetadataMerger metadataMerger;

  // Executor for prefetching files
  private ExecutorService prefetchExecutor;
  private CompletableFuture<PrefetchResult> nextFileContentFuture;
  private StoragePath nextFileToPrefetch;

  // Double buffering for file content
  private byte[] currentBuffer = null;
  private byte[] nextBuffer = null;

  public HoodieParquetFileBinaryCopier(Configuration conf, CompressionCodecName codecName, HoodieFileMetadataMerger metadataMerger) {
    super(conf);
    this.metadataMerger = metadataMerger;
    this.codecName = codecName;
    // Single thread for sequential prefetching
    this.prefetchExecutor = Executors.newSingleThreadExecutor();
  }

  @Override
  protected Map<String, String> finalizeMetadata() {
    return this.extraMetaData;
  }

  /**
   * Merge all inputFilePaths to outputFilePath at block level
   */
  @Override
  public long binaryCopy(List<StoragePath> inputFilePaths, List<StoragePath> outputFilePath, MessageType writeSchema, boolean schemaEvolutionEnabled) throws IOException {
    // Set schema evolution enabled flag
    setSchemaEvolutionEnabled(schemaEvolutionEnabled);

    openInputFiles(inputFilePaths, conf);
    initFileWriter(new Path(outputFilePath.get(0).toUri()), codecName, writeSchema);

    // Start prefetching the first file
    triggerPrefetch();

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
        // Pass null for PageReadStore to avoid reading the whole row group into memory.
        // This avoids double reading of data (once for PageReadStore, once for binary copy).
        // The processBlocksFromReader method handles null store by synthesizing masked columns
        // (like _hoodie_file_name) and using stream copy for others.
        processBlocksFromReader(reader, null, block, createdBy);
      }
      initNextReader();
    }
    extraMetaData.putAll(metadataMerger.getMergedMetaData());
    extraMetaData.put(ORIGINAL_CREATED_BY_KEY, String.join("\n", allOriginalCreatedBys));
    return totalRecordsWritten;
  }

  @Override
  public void close() throws IOException {
    super.close();
    if (prefetchExecutor != null) {
      prefetchExecutor.shutdownNow();
    }
    // Release buffers
    currentBuffer = null;
    nextBuffer = null;
  }

  // Queue input files to be processed
  private void openInputFiles(List<StoragePath> inputFiles, Configuration conf) {
    Preconditions.checkArgument(inputFiles != null && !inputFiles.isEmpty(), "No input files");
    this.inputFiles.addAll(inputFiles);
  }

  private static class PrefetchResult {
    final byte[] buffer;
    final int length;

    PrefetchResult(byte[] buffer, int length) {
      this.buffer = buffer;
      this.length = length;
    }
  }

  private void triggerPrefetch() {
    if (inputFiles.isEmpty()) {
      nextFileContentFuture = null;
      nextFileToPrefetch = null;
      return;
    }

    final StoragePath fileToPrefetch = inputFiles.poll();
    nextFileToPrefetch = fileToPrefetch;

    // Capture the buffer to use for this prefetch
    final byte[] bufferToUse = nextBuffer;

    nextFileContentFuture = CompletableFuture.supplyAsync(() -> {
      try {
        Path path = new Path(fileToPrefetch.toUri());
        FileSystem fs = path.getFileSystem(conf);
        long fileLen = fs.getFileStatus(path).getLen();

        if (fileLen > Integer.MAX_VALUE) {
          log.warn("File {} is too large ({} bytes) for in-memory processing. Skipping prefetch.", fileToPrefetch, fileLen);
          return null;
        }

        int requiredSize = (int) fileLen;
        byte[] targetBuffer = bufferToUse;

        // Resize buffer if needed
        if (targetBuffer == null || targetBuffer.length < requiredSize) {
          // Allocate with some padding (25%) to reduce future reallocations
          int newSize = requiredSize + (requiredSize / 4);
          if (newSize < 0) {
            newSize = requiredSize;
          }
          targetBuffer = new byte[newSize];
        }

        try (FSDataInputStream is = fs.open(path)) {
          is.readFully(targetBuffer, 0, requiredSize);
        }
        return new PrefetchResult(targetBuffer, requiredSize);
      } catch (IOException e) {
        log.error("Failed to prefetch file: " + fileToPrefetch, e);
        throw new RuntimeException(e);
      }
    }, prefetchExecutor);
  }

  // Routines to get reader of next input file and set up relevant states
  private void initNextReader() throws IOException {
    if (reader != null) {
      reader.close();
      log.info("Finish binary copy input file: {}", reader.getFile());
    }

    // If we have no future and no more files, we are done
    if (nextFileContentFuture == null && inputFiles.isEmpty() && nextFileToPrefetch == null) {
      reader = null;
      return;
    }

    // If we haven't triggered prefetch yet (should be handled by triggerPrefetch call in binaryCopy), do it now
    if (nextFileContentFuture == null && !inputFiles.isEmpty()) {
      triggerPrefetch();
    }

    // If still null, we are done
    if (nextFileContentFuture == null) {
      reader = null;
      return;
    }

    StoragePath currentFile = nextFileToPrefetch;
    PrefetchResult result = null;
    try {
      result = nextFileContentFuture.join();
    } catch (Exception e) {
      throw new IOException("Failed to retrieve prefetched content for " + currentFile, e);
    }

    // Update buffers for double buffering
    if (result != null) {
      // The buffer returned by the future becomes our current buffer
      // The old current buffer becomes the next buffer for the next prefetch
      byte[] oldCurrent = currentBuffer;
      currentBuffer = result.buffer;
      nextBuffer = oldCurrent;
    }

    // Trigger prefetch for the NEXT file immediately, using the now-free 'nextBuffer'
    triggerPrefetch();

    try {
      if (result != null) {
        InputFile inMemoryFile = new ByteArrayInputFile(currentBuffer, 0, result.length, currentFile.toString());
        reader = new CompressionConverter.TransParquetFileReader(inMemoryFile, HadoopReadOptions.builder(conf).build());
      } else {
        // Fallback to stream reading if content is null (too large or error)
        reader = new CompressionConverter.TransParquetFileReader(HadoopInputFile.fromPath(new Path(currentFile.toUri()), conf), HadoopReadOptions.builder(conf).build());
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to open input file: " + currentFile, e);
    }
    log.info("Merging input file: {}, remaining files: {}", reader.getFile(), inputFiles.size());
  }

  /**
   * An in-memory implementation of InputFile backed by a byte array.
   */
  private static class ByteArrayInputFile implements InputFile {
    private final byte[] content;
    private final int offset;
    private final int length;
    private final String fileName;

    public ByteArrayInputFile(byte[] content, int offset, int length, String fileName) {
      this.content = content;
      this.offset = offset;
      this.length = length;
      this.fileName = fileName;
    }

    @Override
    public long getLength() throws IOException {
      return length;
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
      return new ByteArraySeekableInputStream(content, offset, length);
    }
  }
}
