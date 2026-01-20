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

package org.apache.hudi.io.storage;

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.io.lance.HoodieBaseLanceWriter;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import com.lancedb.lance.spark.arrow.LanceArrowWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

/**
 * Lance stream writer that writes to an OutputStream using a temporary file.
 *
 * Since the Lance library (LanceFileWriter) does not support direct OutputStream writes
 * and only accepts file paths, this writer:
 * 1. Creates a temporary Lance file in the system temp directory
 * 2. Writes records using standard HoodieBaseLanceWriter logic
 * 3. On close(), copies the temp file contents to the target OutputStream
 * 4. Cleans up the temporary file
 *
 * This approach mirrors HoodieSparkParquetStreamWriter's pattern and is suitable
 * for log block serialization where files are typically small (1-10 MB).
 *
 * Note: This writer does NOT populate Hudi metadata fields (same limitation as
 * HoodieSparkParquetStreamWriter). It is designed for writing data blocks where
 * metadata has been pre-embedded in the InternalRow records.
 */
public class HoodieSparkLanceStreamWriter extends HoodieBaseLanceWriter<InternalRow>
    implements HoodieSparkFileWriter, AutoCloseable {

  private static final String DEFAULT_TIMEZONE = "UTC";
  private static final int COPY_BUFFER_SIZE = 8192;

  private final FSDataOutputStream outputStream;
  private final StructType sparkSchema;
  private final Schema arrowSchema;
  private final File tempFile;
  private LanceArrowWriter writer;

  /**
   * Constructor for Lance stream writer.
   *
   * @param outputStream Target output stream to write Lance data to
   * @param sparkSchema Spark schema for the data
   * @param storage HoodieStorage instance
   * @throws IOException if temp file creation fails
   */
  public HoodieSparkLanceStreamWriter(FSDataOutputStream outputStream,
                                      StructType sparkSchema,
                                      HoodieStorage storage) throws IOException {
    super(storage, createTempPath(), DEFAULT_BATCH_SIZE);
    this.outputStream = outputStream;
    this.sparkSchema = sparkSchema;
    this.arrowSchema = LanceArrowUtils.toArrowSchema(sparkSchema, DEFAULT_TIMEZONE, true, false);
    this.tempFile = new File(path.toString());
  }

  /**
   * Create a temporary file path with unique name to avoid collisions.
   */
  private static StoragePath createTempPath() {
    String tempDir = System.getProperty("java.io.tmpdir");
    String fileName = "hudi-lance-stream-" + UUID.randomUUID() + ".lance";
    return new StoragePath(new File(tempDir, fileName).getAbsolutePath());
  }

  @Override
  public boolean canWrite() {
    return true;
  }

  @Override
  public void writeRow(String key, InternalRow record) throws IOException {
    super.write(record);
  }

  @Override
  public void writeRowWithMetadata(HoodieKey key, InternalRow record) throws IOException {
    // TODO support populating the metadata
    this.writeRow(key.getRecordKey(), record);
  }

  @Override
  protected void populateVectorSchemaRoot(List<InternalRow> records) {
    if (writer == null) {
      writer = LanceArrowWriter.create(this.root, sparkSchema);
    }
    // Reset writer state from previous batch
    writer.reset();
    for (InternalRow record : records) {
      writer.write(record);
    }
    // Finalize the writer (sets row count)
    writer.finish();
  }

  @Override
  protected Schema getArrowSchema() {
    return arrowSchema;
  }

  /**
   * Close the writer, flush data to the OutputStream, and clean up temp file.
   *
   * This method:
   * 1. Calls super.close() to flush buffered records to the temp Lance file
   * 2. Copies the temp file contents to the target OutputStream
   * 3. Deletes the temp file (in finally block to ensure cleanup)
   * 4. Does NOT close the OutputStream (caller manages its lifecycle)
   *
   * @throws IOException if write, copy, or cleanup fails
   */
  @Override
  public void close() throws IOException {
    try {
      // Flush all buffered records to the temp Lance file
      super.close();

      // Copy temp file contents to output stream
      if (tempFile.exists()) {
        copyTempFileToStream();
      }
    } finally {
      // Always attempt to clean up temp file
      if (tempFile.exists()) {
        if (!tempFile.delete()) {
          // Log warning but don't fail - file will be cleaned by OS eventually
          System.err.println("Warning: Failed to delete temporary Lance file: " + tempFile.getAbsolutePath());
        }
      }
      // Note: We do NOT close outputStream - caller manages its lifecycle
    }
  }

  /**
   * Copy the temporary Lance file to the target OutputStream.
   */
  private void copyTempFileToStream() throws IOException {
    byte[] buffer = new byte[COPY_BUFFER_SIZE];
    try (FileInputStream fis = new FileInputStream(tempFile)) {
      int bytesRead;
      while ((bytesRead = fis.read(buffer)) != -1) {
        outputStream.write(buffer, 0, bytesRead);
      }
      outputStream.flush();
    }
  }
}
