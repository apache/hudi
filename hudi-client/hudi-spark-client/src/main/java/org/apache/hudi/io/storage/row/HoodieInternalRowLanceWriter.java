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

package org.apache.hudi.io.storage.row;

import com.lancedb.lance.spark.arrow.LanceArrowWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hudi.io.lance.HoodieBaseLanceWriter;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.util.List;

/**
 * Lance's implementation of {@link HoodieInternalRowFileWriter} to write {@link InternalRow}s.
 * This writer is used for bulk insert operations and other optimized write paths that work
 * directly with InternalRow objects without HoodieRecord wrappers.
 */
public class HoodieInternalRowLanceWriter extends HoodieBaseLanceWriter<InternalRow>
    implements HoodieInternalRowFileWriter {

  private static final long DEFAULT_MAX_FILE_SIZE = 120 * 1024 * 1024; // 120MB
  private static final String DEFAULT_TIMEZONE = "UTC";

  private final StructType sparkSchema;
  private final Schema arrowSchema;
  private LanceArrowWriter writer;

  /**
   * Constructor for Lance InternalRow writer.
   *
   * @param file Path where Lance file will be written
   * @param sparkSchema Spark schema for the data
   * @param storage HoodieStorage instance
   * @throws IOException if writer initialization fails
   */
  public HoodieInternalRowLanceWriter(StoragePath file,
                                       StructType sparkSchema,
                                       HoodieStorage storage) throws IOException {
    super(storage, file, DEFAULT_BATCH_SIZE, DEFAULT_MAX_FILE_SIZE);
    this.sparkSchema = sparkSchema;
    this.arrowSchema = LanceArrowUtils.toArrowSchema(sparkSchema, DEFAULT_TIMEZONE, true, false);
  }

  @Override
  public void writeRow(UTF8String key, InternalRow row) throws IOException {
    // For bloom filter support in the future.
    // For now, just write the row
    super.write(row);
  }

  @Override
  public void writeRow(InternalRow row) throws IOException {
    super.write(row);
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
}