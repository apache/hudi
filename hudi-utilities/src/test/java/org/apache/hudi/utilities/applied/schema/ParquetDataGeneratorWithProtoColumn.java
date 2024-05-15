/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.applied.schema;

import org.apache.hudi.utilities.applied.common.TestCommon;
import org.apache.hudi.utilities.applied.common.UrsaBatchInfo;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.hudi.utilities.applied.schema.UrsaWrapperSchema.READER_PARQUET_SCHEMA;

/**
 * Used to generate test parquet files with proto columns for testing.
 */
public class ParquetDataGeneratorWithProtoColumn {

  public static void main(String[] args) throws IOException {
    // Define the schema
    String schemaStr = READER_PARQUET_SCHEMA;
    Schema schema = new Schema.Parser().parse(schemaStr);
    UrsaBatchInfo batchInfo = TestCommon.getTestBatchInfo();
    for (int i = 0; i < 5; i++) {
      String[] parts = batchInfo.getFiles().get(i).split("/");
      String filename = TestCommon.getPersonParquetDirPath() + parts[parts.length - 1];
      // Define the path to the output Parquet file
      Path path = new Path(filename);
      System.out.println("Writing to parquet file " + path);

      // Create the ParquetWriter
      ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
          .withSchema(schema)
          .withConf(new Configuration())
          .withCompressionCodec(CompressionCodecName.SNAPPY)
          .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
          .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
          .build();

      // Create a Person proto
      try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
        TestCommon.PERSONS[i].writeTo(outputStream);
        byte[] outputBytes = outputStream.toByteArray();
        // Create a record to write
        GenericRecord record = new GenericData.Record(schema);
        record.put("run_uuid", TestCommon.TEST_UUID);
        record.put("timestamp_ns", TestCommon.TEST_TIMESTAMPS_N[i]);
        record.put("serialized_customer_frame", ByteBuffer.wrap(outputBytes));
        // Write the record to the Parquet file
        writer.write(record);
        // Close the writer
        writer.close();
      }
    }
    System.out.println("Parquet files written successfully.");
  }
}
