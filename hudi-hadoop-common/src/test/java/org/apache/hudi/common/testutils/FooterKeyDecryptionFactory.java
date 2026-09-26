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

package org.apache.hudi.common.testutils;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.crypto.DecryptionPropertiesFactory;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A parquet decryption factory ({@code parquet.crypto.factory.class}) that decrypts files written by
 * {@link #writeEncryptedFile} with one fixed footer key, and records the file path of every request.
 */
public class FooterKeyDecryptionFactory implements DecryptionPropertiesFactory {

  public static final String CRYPTO_FACTORY_CLASS = "parquet.crypto.factory.class";
  public static final Schema SCHEMA = SchemaBuilder.record("record").fields()
      .requiredString("_row_key")
      .requiredLong("ts")
      .endRecord();

  private static final byte[] FOOTER_KEY = "0123456789012345".getBytes(StandardCharsets.UTF_8);
  private static final List<Path> REQUESTED_PATHS = Collections.synchronizedList(new ArrayList<>());

  @Override
  public FileDecryptionProperties getFileDecryptionProperties(Configuration hadoopConfig, Path filePath) {
    REQUESTED_PATHS.add(filePath);
    return FileDecryptionProperties.builder().withFooterKey(FOOTER_KEY).build();
  }

  /**
   * Returns and clears the file paths requested so far.
   */
  public static List<Path> drainRequestedPaths() {
    synchronized (REQUESTED_PATHS) {
      List<Path> paths = new ArrayList<>(REQUESTED_PATHS);
      REQUESTED_PATHS.clear();
      return paths;
    }
  }

  /**
   * Writes {@code numRecords} records of {@link #SCHEMA} with an encrypted footer and encrypted columns.
   */
  public static void writeEncryptedFile(Configuration conf, Path path, int numRecords) throws IOException {
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(path, conf))
        .withSchema(SCHEMA)
        .withConf(conf)
        .withEncryption(FileEncryptionProperties.builder(FOOTER_KEY).build())
        .build()) {
      for (int i = 0; i < numRecords; i++) {
        GenericRecord record = new GenericData.Record(SCHEMA);
        record.put("_row_key", "key" + i);
        record.put("ts", (long) i);
        writer.write(record);
      }
    }
  }
}
