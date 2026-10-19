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

package org.apache.parquet.avro;

import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Verifies that Hudi's parquet-avro readers never instantiate classes named by the
 * {@code java-class} property of an Avro schema stored in a parquet file footer.
 */
class TestAvroStringableClassResolution {

  @TempDir
  java.nio.file.Path tempDir;

  private Path filePath;

  public static class StringableProbe {
    static final AtomicInteger INSTANCES = new AtomicInteger();

    public StringableProbe(String value) {
      INSTANCES.incrementAndGet();
    }
  }

  @BeforeEach
  void writeFile() throws IOException {
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    stringSchema.addProp("java-class", StringableProbe.class.getName());
    Schema schema = SchemaBuilder.record("rec").fields().name("f").type(stringSchema).noDefault().endRecord();

    filePath = new Path(tempDir.resolve("stringable.parquet").toUri());
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(filePath)
        .withSchema(schema)
        .withDataModel(GenericData.get())
        .withConf(new Configuration())
        .build()) {
      GenericRecord record = new GenericData.Record(schema);
      record.put("f", "value");
      writer.write(record);
    }
    StringableProbe.INSTANCES.set(0);
  }

  @Test
  void hoodieAvroParquetReaderBuilderIgnoresJavaClass() throws IOException {
    Configuration conf = new Configuration();
    try (ParquetReader<IndexedRecord> reader =
             new HoodieAvroParquetReaderBuilder<IndexedRecord>(HadoopInputFile.fromPath(filePath, conf))
                 .withConf(conf)
                 .build()) {
      IndexedRecord record = reader.read();
      assertEquals("value", record.get(0).toString());
      assertFalse(record.get(0) instanceof StringableProbe);
    }
    assertEquals(0, StringableProbe.INSTANCES.get());
  }

  @Test
  void parquetUtilsReadAvroRecordsIgnoresJavaClass() {
    Configuration conf = new Configuration();
    conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, false);
    StoragePath storagePath = new StoragePath(filePath.toUri());
    List<GenericRecord> records = new ParquetUtils().readAvroRecords(new HoodieHadoopStorage(filePath, conf), storagePath);
    assertEquals(1, records.size());
    assertEquals("value", records.get(0).get("f").toString());
    assertEquals(0, StringableProbe.INSTANCES.get());
  }
}
