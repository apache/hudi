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

package org.apache.hudi.common.util;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestOrcReaderIterator {
  private final Path filePath = new Path(System.getProperty("java.io.tmpdir") + "/f1_1-0-1_000.orc");

  @BeforeEach
  @AfterEach
  public void clearTempFile() {
    File file = new File(filePath.toString());
    if (file.exists()) {
      file.delete();
    }
  }

  @Test
  public void testOrcIteratorReadData() throws Exception {
    final Configuration conf = new Configuration();
    Schema avroSchema = getSchemaFromResource(TestOrcReaderIterator.class, "/simple-test.avsc");
    TypeDescription orcSchema = AvroOrcUtils.createOrcSchema(avroSchema);
    OrcFile.WriterOptions options = OrcFile.writerOptions(conf).setSchema(orcSchema).compress(CompressionKind.ZLIB);
    Writer writer = OrcFile.createWriter(filePath, options);
    VectorizedRowBatch batch = orcSchema.createRowBatch();
    BytesColumnVector nameColumns = (BytesColumnVector) batch.cols[0];
    LongColumnVector numberColumns = (LongColumnVector) batch.cols[1];
    BytesColumnVector colorColumns = (BytesColumnVector) batch.cols[2];
    for (int r = 0; r < 5; ++r) {
      int row = batch.size++;
      byte[] name = ("name" + r).getBytes(StandardCharsets.UTF_8);
      nameColumns.setVal(row, name);
      byte[] color = ("color" + r).getBytes(StandardCharsets.UTF_8);
      colorColumns.setVal(row, color);
      numberColumns.vector[row] = r;
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(filePath, OrcFile.readerOptions(conf));
    RecordReader recordReader = reader.rows(new Reader.Options(conf).schema(orcSchema));
    Iterator<GenericRecord> iterator = new OrcReaderIterator<>(recordReader, avroSchema, orcSchema);
    int recordCount = 0;
    while (iterator.hasNext()) {
      GenericRecord record = iterator.next();
      assertEquals("name" + recordCount, record.get("name").toString());
      assertEquals("color" + recordCount, record.get("favorite_color").toString());
      assertEquals(recordCount, record.get("favorite_number"));
      recordCount++;
    }
    assertEquals(5, recordCount);
  }
}
