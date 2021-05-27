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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.util.AvroOrcUtils;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.ColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHoodieOrcReader {
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
  public void testReadOrcFilePrimitiveData() throws Exception {
    Configuration conf = new Configuration();
    Schema avroSchema = getSchemaFromResource(TestHoodieOrcReader.class, "/simple-test.avsc");
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

    HoodieFileReader<GenericRecord> reader = HoodieFileReaderFactory.getFileReader(conf, filePath);
    Iterator<GenericRecord> iterator = reader.getRecordIterator();
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

  @Test
  public void testReadOrcFileComplexSchema() throws Exception {
    Configuration conf = new Configuration();
    Schema avroSchema = getSchemaFromResource(TestHoodieOrcReader.class, "/complex-test-evolved.avsc");
    TypeDescription orcSchema = AvroOrcUtils.createOrcSchema(avroSchema);
    OrcFile.WriterOptions options = OrcFile.writerOptions(conf).setSchema(orcSchema).compress(CompressionKind.ZLIB);
    Writer writer = OrcFile.createWriter(filePath, options);
    VectorizedRowBatch batch = orcSchema.createRowBatch();
    List<GenericRecord> records = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      GenericRecord record = new GenericData.Record(avroSchema);
      Schema udtSchema = avroSchema.getField("testNestedRecord").schema().getTypes().get(1);
      GenericRecord innerRecord = new GenericData.Record(udtSchema);
      Schema tagSchema = avroSchema.getField("tags").schema().getTypes().get(1).getValueType().getTypes().get(1);
      GenericRecord tagRecord = new GenericData.Record(tagSchema);
      String stringValue = "record" + i;
      record.put("field1", stringValue);
      record.put("field2", stringValue);
      record.put("name", stringValue);
      innerRecord.put("isAdmin", true);
      innerRecord.put("userId", stringValue);
      record.put("testNestedRecord", innerRecord);
      tagRecord.put("item1", stringValue);
      tagRecord.put("item2", stringValue);
      record.put("tags", Collections.singletonMap("key", tagRecord));
      record.put("stringArray", Collections.singletonList(stringValue));
      records.add(record);
    }

    for (int i = 0; i < 5; i++) {
      GenericRecord record = records.get(i);
      for (int c = 0; c < batch.numCols; c++) {
        ColumnVector colVector = batch.cols[c];
        final String thisField = orcSchema.getFieldNames().get(c);
        final TypeDescription type = orcSchema.getChildren().get(c);
        Object fieldValue = record.get(thisField);
        Schema.Field avroField = record.getSchema().getField(thisField);
        AvroOrcUtils.addToVector(type, colVector, avroField.schema(), fieldValue, batch.size);
      }
      batch.size++;
    }
    writer.addRowBatch(batch);
    writer.close();

    HoodieFileReader<GenericRecord> reader = HoodieFileReaderFactory.getFileReader(conf, filePath);
    Iterator<GenericRecord> iterator = reader.getRecordIterator();
    int recordCount = 0;
    while (iterator.hasNext()) {
      String stringValue = "record" + recordCount;
      GenericRecord record = iterator.next();
      assertEquals(stringValue, record.get("field1").toString());
      assertEquals(stringValue, record.get("field2").toString());
      assertEquals(stringValue, record.get("name").toString());
      GenericRecord innerRecord = (GenericRecord) record.get("testNestedRecord");
      assertEquals(true, innerRecord.get("isAdmin"));
      assertEquals(stringValue, innerRecord.get("userId").toString());
      assertEquals(1, ((Map<?,?>)record.get("tags")).size());
      assertEquals(1, ((List<?>)record.get("stringArray")).size());
      recordCount++;
    }
    assertEquals(5, recordCount);
  }
}
