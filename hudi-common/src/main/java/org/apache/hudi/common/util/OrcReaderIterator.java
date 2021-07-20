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

import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.util.Iterator;

/**
 * This class wraps a ORC reader and provides an iterator based api to read from an ORC file.
 */
public class OrcReaderIterator<T> implements Iterator<T> {

  private final RecordReader recordReader;
  private final Schema avroSchema;
  List<String> fieldNames;
  List<TypeDescription> orcFieldTypes;
  Schema[] avroFieldSchemas;
  private VectorizedRowBatch batch;
  private int rowInBatch;
  private T next;

  public OrcReaderIterator(RecordReader recordReader, Schema schema, TypeDescription orcSchema) {
    this.recordReader = recordReader;
    this.avroSchema = schema;
    this.fieldNames = orcSchema.getFieldNames();
    this.orcFieldTypes = orcSchema.getChildren();
    this.avroFieldSchemas = fieldNames.stream()
        .map(fieldName -> avroSchema.getField(fieldName).schema())
        .toArray(size -> new Schema[size]);
    this.batch = orcSchema.createRowBatch();
    this.rowInBatch = 0;
  }

  /**
   * If the current batch is empty, get a new one.
   * @return true if we have rows available.
   * @throws IOException
   */
  private boolean ensureBatch() throws IOException {
    if (rowInBatch >= batch.size) {
      rowInBatch = 0;
      return recordReader.nextBatch(batch);
    }
    return true;
  }

  @Override
  public boolean hasNext() {
    try {
      ensureBatch();
      if (this.next == null) {
        this.next = (T) readRecordFromBatch();
      }
      return this.next != null;
    } catch (IOException io) {
      throw new HoodieIOException("unable to read next record from ORC file ", io);
    }
  }

  @Override
  public T next() {
    try {
      // To handle case when next() is called before hasNext()
      if (this.next == null) {
        if (!hasNext()) {
          throw new HoodieIOException("No more records left to read from ORC file");
        }
      }
      T retVal = this.next;
      this.next = (T) readRecordFromBatch();
      return retVal;
    } catch (IOException io) {
      throw new HoodieIOException("unable to read next record from ORC file ", io);
    }
  }

  private GenericData.Record readRecordFromBatch() throws IOException {
    // No more records left to read from ORC file
    if (!ensureBatch()) {
      return null;
    }

    GenericData.Record record = new Record(avroSchema);
    int numFields = orcFieldTypes.size();
    for (int i = 0; i < numFields; i++) {
      Object data = AvroOrcUtils.readFromVector(orcFieldTypes.get(i), batch.cols[i], avroFieldSchemas[i], rowInBatch);
      record.put(fieldNames.get(i), data);
    }
    rowInBatch++;
    return record;
  }
}