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

package org.apache.hudi.integ.testsuite.generator;

import java.util.Iterator;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 * A GenericRecordGeneratorIterator for the custom schema of the workload. Implements {@link Iterator} to allow for
 * iteration semantics.
 */
public class FlexibleSchemaRecordGenerationIterator implements Iterator<GenericRecord> {

  // Stores how many records to generate as part of this iterator. Ideally, one iterator is started per spark
  // partition.
  private long counter;
  // Use the full payload generator as default
  private GenericRecordFullPayloadGenerator generator;
  // Store last record for the partition path of the first payload to be used for all subsequent generated payloads
  private GenericRecord lastRecord;
  // Partition path field name
  private List<String> partitionPathFieldNames;

  public FlexibleSchemaRecordGenerationIterator(long maxEntriesToProduce, String schema) {
    this(maxEntriesToProduce, GenericRecordFullPayloadGenerator.DEFAULT_PAYLOAD_SIZE, schema, null);
  }

  public FlexibleSchemaRecordGenerationIterator(long maxEntriesToProduce, int minPayloadSize, String schemaStr,
      List<String> partitionPathFieldNames) {
    this.counter = maxEntriesToProduce;
    this.partitionPathFieldNames = partitionPathFieldNames;
    Schema schema = new Schema.Parser().parse(schemaStr);
    this.generator = new GenericRecordFullPayloadGenerator(schema, minPayloadSize);
  }

  @Override
  public boolean hasNext() {
    return this.counter > 0;
  }

  @Override
  public GenericRecord next() {
    this.counter--;
    if (lastRecord == null) {
      GenericRecord record = this.generator.getNewPayload();
      lastRecord = record;
      return record;
    } else {
      return this.generator.randomize(lastRecord, this.partitionPathFieldNames);
    }
  }
}
