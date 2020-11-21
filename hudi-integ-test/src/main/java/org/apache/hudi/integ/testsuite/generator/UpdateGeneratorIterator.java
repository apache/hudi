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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * A lazy update payload generator to generate {@link GenericRecord}s lazily.
 */
public class UpdateGeneratorIterator implements Iterator<GenericRecord> {

  private static Logger LOG = LoggerFactory.getLogger(UpdateGeneratorIterator.class);

  // Use the full payload generator as default
  private GenericRecordFullPayloadGenerator generator;
  private Set<String> blackListedFields;
  // iterator
  private Iterator<GenericRecord> itr;

  public UpdateGeneratorIterator(Iterator<GenericRecord> itr, String schemaStr, List<String> partitionPathFieldNames,
      List<String> recordKeyFieldNames, int minPayloadSize) {
    this.itr = itr;
    this.blackListedFields = new HashSet<>();
    this.blackListedFields.addAll(partitionPathFieldNames);
    this.blackListedFields.addAll(recordKeyFieldNames);
    Schema schema = new Schema.Parser().parse(schemaStr);
    this.generator = new GenericRecordFullPayloadGenerator(schema, minPayloadSize);
  }

  @Override
  public boolean hasNext() {
    return itr.hasNext();
  }

  @Override
  public GenericRecord next() {
    GenericRecord newRecord = itr.next();
    return this.generator.randomize(newRecord, this.blackListedFields);
  }

}
