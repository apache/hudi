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

package org.apache.hudi.common.model;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.util.Option;

import java.io.IOException;
import java.util.Properties;

/**
 * Provides support for seamlessly applying changes captured via Amazon Database Migration Service onto S3.
 *
 * Typically, we get the following pattern of full change records corresponding to DML against the
 * source database
 *
 * - Full load records with no `Op` field
 * - For inserts against the source table, records contain full after image with `Op=I`
 * - For updates against the source table, records contain full after image with `Op=U`
 * - For deletes against the source table, records contain full before image with `Op=D`
 *
 * This payload implementation will issue matching insert, delete, updates against the hudi table
 *
 */
public class AWSDmsAvroPayload extends OverwriteWithLatestAvroPayload {

  public static final String OP_FIELD = "Op";

  public AWSDmsAvroPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public AWSDmsAvroPayload(Option<GenericRecord> record) {
    this(record.get(), 0); // natural order
  }

  /**
   *
   * Handle a possible delete - check for "D" in Op column and return empty row if found.
   * @param insertValue The new row that is being "inserted".
   */
  private Option<IndexedRecord> handleDeleteOperation(IndexedRecord insertValue) throws IOException {
    boolean delete = false;
    if (insertValue instanceof GenericRecord) {
      GenericRecord record = (GenericRecord) insertValue;
      delete = record.get(OP_FIELD) != null && record.get(OP_FIELD).toString().equalsIgnoreCase("D");
    }

    return delete ? Option.empty() : Option.of(insertValue);
  }

  @Override
  public Option<IndexedRecord> getInsertValue(Schema schema, Properties properties) throws IOException {
    return getInsertValue(schema);
  }

  @Override
  public Option<IndexedRecord> getInsertValue(Schema schema) throws IOException {
    IndexedRecord insertValue = super.getInsertValue(schema).get();
    return handleDeleteOperation(insertValue);
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema, Properties properties)
      throws IOException {
    return combineAndGetUpdateValue(currentValue, schema);
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema)
      throws IOException {
    IndexedRecord insertValue = super.getInsertValue(schema).get();
    return handleDeleteOperation(insertValue);
  }
}
