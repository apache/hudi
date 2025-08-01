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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.OrderingValues;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

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
    this(record.isPresent() ? record.get() : null, OrderingValues.getDefault()); // natural order
  }

  /**
   * Handle a possible delete - check for "D" in Op column and return empty row if found.
   *
   * @param insertValueOpt The new row that is being "inserted".
   */
  private Option<IndexedRecord> handleDeleteOperation(Option<IndexedRecord> insertValueOpt) throws IOException {
    boolean delete = insertValueOpt.map(insertValue -> {
      if (insertValue instanceof GenericRecord) {
        GenericRecord record = (GenericRecord) insertValue;
        return isDMSDeleteRecord(record);
      }
      return false;
    }).orElse(false);

    return delete ? Option.empty() : insertValueOpt;
  }

  @Override
  public OverwriteWithLatestAvroPayload preCombine(OverwriteWithLatestAvroPayload oldValue) {
    if (oldValue.recordBytes.length == 0) {
      // use natural order for delete record
      return this;
    }
    if (oldValue.orderingVal.compareTo(orderingVal) > 0) {
      // pick the payload with greatest ordering value
      return oldValue;
    } else {
      return this;
    }
  }

  @Override
  public Option<IndexedRecord> getInsertValue(Schema schema, Properties properties) throws IOException {
    return getInsertValue(schema);
  }

  @Override
  public Option<IndexedRecord> getInsertValue(Schema schema) throws IOException {
    Option<IndexedRecord> insertValue = super.getInsertValue(schema);
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
    Option<IndexedRecord> insertValue = super.getInsertValue(schema);
    if (!insertValue.isPresent()) {
      return Option.empty();
    }
    return handleDeleteOperation(insertValue);
  }

  @Override
  protected boolean isDeleteRecord(GenericRecord record) {
    return isDMSDeleteRecord(record) || super.isDeleteRecord(record);
  }

  private static boolean isDMSDeleteRecord(GenericRecord record) {
    return record.get(OP_FIELD) != null && record.get(OP_FIELD).toString().equalsIgnoreCase("D");
  }
}
