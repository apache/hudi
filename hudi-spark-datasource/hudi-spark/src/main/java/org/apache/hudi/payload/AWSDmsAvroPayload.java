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

package org.apache.hudi.payload;

import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.util.Option;

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
@Deprecated
public class AWSDmsAvroPayload extends org.apache.hudi.common.model.AWSDmsAvroPayload {

  public AWSDmsAvroPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public AWSDmsAvroPayload(Option<GenericRecord> record) {
    super(record);
  }
}
