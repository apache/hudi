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

package org.apache.hudi;

import org.apache.hudi.common.util.HoodieAvroUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.io.Serializable;

/**
 * Base class for all AVRO record based payloads, that can be ordered based on a field.
 */
public abstract class BaseAvroPayload implements Serializable {

  /**
   * Avro data extracted from the source converted to bytes.
   */
  protected final byte[] recordBytes;

  /**
   * For purposes of preCombining.
   */
  protected final Comparable orderingVal;

  /**
   * @param record
   * @param orderingVal
   */
  public BaseAvroPayload(GenericRecord record, Comparable orderingVal) {
    try {
      this.recordBytes = HoodieAvroUtils.avroToBytes(record);
    } catch (IOException io) {
      throw new HoodieIOException("Cannot convert GenericRecord to bytes", io);
    }
    this.orderingVal = orderingVal;
    if (orderingVal == null) {
      throw new HoodieException("Ordering value is null for record: " + record);
    }
  }
}
