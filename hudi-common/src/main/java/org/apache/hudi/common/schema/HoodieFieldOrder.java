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

package org.apache.hudi.common.schema;

import org.apache.avro.Schema;

import java.util.HashMap;
import java.util.Map;

/**
 * Enumeration of field sort orders supported by Hudi schema system.
 *
 * <p>This enum wraps Avro's Schema.Field.Order to provide a consistent interface
 * for field ordering while maintaining binary compatibility with Avro.</p>
 *
 * @since 1.2.0
 */
public enum HoodieFieldOrder {

  /**
   * Ascending sort order
   */
  ASCENDING(Schema.Field.Order.ASCENDING),

  /**
   * Descending sort order
   */
  DESCENDING(Schema.Field.Order.DESCENDING),

  /**
   * Fields are not sorted
   */
  IGNORE(Schema.Field.Order.IGNORE);

  // Cache for efficient reverse lookup
  private static final Map<Schema.Field.Order, HoodieFieldOrder> AVRO_TO_HUDI_MAP = new HashMap<>();

  static {
    for (HoodieFieldOrder hoodieOrder : values()) {
      AVRO_TO_HUDI_MAP.put(hoodieOrder.avroOrder, hoodieOrder);
    }
  }

  private final Schema.Field.Order avroOrder;

  HoodieFieldOrder(Schema.Field.Order avroOrder) {
    this.avroOrder = avroOrder;
  }

  /**
   * Converts an Avro field order to the corresponding Hudi field order.
   *
   * @param avroOrder the Avro field order to convert
   * @return the equivalent HoodieFieldOrder
   * @throws IllegalArgumentException if the Avro order is not supported
   */
  public static HoodieFieldOrder fromAvroOrder(Schema.Field.Order avroOrder) {
    HoodieFieldOrder hoodieOrder = AVRO_TO_HUDI_MAP.get(avroOrder);
    if (hoodieOrder == null) {
      throw new IllegalArgumentException("Unsupported Avro field order: " + avroOrder);
    }
    return hoodieOrder;
  }

  /**
   * Converts this Hudi field order to the corresponding Avro field order.
   *
   * @return the equivalent Avro Schema.Field.Order
   */
  public Schema.Field.Order toAvroOrder() {
    return avroOrder;
  }
}
