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

package org.apache.hudi.schema;

import org.apache.avro.Schema;

import java.util.HashMap;
import java.util.Map;

/**
 * Enumeration of all schema types supported by Hudi schema system.
 *
 * <p>This enum wraps Avro's Schema.Type to provide a consistent interface
 * for schema type handling while maintaining binary compatibility with Avro.
 * Each Hudi schema type corresponds directly to an Avro schema type.</p>
 *
 * <p>Usage example:
 * <pre>{@code
 * HoodieSchemaType type = HoodieSchemaType.STRING;
 * Schema.Type avroType = type.toAvroType();
 * HoodieSchemaType backToHudi = HoodieSchemaType.fromAvroType(avroType);
 * }</pre></p>
 *
 * @since 1.2.0
 */
public enum HoodieSchemaType {

  /**
   * Record type - represents a collection of named fields
   */
  RECORD(Schema.Type.RECORD),

  /**
   * Enum type - represents a symbolic name from a predefined set
   */
  ENUM(Schema.Type.ENUM),

  /**
   * Array type - represents an ordered collection of objects
   */
  ARRAY(Schema.Type.ARRAY),

  /**
   * Map type - represents an associative array of key-value pairs
   */
  MAP(Schema.Type.MAP),

  /**
   * Union type - represents one of several possible schemas
   */
  UNION(Schema.Type.UNION),

  /**
   * Fixed type - represents a fixed-length byte array
   */
  FIXED(Schema.Type.FIXED),

  /**
   * String type - represents Unicode character sequences
   */
  STRING(Schema.Type.STRING),

  /**
   * Bytes type - represents arbitrary byte sequences
   */
  BYTES(Schema.Type.BYTES),

  /**
   * Integer type - represents 32-bit signed integers
   */
  INT(Schema.Type.INT),

  /**
   * Long type - represents 64-bit signed integers
   */
  LONG(Schema.Type.LONG),

  /**
   * Float type - represents single precision floating point numbers
   */
  FLOAT(Schema.Type.FLOAT),

  /**
   * Double type - represents double precision floating point numbers
   */
  DOUBLE(Schema.Type.DOUBLE),

  /**
   * Boolean type - represents true or false values
   */
  BOOLEAN(Schema.Type.BOOLEAN),

  /**
   * Null type - represents the absence of a value
   */
  NULL(Schema.Type.NULL);

  // Cache for efficient reverse lookup
  private static final Map<Schema.Type, HoodieSchemaType> AVRO_TO_HUDI_MAP = new HashMap<>();

  static {
    for (HoodieSchemaType hoodieType : values()) {
      AVRO_TO_HUDI_MAP.put(hoodieType.avroType, hoodieType);
    }
  }

  private final Schema.Type avroType;

  HoodieSchemaType(Schema.Type avroType) {
    this.avroType = avroType;
  }

  /**
   * Converts an Avro schema type to the corresponding Hudi schema type.
   *
   * @param avroType the Avro schema type to convert
   * @return the equivalent HoodieSchemaType
   * @throws IllegalArgumentException if the Avro type is not supported
   */
  public static HoodieSchemaType fromAvroType(Schema.Type avroType) {
    HoodieSchemaType hoodieType = AVRO_TO_HUDI_MAP.get(avroType);
    if (hoodieType == null) {
      throw new IllegalArgumentException("Unsupported Avro schema type: " + avroType);
    }
    return hoodieType;
  }

  /**
   * Converts this Hudi schema type to the corresponding Avro schema type.
   *
   * @return the equivalent Avro Schema.Type
   */
  public Schema.Type toAvroType() {
    return avroType;
  }

  /**
   * Checks if this schema type represents a primitive type.
   *
   * @return true if this type is a primitive type (not RECORD, ENUM, ARRAY, MAP, or UNION)
   */
  public boolean isPrimitive() {
    switch (this) {
      case STRING:
      case BYTES:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
      case NULL:
      case FIXED:
        return true;
      default:
        return false;
    }
  }

  /**
   * Checks if this schema type represents a complex type.
   *
   * @return true if this type is a complex type (RECORD, ENUM, ARRAY, MAP, or UNION)
   */
  public boolean isComplex() {
    return !isPrimitive();
  }

  /**
   * Checks if this schema type represents a numeric type.
   *
   * @return true if this type is INT, LONG, FLOAT, or DOUBLE
   */
  public boolean isNumeric() {
    switch (this) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return true;
      default:
        return false;
    }
  }
}
