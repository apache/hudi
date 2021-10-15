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

package org.apache.hudi.internal.schema.convert;

import org.apache.avro.Schema;
import org.apache.hudi.internal.schema.HoodieSchemaException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.internal.schema.utils.InternalSchemaUtils;

import java.util.List;

import static org.apache.avro.Schema.Type.UNION;

/**
 * auxiliary class.
 * Converts an avro schema into InternalSchema, or convert InternalSchema to an avro schema
 */
public class AvroInternalSchemaConverter {

  /**
   * convert internalSchema to avro Schema.
   *
   * @param internalSchema internal schema.
   * @param tableName the record name.
   * @return an avro Schema.
   */
  public static Schema convert(InternalSchema internalSchema, String tableName) {
    return InternalSchemaUtils.buildAvroSchemaFromInternalSchema(internalSchema, tableName);
  }

  /**
   * convert RecordType to avro Schema.
   *
   * @param type internal schema.
   * @param name the record name.
   * @return an avro Schema.
   */
  public static Schema convert(Types.RecordType type, String name) {
    return InternalSchemaUtils.buildAvroSchemaFromType(type, name);
  }

  /**
   * convert internal type to avro Schema.
   *
   * @param type internal type.
   * @param name the record name.
   * @return an avro Schema.
   */
  public static Schema convert(Type type, String name) {
    return InternalSchemaUtils.buildAvroSchemaFromType(type, name);
  }

  /** convert an avro schema into internal type. */
  public static Type convertToField(Schema schema) {
    return InternalSchemaUtils.buildTypeFromAvroSchema(schema);
  }

  /** convert an avro schema into internalSchema. */
  public static InternalSchema convert(Schema schema) {
    List<Types.Field> fields = ((Types.RecordType) convertToField(schema)).fields();
    return new InternalSchema(fields);
  }

  /** check whether current avro schema is optional?. */
  public static boolean isOptional(Schema schema) {
    if (schema.getType() == UNION && schema.getTypes().size() == 2) {
      return schema.getTypes().get(0).getType() == Schema.Type.NULL || schema.getTypes().get(1).getType() == Schema.Type.NULL;
    }
    return false;
  }

  /** Returns schema with nullable true. */
  public static Schema nullableSchema(Schema schema) {
    if (schema.getType() == UNION) {
      if (!isOptional(schema)) {
        throw new HoodieSchemaException(String.format("Union schemas are not supported: %s", schema));
      }
      return schema;
    } else {
      return Schema.createUnion(Schema.create(Schema.Type.NULL), schema);
    }
  }
}
