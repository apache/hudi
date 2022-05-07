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

package org.apache.hudi.internal.schema.utils;

import org.apache.avro.Schema;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.internal.schema.action.TableChanges;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Utility methods to support evolve old avro schema based on a given schema.
 */
public class AvroSchemaEvolutionUtils {
  /**
   * Support evolution from a new avroSchema.
   * Now hoodie support implicitly add columns when hoodie write operation,
   * This ability needs to be preserved, so implicitly evolution for internalSchema should supported.
   *
   * @param evolvedSchema implicitly evolution of avro when hoodie write operation
   * @param oldSchema old internalSchema
   * @param supportPositionReorder support position reorder
   * @return evolution Schema
   */
  public static InternalSchema evolveSchemaFromNewAvroSchema(Schema evolvedSchema, InternalSchema oldSchema, Boolean supportPositionReorder) {
    InternalSchema evolvedInternalSchema = AvroInternalSchemaConverter.convert(evolvedSchema);
    // do check, only support add column evolution
    List<String> colNamesFromEvolved = evolvedInternalSchema.getAllColsFullName();
    List<String> colNamesFromOldSchema = oldSchema.getAllColsFullName();
    List<String> diffFromOldSchema = colNamesFromOldSchema.stream().filter(f -> !colNamesFromEvolved.contains(f)).collect(Collectors.toList());
    List<Types.Field> newFields = new ArrayList<>();
    if (colNamesFromEvolved.size() == colNamesFromOldSchema.size() && diffFromOldSchema.size() == 0) {
      // no changes happen
      if (supportPositionReorder) {
        evolvedInternalSchema.getRecord().fields().forEach(f -> newFields.add(oldSchema.getRecord().field(f.name())));
        return new InternalSchema(newFields);
      }
      return oldSchema;
    }
    // try to find all added columns
    if (diffFromOldSchema.size() != 0) {
      throw new UnsupportedOperationException("Cannot evolve schema implicitly, find delete/rename operation");
    }

    List<String> diffFromEvolutionSchema = colNamesFromEvolved.stream().filter(f -> !colNamesFromOldSchema.contains(f)).collect(Collectors.toList());
    // Remove redundancy from diffFromEvolutionSchema.
    // for example, now we add a struct col in evolvedSchema, the struct col is " user struct<name:string, age:int> "
    // when we do diff operation: user, user.name, user.age will appeared in the resultSet which is redundancy, user.name and user.age should be excluded.
    // deal with add operation
    TreeMap<Integer, String> finalAddAction = new TreeMap<>();
    for (int i = 0; i < diffFromEvolutionSchema.size(); i++)  {
      String name = diffFromEvolutionSchema.get(i);
      int splitPoint = name.lastIndexOf(".");
      String parentName = splitPoint > 0 ? name.substring(0, splitPoint) : "";
      if (!parentName.isEmpty() && diffFromEvolutionSchema.contains(parentName)) {
        // find redundancy, skip it
        continue;
      }
      finalAddAction.put(evolvedInternalSchema.findIdByName(name), name);
    }

    TableChanges.ColumnAddChange addChange = TableChanges.ColumnAddChange.get(oldSchema);
    finalAddAction.entrySet().stream().forEach(f -> {
      String name = f.getValue();
      int splitPoint = name.lastIndexOf(".");
      String parentName = splitPoint > 0 ? name.substring(0, splitPoint) : "";
      String rawName = splitPoint > 0 ? name.substring(splitPoint + 1) : name;
      addChange.addColumns(parentName, rawName, evolvedInternalSchema.findType(name), null);
    });

    InternalSchema res = SchemaChangeUtils.applyTableChanges2Schema(oldSchema, addChange);
    if (supportPositionReorder) {
      evolvedInternalSchema.getRecord().fields().forEach(f -> newFields.add(oldSchema.getRecord().field(f.name())));
      return new InternalSchema(newFields);
    } else {
      return res;
    }
  }

  public static InternalSchema evolveSchemaFromNewAvroSchema(Schema evolvedSchema, InternalSchema oldSchema) {
    return evolveSchemaFromNewAvroSchema(evolvedSchema, oldSchema, false);
  }

  /**
   * Canonical the nullability.
   * Do not allow change cols Nullability field from optional to required.
   * If above problem occurs, try to correct it.
   *
   * @param writeSchema writeSchema hoodie used to write data.
   * @param readSchema read schema
   * @return canonical Schema
   */
  public static Schema canonicalizeColumnNullability(Schema writeSchema, Schema readSchema) {
    if (writeSchema.getFields().isEmpty() || readSchema.getFields().isEmpty()) {
      return writeSchema;
    }
    InternalSchema writeInternalSchema = AvroInternalSchemaConverter.convert(writeSchema);
    InternalSchema readInternalSchema = AvroInternalSchemaConverter.convert(readSchema);
    List<String> colNamesWriteSchema = writeInternalSchema.getAllColsFullName();
    List<String> colNamesFromReadSchema = readInternalSchema.getAllColsFullName();
    // try to deal with optional change. now when we use sparksql to update hudi table,
    // sparksql Will change the col type from optional to required, this is a bug.
    List<String> candidateUpdateCols = colNamesWriteSchema.stream().filter(f -> {
      boolean exist = colNamesFromReadSchema.contains(f);
      if (exist && (writeInternalSchema.findField(f).isOptional() != readInternalSchema.findField(f).isOptional())) {
        return true;
      } else {
        return false;
      }
    }).collect(Collectors.toList());
    if (candidateUpdateCols.isEmpty()) {
      return writeSchema;
    }
    // try to correct all changes
    TableChanges.ColumnUpdateChange updateChange = TableChanges.ColumnUpdateChange.get(writeInternalSchema);
    candidateUpdateCols.stream().forEach(f -> updateChange.updateColumnNullability(f, true));
    Schema result = AvroInternalSchemaConverter.convert(SchemaChangeUtils.applyTableChanges2Schema(writeInternalSchema, updateChange), writeSchema.getName());
    return result;
  }
}

