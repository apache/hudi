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

import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.action.TableChanges;

import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.CollectionUtils.reduce;
import static org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter.convert;

/**
 * Utility methods to support evolve old avro schema based on a given schema.
 */
public class AvroSchemaEvolutionUtils {

  /**
   * Support reconcile from a new avroSchema.
   * 1) incoming data has missing columns that were already defined in the table –> null values will be injected into missing columns
   * 2) incoming data contains new columns not defined yet in the table -> columns will be added to the table schema (incoming dataframe?)
   * 3) incoming data has missing columns that are already defined in the table and new columns not yet defined in the table ->
   * new columns will be added to the table schema, missing columns will be injected with null values
   * 4) support type change
   * 5) support nested schema change.
   * Notice:
   * the incoming schema should not have delete/rename semantics.
   * for example: incoming schema:  int a, int b, int d;   oldTableSchema int a, int b, int c, int d
   * we must guarantee the column c is missing semantic, instead of delete semantic.
   *
   * @param incomingSchema implicitly evolution of avro when hoodie write operation
   * @param oldTableSchema old internalSchema
   * @return reconcile Schema
   */
  public static InternalSchema reconcileSchema(Schema incomingSchema, InternalSchema oldTableSchema) {
    /* If incoming schema is null, we fall back on table schema. */
    if (incomingSchema.getType() == Schema.Type.NULL) {
      return oldTableSchema;
    }
    InternalSchema inComingInternalSchema = convert(incomingSchema);
    // check column add/missing
    List<String> colNamesFromIncoming = inComingInternalSchema.getAllColsFullName();
    List<String> colNamesFromOldSchema = oldTableSchema.getAllColsFullName();
    List<String> diffFromOldSchema = colNamesFromOldSchema.stream().filter(f -> !colNamesFromIncoming.contains(f)).collect(Collectors.toList());
    List<String> diffFromEvolutionColumns = colNamesFromIncoming.stream().filter(f -> !colNamesFromOldSchema.contains(f)).collect(Collectors.toList());
    // check type change.
    List<String> typeChangeColumns = colNamesFromIncoming
        .stream()
        .filter(f -> colNamesFromOldSchema.contains(f) && !inComingInternalSchema.findType(f).equals(oldTableSchema.findType(f)))
        .collect(Collectors.toList());
    if (colNamesFromIncoming.size() == colNamesFromOldSchema.size() && diffFromOldSchema.size() == 0 && typeChangeColumns.isEmpty()) {
      return oldTableSchema;
    }

    // Remove redundancy from diffFromEvolutionSchema.
    // for example, now we add a struct col in evolvedSchema, the struct col is " user struct<name:string, age:int> "
    // when we do diff operation: user, user.name, user.age will appeared in the resultSet which is redundancy, user.name and user.age should be excluded.
    // deal with add operation
    TreeMap<Integer, String> finalAddAction = new TreeMap<>();
    for (int i = 0; i < diffFromEvolutionColumns.size(); i++) {
      String name = diffFromEvolutionColumns.get(i);
      int splitPoint = name.lastIndexOf(".");
      String parentName = splitPoint > 0 ? name.substring(0, splitPoint) : "";
      if (!parentName.isEmpty() && diffFromEvolutionColumns.contains(parentName)) {
        // find redundancy, skip it
        continue;
      }
      finalAddAction.put(inComingInternalSchema.findIdByName(name), name);
    }

    TableChanges.ColumnAddChange addChange = TableChanges.ColumnAddChange.get(oldTableSchema);
    finalAddAction.entrySet().stream().forEach(f -> {
      String name = f.getValue();
      int splitPoint = name.lastIndexOf(".");
      String parentName = splitPoint > 0 ? name.substring(0, splitPoint) : "";
      String rawName = splitPoint > 0 ? name.substring(splitPoint + 1) : name;
      // try to infer add position.
      java.util.Optional<String> inferPosition =
          colNamesFromIncoming.stream().filter(c ->
              c.lastIndexOf(".") == splitPoint
                  && c.startsWith(parentName)
                  && inComingInternalSchema.findIdByName(c) > inComingInternalSchema.findIdByName(name)
                  && oldTableSchema.findIdByName(c) > 0).sorted((s1, s2) -> oldTableSchema.findIdByName(s1) - oldTableSchema.findIdByName(s2)).findFirst();
      addChange.addColumns(parentName, rawName, inComingInternalSchema.findType(name), null);
      inferPosition.map(i -> addChange.addPositionChange(name, i, "before"));
    });

    // do type evolution.
    InternalSchema internalSchemaAfterAddColumns = SchemaChangeUtils.applyTableChanges2Schema(oldTableSchema, addChange);
    TableChanges.ColumnUpdateChange typeChange = TableChanges.ColumnUpdateChange.get(internalSchemaAfterAddColumns);
    typeChangeColumns.stream().filter(f -> !inComingInternalSchema.findType(f).isNestedType()).forEach(col -> {
      typeChange.updateColumnType(col, inComingInternalSchema.findType(col));
    });

    return SchemaChangeUtils.applyTableChanges2Schema(internalSchemaAfterAddColumns, typeChange);
  }

  public static Schema reconcileSchema(Schema incomingSchema, Schema oldTableSchema) {
    return convert(reconcileSchema(incomingSchema, convert(oldTableSchema)), oldTableSchema.getFullName());
  }

  /**
   * Reconciles nullability and datatype requirements b/w {@code source} and {@code target} schemas,
   * by adjusting these of the {@code source} schema to be in-line with the ones of the
   * {@code target} one. Source is considered to be new incoming schema, while target could refer to prev table schema.
   * For example,
   * if colA in source is non-nullable, but is nullable in target, output schema will have colA as nullable.
   * if "hoodie.datasource.write.new.columns.nullable" is set to true and if colB is not present in source, but
   * is present in target, output schema will have colB as nullable.
   * if colC has different data type in source schema compared to target schema and if its promotable, (say source is int,
   * and target is long and since int can be promoted to long), colC will be long data type in output schema.
   *
   *
   * @param sourceSchema source schema that needs reconciliation
   * @param targetSchema target schema that source schema will be reconciled against
   * @return schema (based off {@code source} one) that has nullability constraints and datatypes reconciled
   */
  public static Schema reconcileSchemaRequirements(Schema sourceSchema, Schema targetSchema) {
    if (targetSchema.getType() == Schema.Type.NULL || targetSchema.getFields().isEmpty()) {
      return sourceSchema;
    }

    if (sourceSchema == null || sourceSchema.getType() == Schema.Type.NULL || sourceSchema.getFields().isEmpty()) {
      return targetSchema;
    }

    InternalSchema sourceInternalSchema = convert(sourceSchema);
    InternalSchema targetInternalSchema = convert(targetSchema);

    List<String> colNamesSourceSchema = sourceInternalSchema.getAllColsFullName();
    List<String> colNamesTargetSchema = targetInternalSchema.getAllColsFullName();

    List<String> nullableUpdateColsInSource = new ArrayList<>();
    List<String> typeUpdateColsInSource = new ArrayList<>();
    colNamesSourceSchema.forEach(field -> {
      // handle columns that needs to be made nullable
      if (colNamesTargetSchema.contains(field) && sourceInternalSchema.findField(field).isOptional() != targetInternalSchema.findField(field).isOptional()) {
        nullableUpdateColsInSource.add(field);
      }
      // handle columns that needs type to be updated
      if (colNamesTargetSchema.contains(field) && SchemaChangeUtils.shouldPromoteType(sourceInternalSchema.findType(field), targetInternalSchema.findType(field))) {
        typeUpdateColsInSource.add(field);
      }
    });

    if (nullableUpdateColsInSource.isEmpty() && typeUpdateColsInSource.isEmpty()) {
      //standardize order of unions
      return convert(sourceInternalSchema, sourceSchema.getFullName());
    }

    TableChanges.ColumnUpdateChange schemaChange = TableChanges.ColumnUpdateChange.get(sourceInternalSchema);

    // Reconcile nullability constraints (by executing phony schema change)
    if (!nullableUpdateColsInSource.isEmpty()) {
      schemaChange = reduce(nullableUpdateColsInSource, schemaChange,
          (change, field) -> change.updateColumnNullability(field, true));
    }

    // Reconcile type promotions
    if (!typeUpdateColsInSource.isEmpty()) {
      schemaChange = reduce(typeUpdateColsInSource, schemaChange,
          (change, field) -> change.updateColumnType(field, targetInternalSchema.findType(field)));
    }


    return convert(SchemaChangeUtils.applyTableChanges2Schema(sourceInternalSchema, schemaChange), sourceSchema.getFullName());
  }
}

