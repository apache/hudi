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

import static org.apache.avro.Schema.Type.UNION;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.internal.schema.action.TableChanges;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class AvroSchemaUtil {
  private AvroSchemaUtil() {
  }

  /**
   * Given a avro record with a given schema, rewrites it into the new schema while setting fields only from the new schema.
   * support deep rewrite for nested record.
   * This particular method does the following things :
   * a) Create a new empty GenericRecord with the new schema.
   * b) For GenericRecord, copy over the data from the old schema to the new schema or set default values for all fields of this transformed schema
   *
   * @param oldRecord oldRecord to be rewrite
   * @param newSchema newSchema used to rewrite oldRecord
   * @return newRecord for new Schema
   */
  public static GenericRecord rewriteRecord(GenericRecord oldRecord, Schema newSchema) {
    Object newRecord = rewriteRecord(oldRecord, oldRecord.getSchema(), newSchema);
    return (GenericData.Record) newRecord;
  }

  private static Object rewriteRecord(Object oldRecord, Schema oldSchema, Schema newSchema) {
    if (oldRecord == null) {
      return null;
    }
    switch (newSchema.getType()) {
      case RECORD:
        if (!(oldRecord instanceof IndexedRecord)) {
          throw new IllegalArgumentException("cannot rewrite record with different type");
        }
        IndexedRecord indexedRecord = (IndexedRecord) oldRecord;
        List<Schema.Field> fields = newSchema.getFields();
        Map<Integer, Object> helper = new HashMap<>();

        for (int i = 0; i < fields.size(); i++) {
          Schema.Field field = fields.get(i);
          if (oldSchema.getField(field.name()) != null) {
            Schema.Field oldField = oldSchema.getField(field.name());
            helper.put(i, rewriteRecord(indexedRecord.get(oldField.pos()), oldField.schema(), fields.get(i).schema()));
          }
        }
        GenericData.Record newRecord = new GenericData.Record(newSchema);
        for (int i = 0; i < fields.size(); i++) {
          if (helper.containsKey(i)) {
            newRecord.put(i, helper.get(i));
          } else {
            if (fields.get(i).defaultVal() instanceof JsonProperties.Null) {
              newRecord.put(i, null);
            } else {
              newRecord.put(i, fields.get(i).defaultVal());
            }
          }
        }
        return newRecord;
      case ARRAY:
        if (!(oldRecord instanceof Collection)) {
          throw new IllegalArgumentException("cannot rewrite record with different type");
        }
        Collection array = (Collection)oldRecord;
        List<Object> newArray = new ArrayList();
        for (Object element : array) {
          newArray.add(rewriteRecord(element, oldSchema.getElementType(), newSchema.getElementType()));
        }
        return newArray;
      case MAP:
        if (!(oldRecord instanceof Map)) {
          throw new IllegalArgumentException("cannot rewrite record with different type");
        }
        Map<Object, Object> map = (Map<Object, Object>) oldRecord;
        Map<Object, Object> newMap = new HashMap<>();
        for (Map.Entry<Object, Object> entry : map.entrySet()) {
          newMap.put(entry.getKey(), rewriteRecord(entry.getValue(), oldSchema.getValueType(), newSchema.getValueType()));
        }
        return newMap;
      case UNION:
        return rewriteRecord(oldRecord, getActualSchemaFromUnion(oldSchema, oldRecord), getActualSchemaFromUnion(newSchema, oldRecord));
      default:
        return oldRecord;
    }
  }

  private static Schema getActualSchemaFromUnion(Schema schema, Object data) {
    Schema actualSchema;
    if (!schema.getType().equals(UNION)) {
      return schema;
    }
    if (schema.getTypes().size() == 2
        && schema.getTypes().get(0).getType() == Schema.Type.NULL) {
      actualSchema = schema.getTypes().get(1);
    } else if (schema.getTypes().size() == 2
        && schema.getTypes().get(1).getType() == Schema.Type.NULL) {
      actualSchema = schema.getTypes().get(0);
    } else if (schema.getTypes().size() == 1) {
      actualSchema = schema.getTypes().get(0);
    } else {
      // deal complex union. this should not happened in hoodie,
      // since flink/spark do not write this type.
      int i = GenericData.get().resolveUnion(schema, data);
      actualSchema = schema.getTypes().get(i);
    }
    return actualSchema;
  }

  /**
   * Given avro records, rewrites them with new schema.
   *
   * @param oldRecords oldRecords to be rewrite
   * @param newSchema newSchema used to rewrite oldRecord
   * @return a iterator of rewrote GeneriRcords
   */
  public static Iterator<GenericRecord> rewriteRecords(Iterator<GenericRecord> oldRecords, Schema newSchema) {
    if (oldRecords == null || newSchema == null) {
      return Collections.emptyIterator();
    }
    return new Iterator<GenericRecord>() {
      @Override
      public boolean hasNext() {
        return oldRecords.hasNext();
      }

      @Override
      public GenericRecord next() {
        return rewriteRecord(oldRecords.next(), newSchema);
      }
    };
  }

  /**
   * support evolution from a new avroSchema.
   * notice: this is not a universal method,
   * now hoodie support implicitly add columns when hoodie write operation,
   * This ability needs to be preserved, so implicitly evolution for internalSchema should supported.
   *
   * @param evolvedSchema implicitly evolution of avro when hoodie write operation
   * @param oldSchema old internalSchema
   * @param supportPositionReorder support position reorder
   * @return evolution Schema
   */
  public static InternalSchema evolutionSchemaFromNewAvroSchema(Schema evolvedSchema, InternalSchema oldSchema, Boolean supportPositionReorder) {
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
      throw new UnsupportedOperationException("cannot evolution schema implicitly, find delete/rename operation");
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

  public static InternalSchema evolutionSchemaFromNewAvroSchema(Schema evolvedSchema, InternalSchema oldSchema) {
    return evolutionSchemaFromNewAvroSchema(evolvedSchema, oldSchema, false);
  }

  /**
   * canonical the nullability.
   * do not allow change cols Nullability field from optional to required.
   * if above problem occurs, try to correct it.
   *
   * @param writeSchema writeSchema hoodie used to write data.
   * @param readSchema read schema
   * @return canonical Schema
   */
  public static Schema canonicalColumnNullability(Schema writeSchema, Schema readSchema) {
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

