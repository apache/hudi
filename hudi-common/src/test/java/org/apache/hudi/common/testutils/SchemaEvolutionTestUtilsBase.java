/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.testutils;

import org.apache.hudi.avro.AvroSchemaUtils;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.AVRO_SCHEMA;

class SchemaEvolutionTestUtilsBase {
  public static class SchemaEvolutionConfigBase {
    public Schema schema = AVRO_SCHEMA;
    public boolean nestedSupport = true;
    public boolean mapSupport = true;
    public boolean arraySupport = true;
    // TODO: [HUDI-9603] Flink 1.18 array values incorrect in fg reader test
    public boolean anyArraySupport = true;
  }

  protected static Schema generateExtendedSchema(SchemaEvolutionConfigBase configs, List<Pair<Schema.Type, LogicalType>> baseFields) {
    return generateExtendedSchema(configs.schema, configs, baseFields, "customField", true);
  }

  private static Schema generateExtendedSchema(Schema baseSchema, SchemaEvolutionConfigBase configs, List<Pair<Schema.Type, LogicalType>> baseFields, String fieldPrefix, boolean toplevel) {
    List<Schema.Field> fields =  baseSchema.getFields();
    List<Schema.Field> finalFields = new ArrayList<>(fields.size() + baseFields.size());
    boolean addedFields = false;
    for (Schema.Field field : fields) {
      if (configs.nestedSupport && field.name().equals("fare") && field.schema().getType() == Schema.Type.RECORD) {
        finalFields.add(new Schema.Field(field.name(), generateExtendedSchema(field.schema(), configs, baseFields, "customFare", false), field.doc(), field.defaultVal()));
      } else if (configs.anyArraySupport || !field.name().equals("tip_history")) {
        //TODO: [HUDI-9603] remove the if condition when the issue is fixed
        if (field.name().equals("_hoodie_is_deleted")) {
          addedFields = true;
          addFields(configs, finalFields, baseFields, fieldPrefix, baseSchema.getNamespace(), toplevel);
        }
        finalFields.add(new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal()));
      }
    }
    if (!addedFields) {
      addFields(configs, finalFields, baseFields, fieldPrefix, baseSchema.getNamespace(), toplevel);
    }
    Schema finalSchema = Schema.createRecord(baseSchema.getName(), baseSchema.getDoc(),
        baseSchema.getNamespace(), baseSchema.isError());
    finalSchema.setFields(finalFields);
    return finalSchema;
  }

  private static void addFields(SchemaEvolutionConfigBase configs, List<Schema.Field> finalFields, List<Pair<Schema.Type, LogicalType>> baseFields, String fieldPrefix, String namespace, boolean toplevel) {
    if (toplevel) {
      if (configs.mapSupport) {
        List<Schema.Field> mapFields = new ArrayList<>(baseFields.size());
        addFieldsHelper(mapFields, baseFields, fieldPrefix + "Map");
        finalFields.add(new Schema.Field(fieldPrefix + "Map", Schema.createMap(Schema.createRecord("customMapRecord", "", namespace, false, mapFields)), "", null));
      }

      if (configs.arraySupport) {
        List<Schema.Field> arrayFields = new ArrayList<>(baseFields.size());
        addFieldsHelper(arrayFields, baseFields, fieldPrefix + "Array");
        finalFields.add(new Schema.Field(fieldPrefix + "Array", Schema.createArray(Schema.createRecord("customArrayRecord", "", namespace, false, arrayFields)), "", null));
      }
    }
    addFieldsHelper(finalFields, baseFields, fieldPrefix);
  }

  private static void addFieldsHelper(List<Schema.Field> finalFields, List<Pair<Schema.Type, LogicalType>> baseFields, String fieldPrefix) {
    for (int i = 0; i < baseFields.size(); i++) {
      if (baseFields.get(i).getRight() == null) {
        if (baseFields.get(i).getLeft() == Schema.Type.BOOLEAN) {
          // boolean fields are added fields
          finalFields.add(new Schema.Field(fieldPrefix + i, AvroSchemaUtils.createNullableSchema(Schema.Type.BOOLEAN), "", null));
        } else {
          finalFields.add(new Schema.Field(fieldPrefix + i, Schema.create(baseFields.get(i).getLeft()), "", null));
        }
      } else {
        if (baseFields.get(i).getRight() instanceof LogicalTypes.Decimal) {
          LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) baseFields.get(i).getRight();
          Schema fieldSchema;
          if (baseFields.get(i).getLeft() == Schema.Type.FIXED) {
            fieldSchema = Schema.createFixed(fieldPrefix + i, "", "", 30);
          } else {
            fieldSchema = Schema.create(baseFields.get(i).getLeft());
          }

          finalFields.add(new Schema.Field(fieldPrefix + i, LogicalTypes.decimal(decimal.getPrecision(), decimal.getScale()).addToSchema(fieldSchema), "", null));
        } else if (baseFields.get(i).getRight() instanceof LogicalTypes.Date) {
          Schema fieldSchema = LogicalTypes.date().addToSchema(Schema.create(baseFields.get(i).getLeft()));
          finalFields.add(new Schema.Field(fieldPrefix + i, fieldSchema, "", null));
        } else {
          throw new IllegalStateException("Unsupported logical type: " + baseFields.get(i).getRight());
        }
      }
    }
  }
}
