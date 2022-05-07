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

package org.apache.hudi.integ.testsuite.schema;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.integ.testsuite.dag.WriterContext;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Appends source ordering field to both source and target schemas. This is required to assist in validation to differentiate records written in different batches.
 */
public class TestSuiteFileBasedSchemaProvider extends FilebasedSchemaProvider {

  protected static Logger log = LogManager.getLogger(WriterContext.class);

  public TestSuiteFileBasedSchemaProvider(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
    this.sourceSchema = addSourceOrderingFieldToSchema(sourceSchema);
    this.targetSchema = addSourceOrderingFieldToSchema(targetSchema);
  }

  private Schema addSourceOrderingFieldToSchema(Schema schema) {
    List<Field> fields = new ArrayList<>();
    for (Schema.Field field : schema.getFields()) {
      Schema.Field newField = new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal());
      for (Map.Entry<String, Object> prop : field.getObjectProps().entrySet()) {
        newField.addProp(prop.getKey(), prop.getValue());
      }
      fields.add(newField);
    }
    Schema.Field sourceOrderingField =
        new Schema.Field(SchemaUtils.SOURCE_ORDERING_FIELD, Schema.create(Type.INT), "", 0);
    fields.add(sourceOrderingField);
    Schema mergedSchema = Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), false);
    mergedSchema.setFields(fields);
    return mergedSchema;
  }

}
