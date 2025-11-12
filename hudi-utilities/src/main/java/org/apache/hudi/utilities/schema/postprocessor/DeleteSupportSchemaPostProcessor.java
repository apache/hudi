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

package org.apache.hudi.utilities.schema.postprocessor;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.utilities.schema.SchemaPostProcessor;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hudi.avro.HoodieAvroUtils.createNewSchemaField;

/**
 * An implementation of {@link SchemaPostProcessor} which will add a column named "_hoodie_is_deleted" to the end of
 * a given schema.
 */
public class DeleteSupportSchemaPostProcessor extends SchemaPostProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(DeleteSupportSchemaPostProcessor.class);

  public DeleteSupportSchemaPostProcessor(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
  }

  @Override
  public Schema processSchema(Schema schema) {

    if (schema.getField(HoodieRecord.HOODIE_IS_DELETED_FIELD) != null) {
      LOG.warn("column {} already exists!", HoodieRecord.HOODIE_IS_DELETED_FIELD);
      return schema;
    }

    List<Schema.Field> sourceFields = schema.getFields();
    List<Schema.Field> targetFields = new ArrayList<>(sourceFields.size() + 1);
    // copy existing columns
    for (Schema.Field sourceField : sourceFields) {
      targetFields.add(createNewSchemaField(sourceField));
    }
    // add _hoodie_is_deleted column
    targetFields.add(new Schema.Field(HoodieRecord.HOODIE_IS_DELETED_FIELD, Schema.create(Schema.Type.BOOLEAN), null, false));

    return Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), false, targetFields);
  }

}


