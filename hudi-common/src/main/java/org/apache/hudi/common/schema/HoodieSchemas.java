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

package org.apache.hudi.common.schema;

import org.apache.hudi.common.model.HoodieRecord;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.schema.HoodieSchemaUtils.createNewSchemaField;

/**
 * Factory class for {@link HoodieSchema}.
 */
public class HoodieSchemas {

  public static HoodieSchema createDeleteLogSchema(HoodieSchema tableSchema, List<String> orderingFieldNames) {
    // Native delete logs store only the record key plus optional ordering values, so ordering fields in
    // the delete-log schema must always be nullable even when the table schema marks them required.
    // A delete record such as HoodieEmptyRecord may carry OrderingValues.getDefault() as an in-memory
    // sentinel rather than a real field value. Persist NULL for that missing value so readers can map it
    // back to the default ordering without confusing it with a real business value such as 0.
    List<HoodieSchemaField> fields = Stream.concat(
        Stream.of(createNewSchemaField(
            HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieSchema.create(HoodieSchemaType.STRING), null, null)),
        orderingFieldNames.stream().map(orderingFieldName -> tableSchema.getField(orderingFieldName)
            .map(field -> createNewSchemaField(
                field.name(), HoodieSchema.createNullable(field.schema()), field.doc().orElse(null), HoodieSchema.NULL_VALUE))
            .orElseThrow(() ->
                new IllegalArgumentException("Ordering field " + orderingFieldName + " not found in table schema"))))
        .collect(Collectors.toList());
    return HoodieSchema.createRecord("hudi_delete_log_record", null, null, fields);
  }
}
