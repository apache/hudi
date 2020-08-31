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
package org.apache.hudi.common.model;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.util.Option;

import java.io.IOException;
import java.util.List;

/**
 * subclass of OverwriteWithLatestAvroPayload used for delta streamer.
 * <p>
 *  1. preCombine - Picks the latest delta record for a key, based on an ordering field.
 *  2. combineAndGetUpdateValue/getInsertValue - overwrite storage for specified fields
 *  that doesn't equal defaultValue.
 */
public class OverwriteMulColAvroPayload extends OverwriteWithLatestAvroPayload {

    /**
     * @param record      Generic record for the payload.
     * @param orderingVal {@link Comparable} to be used in pre combine.
     */
    public OverwriteMulColAvroPayload(GenericRecord record, Comparable orderingVal) {
        super(record, orderingVal);
    }

    public OverwriteMulColAvroPayload(Option<GenericRecord> record) {
        this(record.isPresent() ? record.get() : null, (record1) -> 0); // natural order
    }

    @Override
    public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {

        Option<IndexedRecord> recordOption = getInsertValue(schema);
        if (!recordOption.isPresent()) {
            return Option.empty();
        }

        GenericRecord insertRecord = (GenericRecord) recordOption.get();
        GenericRecord currentRecord = (GenericRecord) currentValue;

        Object deleteMarker = insertRecord.get("_hoodie_is_deleted");
        if (deleteMarker instanceof Boolean && (boolean) deleteMarker) {
            return Option.empty();
        } else {
            List<Schema.Field> fields = schema.getFields();
            for (int i = 0; i < fields.size(); i++) {
                Object value = insertRecord.get(fields.get(i).name());
                Object defaultValue = fields.get(i).defaultVal();
                if (fieldJudge(value,defaultValue)) {
                    continue;
                } else {
                    currentRecord.put(fields.get(i).name(), value);
                }
            }
            return Option.of(currentRecord);
        }
    }
}
