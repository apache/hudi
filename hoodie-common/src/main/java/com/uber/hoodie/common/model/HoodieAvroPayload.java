/*
 *  Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.common.model;

import com.uber.hoodie.common.util.HoodieAvroUtils;

import java.util.Optional;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;

/**
 * This is a payload to wrap a existing Hoodie Avro Record.
 * Useful to create a HoodieRecord over existing GenericRecords in a hoodie datasets (useful in compactions)
 *
 */
public class HoodieAvroPayload implements HoodieRecordPayload<HoodieAvroPayload> {
    private final Optional<GenericRecord> record;

    public HoodieAvroPayload(Optional<GenericRecord> record) {
        this.record = record;
    }

    @Override
    public HoodieAvroPayload preCombine(HoodieAvroPayload another) {
        return this;
    }

    @Override
    public Optional<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema)
            throws IOException {
        return getInsertValue(schema);
    }

    @Override
    public Optional<IndexedRecord> getInsertValue(Schema schema) throws IOException {
        return record.map(r -> HoodieAvroUtils.rewriteRecord(r, schema));
    }
}
