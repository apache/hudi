/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.utilities.sources;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import com.uber.hoodie.avro.MercifulJsonConverter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.io.Serializable;

/**
 * Convert a variety of {@link SourceDataFormat} into Avro GenericRecords. Has a bunch of lazy fields
 * to circumvent issues around serializing these objects from driver to executors
 */
public class AvroConvertor implements Serializable {

    /**
     * To be lazily inited on executors
     */
    private transient Schema schema;

    private final String schemaStr;

    /**
     * To be lazily inited on executors
     */
    private transient MercifulJsonConverter jsonConverter;


    /**
     * To be lazily inited on executors
     */
    private transient Injection<GenericRecord, byte[]> recordInjection;


    public AvroConvertor(String schemaStr) {
        this.schemaStr = schemaStr;
    }


    private void initSchema() {
        if (schema == null) {
            Schema.Parser parser = new Schema.Parser();
            schema = parser.parse(schemaStr);
        }
    }

    private void initInjection() {
        if (recordInjection == null) {
            recordInjection = GenericAvroCodecs.toBinary(schema);
        }
    }

    private void initJsonConvertor() {
        if (jsonConverter == null) {
            jsonConverter = new MercifulJsonConverter(schema);
        }
    }


    public GenericRecord fromJson(String json) throws IOException {
        initSchema();
        initJsonConvertor();
        return jsonConverter.convert(json);
    }


    public GenericRecord fromAvroBinary(byte[] avroBinary) throws IOException {
        initSchema();
        initInjection();
        return recordInjection.invert(avroBinary).get();
    }
}
