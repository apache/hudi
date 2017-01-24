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

package com.uber.hoodie.common.util;

import com.uber.hoodie.exception.HoodieIOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SchemaTestUtil {
    public static Schema getSimpleSchema() throws IOException {
        return new Schema.Parser()
            .parse(SchemaTestUtil.class.getResourceAsStream("/simple-test.avro"));
    }

    public static List<IndexedRecord> generateTestRecords(int from, int limit)
        throws IOException, URISyntaxException {
        return toRecords(getSimpleSchema(), getSimpleSchema(), from, limit);
    }

    private static List<IndexedRecord> toRecords(Schema writerSchema, Schema readerSchema, int from,
        int limit) throws IOException, URISyntaxException {
        GenericDatumReader<IndexedRecord> reader =
            new GenericDatumReader<>(writerSchema, readerSchema);
        try (Stream<String> stream = Files
            .lines(Paths.get(SchemaTestUtil.class.getResource("/sample.data").toURI()))) {
            return stream.skip(from).limit(limit).map(s -> {
                try {
                    return reader.read(null, DecoderFactory.get().jsonDecoder(readerSchema, s));
                } catch (IOException e) {
                    throw new HoodieIOException("Could not read data from simple_data.json", e);
                }
            }).collect(Collectors.toList());
        } catch (IOException e) {
            throw new HoodieIOException("Could not read data from simple_data.json", e);
        }
    }

}
