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

import com.google.common.collect.Lists;
import com.uber.hoodie.common.model.HoodieAvroPayload;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.exception.HoodieIOException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.fs.AvroFSInput;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;

public class AvroUtils {

    public static List<HoodieRecord<HoodieAvroPayload>> loadFromFiles(FileSystem fs,
        List<String> deltaFilePaths, Schema expectedSchema) {

        List<HoodieRecord<HoodieAvroPayload>> loadedRecords = Lists.newArrayList();
        deltaFilePaths.forEach(s -> {
            Path path = new Path(s);
            try {
                SeekableInput input =
                    new AvroFSInput(FileContext.getFileContext(fs.getConf()), path);
                GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>();
                // Set the expected schema to be the current schema to account for schema evolution
                reader.setExpected(expectedSchema);

                FileReader<GenericRecord> fileReader = DataFileReader.openReader(input, reader);
                for (GenericRecord deltaRecord : fileReader) {
                    String key = deltaRecord.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
                    String partitionPath =
                        deltaRecord.get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString();
                    loadedRecords.add(new HoodieRecord<>(new HoodieKey(key, partitionPath),
                        new HoodieAvroPayload(deltaRecord)));
                }
                fileReader.close(); // also closes underlying FsInput
            } catch (IOException e) {
                throw new HoodieIOException("Could not read avro records from path " + s, e);
            }
        });
        return loadedRecords;
    }
}
