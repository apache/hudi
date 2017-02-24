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

package com.uber.hoodie.common.table.log.avro;

import com.google.common.collect.Lists;
import com.uber.hoodie.common.table.log.HoodieLogAppender;
import com.uber.hoodie.common.table.log.HoodieLogFile;
import com.uber.hoodie.exception.HoodieIOException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.AvroFSInput;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * AvroLogReader allows reading blocks of records given a offset as written by AvroLogAppender
 * Avro Log files are never streamed entirely - because of fault tolerance.
 * If a block is corrupted, then random access with offset bypasses any corrupt blocks.
 * Metadata about offset should be saved when writing blocks and passed in readBlock()
 *
 * @see AvroLogAppender
 */
public class AvroLogReader {
    private final DataFileReader<GenericRecord> reader;
    private final HoodieLogFile file;

    public AvroLogReader(HoodieLogFile file, FileSystem fs, Schema readerSchema)
        throws IOException {
        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        datumReader.setExpected(readerSchema);
        final AvroFSInput input = new AvroFSInput(FileContext.getFileContext(fs.getConf()), file.getPath());
        this.reader = (DataFileReader<GenericRecord>) DataFileReader.openReader(input, datumReader);
        this.file = file;
    }

    public Iterator<GenericRecord> readBlock(long startOffset) throws IOException {
        // We keep track of exact offset for blocks, just seek to it directly
        reader.seek(startOffset);

        List<GenericRecord> records = Lists.newArrayList();
        try {
            // First check if we are past the sync market and then check reader.hasNext,
            // hasNext will load a block in memory and this will fail if a block is corrupted.
            while (!reader.pastSync(startOffset) && reader.hasNext()) {
                records.add(reader.next());
            }
        } catch (IOException e) {
            throw new HoodieIOException("Failed to read avro records from " + file);
        }
        return records.iterator();
    }

    public HoodieLogFile getFile() {
        return file;
    }

    public void close() throws IOException {
        reader.close();
    }


}
