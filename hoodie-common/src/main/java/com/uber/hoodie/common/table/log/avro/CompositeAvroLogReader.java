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

import com.uber.hoodie.common.table.log.HoodieLogFile;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.exception.HoodieIOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * CompositeAvroLogReader reads all versions of the logs for a given fileId.
 * It gives a iterator which iterates through all the versions and the list of blocks for that specific version
 * Useful for merging records in RecordReader and compacting all the delta versions
 *
 * @see AvroLogReader
 */
public class CompositeAvroLogReader {
    private final Map<Integer, AvroLogReader> readers;

    public CompositeAvroLogReader(Path partitionPath, String fileId, String baseCommitTime, FileSystem fs,
        Schema readerSchema, String logFileExtension) throws IOException {
        Stream<HoodieLogFile> allLogFiles =
            FSUtils.getAllLogFiles(fs, partitionPath, fileId, logFileExtension, baseCommitTime);
        this.readers = allLogFiles.map(hoodieLogFile -> {
            try {
                return new AvroLogReader(hoodieLogFile, fs, readerSchema);
            } catch (IOException e) {
                throw new HoodieIOException(
                    "Could not read avro records from path " + hoodieLogFile);
            }
        }).collect(Collectors.toMap(new Function<AvroLogReader, Integer>() {
            @Override
            public Integer apply(AvroLogReader avroLogReader) {
                return avroLogReader.getFile().getLogVersion();
            }
        }, Function.identity()));
    }

    /**
     * Reads all the versions (in the order specified) and all the blocks starting with the offset specified
     *
     * @param filesToOffsetMap
     * @return
     * @throws IOException
     */
    public Iterator<GenericRecord> readBlocks(SortedMap<Integer, List<Long>> filesToOffsetMap)
        throws IOException {
        return new Iterators(filesToOffsetMap, readers);
    }

    public void close() throws IOException {
        readers.values().forEach(s -> {
            try {
                s.close();
            } catch (IOException e) {
                throw new HoodieIOException("Unable to close " + s.getFile(), e);
            }
        });
    }

    public class Iterators implements Iterator<GenericRecord> {

        private final Map<Integer, AvroLogReader> readers;
        private final Map<Integer, List<Long>> versionsToOffsetMap;
        private Integer currentVersion;
        private Iterator<Integer> currentVersionIterator;
        private Iterator<Long> currentOffsetIterator;
        private Iterator<GenericRecord> currentRecordIterator;

        public Iterators(Map<Integer, List<Long>> versionToOffsetMap,
            Map<Integer, AvroLogReader> readers) {
            this.currentVersionIterator = versionToOffsetMap.keySet().iterator();
            this.readers = readers;
            this.versionsToOffsetMap = versionToOffsetMap;
        }

        private Iterator<GenericRecord> findNextBlock() throws IOException {
            if (currentOffsetIterator != null) {
                while (currentOffsetIterator.hasNext()) {
                    // we have more offsets to process for this file
                    long currentOffset = currentOffsetIterator.next();
                    Iterator<GenericRecord> currentBlock =
                        readers.get(currentVersion).readBlock(currentOffset);
                    if (currentBlock.hasNext()) {
                        return currentBlock;
                    }
                }
            }
            return null;
        }

        private Iterator<GenericRecord> findNext() {
            try {
                Iterator<GenericRecord> nextBlock = findNextBlock();
                if (nextBlock != null) {
                    // we have more offsets to process for this version
                    return nextBlock;
                }

                // We have no more offsets to process for the version, lets move on to the next version
                while (currentVersionIterator.hasNext()) {
                    currentVersion = currentVersionIterator.next();
                    currentOffsetIterator = versionsToOffsetMap.get(currentVersion).iterator();
                    nextBlock = findNextBlock();
                    if (nextBlock != null) {
                        return nextBlock;
                    }
                }
            } catch (IOException e) {
                throw new HoodieIOException(
                    "Could not read avro records from " + readers.get(currentVersion).getFile());
            }
            return null;
        }

        @Override
        public boolean hasNext() {
            if (currentRecordIterator == null || !currentRecordIterator.hasNext()) {
                currentRecordIterator = findNext();
            }
            return (currentRecordIterator != null && currentRecordIterator.hasNext());
        }

        @Override
        public GenericRecord next() {
            return currentRecordIterator.next();
        }
    }
}
