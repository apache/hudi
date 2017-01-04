/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.hadoop;

import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieTestUtils;
import com.uber.hoodie.common.util.FSUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class InputFormatTestUtil {
    public static File prepareDataset(TemporaryFolder basePath, int numberOfFiles,
        String commitNumber) throws IOException {
        basePath.create();
        HoodieTestUtils.init(basePath.getRoot().toString());
        File partitionPath = basePath.newFolder("2016", "05", "01");
        for (int i = 0; i < numberOfFiles; i++) {
            File dataFile =
                new File(partitionPath, FSUtils.makeDataFileName(commitNumber, 1, "fileid" + i));
            dataFile.createNewFile();
        }
        return partitionPath;
    }

    public static void simulateUpdates(File directory, final String originalCommit, int numberOfFilesUpdated,
        String newCommit, boolean randomize) throws IOException {
        List<File> dataFiles = Arrays.asList(directory.listFiles(new FilenameFilter() {
            @Override public boolean accept(File dir, String name) {
                String commitTs = FSUtils.getCommitTime(name);
                return originalCommit.equals(commitTs);
            }
        }));
        if(randomize) {
            Collections.shuffle(dataFiles);
        }
        List<File> toUpdateList =
            dataFiles.subList(0, Math.min(numberOfFilesUpdated, dataFiles.size()));
        for (File file : toUpdateList) {
            String fileId = FSUtils.getFileId(file.getName());
            File dataFile = new File(directory, FSUtils.makeDataFileName(newCommit, 1, fileId));
            dataFile.createNewFile();
        }
    }

    public static void commit(TemporaryFolder basePath, String commitNumber) throws IOException {
        // create the commit
        new File(basePath.getRoot().toString() + "/.hoodie/", commitNumber + ".commit").createNewFile();
    }

    public static void setupIncremental(JobConf jobConf, String startCommit, int numberOfCommitsToPull) {
        String modePropertyName = String.format(HoodieHiveUtil.HOODIE_CONSUME_MODE_PATTERN,
            HoodieTestUtils.RAW_TRIPS_TEST_NAME);
        jobConf.set(modePropertyName, HoodieHiveUtil.INCREMENTAL_SCAN_MODE);

        String startCommitTimestampName = String.format(HoodieHiveUtil.HOODIE_START_COMMIT_PATTERN, HoodieTestUtils.RAW_TRIPS_TEST_NAME);
        jobConf.set(startCommitTimestampName, startCommit);

        String maxCommitPulls = String.format(HoodieHiveUtil.HOODIE_MAX_COMMIT_PATTERN, HoodieTestUtils.RAW_TRIPS_TEST_NAME);
        jobConf.setInt(maxCommitPulls, numberOfCommitsToPull);
    }

    public static Schema readSchema(String location) throws IOException {
        return new Schema.Parser().parse(InputFormatTestUtil.class.getResourceAsStream(location));
    }

    public static File prepareParquetDataset(TemporaryFolder basePath, Schema schema, int numberOfFiles, int numberOfRecords,
        String commitNumber) throws IOException {
        basePath.create();
        HoodieTestUtils.init(basePath.getRoot().toString());
        File partitionPath = basePath.newFolder("2016", "05", "01");
        AvroParquetWriter parquetWriter;
        for (int i = 0; i < numberOfFiles; i++) {
            File dataFile =
                new File(partitionPath, FSUtils.makeDataFileName(commitNumber, 1, "fileid" + i));
            // dataFile.createNewFile();
            parquetWriter = new AvroParquetWriter(new Path(dataFile.getAbsolutePath()),
                schema);
            try {
                for (GenericRecord record : generateAvroRecords(schema, numberOfRecords, commitNumber)) {
                    parquetWriter.write(record);
                }
            } finally {
                parquetWriter.close();
            }
        }
        return partitionPath;

    }

    private static Iterable<? extends GenericRecord> generateAvroRecords(Schema schema, int numberOfRecords, String commitTime) {
        List<GenericRecord> records = new ArrayList<>(numberOfRecords);
        for(int i=0;i<numberOfRecords;i++) {
            records.add(generateAvroRecord(schema, i, commitTime));
        }
        return records;
    }

    private static GenericRecord generateAvroRecord(Schema schema, int recordNumber,
        String commitTime) {
        return new GenericRecordBuilder(schema).set("field1", "field" + recordNumber)
            .set("field2", "field" + recordNumber)
            .set(HoodieRecord.COMMIT_TIME_METADATA_FIELD, commitTime)
            .set(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, commitTime + "_" + recordNumber).build();
    }

    public static void simulateParquetUpdates(File directory, Schema schema, String originalCommit,
        int totalNumberOfRecords, int numberOfRecordsToUpdate,
        String newCommit) throws IOException {
        File fileToUpdate = directory.listFiles(new FilenameFilter() {
            @Override public boolean accept(File dir, String name) {
                return name.endsWith("parquet");
            }
        })[0];
        String fileId = FSUtils.getFileId(fileToUpdate.getName());
        File dataFile = new File(directory, FSUtils.makeDataFileName(newCommit, 1, fileId));
        AvroParquetWriter parquetWriter = new AvroParquetWriter(new Path(dataFile.getAbsolutePath()),
            schema);
        try {
            for (GenericRecord record : generateAvroRecords(schema, totalNumberOfRecords,
                originalCommit)) {
                if (numberOfRecordsToUpdate > 0) {
                    // update this record
                    record.put(HoodieRecord.COMMIT_TIME_METADATA_FIELD, newCommit);
                    String oldSeqNo = (String) record.get(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD);
                    record.put(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD,
                        oldSeqNo.replace(originalCommit, newCommit));
                    numberOfRecordsToUpdate--;
                }
                parquetWriter.write(record);
            }
        } finally {
            parquetWriter.close();
        }

    }
}
