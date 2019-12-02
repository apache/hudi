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

package org.apache.hudi.utilities.logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hudi.common.HoodieTestDataGenerator;
import org.apache.hudi.common.model.HoodieRecord;

public class ParquetLogging {
  
  public static void main(String[] args) throws Exception {

    /*
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    String commitTime = "001";
    List<HoodieRecord> hoodieRecords = dataGenerator.generateInsertsWithHoodieAvroPayload(commitTime, 1000);
    Path memoryBufferPath = new Path(InlineFileSystem.SCHEME + ":///tmp/file1");
    Configuration conf = new Configuration();
    conf.set("fs." + InlineFileSystem.SCHEME + ".impl", InlineFileSystem.class.getName());

    // TODO: this is ugly, but since we don't control filesystem init, there is no way to pass this to InlineFileSystem
    // TODO: This is needed for being able to properly read the outer file based on scheme (s3, gs, hdfs etc)
    InlineFileSystem.CONF = conf;

    // Initialize parquet writer & write date
    AvroParquetWriter parquetWriter = new AvroParquetWriter(memoryBufferPath, HoodieTestDataGenerator.avroSchema,
        CompressionCodecName.GZIP, 100 * 1024 * 1024, 1024 * 1024, true, conf);
    for (HoodieRecord record : hoodieRecords) {
      parquetWriter.write(record.getData().getInsertValue(HoodieTestDataGenerator.avroSchema).get());
    }
    parquetWriter.close();
    InlineFileSystem inlineFs = (InlineFileSystem) memoryBufferPath.getFileSystem(conf);
    byte[] parquetBytes = inlineFs.getFileAsBytes();

    // Wrap it into a file with a 8 byte header, parquet bytes, 8 byte footer
    Path wrapperFile = new Path("file:///tmp/outerfile");
    FSDataOutputStream wrappedOut = wrapperFile.getFileSystem(conf).create(wrapperFile, true);
    wrappedOut.writeUTF("[header]");
    wrappedOut.writeInt(parquetBytes.length);
    long startOffset = wrappedOut.getPos();
    wrappedOut.write(parquetBytes);
    wrappedOut.writeUTF("[footer]");
    wrappedOut.hsync();
    wrappedOut.close();

    // Try reading it using Parquet Reader with projections etc
    Path offsetedPath = new Path(
        InlineFileSystem.SCHEME + ":///tmp/outerfile/file/" + startOffset + "/" + parquetBytes.length);
    FileSystem readFs = offsetedPath.getFileSystem(conf);
    FileStatus status = readFs.getFileStatus(offsetedPath);
    System.out.println(status);

    ParquetReader reader = AvroParquetReader.builder(offsetedPath).build();
    Object obj = reader.read();
    while (obj != null) {
      if (obj instanceof GenericRecord) {
        System.out.println(obj);
      }
      obj = reader.read();
    }
    reader.close();
  }
*/

    /*
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    String commitTime = "001";
    List<HoodieRecord> hoodieRecords = dataGenerator.generateInsertsWithHoodieAvroPayload(commitTime, 1000);
    Path memoryBufferPath = new Path(InlineFileSystem.SCHEME + ":///tmp/file1");
    Configuration conf = new Configuration();
    conf.set("fs." + InlineFileSystem.SCHEME + ".impl", InlineFileSystem.class.getName());

    // TODO: this is ugly, but since we don't control filesystem init, there is no way to pass this to InlineFileSystem
    // TODO: This is needed for being able to properly read the outer file based on scheme (s3, gs, hdfs etc)
    InlineFileSystem.CONF = conf;

    // Initialize writer

    InLineWriter<GenericRecord> writer = new InLineParquetWriter(memoryBufferPath, conf);
    List<GenericRecord> toWrite = new ArrayList<>();
    for (HoodieRecord record : hoodieRecords) {
      toWrite.add((GenericRecord) record.getData().getInsertValue(HoodieTestDataGenerator.avroSchema).get());
    }
    writer.write(toWrite);
    writer.close();
    InlineFileSystem inlineFs = (InlineFileSystem) memoryBufferPath.getFileSystem(conf);
    byte[] parquetBytes = inlineFs.getFileAsBytes();

    // Wrap it into a file with a 8 byte header, parquet bytes, 8 byte footer
    Path wrapperFile = new Path("file:///tmp/outerfile");
    FSDataOutputStream wrappedOut = wrapperFile.getFileSystem(conf).create(wrapperFile, true);
    wrappedOut.writeUTF("[header]");
    wrappedOut.writeInt(parquetBytes.length);
    long startOffset = wrappedOut.getPos();
    wrappedOut.write(parquetBytes);
    wrappedOut.writeUTF("[footer]");
    wrappedOut.hsync();
    wrappedOut.close();

    // Try reading it using Parquet Reader with projections etc
    Path offsetedPath = new Path(
        InlineFileSystem.SCHEME + ":///tmp/outerfile/file/" + startOffset + "/" + parquetBytes.length);
    FileSystem readFs = offsetedPath.getFileSystem(conf);
    FileStatus status = readFs.getFileStatus(offsetedPath);
    System.out.println(status);

    InLineReader<GenericRecord> reader = new InLineParquetReader(offsetedPath);
    List<GenericRecord> records = reader.readAll();
    for (GenericRecord record : records) {
      System.out.println(" Record :: "+ record);
      Assert.assertTrue(toWrite.contains(record));
    }
    System.out.println("All records matched ");
    reader.close();
    */

    /*
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    String commitTime = "001";
    List<HoodieRecord> hoodieRecords = dataGenerator.generateInsertsWithHoodieAvroPayload(commitTime, 10);
    Path memoryBufferPath = new Path(InlineFileSystem.SCHEME + ":///tmp/file1");
    Configuration conf = new Configuration();
    conf.set("fs." + InlineFileSystem.SCHEME + ".impl", InlineFileSystem.class.getName());

    // TODO: this is ugly, but since we don't control filesystem init, there is no way to pass this to InlineFileSystem
    // TODO: This is needed for being able to properly read the outer file based on scheme (s3, gs, hdfs etc)
    InlineFileSystem.CONF = conf;

    // Initialize writer

    InLineWriter<Pair<String, String>> writer = new InLineHFileWriter(memoryBufferPath, conf);
    List<Pair<String, String>> toWrite = new ArrayList<>();
    for (HoodieRecord record : hoodieRecords) {
      toWrite.add(Pair.newPair(record.getRecordKey(), record.getPartitionPath() + "," + record.getPartitionPath()));
    }
    Collections.sort(toWrite, (o1, o2) -> o1.getFirst().compareTo(o2.getFirst()));
    writer.write(toWrite);
    writer.close();
    InlineFileSystem inlineFs = (InlineFileSystem) memoryBufferPath.getFileSystem(conf);
    byte[] parquetBytes = inlineFs.getFileAsBytes();

    // Wrap it into a file with a 8 byte header, parquet bytes, 8 byte footer
    Path wrapperFile = new Path("file:///tmp/outerfile");
    FSDataOutputStream wrappedOut = wrapperFile.getFileSystem(conf).create(wrapperFile, true);
    wrappedOut.writeUTF("[header]");
    wrappedOut.writeInt(parquetBytes.length);
    long startOffset = wrappedOut.getPos();
    wrappedOut.write(parquetBytes);
    wrappedOut.writeUTF("[footer]");
    wrappedOut.hsync();
    wrappedOut.close();

    // Try reading it using Parquet Reader with projections etc
    Path offsetedPath = new Path(
        InlineFileSystem.SCHEME + ":///tmp/outerfile/file/" + startOffset + "/" + parquetBytes.length);
    FileSystem readFs = offsetedPath.getFileSystem(conf);
    FileStatus status = readFs.getFileStatus(offsetedPath);
    System.out.println(status);

    InLineReader<Pair<String, String>> reader = new InLineHFileReader(offsetedPath, conf);
    List<Pair<String, String>> records = reader.readAll();
    for (Pair<String, String> record : records) {
      System.out.println(" Record :: " + record);
      Assert.assertTrue(toWrite.contains(record));
    }
    System.out.println("All records matched ");
    reader.close();

*/

    LogFileType logFileType = LogFileType.HFILE_INDEX;
    // LogFileType logFileType = LogFileType.PARQUET_HOODIE_RECORD;

    // initialize configs
    Configuration conf = new Configuration();
    conf.set("fs." + InlineFileSystem.SCHEME + ".impl", InlineFileSystem.class.getName());
    // TODO: this is ugly, but since we don't control filesystem init, there is no way to pass this to InlineFileSystem
    // TODO: This is needed for being able to properly read the outer file based on scheme (s3, gs, hdfs etc)
    InlineFileSystem.CONF = conf;

    // Initialize HudiLogFile
    HudiLogFile hudiLogFile =
        logFileType == LogFileType.PARQUET_HOODIE_RECORD ? new HudiLogFile<GenericRecord>(logFileType,
            Collections.EMPTY_MAP,
            conf, new Path("file:///tmp/outerfile" + logFileType), InlineFileSystem.SCHEME,
            HoodieTestDataGenerator.avroSchema) :
            new HudiLogFile<Pair<String, String>>(logFileType,
                Collections.EMPTY_MAP,
                conf, new Path("file:///tmp/outerfile" + logFileType), InlineFileSystem.SCHEME);

    // Initialize writer
    InLineWriter inLineWriter = hudiLogFile.getWriter();
    // write inline content
    if (logFileType == LogFileType.PARQUET_HOODIE_RECORD) {
      inLineWriter.write(getParquetHoodieRecords());
    } else {
      inLineWriter.write(getHFileEntriesToWrite());
    }
    hudiLogFile.closeInLineWriter();

    // serialize whole file
    hudiLogFile.write();

    // read entries using inline reader
    if (logFileType == LogFileType.PARQUET_HOODIE_RECORD) {
      readParquetGenericRecords(hudiLogFile.getInLineReader());
    } else {
      readHFileEntries(hudiLogFile.getInLineReader());
    }
    hudiLogFile.closeInLineReader();
  }

  static List<Pair<String, String>> getHFileEntriesToWrite() throws IOException {
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    String commitTime = "001";
    List<HoodieRecord> hoodieRecords = dataGenerator.generateInsertsWithHoodieAvroPayload(commitTime, 10);
    List<Pair<String, String>> toWrite = new ArrayList<>();
    for (HoodieRecord record : hoodieRecords) {
      toWrite.add(Pair.newPair(record.getRecordKey(), record.getPartitionPath() + "," + record.getPartitionPath()));
    }
    Collections.sort(toWrite, Comparator.comparing(Pair::getFirst));
    return toWrite;
  }

  static void readHFileEntries(InLineReader reader) throws IOException {
    List<Pair<String, String>> records = reader.readAll();
    for (Pair<String, String> record : records) {
      System.out.println(" Record :: " + record);
      // Assert.assertTrue(toWrite.contains(record));
    }
  }

  static List<GenericRecord> getParquetHoodieRecords() throws IOException {
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    String commitTime = "001";
    List<HoodieRecord> hoodieRecords = dataGenerator.generateInsertsWithHoodieAvroPayload(commitTime, 10);
    List<GenericRecord> toReturn = new ArrayList<>();
    for (HoodieRecord record : hoodieRecords) {
      toReturn.add((GenericRecord) record.getData().getInsertValue(HoodieTestDataGenerator.avroSchema).get());
    }
    return toReturn;
  }

  static void readParquetGenericRecords(InLineReader reader) throws IOException {
    List<GenericRecord> records = reader.readAll();
    for (GenericRecord record : records) {
      System.out.println("Reading " + record);
    }
  }
}