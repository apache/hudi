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
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;

public class HudiLogFile<T> {

  Map<String, String> header;
  Map<String, String> footer;
  LogFileType logFileType;
  Configuration conf;
  Path path;
  String scheme;
  String dummyInLineFile = ":///tmp/file1";
  Path inLineMemoryPath;
  InLineWriter<T> inLineWriter;
  InLineReader<T> inLineReader;
  boolean inLineWriteCloseCalled = false;
  byte[] inLineBytes;
  long inLineStartOffset;
  int inLineLength;
  Schema schema;

  HudiLogFile(LogFileType logFileType, Map<String, String> header, Configuration conf,
      Path path, String scheme, Schema schema) {
    this.logFileType = logFileType;
    this.header = header;
    this.conf = conf;
    this.path = path;
    this.scheme = scheme;
    this.schema = schema;
    inLineMemoryPath = new Path(scheme + dummyInLineFile);
  }

  HudiLogFile(LogFileType logFileType, Map<String, String> header, Configuration conf,
      Path path, String scheme) {
    this(logFileType, header, conf, path, scheme, Schema.create(Type.NULL));
  }

  void addFooter(Map<String, String> footer) {
    this.footer = footer;
  }

  void write() throws IOException {
    // Path wrapperFile = new Path("file:///tmp/outerfile");
    FSDataOutputStream wrappedOut = path.getFileSystem(conf).create(path, true);
    wrappedOut.writeUTF("[header]");
    // write header map
    wrappedOut.writeInt(inLineBytes.length);
    inLineStartOffset = wrappedOut.getPos();
    inLineLength = inLineBytes.length;
    wrappedOut.write(inLineBytes);
    wrappedOut.writeUTF("[footer]");
    // write footer map
    wrappedOut.hsync();
    wrappedOut.close();
  }

  InLineWriter<T> getWriter() throws IOException {
    if (logFileType == LogFileType.PARQUET_HOODIE_RECORD) {
      inLineWriter = (InLineWriter<T>) new InLineParquetWriter(inLineMemoryPath, conf, schema);
    } else if (logFileType == LogFileType.HFILE_INDEX) {
      inLineWriter = (InLineWriter<T>) new InLineHFileWriter(inLineMemoryPath, conf);
      return inLineWriter;
    }
    throw new IllegalArgumentIOException("LogFileType not recognizable ");
  }

  void closeInLineWriter() throws IOException {
    inLineWriter.close();
    inLineWriteCloseCalled = true;
    InlineFileSystem inlineFs = (InlineFileSystem) inLineMemoryPath.getFileSystem(conf);
    inLineBytes = inlineFs.getFileAsBytes();
  }

  InLineReader<T> getInLineReader() throws IOException {
    String subPath = path.toString().substring(path.toString().indexOf(":") + 1);
    Path offsetedPath = new Path(
        InlineFileSystem.SCHEME + "://" + subPath + "/file/" + inLineStartOffset + "/" + inLineLength);
    FileSystem readFs = offsetedPath.getFileSystem(conf);
    //FileStatus status = readFs.getFileStatus(offsetedPath);
    // System.out.println(status);
    inLineReader = (InLineReader<T>) (logFileType == LogFileType.PARQUET_HOODIE_RECORD ? new InLineParquetReader(
        offsetedPath) : new InLineHFileReader(offsetedPath, conf));
    return inLineReader;
  }

  void closeInLineReader() throws IOException {
    inLineReader.close();
  }

}