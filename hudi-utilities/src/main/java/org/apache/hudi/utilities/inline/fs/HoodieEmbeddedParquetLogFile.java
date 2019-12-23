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

package org.apache.hudi.utilities.inline.fs;

import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.Map;

public class HoodieEmbeddedParquetLogFile {

  private Configuration conf;
  private Path path;
  private Map<String, String> header;
  private Map<String, String> footer;
  private String outerFileScheme;
  private Path inLineEmbeddedFSPath;
  private AvroParquetWriter inlineWriter;
  private ParquetReader inLineReader;
  private boolean inLineWriteCloseCalled = false;
  private byte[] inLineBytes;
  private long inLineStartOffset;
  private int inLineLength;
  private Schema schema;

  HoodieEmbeddedParquetLogFile(Map<String, String> header, Configuration conf,
                               Path path, Schema schema) {
    this.header = header;
    this.conf = conf;
    this.path = path;
    this.schema = schema;
    this.outerFileScheme = path.toString().substring(0, path.toString().indexOf(':'));
    inLineEmbeddedFSPath = new Path(InlineFileSystem.SCHEME + path.toString().substring(path.toString().indexOf(':')));
  }

  HoodieEmbeddedParquetLogFile(Map<String, String> header, Configuration conf,
                               Path path) {
    this(header, conf, path, Schema.create(Schema.Type.NULL));
  }

  void addFooter(Map<String, String> footer) {
    this.footer = footer;
  }

  ParquetWriter getInlineParquetWriter() throws IOException {
    inlineWriter = new AvroParquetWriter(inLineEmbeddedFSPath, schema,
        CompressionCodecName.GZIP, 100 * 1024 * 1024, 1024 * 1024, true, conf);
    return inlineWriter;
  }

  void closeInlineParquetWriter() throws IOException {
    if (inlineWriter == null) {
      throw new HoodieIOException("InlineParquetWriter not instantiated");
    }
    inlineWriter.close();
    InlineFileSystem inlineFs = (InlineFileSystem) inLineEmbeddedFSPath.getFileSystem(conf);
    inLineBytes = inlineFs.getFileAsBytes();
    inLineWriteCloseCalled = true;
  }

  void write() throws IOException {
    if (!inLineWriteCloseCalled) {
      closeInlineParquetWriter();
    }
    FSDataOutputStream wrappedOut = path.getFileSystem(conf).create(path, true);
    wrappedOut.writeUTF("[header]");
    // write header map
    // wrappedOut.writeInt(inLineBytes.length);
    inLineStartOffset = wrappedOut.getPos();
    inLineLength = inLineBytes.length;
    wrappedOut.write(inLineBytes);
    wrappedOut.writeUTF("[footer]");
    // write footer map
    wrappedOut.hsync();
    wrappedOut.close();
  }

  ParquetReader getInlineParquetReader() throws IOException {
    Path offsetedPath = InLineFSUtils.getEmbeddedInLineFilePath(path, outerFileScheme, inLineStartOffset, inLineLength);
    inLineReader = AvroParquetReader.builder(offsetedPath).build();
    return inLineReader;
  }

  void closeInLineParquetReader() throws IOException {
    if(inLineReader == null) {
      throw new HoodieIOException("Closing inlineParquetReader without instantiating one");
    }
    inLineReader.close();
  }
}
