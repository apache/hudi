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
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFile.Writer;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

public class InLineHFileWriter implements InLineWriter<Pair<String, String>> {

  Writer writer;

  InLineHFileWriter(Path path, Configuration conf) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    HFileContext context = new HFileContextBuilder().withIncludesTags(false).build();
    writer = HFile.getWriterFactoryNoCache(conf).withFileContext(context).withPath(fs, path).create();
  }

  @Override
  public void write(Pair<String, String> keyValuePair) throws IOException {
    writer.append(new KeyValue(Bytes.toBytes(keyValuePair.getFirst()), Bytes.toBytes("family"), Bytes.toBytes("qual"),
        HConstants.LATEST_TIMESTAMP, Bytes.toBytes(keyValuePair.getSecond())));
  }

  @Override
  public void write(List<Pair<String, String>> records) throws IOException {
    System.out.println("Appending :: " + records.size());
    for (Pair<String, String> entry : records) {
      KeyValue keyValue = new KeyValue(Bytes.toBytes(entry.getFirst()), Bytes.toBytes("family"),
          Bytes.toBytes("qual"),
          HConstants.LATEST_TIMESTAMP, Bytes.toBytes(entry.getSecond()));
      writer.append(keyValue);
      System.out.println("Appending :: " + entry.getFirst() + " -> " + entry.getSecond());
      System.out
          .println("Appending ::: " + keyValue.getKeyString() + " --> " + Bytes.toString(keyValue.getValueArray()));
    }
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }
}
