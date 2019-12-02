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
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

public class InLineHFileReader implements InLineReader<Pair<String, String>> {

  HFileScanner scanner;

  InLineHFileReader(Path path, Configuration conf) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    HFile.Reader reader = HFile.createReader(fs, path, new CacheConfig(conf), conf);
    scanner = reader.getScanner(true, true);
    scanner.seekTo();
  }

  @Override
  public Pair<String, String> read() throws IOException {
    if (scanner.next()) {
      String key = Bytes.toString(scanner.getKey().array());
      String value = Bytes.toString(scanner.getValue().array());
      return Pair.newPair(key, value);
    }
    return null;
  }

  @Override
  public List<Pair<String, String>> readAll() throws IOException {
    List<Pair<String, String>> toReturn = new ArrayList<>();
    while (scanner.next()) {
      String key = Bytes.toString(scanner.getKey().array());
      String value = Bytes.toString(scanner.getValue().array());
      System.out.println("Returning " + key + " -> " + value);
      toReturn.add(Pair.newPair(key, value));
    }
    return toReturn;
  }

  @Override
  public void close() throws IOException {
    scanner.getReader().close();
  }
}