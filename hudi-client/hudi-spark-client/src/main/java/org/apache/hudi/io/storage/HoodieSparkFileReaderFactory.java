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

package org.apache.hudi.io.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.exception.HoodieIOException;

import java.io.IOException;

public class HoodieSparkFileReaderFactory extends HoodieFileReaderFactory  {

  private static class SingletonHolder {
    private static HoodieSparkFileReaderFactory instance = new HoodieSparkFileReaderFactory();
  }

  private HoodieSparkFileReaderFactory() {
  }

  public static HoodieFileReaderFactory getFileReaderFactory() {
    return SingletonHolder.instance;
  }

  protected HoodieFileReader newParquetFileReader(Configuration conf, Path path) {
    return new HoodieSparkParquetReader(conf, path);
  }

  protected HoodieFileReader newHFileFileReader(Configuration conf, Path path) throws IOException {
    throw new HoodieIOException("Not support read HFile");
  }

  protected HoodieFileReader newOrcFileReader(Configuration conf, Path path) {
    throw new HoodieIOException("Not support read orc file");
  }
}
