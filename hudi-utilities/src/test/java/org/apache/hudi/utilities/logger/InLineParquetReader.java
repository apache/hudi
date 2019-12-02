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
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

public class InLineParquetReader implements InLineReader<GenericRecord> {

  ParquetReader reader;

  InLineParquetReader(Path path) throws IOException {
    reader = AvroParquetReader.builder(path).build();
  }

  @Override
  public GenericRecord read() throws IOException {
    Object obj = reader.read();
    if (obj != null && obj instanceof GenericRecord) {
      System.out.println(obj);
      return (GenericRecord) obj;
    } else {
      return null;
    }
  }

  @Override
  public List<GenericRecord> readAll() throws IOException {
    List<GenericRecord> toReturn = new ArrayList<>();
    Object obj = reader.read();
    while (obj != null && obj instanceof GenericRecord) {
      toReturn.add((GenericRecord) obj);
      obj = reader.read();
    }
    return toReturn;
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }
}