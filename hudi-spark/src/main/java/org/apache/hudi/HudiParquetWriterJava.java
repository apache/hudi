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

package org.apache.hudi;

import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.fs.FSUtils;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

public class HudiParquetWriterJava implements FlatMapFunction<Iterator<GenericRecord>, Boolean> {

  String basePath;
  SerializableConfiguration serConfig;

  HudiParquetWriterJava(String basePath, SerializableConfiguration serConfig) {
    this.basePath = basePath;
    this.serConfig = serConfig;
  }

  @Override
  public Iterator<Boolean> call(Iterator<GenericRecord> rowIterator) throws Exception {
    String fileId = UUID.randomUUID().toString();
    Path basePathDir = new Path(basePath);
    final FileSystem fs = FSUtils.getFs(basePath, serConfig.get());
    if (!fs.exists(basePathDir)) {
      fs.mkdirs(basePathDir);
    }
    Path preFilePath = new Path(fs.resolvePath(basePathDir).toString() + "/" + fileId);
    System.out.println("File path chosen " + preFilePath.toString());
    List<GenericRecord> rows = new ArrayList<>();
    while (rowIterator.hasNext()) {
      rows.add(rowIterator.next());
    }
    System.out.println("Total records collected " + rows.size());
    System.out.println("Total records :: " + Arrays.toString(rows.toArray()));

    try {
      System.out.println("resolved path " + preFilePath.toString());
      System.out.println("Schema :: " + rows.get(0).getSchema().toString());
      ParquetWriter<GenericRecord> parquetWriter =
          AvroParquetWriter.<GenericRecord>builder(preFilePath).withSchema(rows.get(0).getSchema()).build();
      System.out.println("Instantiated writer successfully ");
      int count = 0;
      for (GenericRecord row : rows) {
        System.out.println("Writing Internal row " + (count++) + " :: " + row.toString());
        parquetWriter.write(row);
      }
      parquetWriter.close();
      return Collections.singleton(true).iterator();
    } catch (Exception e) {
      System.out.println("Exception thrown while instantiating or writing to Parquet " + e.getMessage() + " ... cause " + e.getCause());
      e.printStackTrace();
      throw new IllegalStateException("Exception thrown while instantiating or writing to Parquet " + e.getMessage() + " ... cause " + e.getCause());
    }
  }
}
