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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class HudiRowParquetReaderFlatMapFunc implements FlatMapFunction<FileStatus, Row> {

  Path basePath;
  ExpressionEncoder<Row> encoder;
  SerializableConfiguration serConfig;
  String compressionCodec;
  String schemaJson;

  private static final Logger LOG = LoggerFactory.getLogger(HudiRowParquetMapPartitionFunc.class);

  public HudiRowParquetReaderFlatMapFunc(Path basePath, ExpressionEncoder<Row> encoder, SerializableConfiguration serConfig,
                              String compressionCodec, String schemaJson) {
    this.basePath = basePath;
    this.encoder = encoder;
    this.serConfig = serConfig;
    this.compressionCodec = compressionCodec;
    this.schemaJson = schemaJson;
  }

  @Override
  public Iterator<Row> call(FileStatus fileStatus) throws Exception {
    try {
      // reading.
      Configuration config = serConfig.get();
      ParquetReadSupport readSupport = new ParquetReadSupport();
      config.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA(), schemaJson);
      config.set("spark.sql.parquet.binaryAsString", "true");
      config.set("spark.sql.parquet.int96AsTimestamp", "false");
      ParquetReader<InternalRow> reader = new ParquetReader(config, fileStatus.getPath(), readSupport);
      InternalRow internalRowRead = reader.read();
      List<Row> readRows = new ArrayList<>();
      while (internalRowRead != null) {
        Row row = encoder.fromRow(internalRowRead);
        readRows.add(row);
        internalRowRead = reader.read();
      }
      reader.close();
      System.out.println("Total found " + readRows.size());
      LOG.info("Read complete :::::::::: ");
      return readRows.iterator();
    } catch (Exception e) {
      System.out.println("Exception thrown while instantiating or writing to Parquet " + e.getMessage() + " ... cause " + e.getCause());
      e.printStackTrace();
      throw new IllegalStateException("Exception thrown while instantiating or writing to Parquet " + e.getMessage() + " ... cause " + e.getCause());
    }
  }
}
