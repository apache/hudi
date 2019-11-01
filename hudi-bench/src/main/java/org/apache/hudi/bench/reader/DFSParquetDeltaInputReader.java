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

package org.apache.hudi.bench.reader;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hudi.bench.DeltaInputFormat;
import org.apache.hudi.bench.DeltaOutputType;
import org.apache.hudi.common.util.Option;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

/**
 * A reader of {@link DeltaOutputType#DFS} and {@link DeltaInputFormat#PARQUET}
 */
public class DFSParquetDeltaInputReader extends DFSDeltaInputReader {

  private static final String PARQUET_EXTENSION = ".parquet";
  private final SparkSession sparkSession;
  private final String basePath;
  private final Option<String> structName;
  private final Option<String> nameSpace;
  protected PathFilter filter = (path) -> {
    if (path.toUri().toString().contains(PARQUET_EXTENSION)) {
      return true;
    } else {
      return false;
    }
  };

  public DFSParquetDeltaInputReader(SparkSession sparkSession, String schemaStr, String basePath,
      Option<String> structName, Option<String> nameSpace) {
    this.sparkSession = sparkSession;
    this.basePath = basePath;
    this.structName = structName;
    this.nameSpace = nameSpace;
  }

  @Override
  public JavaRDD<GenericRecord> read(long totalRecordsToRead) throws IOException {
    List<String> parquetFiles = getFilePathsToRead(basePath, filter, totalRecordsToRead);
    if (parquetFiles.size() > 0) {
      return SparkBasedReader.readParquet(sparkSession, parquetFiles, structName, nameSpace);
    } else {
      throw new UnsupportedOperationException("Cannot read other format");
    }
  }

  @Override
  public JavaRDD<GenericRecord> read(int numPartitions, long approxNumRecords) {
    throw new UnsupportedOperationException("cannot generate updates");
  }

  @Override
  public JavaRDD<GenericRecord> read(int numPartitions, int numFiles, long approxNumRecords) {
    throw new UnsupportedOperationException("cannot generate updates");
  }

  @Override
  public JavaRDD<GenericRecord> read(int numPartitions, int numFiles, double percentageRecordsPerFile) {
    throw new UnsupportedOperationException("cannot generate updates");
  }

  @Override
  protected long analyzeSingleFile(String filePath) {
    JavaRDD<GenericRecord> recordsFromOneFile = SparkBasedReader.readParquet(sparkSession, Arrays.asList(filePath),
        structName, nameSpace);
    return recordsFromOneFile.count();
  }

}
